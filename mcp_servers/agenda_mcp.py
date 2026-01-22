# mcp_servers/agenda_mcp.py
import os
import json
import base64
import hmac
import hashlib
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv

from fastmcp import FastMCP
from fastmcp.server.auth.providers.jwt import StaticTokenVerifier  # dev-only
from sqlalchemy import create_engine, text

load_dotenv()

def _utcnow() -> datetime:
    # MariaDB DATETIME naive en UTC
    return datetime.utcnow()


def _db_engine():
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "rootpasswd")
    db = os.getenv("DB_NAME", "tfg")
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True, pool_recycle=1800)


ENGINE = _db_engine()

# -----------------------
# Auth (opcional, dev)
# -----------------------
MCP_ACCESS_TOKEN = os.getenv("MCP_ACCESS_TOKEN", "").strip()
if MCP_ACCESS_TOKEN:
    # Dev-only: token plano (no JWT real). No usar en prod.
    verifier = StaticTokenVerifier(tokens={MCP_ACCESS_TOKEN: {"client_id": "openai"}}, required_scopes=[])
    mcp = FastMCP("Padel Agenda MCP", auth=verifier)
else:
    mcp = FastMCP("Padel Agenda MCP")

def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)

def _get_app_user_by_telegram(telegram_user_id: int) -> Optional[Dict[str, Any]]:
    with ENGINE.connect() as cn:
        row = cn.execute(
            text("SELECT id, role, status, full_name FROM app_users WHERE telegram_user_id=:tid"),
            {"tid": telegram_user_id},
        ).mappings().first()
    return dict(row) if row else None

def _ensure_client_user(telegram_user_id: int, full_name: Optional[str]) -> Tuple[int, int]:
    now = _utcnow()
    with ENGINE.begin() as cn:
        u = cn.execute(
            text("SELECT id, role, status, full_name FROM app_users WHERE telegram_user_id=:tid"),
            {"tid": telegram_user_id},
        ).mappings().first()

        if u:
            if u["status"] != "active":
                raise ValueError("user_blocked")
            user_id = int(u["id"])
            # Si ya es coach/admin no lo tocamos: un coach no “auto-reserva” como client aquí.
            if u["role"] == "client" and full_name and not u.get("full_name"):
                cn.execute(
                    text("UPDATE app_users SET full_name=:n, updated_at=:now WHERE id=:id"),
                    {"n": full_name, "now": now, "id": user_id},
                )
        else:
            res = cn.execute(
                text("""
                    INSERT INTO app_users(telegram_user_id, role, full_name, status, created_at, updated_at)
                    VALUES (:tid, 'client', :n, 'active', :now, :now)
                """),
                {"tid": telegram_user_id, "n": full_name, "now": now},
            )
            user_id = int(res.lastrowid)

        c = cn.execute(text("SELECT id FROM clients WHERE user_id=:telegram_user_id"), {"telegram_user_id": user_id}).first()
        if c:
            client_id = int(c[0])
        else:
            res2 = cn.execute(
                text("INSERT INTO clients(user_id, created_at, updated_at) VALUES (:telegram_user_id, :now, :now)"),
                {"telegram_user_id": user_id, "now": now},
            )
            client_id = int(res2.lastrowid)

    return user_id, client_id

def _get_coach_by_id(coach_id: int) -> Dict[str, Any]:
    with ENGINE.connect() as cn:
        row = cn.execute(
            text("SELECT id, user_id, timezone, default_lesson_minutes FROM coaches WHERE id=:id"),
            {"id": coach_id},
        ).mappings().first()
    if not row:
        raise ValueError(f"Coach no existe: {coach_id}")
    return dict(row)

def _get_coach_id_for_telegram_user(telegram_user_id: int) -> Optional[int]:
    with ENGINE.connect() as cn:
        row = cn.execute(
            text("""
                SELECT c.id
                  FROM coaches c
                  JOIN app_users u ON u.id = c.user_id
                 WHERE u.telegram_user_id=:tid
            """),
            {"tid": telegram_user_id},
        ).first()
    return int(row[0]) if row else None

def _get_client_id_for_telegram_user(telegram_user_id: int) -> Optional[int]:
    with ENGINE.connect() as cn:
        row = cn.execute(
            text("""
                SELECT cl.id
                  FROM clients cl
                  JOIN app_users u ON u.id = cl.user_id
                 WHERE u.telegram_user_id=:tid
            """),
            {"tid": telegram_user_id},
        ).first()
    return int(row[0]) if row else None

def _require_active(u: Dict[str, Any]) -> None:
    if u.get("status") != "active":
        raise ValueError("user_blocked")

def _require_coach_or_admin(u: Dict[str, Any]) -> None:
    if u.get("role") not in ("coach", "admin"):
        raise ValueError("forbidden_requires_coach")

def _parse_utc_iso(iso: str) -> datetime:
    v = iso.replace("Z", "+00:00")
    dt = datetime.fromisoformat(v)
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

# ----------------------------------------------------------------------
# TOOLS MCP
# ----------------------------------------------------------------------

@mcp.tool
def db_ping() -> Dict[str, Any]:
    """Comprueba conectividad DB desde el MCP (SELECT 1)."""
    with ENGINE.connect() as cn:
        v = cn.execute(text("SELECT 1")).scalar()
    return {"ok": True, "db": int(v)}

@mcp.tool
def list_coaches(active_only: bool = True) -> List[Dict[str, Any]]:
    q = """
        SELECT c.id AS coach_id, u.full_name, c.timezone, c.default_lesson_minutes
          FROM coaches c
          JOIN app_users u ON u.id = c.user_id
    """
    
    if active_only:
        q += " WHERE u.status='active'"
    
    q += " ORDER BY c.id"
    with ENGINE.connect() as cn:
        rows = cn.execute(text(q)).mappings().all()
        
    return [dict(r) for r in rows]

@mcp.tool
def list_services(active_only: bool = True) -> List[Dict[str, Any]]:
    """Lista servicios (clases) disponibles."""
    q = "SELECT id, name, duration_minutes, price_cents, currency, is_active FROM services"
    if active_only:
        q += " WHERE is_active = 1"
    q += " ORDER BY id"
    with ENGINE.connect() as cn:
        rows = cn.execute(text(q)).mappings().all()
    return [dict(r) for r in rows]

def _get_service_duration(service_id: Optional[int]) -> Optional[int]:
    if not service_id:
        return None
    with ENGINE.connect() as cn:
        row = cn.execute(
            text("SELECT duration_minutes FROM services WHERE id=:id AND is_active=1"),
            {"id": service_id},
        ).first()
    return int(row[0]) if row else None

def _get_day_bounds_utc(d: date, tz: ZoneInfo) -> Tuple[datetime, datetime]:
    day_start_local = datetime.combine(d, time(0, 0), tzinfo=tz)
    day_end_local = day_start_local + timedelta(days=1)
    day_start_utc = day_start_local.astimezone(timezone.utc).replace(tzinfo=None)
    day_end_utc = day_end_local.astimezone(timezone.utc).replace(tzinfo=None)
    return day_start_utc, day_end_utc

def _list_available_slots_impl(
    coach_id: int,
    day: str,  # "YYYY-MM-DD" en TZ del coach
    service_id: Optional[int] = None,
) -> Dict[str, Any]:
    coach = _get_coach_by_id(coach_id)
    tz = ZoneInfo(coach["timezone"] or "Europe/Madrid")
    duration = _get_service_duration(service_id) or int(coach["default_lesson_minutes"] or 60)

    #print("[DBG] _list_available_slots_impl FILE=", __file__)
    #print("[DBG] _list_available_slots_impl ID  =", id(_list_available_slots_impl))
    
    d = date.fromisoformat(day)
    weekday = d.isoweekday()  # 1=Mon ... 7=Sun

    day_start_utc, day_end_utc = _get_day_bounds_utc(d, tz)

    with ENGINE.connect() as cn:
        rules = cn.execute(
            text("""
                SELECT start_time, end_time, slot_minutes, valid_from, valid_to
                FROM availability_rules
                WHERE coach_id=:coach_id
                  AND weekday=:weekday
                  AND (valid_from IS NULL OR valid_from <= :d)
                  AND (valid_to IS NULL OR valid_to >= :d)
                ORDER BY start_time
            """),
            {"coach_id": coach_id, "weekday": weekday, "d": d},
        ).mappings().all()

        # Excepciones del día
        exc = cn.execute(
            text("""
                SELECT type, start_at, end_at, reason
                  FROM availability_exceptions
                 WHERE coach_id=:coach_id
                   AND start_at < :day_end
                   AND end_at > :day_start
            """),
            {"coach_id": coach_id, "day_start": day_start_utc, "day_end": day_end_utc},
        ).mappings().all()

        bookings = cn.execute(
            text("""
                SELECT start_at, end_at
                  FROM bookings
                 WHERE coach_id=:coach_id
                   AND status IN ('tentative','confirmed')
                   AND start_at < :day_end
                   AND end_at > :day_start
            """),
            {"coach_id": coach_id, "day_start": day_start_utc, "day_end": day_end_utc},
        ).mappings().all()

    busy = [(b["start_at"], b["end_at"]) for b in bookings]
    blocked = [(e["start_at"], e["end_at"]) for e in exc if e["type"] == "blocked"]
    extra = [(e["start_at"], e["end_at"]) for e in exc if e["type"] == "extra"]

    slot_td = timedelta(minutes=duration)

    def overlaps(rng: Tuple[datetime, datetime], others: List[Tuple[datetime, datetime]]) -> bool:
        s, e = rng
        return any(not (e <= os or s >= oe) for (os, oe) in others)

    slots: List[Dict[str, Any]] = []

    def add_window_local(start_local: datetime, end_local: datetime, step_minutes: Optional[int]):
        step_minutes = int(step_minutes or duration)
        if step_minutes < 1:
            step_minutes = duration  # fallback defensivo

        step_td = timedelta(minutes=step_minutes)
        cur = start_local

        while cur + slot_td <= end_local:
            s_utc = cur.astimezone(timezone.utc).replace(tzinfo=None)
            e_utc = (cur + slot_td).astimezone(timezone.utc).replace(tzinfo=None)

            if overlaps((s_utc, e_utc), busy) or overlaps((s_utc, e_utc), blocked):
                cur += step_td
                continue

            slots.append(
                {
                    "start_local": cur.isoformat(),
                    "end_local": (cur + slot_td).isoformat(),
                    "start_utc": s_utc.isoformat(),
                    "end_utc": e_utc.isoformat(),
                }
            )

            cur += step_td

    # Reglas recurrentes
    for r in rules:
        start_local = datetime.combine(d, r["start_time"], tzinfo=tz)
        end_local = datetime.combine(d, r["end_time"], tzinfo=tz)
        add_window_local(start_local, end_local, step_minutes=int(r["slot_minutes"] or duration))

    # Extra availability (UTC -> local)
    for s_utc, e_utc in extra:
        s_local = s_utc.replace(tzinfo=timezone.utc).astimezone(tz)
        e_local = e_utc.replace(tzinfo=timezone.utc).astimezone(tz)
        if s_local.date() != d and e_local.date() != d:
            continue
        add_window_local(s_local, e_local, step_minutes=duration)

    slots.sort(key=lambda x: x["start_utc"])
    #print("[DBG] returning slots=", len(slots))
    return {
            "coach_id": coach_id,
            "day": day,
            "timezone": str(tz),
            "duration_minutes": duration,
            "slots": slots,  # aunque sea []
        }

@mcp.tool
def list_available_slots(
    coach_id: int,
    day: str,
    service_id: Optional[int] = None,
) -> Dict[str, Any]:
    return _list_available_slots_impl(coach_id=coach_id, day=day, service_id=service_id)

@mcp.tool
def list_available_classes_week(
    coach_id: int,
    service_id: Optional[int] = None,
    include_past_days: bool = False,
    only_non_empty_days: bool = True,
    max_slots_per_day: int = 4,
    max_total_slots: int = 20,
) -> Dict[str, Any]:
    """
    Devuelve la lista de clases (slots) disponibles para un coach, limitada a la semana en curso
    (ISO week Lunes..Domingo) en la zona horaria del coach.

    - Por defecto, include_past_days=False: devuelve desde hoy (TZ del coach) hasta el Domingo.
    - Se apoya en list_available_slots() para cada día.
    - Aplica límites para evitar respuestas demasiado grandes.
    """
    #print("[DBG] list_available_classes_week FILE=", __file__)
    #print("[DBG] calling _list_available_slots_impl ID =", id(_list_available_slots_impl))

    if max_slots_per_day < 1:
        raise ValueError("max_slots_per_day must be >= 1")
    if max_total_slots < 1:
        raise ValueError("max_total_slots must be >= 1")

    coach = _get_coach_by_id(coach_id)
    tz = ZoneInfo(coach["timezone"] or "Europe/Madrid")

    now_local = datetime.now(timezone.utc).astimezone(tz)
    today_local = now_local.date()

    # ISO week: Monday..Sunday
    week_start = today_local - timedelta(days=today_local.weekday())  # Monday
    week_end = week_start + timedelta(days=6)  # Sunday

    start_day = week_start if include_past_days else today_local
    if start_day > week_end:
        start_day = week_end

    days: List[Dict[str, Any]] = []
    total_slots = 0
    truncated = False

    d = start_day
    while d <= week_end:
        day_iso = d.isoformat()
        day_payload = _list_available_slots_impl(coach_id=int(coach_id), day=day_iso, service_id=service_id)

        # Defensive: si la impl devuelve None o algo inesperado, lo tratamos como "sin slots"
        if not isinstance(day_payload, dict):
            day_payload = {
                "slots": [],
                "duration_minutes": None,
                "timezone": str(tz),
                "error": "list_available_slots_impl_returned_none",
            }

        slots = day_payload.get("slots", []) or []

        if len(slots) > max_slots_per_day:
            slots = slots[:max_slots_per_day]
            truncated = True

        if slots or not only_non_empty_days:
            days.append(
                {
                    "day": day_iso,
                    "duration_minutes": day_payload.get("duration_minutes"),
                    "timezone": day_payload.get("timezone"),
                    "slots": slots,
                }
            )
            total_slots += len(slots)
            if total_slots >= max_total_slots:
                truncated = True
                break

        d = d + timedelta(days=1)

    return {
        "coach_id": coach_id,
        "timezone": str(tz),
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "start_day": start_day.isoformat(),
        "days": days,
        "total_slots": total_slots,
        "truncated": truncated,
        "limits": {"max_slots_per_day": max_slots_per_day, "max_total_slots": max_total_slots},
    }

@mcp.tool
def upsert_service(
    name: str,
    duration_minutes: int,
    price_cents: Optional[int] = None,
    currency: Optional[str] = None,
    is_active: bool = True,
    telegram_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    if not telegram_user_id:
        raise ValueError("actor_telegram_user_id_required")
    
    u = _get_app_user_by_telegram(telegram_user_id)
    if not u:
        raise ValueError("actor_not_registered")
    _require_active(u)
    _require_coach_or_admin(u)

    now = _utcnow()
    with ENGINE.begin() as cn:
        res = cn.execute(
            text("""
                INSERT INTO services(name, duration_minutes, price_cents, currency, is_active, created_at, updated_at)
                VALUES (:name, :dur, :price, :cur, :act, :now, :now)
                ON DUPLICATE KEY UPDATE
                    duration_minutes=VALUES(duration_minutes),
                    price_cents=VALUES(price_cents),
                    currency=VALUES(currency),
                    is_active=VALUES(is_active),
                    updated_at=VALUES(updated_at),
                    id=LAST_INSERT_ID(id)
            """),
            {"name": name, "dur": duration_minutes, "price": price_cents, "cur": currency, "act": 1 if is_active else 0, "now": now},
        )
        service_id = int(res.lastrowid)
    return {"ok": True, "service_id": service_id}

@mcp.tool
def set_availability_rules(
    coach_id: int,
    rules: List[Dict[str, Any]],
    replace_all: bool = True,
    telegram_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    if not telegram_user_id:
        raise ValueError("actor_telegram_user_id_required")
    u = _get_app_user_by_telegram(telegram_user_id)
    if not u:
        raise ValueError("actor_not_registered")
    _require_active(u)
    _require_coach_or_admin(u)

    # Si es coach, solo puede tocar su propio coach_id
    if u["role"] == "coach":
        my_coach_id = _get_coach_id_for_telegram_user(telegram_user_id)
        if not my_coach_id or int(my_coach_id) != int(coach_id):
            raise ValueError("forbidden_other_coach")

    now = _utcnow()
    with ENGINE.begin() as cn:
        if replace_all:
            cn.execute(text("DELETE FROM availability_rules WHERE coach_id=:cid"), {"cid": coach_id})

        inserted = 0
        for r in rules:
            cn.execute(
                text("""
                    INSERT INTO availability_rules(
                        coach_id, weekday, start_time, end_time, slot_minutes, valid_from, valid_to, created_at, updated_at
                    ) VALUES (
                        :coach_id, :weekday, :start_time, :end_time, :slot_minutes, :valid_from, :valid_to, :now, :now
                    )
                """),
                {
                    "coach_id": coach_id,
                    "weekday": int(r["weekday"]),
                    "start_time": r["start_time"],
                    "end_time": r["end_time"],
                    "slot_minutes": int(r.get("slot_minutes") or 60),
                    "valid_from": r.get("valid_from"),
                    "valid_to": r.get("valid_to"),
                    "now": now,
                },
            )
            inserted += 1

    return {"ok": True, "coach_id": coach_id, "inserted": inserted, "replace_all": replace_all}

@mcp.tool
def add_availability_exception(
    coach_id: int,
    type: str,  # 'blocked' | 'extra'
    start_utc: str,
    end_utc: str,
    reason: Optional[str] = None,
    telegram_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    if not telegram_user_id:
        raise ValueError("actor_telegram_user_id_required")
    u = _get_app_user_by_telegram(telegram_user_id)
    if not u:
        raise ValueError("actor_not_registered")
    _require_active(u)
    _require_coach_or_admin(u)

    if u["role"] == "coach":
        my_coach_id = _get_coach_id_for_telegram_user(telegram_user_id)
        if not my_coach_id or int(my_coach_id) != int(coach_id):
            raise ValueError("forbidden_other_coach")

    if type not in ("blocked", "extra"):
        raise ValueError("invalid_exception_type")

    s = _parse_utc_iso(start_utc)
    e = _parse_utc_iso(end_utc)
    if e <= s:
        raise ValueError("invalid_time_range")

    now = _utcnow()
    with ENGINE.begin() as cn:
        res = cn.execute(
            text("""
                INSERT INTO availability_exceptions(coach_id, type, start_at, end_at, reason, created_at, updated_at)
                VALUES (:cid, :type, :s, :e, :r, :now, :now)
            """),
            {"cid": coach_id, "type": type, "s": s, "e": e, "r": reason, "now": now},
        )
        ex_id = int(res.lastrowid)
    return {"ok": True, "exception_id": ex_id}

@mcp.tool
def list_bookings(
    coach_id: int,
    start_utc: str,
    end_utc: str,
    include_cancelled: bool = False,
    telegram_user_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if not telegram_user_id:
        raise ValueError("actor_telegram_user_id_required")
    u = _get_app_user_by_telegram(telegram_user_id)
    if not u:
        raise ValueError("actor_not_registered")
    _require_active(u)
    _require_coach_or_admin(u)

    if u["role"] == "coach":
        my_coach_id = _get_coach_id_for_telegram_user(telegram_user_id)
        if not my_coach_id or int(my_coach_id) != int(coach_id):
            raise ValueError("forbidden_other_coach")

    s = _parse_utc_iso(start_utc)
    e = _parse_utc_iso(end_utc)

    q = """
        SELECT b.id, b.start_at, b.end_at, b.status, b.client_id, b.service_id,
               u.full_name AS client_name
          FROM bookings b
          JOIN clients c ON c.id = b.client_id
          JOIN app_users u ON u.id = c.user_id
         WHERE b.coach_id=:cid
           AND b.start_at < :end
           AND b.end_at > :start
    """
    if not include_cancelled:
        q += " AND b.status <> 'cancelled'"
    q += " ORDER BY b.start_at"

    with ENGINE.connect() as cn:
        rows = cn.execute(text(q), {"cid": coach_id, "start": s, "end": e}).mappings().all()
    return [dict(r) for r in rows]

@mcp.tool
def list_my_bookings(
    start_utc: str,
    end_utc: str,
    include_cancelled: bool = False,
    telegram_user_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if not telegram_user_id:
        raise ValueError("actor_telegram_user_id_required")
    u = _get_app_user_by_telegram(telegram_user_id)
    if not u:
        # auto-crea como client si falta (MVP)
        _ensure_client_user(telegram_user_id, None)
        u = _get_app_user_by_telegram(telegram_user_id)

    _require_active(u)
    if u["role"] != "client":
        raise ValueError("forbidden_requires_client")

    client_id = _get_client_id_for_telegram_user(telegram_user_id)
    if not client_id:
        raise ValueError("client_not_found")

    s = _parse_utc_iso(start_utc)
    e = _parse_utc_iso(end_utc)

    q = """
        SELECT b.id, b.coach_id, b.start_at, b.end_at, b.status, b.service_id
          FROM bookings b
         WHERE b.client_id=:clid
           AND b.start_at < :end
           AND b.end_at > :start
    """
    if not include_cancelled:
        q += " AND b.status <> 'cancelled'"
    q += " ORDER BY b.start_at"

    with ENGINE.connect() as cn:
        rows = cn.execute(text(q), {"clid": client_id, "start": s, "end": e}).mappings().all()
    return [dict(r) for r in rows]

def _slot_allowed(coach_id: int, start_dt: datetime, end_dt: datetime) -> bool:
    coach = _get_coach_by_id(coach_id)
    tz = ZoneInfo(coach["timezone"] or "Europe/Madrid")
    start_local = start_dt.replace(tzinfo=timezone.utc).astimezone(tz)
    end_local = end_dt.replace(tzinfo=timezone.utc).astimezone(tz)
    if start_local.date() != end_local.date():
        return False
    d = start_local.date()
    weekday = d.isoweekday()

    with ENGINE.connect() as cn:
        rules = cn.execute(
            text("""
                SELECT start_time, end_time, valid_from, valid_to
                  FROM availability_rules
                 WHERE coach_id=:cid
                   AND weekday=:w
                   AND (valid_from IS NULL OR valid_from <= :d)
                   AND (valid_to IS NULL OR valid_to >= :d)
            """),
            {"cid": coach_id, "w": weekday, "d": d},
        ).mappings().all()

        day_start_utc, day_end_utc = _get_day_bounds_utc(d, tz)
        extra = cn.execute(
            text("""
                SELECT start_at, end_at
                  FROM availability_exceptions
                 WHERE coach_id=:cid
                   AND type='extra'
                   AND start_at < :day_end
                   AND end_at > :day_start
            """),
            {"cid": coach_id, "day_start": day_start_utc, "day_end": day_end_utc},
        ).mappings().all()

    # dentro de alguna regla
    for r in rules:
        rs = datetime.combine(d, r["start_time"], tzinfo=tz)
        re_ = datetime.combine(d, r["end_time"], tzinfo=tz)
        if start_local >= rs and end_local <= re_:
            return True

    # dentro de algún extra (UTC)
    for ex in extra:
        if start_dt >= ex["start_at"] and end_dt <= ex["end_at"]:
            return True

    return False

@mcp.tool
def create_booking(
    coach_id: int,
    start_utc: str,
    duration_minutes: int,
    service_id: Optional[int] = None,
    notes: Optional[str] = None,
    client_id: Optional[int] = None,
    telegram_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    if not telegram_user_id:
        raise ValueError("actor_telegram_user_id_required")
    u = _get_app_user_by_telegram(telegram_user_id)
    if not u:
        # MVP: si no existe, lo creamos como client
        _ensure_client_user(telegram_user_id, None)
        u = _get_app_user_by_telegram(telegram_user_id)

    _require_active(u)
    actor_user_id = int(u["id"])

    start_dt = _parse_utc_iso(start_utc)
    end_dt = start_dt + timedelta(minutes=int(duration_minutes))

    # Permisos + resolver client_id
    if u["role"] == "client":
        # fuerza a “self”
        my_client_id = _get_client_id_for_telegram_user(telegram_user_id)
        if not my_client_id:
            _ensure_client_user(telegram_user_id, u.get("full_name"))
            my_client_id = _get_client_id_for_telegram_user(telegram_user_id)
        client_id = int(my_client_id)
    else:
        _require_coach_or_admin(u)
        if not client_id:
            return {"ok": False, "error": "client_id_required_for_coach"}

    # Valida dentro de disponibilidad (reglas/extra)
    if not _slot_allowed(coach_id, start_dt, end_dt):
        return {"ok": False, "error": "outside_availability"}

    now = _utcnow()
    with ENGINE.begin() as cn:
        # Bloqueos
        blocked = cn.execute(
            text("""
                SELECT id
                  FROM availability_exceptions
                 WHERE coach_id=:cid
                   AND type='blocked'
                   AND start_at < :end_at
                   AND end_at > :start_at
                 LIMIT 1
            """),
            {"cid": coach_id, "start_at": start_dt, "end_at": end_dt},
        ).first()
        if blocked:
            return {"ok": False, "error": "blocked_by_exception", "exception_id": int(blocked[0])}

        # Conflicto con reservas
        conflict = cn.execute(
            text("""
                SELECT id
                  FROM bookings
                 WHERE coach_id=:coach_id
                   AND status IN ('tentative','confirmed')
                   AND start_at < :end_at
                   AND end_at > :start_at
                 LIMIT 1
            """),
            {"coach_id": coach_id, "start_at": start_dt, "end_at": end_dt},
        ).first()
        if conflict:
            return {"ok": False, "error": "slot_not_available", "conflict_booking_id": int(conflict[0])}

        res = cn.execute(
            text("""
                INSERT INTO bookings
                    (coach_id, client_id, service_id, start_at, end_at, status, notes,
                     created_by_user_id, created_at, updated_at)
                VALUES
                    (:coach_id, :client_id, :service_id, :start_at, :end_at, 'confirmed', :notes,
                     :created_by_user_id, :now, :now)
            """),
            {
                "coach_id": coach_id,
                "client_id": int(client_id),
                "service_id": service_id,
                "start_at": start_dt,
                "end_at": end_dt,
                "notes": notes,
                "created_by_user_id": actor_user_id,
                "now": now,
            },
        )
        booking_id = int(res.lastrowid)

    return {"ok": True, "booking_id": booking_id, "start_utc": start_dt.isoformat(), "end_utc": end_dt.isoformat()}

@mcp.tool
def cancel_booking(
    booking_id: int,
    reason: Optional[str] = None,
    telegram_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    if not telegram_user_id:
        raise ValueError("actor_telegram_user_id_required")    
    u = _get_app_user_by_telegram(telegram_user_id)
    if not u:
        raise ValueError("actor_not_registered")
    _require_active(u)

    actor_user_id = int(u["id"])
    my_client_id = _get_client_id_for_telegram_user(telegram_user_id) if u["role"] == "client" else None
    my_coach_id = _get_coach_id_for_telegram_user(telegram_user_id) if u["role"] == "coach" else None

    now = _utcnow()
    with ENGINE.begin() as cn:
        row = cn.execute(
            text("SELECT coach_id, client_id, status FROM bookings WHERE id=:id"),
            {"id": booking_id},
        ).first()
        if not row:
            return {"ok": False, "error": "not_found"}

        coach_id, client_id, status = int(row[0]), int(row[1]), str(row[2])
        if status == "cancelled":
            return {"ok": True, "booking_id": booking_id, "status": "cancelled"}

        # Permisos
        if u["role"] == "client":
            if not my_client_id or int(my_client_id) != client_id:
                return {"ok": False, "error": "forbidden_not_your_booking"}
        elif u["role"] == "coach":
            if not my_coach_id or int(my_coach_id) != coach_id:
                return {"ok": False, "error": "forbidden_other_coach_booking"}
        else:
            # admin ok
            pass

        cn.execute(
            text("""
                UPDATE bookings
                   SET status='cancelled',
                       cancelled_by_user_id=:telegram_user_id,
                       cancelled_at=:now,
                       cancel_reason=:reason,
                       updated_at=:now
                 WHERE id=:id
            """),
            {"id": booking_id, "telegram_user_id": actor_user_id, "reason": reason, "now": now},
        )

    return {"ok": True, "booking_id": booking_id, "status": "cancelled"}


def build_mcp_http_app(server: FastMCP, path: str = "/mcp"):
    """
    Devuelve el ASGI app del MCP para integrarlo en FastAPI.
    Importante: combinar lifespan al montar (ver main.py).
    """
    return server.http_app(path=path)
