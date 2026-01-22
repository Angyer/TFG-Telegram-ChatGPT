import os
import re
import json
import hashlib
import base64
import hmac
import secrets
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, Header
from pydantic import BaseModel, Field
from openai import OpenAI
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from mcp_servers.agenda_mcp import mcp, build_mcp_http_app
from contextlib import asynccontextmanager


load_dotenv()


# ====================================================================================================================================
# Config
# ====================================================================================================================================

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

# Reusable Prompt (Dashboard)
# If OPENAI_PROMPT_ID is set, we use the Prompt object (id/version/variables) instead of `instructions`.
OPENAI_PROMPT_ID = os.getenv("OPENAI_PROMPT_ID", "").strip()
OPENAI_PROMPT_VERSION = os.getenv("OPENAI_PROMPT_VERSION", "").strip()  # optional; if empty -> uses "current"

DEFAULT_COACH_TIMEZONE = os.getenv("DEFAULT_COACH_TIMEZONE", "Atlantic/Canary").strip()


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram/webhook")

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "").strip()

# MCP (Remote tool host)
# Recomendado:
#   - en local: MCP_SERVER_URL=http://127.0.0.1:8000/mcp/
#   - en prod (OpenAI necesita acceso público): MCP_SERVER_URL=https://<ngrok>.ngrok-free.app/mcp/
MCP_SERVER_URL = os.getenv("MCP_SERVER_URL") or (
    (PUBLIC_BASE_URL.rstrip("/") + "/mcp/") if PUBLIC_BASE_URL else ""
)
MCP_ACCESS_TOKEN = os.getenv("MCP_ACCESS_TOKEN", "").strip()  # opcional (dev)


# MariaDB settings
# - FastAPI en host: DB_HOST=127.0.0.1
# - FastAPI en docker compose: DB_HOST=mariadb
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "tfg")
DB_USER = os.getenv("DB_USER", "tfg_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "userpasswd")
DATABASE_URL = os.getenv("DATABASE_URL")

MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR", "migrations")

# Optional: store OpenAI response JSON into DB messages.openai_output_json (can be large)
STORE_OPENAI_OUTPUT_JSON = os.getenv("STORE_OPENAI_OUTPUT_JSON", "0") == "1"

if not OPENAI_API_KEY:
    raise RuntimeError("Falta OPENAI_API_KEY en entorno.")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Falta TELEGRAM_BOT_TOKEN en entorno.")

TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

INSTRUCTIONS_FILE = os.getenv("INSTRUCTIONS_FILE", "prompts/agenda_instructions.txt")

_instructions_cache: str | None = None
_instructions_mtime: float | None = None

def load_instructions(force: bool = False) -> str:
    """
    Carga instructions desde fichero.
    - Cachea en memoria.
    - Si force=True o el fichero cambió (mtime), recarga.
    """
    global _instructions_cache, _instructions_mtime

    path = Path(INSTRUCTIONS_FILE)
    if not path.exists():
        raise RuntimeError(f"Instructions file not found: {path.resolve()}")

    mtime = path.stat().st_mtime
    if force or _instructions_cache is None or _instructions_mtime != mtime:
        _instructions_cache = path.read_text(encoding="utf-8")
        _instructions_mtime = mtime

    return _instructions_cache

# ================================================================================================================
# MariaDB (SQLAlchemy)
# ================================================================================================================

if not DATABASE_URL:
    DATABASE_URL = URL.create(
        drivername="mysql+pymysql",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        query={"charset": "utf8mb4"},
    )

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
)

client = OpenAI(api_key=OPENAI_API_KEY)

# ======================================================================================================================
# MCP server (FastMCP) mounted inside FastAPI at /mcp
#   Requiere:
#     - carpeta mcp_servers/ con __init__.py
#     - fichero mcp_servers/agenda_mcp.py que define `mcp = FastMCP(...)`
# ======================================================================================================================

try:
    from mcp_servers.agenda_mcp import mcp  # noqa: F401
except Exception as e:
    raise RuntimeError(
        "No puedo importar mcp_servers.agenda_mcp. "
        "Crea mcp_servers/__init__.py y mcp_servers/agenda_mcp.py (FastMCP). "
        f"Detalle: {repr(e)}"
    )

mcp_app = mcp.http_app(path="/mcp")

# ======================================================================================================================
# Assistant config (Agenda)
# ======================================================================================================================

TOOLS: List[Dict[str, Any]] = [
    {
        "type": "mcp",
        "server_label": "agenda_mcp",
        "server_description": "Agenda del monitor de pádel: disponibilidad, reservas, cancelaciones.",
        "server_url": MCP_SERVER_URL,
        # Para desarrollo: cuando confías en tu servidor MCP
        "require_approval": "never",
        "allowed_tools": [
            "db_ping",
            "list_coaches",
            "list_services",
            "list_available_slots",
            "create_booking",
            "cancel_booking",
            "list_my_bookings",
            "list_bookings",
            "upsert_service",
            "set_availability_rules",
            "add_availability_exception",
        ],
        # Auth: el campo oficial que documenta OpenAI para MCP/Connectors es `authorization`.
        # Si tu MCP valida Bearer tokens: usa "Bearer <token>".
        **({"authorization": f"Bearer {MCP_ACCESS_TOKEN}"} if MCP_ACCESS_TOKEN else {}),
    }
]

BASE_MCP_TOOL: Dict[str, Any] = {
    "type": "mcp",
    "server_label": "agenda_mcp",
    "server_description": "Agenda del monitor de pádel: disponibilidad, reservas, cancelaciones.",
    "server_url": MCP_SERVER_URL,
    "require_approval": "never",
    **({"authorization": f"Bearer {MCP_ACCESS_TOKEN}"} if MCP_ACCESS_TOKEN else {}),
}

CLIENT_ALLOWED_TOOLS: List[str] = [
    "db_ping",
    "list_coaches",
    "list_services",
    "list_available_slots",
    "create_booking",
    "cancel_booking",
    "list_my_bookings",
]

COACH_ALLOWED_TOOLS: List[str] = [
    "db_ping",
    "list_coaches",
    "list_services",
    "list_available_slots",
    "list_bookings",
    "create_booking",
    "cancel_booking",
    "upsert_service",
    "set_availability_rules",
    "add_availability_exception",
]

def build_tools_for_role(role: str) -> List[Dict[str, Any]]:
    tool = dict(BASE_MCP_TOOL)
    tool["allowed_tools"] = COACH_ALLOWED_TOOLS if role in ("coach", "admin") else CLIENT_ALLOWED_TOOLS
    return [tool]

# Fallback si no usas Prompt reusable del Dashboard
def get_instructions_text() -> str:
    return load_instructions()

# ========================================================================================================================
# Common helpers
# ========================================================================================================================
def utc_now_dt() -> datetime:
    """Devuelve DATETIME naive en UTC para MariaDB."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _today_local_iso(tz_name: str) -> str:
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("Atlantic/Canary")
    return datetime.now(tz).date().isoformat()


def build_prompt_object(variables: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """
    Construye el objeto `prompt` para Responses API:
      { "id": "pmpt_...", "version": "...", "variables": {...} }
    Si OPENAI_PROMPT_ID no está configurado, devuelve None (se usará `instructions`).
    """
    if not OPENAI_PROMPT_ID:
        return None

    prompt_obj: Dict[str, Any] = {"id": OPENAI_PROMPT_ID, "variables": variables}
    if OPENAI_PROMPT_VERSION:
        prompt_obj["version"] = OPENAI_PROMPT_VERSION
    # Si no se especifica version, OpenAI usará la "current" del Dashboard.
    return prompt_obj

# ================================================================================================================
# DB Helpers (MariaDB + SQLAlchemy) — Alternativa A (PK compuesta en sessions)
# ================================================================================================================

def init_db() -> None:
    """
    Crea las tablas "infra" del bot (si no existen):
      - telegram_users
      - sessions
      - telegram_updates
      - messages
      - tool_calls
    Nota: las tablas de dominio (agenda) se gestionan por migraciones (.sql).
    """
    ddl_statements = [
        # 1) Usuarios de Telegram
        """
        CREATE TABLE IF NOT EXISTS telegram_users (
          telegram_user_id BIGINT NOT NULL,
          username         VARCHAR(64) NULL,
          first_name       VARCHAR(128) NULL,
          last_name        VARCHAR(128) NULL,
          language_code    VARCHAR(16) NULL,
          is_bot           TINYINT(1) NULL,
          created_at       DATETIME(6) NOT NULL,
          updated_at       DATETIME(6) NOT NULL,
          PRIMARY KEY (telegram_user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,

        # 2) Sesiones por (usuario, chat)
        """
        CREATE TABLE IF NOT EXISTS sessions (
          telegram_user_id        BIGINT NOT NULL,
          telegram_chat_id        BIGINT NOT NULL,
          history_json            LONGTEXT NULL,
          openai_conversation_id  VARCHAR(128) NULL,
          openai_last_response_id VARCHAR(128) NULL,
          created_at              DATETIME(6) NOT NULL,
          updated_at              DATETIME(6) NOT NULL,
          PRIMARY KEY (telegram_user_id, telegram_chat_id),
          CONSTRAINT fk_sessions_user
            FOREIGN KEY (telegram_user_id)
            REFERENCES telegram_users(telegram_user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,

        # 3) Idempotencia de updates
        """
        CREATE TABLE IF NOT EXISTS telegram_updates (
          update_id   BIGINT NOT NULL,
          received_at DATETIME(6) NOT NULL,
          PRIMARY KEY (update_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,

        # 4) Mensajes (audit log)
        """
        CREATE TABLE IF NOT EXISTS messages (
          id                  BIGINT NOT NULL AUTO_INCREMENT,
          telegram_user_id    BIGINT NOT NULL,
          telegram_chat_id    BIGINT NOT NULL,
          direction           ENUM('in','out') NOT NULL,
          telegram_update_id  BIGINT NULL,
          telegram_message_id BIGINT NULL,
          role                VARCHAR(16) NULL,
          text                LONGTEXT NULL,
          openai_response_id  VARCHAR(128) NULL,
          openai_output_json  LONGTEXT NULL,
          created_at          DATETIME(6) NOT NULL,
          PRIMARY KEY (id),

          INDEX idx_messages_session_time (telegram_user_id, telegram_chat_id, created_at),
          INDEX idx_messages_update (telegram_update_id),

          CONSTRAINT fk_messages_session
            FOREIGN KEY (telegram_user_id, telegram_chat_id)
            REFERENCES sessions(telegram_user_id, telegram_chat_id),

          CONSTRAINT fk_messages_update
            FOREIGN KEY (telegram_update_id)
            REFERENCES telegram_updates(update_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,

        # 5) Tool calls vinculadas a un message concreto
        """
        CREATE TABLE IF NOT EXISTS tool_calls (
          id             BIGINT NOT NULL AUTO_INCREMENT,
          message_id     BIGINT NOT NULL,
          openai_call_id VARCHAR(128) NULL,
          tool_name      VARCHAR(64) NOT NULL,
          arguments_json LONGTEXT NULL,
          output_text    LONGTEXT NULL,
          created_at     DATETIME(6) NOT NULL,
          PRIMARY KEY (id),

          INDEX idx_tool_calls_message (message_id),

          CONSTRAINT fk_toolcalls_message
            FOREIGN KEY (message_id)
            REFERENCES messages(id)
            ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
    ]
    with engine.begin() as conn:
        for stmt in ddl_statements:
            conn.execute(text(stmt))

def get_app_user_by_telegram(telegram_user_id: int) -> Optional[Dict[str, Any]]:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, role, status, full_name FROM app_users WHERE telegram_user_id=:tid"),
            {"tid": telegram_user_id},
        ).mappings().first()
    return dict(row) if row else None


def get_client_id_by_user_id(user_id: int) -> Optional[int]:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT id FROM clients WHERE user_id=:uid"), {"uid": user_id}).first()
    return int(row[0]) if row else None


def get_coach_id_by_user_id(user_id: int) -> Optional[int]:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT id FROM coaches WHERE user_id=:uid"), {"uid": user_id}).first()
    return int(row[0]) if row else None


def ensure_client_user(telegram_user_id: int, full_name: Optional[str]) -> Tuple[int, int]:
    """
    Garantiza que existe app_users(role=client) + clients para este telegram_user_id.
    Devuelve (app_user_id, client_id).
    """
    now = utc_now_dt()
    with engine.begin() as conn:
        u = conn.execute(
            text("SELECT id, role, status, full_name FROM app_users WHERE telegram_user_id=:tid"),
            {"tid": telegram_user_id},
        ).mappings().first()
        if u:
            if u["status"] != "active":
                raise RuntimeError("Usuario bloqueado")
            user_id = int(u["id"])
            # No degradamos roles (si ya es coach/admin, lo respetamos)
            if u["role"] == "client" and full_name and not u.get("full_name"):
                conn.execute(
                    text("UPDATE app_users SET full_name=:n, updated_at=:now WHERE id=:id"),
                    {"n": full_name, "now": now, "id": user_id},
                )
        else:
            res = conn.execute(
                text("""
                    INSERT INTO app_users(telegram_user_id, role, full_name, status, created_at, updated_at)
                    VALUES (:tid, 'client', :n, 'active', :now, :now)
                """),
                {"tid": telegram_user_id, "n": full_name, "now": now},
            )
            user_id = int(res.lastrowid)

        c = conn.execute(text("SELECT id FROM clients WHERE user_id=:uid"), {"uid": user_id}).first()
        if c:
            client_id = int(c[0])
        else:
            res2 = conn.execute(
                text("INSERT INTO clients(user_id, created_at, updated_at) VALUES (:uid, :now, :now)"),
                {"uid": user_id, "now": now},
            )
            client_id = int(res2.lastrowid)

    return user_id, client_id


def get_active_coach_id(telegram_user_id: int, telegram_chat_id: int) -> Optional[int]:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT active_coach_id
                      FROM sessions
                     WHERE telegram_user_id=:uid AND telegram_chat_id=:cid
                """),
                {"uid": telegram_user_id, "cid": telegram_chat_id},
            ).first()
        if not row:
            return None
        return int(row[0]) if row[0] is not None else None
    except Exception:
        return None


def set_active_coach_id(telegram_user_id: int, telegram_chat_id: int, coach_id: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE sessions
                   SET active_coach_id=:coach_id,
                       updated_at=:now
                 WHERE telegram_user_id=:uid AND telegram_chat_id=:cid
            """),
            {"coach_id": coach_id, "now": utc_now_dt(), "uid": telegram_user_id, "cid": telegram_chat_id},
        )


def count_coaches() -> int:
    with engine.connect() as conn:
        v = conn.execute(text("SELECT COUNT(*) FROM coaches")).scalar()
    return int(v or 0)


def get_single_coach_id_if_unique() -> Optional[int]:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT id FROM coaches ORDER BY id")).fetchall()
    if len(row) == 1:
        return int(row[0][0])
    return None


def _sha256_hex_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def consume_coach_invite(token: str, telegram_user_id: int) -> int:
    """
    Consume un token de coach_invites:
      - valida token_hash, expiración, used_at
      - crea/actualiza app_users(role=coach)
      - crea coaches si no existe
      - marca invite como usada
    Devuelve coach_id.
    """
    token_hash = _sha256_hex_str(token)
    now = utc_now_dt()
    with engine.begin() as conn:
        inv = conn.execute(
            text("""
                SELECT id, proposed_full_name, proposed_timezone, proposed_default_lesson_minutes, note,
                       expires_at, used_at
                  FROM coach_invites
                 WHERE token_hash=:h
                 LIMIT 1
            """),
            {"h": token_hash},
        ).mappings().first()
        if not inv:
            raise ValueError("invalid_token")
        if inv["used_at"] is not None:
            raise ValueError("already_used")
        if inv["expires_at"] is not None and inv["expires_at"] < now:
            raise ValueError("expired")

        u = conn.execute(
            text("SELECT id, role, status, full_name FROM app_users WHERE telegram_user_id=:tid"),
            {"tid": telegram_user_id},
        ).mappings().first()

        proposed_name = inv.get("proposed_full_name")
        if u:
            if u["status"] != "active":
                raise ValueError("user_blocked")
            user_id = int(u["id"])
            # Admin mantiene admin. Si era client, se promociona.
            if u["role"] != "admin":
                conn.execute(
                    text("""
                        UPDATE app_users
                           SET role='coach',
                               full_name=COALESCE(full_name, :name),
                               updated_at=:now
                         WHERE id=:id
                    """),
                    {"name": proposed_name, "now": now, "id": user_id},
                )
        else:
            res = conn.execute(
                text("""
                    INSERT INTO app_users(telegram_user_id, role, full_name, status, created_at, updated_at)
                    VALUES (:tid, 'coach', :name, 'active', :now, :now)
                """),
                {"tid": telegram_user_id, "name": proposed_name, "now": now},
            )
            user_id = int(res.lastrowid)

        # Crear coach si no existe
        row = conn.execute(text("SELECT id FROM coaches WHERE user_id=:uid"), {"uid": user_id}).first()
        if row:
            coach_id = int(row[0])
        else:
            tz = inv.get("proposed_timezone") or DEFAULT_COACH_TIMEZONE or "Europe/Madrid"
            mins = int(inv.get("proposed_default_lesson_minutes") or 60)
            notes = inv.get("note")
            res2 = conn.execute(
                text("""
                    INSERT INTO coaches(user_id, timezone, default_lesson_minutes, notes, created_at, updated_at)
                    VALUES (:uid, :tz, :mins, :notes, :now, :now)
                """),
                {"uid": user_id, "tz": tz, "mins": mins, "notes": notes, "now": now},
            )
            coach_id = int(res2.lastrowid)

        conn.execute(
            text("""
                UPDATE coach_invites
                   SET used_at=:now,
                       used_by_telegram_user_id=:tid
                 WHERE id=:id
            """),
            {"now": now, "tid": telegram_user_id, "id": int(inv["id"])},
        )

    return coach_id

# ==============================================================================================================
# DB: Infra operations
# ==============================================================================================================

def upsert_telegram_user(
    telegram_user_id: int,
    username: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    language_code: Optional[str] = None,
    is_bot: Optional[bool] = None,
) -> None:
    """
    Inserta o actualiza el usuario de Telegram.
    Recomendación: llamar al inicio de process_update() antes de ensure_session().
    """
    now = utc_now_dt()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO telegram_users (
                  telegram_user_id, username, first_name, last_name, language_code, is_bot, created_at, updated_at
                ) VALUES (
                  :telegram_user_id, :username, :first_name, :last_name, :language_code, :is_bot, :created_at, :updated_at
                )
                ON DUPLICATE KEY UPDATE
                  username      = VALUES(username),
                  first_name    = VALUES(first_name),
                  last_name     = VALUES(last_name),
                  language_code = VALUES(language_code),
                  is_bot        = VALUES(is_bot),
                  updated_at    = VALUES(updated_at)
                """
            ),
            {
                "telegram_user_id": telegram_user_id,
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "language_code": language_code,
                "is_bot": 1 if is_bot is True else (0 if is_bot is False else None),
                "created_at": now,
                "updated_at": now,
            },
        )


def ensure_session(
    telegram_user_id: int,
    telegram_chat_id: int,
) -> None:
    """
    Garantiza que existe una fila en sessions para (telegram_user_id, telegram_chat_id).
    OJO: como sessions tiene FK a telegram_users, debes haber ejecutado upsert_telegram_user()
    antes de llamar a ensure_session().
    """
    now = utc_now_dt()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO sessions (
                  telegram_user_id, telegram_chat_id, history_json,
                  openai_conversation_id, openai_last_response_id,
                  created_at, updated_at
                )
                VALUES (
                  :uid, :cid, :history_json,
                  NULL, NULL,
                  :created_at, :updated_at
                )
                ON DUPLICATE KEY UPDATE
                  updated_at = VALUES(updated_at)
                """
            ),
            {
                "uid": telegram_user_id,
                "cid": telegram_chat_id,
                "history_json": json.dumps([], ensure_ascii=False),
                "created_at": now,
                "updated_at": now,
            },
        )

def get_session(telegram_user_id: int, telegram_chat_id: int) -> Tuple[Optional[str], Optional[str]]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT openai_conversation_id, openai_last_response_id
                  FROM sessions
                 WHERE telegram_user_id = :uid AND telegram_chat_id = :cid
                """
            ),
            {"uid": telegram_user_id, "cid": telegram_chat_id},
        ).fetchone()

    if not row:
        return None, None

    m = row._mapping
    return m["openai_conversation_id"], m["openai_last_response_id"]


def set_openai_conversation_id(
    telegram_user_id: int,
    telegram_chat_id: int,
    conversation_id: str,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE sessions
                   SET openai_conversation_id = :conversation_id,
                       updated_at = :updated_at
                 WHERE telegram_user_id = :uid AND telegram_chat_id = :cid
                """
            ),
            {
                "conversation_id": conversation_id,
                "updated_at": utc_now_dt(),
                "uid": telegram_user_id,
                "cid": telegram_chat_id,
            },
        )


def set_openai_last_response_id(
    telegram_user_id: int,
    telegram_chat_id: int,
    response_id: str,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE sessions
                   SET openai_last_response_id = :response_id,
                       updated_at = :updated_at
                 WHERE telegram_user_id = :uid AND telegram_chat_id = :cid
                """
            ),
            {
                "response_id": response_id,
                "updated_at": utc_now_dt(),
                "uid": telegram_user_id,
                "cid": telegram_chat_id,
            },
        )


def mark_update_received(update_id: int) -> bool:
    """
    Idempotencia: inserta el update_id una sola vez.
    Devuelve True si este update se inserta ahora; False si ya existía.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT IGNORE INTO telegram_updates(update_id, received_at)
                VALUES (:update_id, :received_at)
                """
            ),
            {"update_id": update_id, "received_at": utc_now_dt()},
        )
        # rowcount=1 si insertó, 0 si ignoró
        return (result.rowcount or 0) == 1


def log_message(
    telegram_user_id: int,
    telegram_chat_id: int,
    direction: str,
    telegram_update_id: Optional[int],
    telegram_message_id: Optional[int],
    role: Optional[str],
    text_content: Optional[str],
    openai_response_id: Optional[str] = None,
    openai_output_json: Optional[str] = None,
) -> int:
    """
    Inserta un mensaje (in/out) y devuelve messages.id.
    Requisito: debe existir sessions(uid,cid) por la FK compuesta.
    """
    now = utc_now_dt()
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                INSERT INTO messages(
                  telegram_user_id, telegram_chat_id,
                  direction, telegram_update_id, telegram_message_id,
                  role, text,
                  openai_response_id, openai_output_json,
                  created_at
                ) VALUES (
                  :uid, :cid,
                  :direction, :update_id, :message_id,
                  :role, :text,
                  :openai_response_id, :openai_output_json,
                  :created_at
                )
                """
            ),
            {
                "uid": telegram_user_id,
                "cid": telegram_chat_id,
                "direction": direction,
                "update_id": telegram_update_id,
                "message_id": telegram_message_id,
                "role": role,
                "text": text_content,
                "openai_response_id": openai_response_id,
                "openai_output_json": openai_output_json,
                "created_at": now,
            },
        )
        # SQLAlchemy + PyMySQL: lastrowid disponible
        return int(res.lastrowid)


def log_tool_call(
    message_id: int,
    tool_name: str,
    openai_call_id: Optional[str] = None,
    arguments_json: Optional[str] = None,
    output_text: Optional[str] = None,
) -> int:
    """
    Inserta una tool_call vinculada a messages.id (FK).
    Devuelve tool_calls.id.
    """
    now = utc_now_dt()
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                INSERT INTO tool_calls(
                  message_id, openai_call_id, tool_name, arguments_json, output_text, created_at
                ) VALUES (
                  :message_id, :openai_call_id, :tool_name, :arguments_json, :output_text, :created_at
                )
                """
            ),
            {
                "message_id": message_id,
                "openai_call_id": openai_call_id,
                "tool_name": tool_name,
                "arguments_json": arguments_json,
                "output_text": output_text,
                "created_at": now,
            },
        )
        return int(res.lastrowid)
    

# =====================================================================================================
# DB: Simple migrations system (schema_migrations + apply_migrations)
# =====================================================================================================
def ensure_schema_migrations_table() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version   VARCHAR(32) NOT NULL,
      filename  VARCHAR(255) NOT NULL,
      checksum  CHAR(64) NOT NULL,
      applied_at DATETIME(6) NOT NULL,
      PRIMARY KEY (version),
      UNIQUE KEY uq_schema_migrations_filename (filename)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _migration_version_from_filename(filename: str) -> str:
    """
    Extrae version de nombres tipo:
      001_agenda_schema.sql  -> '001'
      20260105_init.sql      -> '20260105'
    Regla: toma el prefijo numérico inicial.
    """
    m = re.match(r"^(\d+)", filename)
    if not m:
        raise ValueError(f"Nombre de migración inválido (debe empezar por dígitos): {filename}")
    return m.group(1)


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_sql_file(path: Path) -> Tuple[str, str]:
    raw = path.read_bytes()
    checksum = _sha256_hex(raw)
    # tolera UTF-8 con BOM
    sql = raw.decode("utf-8-sig")
    return sql, checksum


def _split_sql_statements(sql: str) -> List[str]:
    """
    Split razonablemente robusto por ';' evitando cortar dentro de:
      - strings '...'
      - strings "..."
      - identifiers `...`
      - comentarios -- ... y /* ... */
    Nota: no es un parser completo de SQL; para migraciones convencionales funciona bien.
    """
    statements: List[str] = []
    buf: List[str] = []

    in_single = False
    in_double = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False

    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        # Comentario de línea
        if not (in_single or in_double or in_backtick or in_block_comment) and not in_line_comment:
            if ch == "-" and nxt == "-":
                in_line_comment = True
                i += 2
                continue

        # Comentario de bloque
        if not (in_single or in_double or in_backtick or in_line_comment) and not in_block_comment:
            if ch == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                buf.append(ch)  # conserva saltos de línea
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue

        # Toggle de comillas
        if ch == "'" and not (in_double or in_backtick):
            # Manejo de escape '' dentro de string
            if in_single and nxt == "'":
                buf.append(ch)
                buf.append(nxt)
                i += 2
                continue
            in_single = not in_single
            buf.append(ch)
            i += 1
            continue

        if ch == '"' and not (in_single or in_backtick):
            in_double = not in_double
            buf.append(ch)
            i += 1
            continue

        if ch == "`" and not (in_single or in_double):
            in_backtick = not in_backtick
            buf.append(ch)
            i += 1
            continue

        # Fin de statement
        if ch == ";" and not (in_single or in_double or in_backtick):
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)

    return statements


def _get_applied_migrations() -> Dict[str, Dict[str, str]]:
    """
    Devuelve dict:
      { version: {"filename":..., "checksum":...}, ... }
    """
    ensure_schema_migrations_table()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT version, filename, checksum FROM schema_migrations")
        ).fetchall()

    applied: Dict[str, Dict[str, str]] = {}
    for r in rows:
        m = r._mapping
        applied[m["version"]] = {"filename": m["filename"], "checksum": m["checksum"]}
    return applied


def apply_migrations(migrations_dir: str = MIGRATIONS_DIR) -> None:
    """
    Aplica migraciones pendientes en orden ascendente por filename.
    - Requiere ficheros .sql con prefijo numérico.
    - Registra cada migración aplicada en schema_migrations.
    - Detecta drift: si una migración ya aplicada cambia (checksum distinto), falla.
    """
    ensure_schema_migrations_table()
    applied = _get_applied_migrations()

    base = Path(migrations_dir)
    if not base.exists():
        return

    files = sorted([p for p in base.iterdir() if p.is_file() and p.suffix.lower() == ".sql"])

    for path in files:
        filename = path.name
        version = _migration_version_from_filename(filename)
        sql, checksum = _read_sql_file(path)

        if version in applied:
            # Drift detection
            prev = applied[version]
            if prev["checksum"] != checksum or prev["filename"] != filename:
                raise RuntimeError(
                    f"Drift detectado en migración {version}.\n"
                    f"En DB: filename={prev['filename']} checksum={prev['checksum']}\n"
                    f"En disco: filename={filename} checksum={checksum}\n"
                    f"Solución: NO edites migraciones ya aplicadas; crea una nueva (p.ej. 00X_...)."
                )
            continue  # ya aplicada

        statements = _split_sql_statements(sql)

        # Importante: DDL en MySQL/MariaDB suele hacer commits implícitos.
        # Aun así, engine.begin() ayuda a que la inserción en schema_migrations
        # solo se haga si todas las sentencias han ejecutado sin error.
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))

            conn.execute(
                text(
                    """
                    INSERT INTO schema_migrations(version, filename, checksum, applied_at)
                    VALUES (:version, :filename, :checksum, :applied_at)
                    """
                ),
                {
                    "version": version,
                    "filename": filename,
                    "checksum": checksum,
                    "applied_at": utc_now_dt(),
                },
            )


def mark_migration_as_applied(version: str, filename: str, checksum: str) -> None:
    """
    Bootstrap manual: registra una migración como aplicada sin ejecutarla.
    Útil si ya aplicaste una migración a mano (como tu 001) y quieres empezar
    a usar apply_migrations sin re-ejecutarla.
    """
    ensure_schema_migrations_table()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO schema_migrations(version, filename, checksum, applied_at)
                VALUES (:version, :filename, :checksum, :applied_at)
                ON DUPLICATE KEY UPDATE
                  filename=VALUES(filename),
                  checksum=VALUES(checksum)
                """
            ),
            {
                "version": version,
                "filename": filename,
                "checksum": checksum,
                "applied_at": utc_now_dt(),
            },
        )
def _table_exists(table_name: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT 1
                  FROM information_schema.tables
                 WHERE table_schema = DATABASE()
                   AND table_name = :t
                 LIMIT 1
                """
            ),
            {"t": table_name},
        ).fetchone()
    return bool(row)

def bootstrap_legacy_migrations_if_needed(migrations_dir: str = MIGRATIONS_DIR) -> None:
    """
    Caso común en desarrollo: aplicaste 001 a mano y luego quieres empezar a usar apply_migrations().
    Si schema_migrations está vacío pero detectamos que una tabla de dominio existe, marcamos 001 como aplicada.
    """
    ensure_schema_migrations_table()

    with engine.connect() as conn:
        row = conn.execute(text("SELECT COUNT(*) AS c FROM schema_migrations")).fetchone()
        count = int(row._mapping["c"]) if row else 0

    if count > 0:
        return

    # Heurística: si existe una tabla "bookings" asumimos que 001_agenda_schema.sql ya se aplicó.
    if not _table_exists("bookings"):
        return

    base = Path(migrations_dir)
    candidate = base / "001_agenda_schema.sql"
    if not candidate.exists():
        return

    sql, checksum = _read_sql_file(candidate)
    _ = sql  # no se ejecuta aquí
    version = _migration_version_from_filename(candidate.name)
    mark_migration_as_applied(version=version, filename=candidate.name, checksum=checksum)



# =====================================================================================
# Telegram helpers
# =====================================================================================
async def telegram_send_message(chat_id: int, text_msg: str) -> None:
    payload = {"chat_id": chat_id, "text": text_msg}
    async with httpx.AsyncClient(timeout=20) as http:
        r = await http.post(f"{TELEGRAM_API_BASE}/sendMessage", json=payload)
        r.raise_for_status()


async def telegram_set_webhook(webhook_url: str, secret_token: str) -> Dict[str, Any]:
    payload = {"url": webhook_url}
    if secret_token:
        payload["secret_token"] = secret_token

    async with httpx.AsyncClient(timeout=20) as http:
        r = await http.post(f"{TELEGRAM_API_BASE}/setWebhook", json=payload)
        r.raise_for_status()
        return r.json()


# ==============================================================================================================
# OpenAI (Responses) orchestration — Conversation-based
# ==============================================================================================================
"""
def _redact(d: dict) -> dict:
    d = dict(d)
    # Redactar variables sensibles dentro de prompt
    if "prompt" in d and isinstance(d["prompt"], dict):
        pv = d["prompt"].get("variables")
        if isinstance(pv, dict):
            pv = dict(pv)
            for k in list(pv.keys()):
                lk = k.lower()
                if "token" in lk or "secret" in lk or "key" in lk:
                    pv[k] = "***REDACTED***"
            d["prompt"]["variables"] = pv
    # Redactar herramientas si incluyes headers (por si acaso)
    if "tools" in d and isinstance(d["tools"], list):
        tools = []
        for t in d["tools"]:
            t2 = dict(t)
            if "headers" in t2 and t2["headers"]:
                t2["headers"] = "***REDACTED***"
            tools.append(t2)
        d["tools"] = tools
    return d
"""
def run_agenda_assistant_in_conversation(
    conversation_id: str,
    user_text: str,
    user_role: str,
    prompt_variables: Dict[str, Any],
    debug_tool: str | None = None,
) -> Tuple[str, str, List[Dict[str, Any]], Optional[str]]:
    """
    Turno con conversación persistente y herramientas MCP remotas.
	- responses.create(conversation=...) con input del usuario
	- MCP se ejecuta "hosted" dentro de OpenAI (no hay loop de tool outputs aquí)
    
    Devuelve:
      (texto_final, response_id_final, tool_execs, final_response_json_opt)

    Nota:
      - Para MCP, los tool calls aparecen como items type='mcp_call' con campos:
        id, name, arguments, output, error, server_label, etc.
      - No hay que ejecutar tools manualmente (OpenAI hace la llamada al MCP).
    """
    if not MCP_SERVER_URL:
        raise RuntimeError(
            "Falta MCP_SERVER_URL (o PUBLIC_BASE_URL para inferirlo). "
            "OpenAI necesita una URL pública para llamar al MCP."
        )

    tool_execs: List[Dict[str, str]] = []
    final_response_json: Optional[str] = None
    #tools = build_tools_for_role(user_role)
    tools = TOOLS
    prompt_obj = build_prompt_object(prompt_variables)

    input_msgs: List[Dict[str, Any]] = []
    if not prompt_obj:
        # Si no usas prompt reusable, añadimos contexto como developer message.
        ctx_lines = [f"{k}={v}" for k, v in prompt_variables.items() if v]
        input_msgs.append({"role": "developer", "content": "Contexto:\n" + "\n".join(ctx_lines)})
    input_msgs.append({"role": "user", "content": user_text})

    create_kwargs: Dict[str, Any] = {
        "model": OPENAI_MODEL,
        "conversation": {"id": conversation_id},
        "tools": tools,
        "input": user_text,
        "temperature": 0.1,
    }
    if prompt_obj:
        create_kwargs["prompt"] = prompt_obj
    else:
        create_kwargs["instructions"] = get_instructions_text()

    if debug_tool:
        create_kwargs["tool_choice"] = {
            "type": "mcp",
            "server_label": "agenda_mcp",
            "name": debug_tool,
    }
    #print("DEBUG allowed_tools:", create_kwargs["tools"][0].get("allowed_tools"))
    print("DEBUG tool_choice:", create_kwargs.get("tool_choice"))
    try:
        with open("variables.jsonl", "a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "conversation_id": conversation_id,
                        "user_role": user_role,
                        "prompt_variables": prompt_variables,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    except Exception:
        # No bloquees el flujo principal si falla el log
        pass
    with open("create_kwargs.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps((create_kwargs), ensure_ascii=False) + "\n")

    response = client.responses.create(**create_kwargs)
    #items = client.responses.input_items.list(response.id)
    #print(items.data)

    # Debug: guardar respuestas crudas
    try:
        with open("output.jsonl", "a", encoding="utf-8") as f:
            f.write(response.model_dump_json())
            f.write("\n")
    except Exception:
        pass

    dumped = response.model_dump()
    output_items = dumped.get("output", [])

    # En la documentación aparecen items como `mcp_list_tools` y `mcp_tool_call`.
    # Para logging, guardamos todos los items MCP relevantes.
    # Log MCP tool calls (si hubo)
    for it in output_items:
        if it.get("type") != "mcp_call":
            continue
        tool_execs.append(
            {
                "call_id": it.get("id", ""),  # el item id es el identificador más estable del call
                "name": it.get("name", ""),
                "arguments": it.get("arguments", "") or "",
                "output": (it.get("output", "") or "") if it.get("error") is None else json.dumps({"error": it.get("error")}),
            }
        )


    final_text = getattr(response, "output_text", "") or ""
    final_id = getattr(response, "id", "") or ""

    final_response_json: Optional[str] = None
    if STORE_OPENAI_OUTPUT_JSON:
        try:
            final_response_json = response.model_dump_json()
        except Exception:
            final_response_json = None

    if not final_text.strip():
        final_text = (
            "No he podido generar una respuesta. "
            "Ejemplos: '¿Hay hueco mañana por la tarde?', 'Reserva una clase el viernes a las 18:00', "
            "'Cancela mi clase de mañana'."
        )

    return final_text, final_id, tool_execs, final_response_json

# ======================================================================================================================
# Lifespan (combina tu init/migrations + lifespan del MCP)
# ======================================================================================================================
@asynccontextmanager
async def app_lifespan(_app: FastAPI):
    init_db()
    ensure_schema_migrations_table()
    bootstrap_legacy_migrations_if_needed()
    apply_migrations()
    yield

@asynccontextmanager
async def combined_lifespan(app: FastAPI):
    async with app_lifespan(app):
        async with mcp_app.lifespan(app):
            yield

# ======================================================================================================================
# FastAPI app (incluye rutas MCP)
# ======================================================================================================================

app = FastAPI(
    title="Telegram + OpenAI Responses (Agenda Pádel) - MariaDB",
    lifespan=combined_lifespan,
    routes=[*mcp_app.routes],
)

# =======================================================================================
# Admin API: provisioning por invitación (coach_invites)
# =======================================================================================

class CoachInviteCreateRequest(BaseModel):
    proposed_full_name: Optional[str] = None
    proposed_timezone: Optional[str] = None
    proposed_default_lesson_minutes: Optional[int] = Field(default=None, ge=15, le=240)
    note: Optional[str] = None
    expires_in_hours: int = Field(default=72, ge=1, le=720)


@app.post("/admin/coach-invites")
def admin_create_coach_invite(
    payload: CoachInviteCreateRequest,
    x_admin_key: str = Header("", alias="X-Admin-Key"),
) -> Dict[str, Any]:
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY no configurada en entorno.")
    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid admin key.")

    token = "CI_" + secrets.token_urlsafe(24)
    token_hash = _sha256_hex_str(token)

    now = utc_now_dt()
    expires_at = now + timedelta(hours=int(payload.expires_in_hours))

    with engine.begin() as conn:
        res = conn.execute(
            text("""
                INSERT INTO coach_invites(
                  token_hash, proposed_full_name, proposed_timezone, proposed_default_lesson_minutes, note,
                  expires_at, used_at, used_by_telegram_user_id, created_at
                ) VALUES (
                  :token_hash, :name, :tz, :mins, :note,
                  :expires_at, NULL, NULL, :created_at
                )
            """),
            {
                "token_hash": token_hash,
                "name": payload.proposed_full_name,
                "tz": payload.proposed_timezone,
                "mins": payload.proposed_default_lesson_minutes,
                "note": payload.note,
                "expires_at": expires_at,
                "created_at": now,
            },
        )
        invite_id = int(res.lastrowid)

    # Importante: solo devolvemos el token en claro aquí (una vez).
    return {
        "ok": True,
        "invite_id": invite_id,
        "token": token,
        "expires_at": expires_at.isoformat(),
    }

# =======================================================================================
# Web endpoints
# =======================================================================================
@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}

@app.get("/health/db")
def health_db():
    try:
        with engine.connect() as conn:
            val = conn.execute(text("SELECT 1")).scalar_one()
        return {"ok": True, "db": int(val)}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {e}")

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request, background_tasks: BackgroundTasks) -> Dict[str, bool]:
    if TELEGRAM_WEBHOOK_SECRET:
        got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if got != TELEGRAM_WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="Invalid Telegram webhook secret token.")

    update = await request.json()
    background_tasks.add_task(process_update, update)
    return {"ok": True}


@app.post("/telegram/set-webhook")
async def set_webhook() -> Dict[str, Any]:
    if not PUBLIC_BASE_URL:
        raise HTTPException(400, detail="Falta PUBLIC_BASE_URL en entorno.")
    url = PUBLIC_BASE_URL.rstrip("/") + WEBHOOK_PATH
    result = await telegram_set_webhook(url, TELEGRAM_WEBHOOK_SECRET)
    return {"webhook_url": url, "telegram_result": result}

@app.post("/admin/reload-instructions")
def admin_reload_instructions(x_admin_key: str = Header("", alias="X-Admin-Key")):
    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid admin key.")
    load_instructions(force=True)
    return {"ok": True}

# =======================================================================================
# Update processing
# =======================================================================================
async def process_update(update: Dict[str, Any]) -> None:
    """
    Procesa un Update de Telegram.
    Para este MVP solo atendemos mensajes de texto en update.message.text.
    """
    try:
        update_id = int(update.get("update_id"))
        msg = update.get("message") or {}
        text_msg = msg.get("text")
        if not text_msg:
            return

        chat = msg.get("chat") or {}
        user = msg.get("from") or {}
        telegram_chat_id = int(chat.get("id"))
        telegram_user_id = int(user.get("id"))
        telegram_message_id = int(msg.get("message_id", 0)) or None

        # Idempotencia con update_id (Telegram puede reintentar webhooks).
        if not mark_update_received(update_id):
            return

        # 2) Upsert usuario (requerido por FK de sessions)
        upsert_telegram_user(
            telegram_user_id=telegram_user_id,
            username=user.get("username"),
            first_name=user.get("first_name"),
            last_name=user.get("last_name"),
            language_code=user.get("language_code"),
            is_bot=user.get("is_bot"),
        )
        # 3) Garantiza sesión
        ensure_session(telegram_user_id, telegram_chat_id)

        # 4) Log inbound (necesario para tool_calls.message_id)
        inbound_message_id = log_message(
            telegram_user_id=telegram_user_id,
            telegram_chat_id=telegram_chat_id,
            direction="in",
            telegram_update_id=update_id,
            telegram_message_id=telegram_message_id,
            role="user",
            text_content=text_msg,
        )

        # 5) Obtener/crear conversation_id de OpenAI (persistente por sesión)
        conv_id, _last_resp_id = get_session(telegram_user_id, telegram_chat_id)
        if not conv_id:
            conversation = client.conversations.create(
                metadata={
                    "telegram_user_id": str(telegram_user_id),
                    "telegram_chat_id": str(telegram_chat_id),
                }
            )
            conv_id = conversation.id
            set_openai_conversation_id(telegram_user_id, telegram_chat_id, conv_id)
        
        # 5.1) Comando provisioning: /coach activate <TOKEN>
        m = re.match(r"^\s*/coach\s+activate\s+(\S+)\s*$", text_msg, flags=re.IGNORECASE)
        if not m:
            m = re.match(r"^\s*/activar_coach\s+(\S+)\s*$", text_msg, flags=re.IGNORECASE)
        if m:
            token = m.group(1).strip()
            try:
                coach_id = consume_coach_invite(token=token, telegram_user_id=telegram_user_id)
                set_active_coach_id(telegram_user_id, telegram_chat_id, coach_id)
                reply = f"Activación completada. Tu coach_id es {coach_id}. Ya puedes gestionar servicios, disponibilidad y reservas desde aquí."
            except ValueError as ve:
                code = str(ve)
                if code == "invalid_token":
                    reply = "Token inválido. Revisa el código de invitación."
                elif code == "already_used":
                    reply = "Este token ya fue utilizado."
                elif code == "expired":
                    reply = "Este token ha caducado. Pide una invitación nueva."
                elif code == "user_blocked":
                    reply = "Tu usuario está bloqueado. Contacta con el administrador."
                else:
                    reply = f"No se pudo activar el coach ({code})."
            except Exception as e:
                reply = f"Error activando coach: {e}"

            await telegram_send_message(telegram_chat_id, reply)
            log_message(
                telegram_user_id=telegram_user_id,
                telegram_chat_id=telegram_chat_id,
                direction="out",
                telegram_update_id=update_id,
                telegram_message_id=None,
                role="assistant",
                text_content=reply,
            )
            return

        # 6) Determinar rol/ids (si no existe app_user, lo creamos como client por defecto)
        full_name = " ".join([p for p in [user.get("first_name"), user.get("last_name")] if p]).strip() or None
        au = get_app_user_by_telegram(telegram_user_id)
        if not au:
            app_user_id, client_id = ensure_client_user(telegram_user_id, full_name)
            user_role = "client"
            coach_id = None
        else:
            app_user_id = int(au["id"])
            user_role = str(au["role"])
            client_id = get_client_id_by_user_id(app_user_id) if user_role == "client" else None
            coach_id = get_coach_id_by_user_id(app_user_id) if user_role in ("coach", "admin") else None

        # Active coach en sesión
        active_coach_id = get_active_coach_id(telegram_user_id, telegram_chat_id)
        if user_role in ("coach", "admin") and coach_id:
            if active_coach_id != coach_id:
                set_active_coach_id(telegram_user_id, telegram_chat_id, coach_id)
                active_coach_id = coach_id
        else:
            if active_coach_id is None:
                only_coach = get_single_coach_id_if_unique()
                if only_coach is not None:
                    set_active_coach_id(telegram_user_id, telegram_chat_id, only_coach)
                    active_coach_id = only_coach

        # Variables para Prompt reusable (Dashboard) o fallback
        prompt_variables: Dict[str, str] = {
            "telegram_user_id": str(telegram_user_id),
            "telegram_chat_id": str(telegram_chat_id),
            "user_role": user_role,
            "app_user_id": str(app_user_id),
            "client_id": str(client_id or ""),
            "coach_id": str(coach_id or ""),
            "active_coach_id": str(active_coach_id or ""),
            "today_local": _today_local_iso(DEFAULT_COACH_TIMEZONE),
            "timezone": DEFAULT_COACH_TIMEZONE,
            "coaches_count": str(count_coaches()),
        }

        debug_tool = None
        if text_msg.strip() == "/debug_ping":
            debug_tool = "db_ping"
        elif text_msg.strip() == "/debug_coaches":
            debug_tool = "list_coaches"

        # Ejecutar assistant (sync) en thread para no bloquear el event loop
        import asyncio
        final_text, final_resp_id, tool_execs, final_resp_json = await asyncio.to_thread(
            run_agenda_assistant_in_conversation, conv_id, text_msg, user_role, prompt_variables, debug_tool
        )

        # Guardar last_response_id (útil para depuración / futuras extensiones)
        if final_resp_id:
            set_openai_last_response_id(telegram_user_id, telegram_chat_id, final_resp_id)

        # Persistir tool calls del turno (MVP: guardamos lo mínimo)
        for t in tool_execs:
            log_tool_call(
                message_id=inbound_message_id,
                tool_name=t.get("name", ""),
                openai_call_id=t.get("call_id", ""),
                arguments_json=t.get("arguments", ""),
                output_text=t.get("output", ""),
            )

        # Enviar respuesta a Telegram
        await telegram_send_message(telegram_chat_id, final_text)

        # Log salida
        log_message(
            telegram_user_id=telegram_user_id,
            telegram_chat_id=telegram_chat_id,
            direction="out",
            telegram_update_id=update_id,
            telegram_message_id=None,
            role="assistant",
            text_content=final_text,
            openai_response_id=final_resp_id,
            openai_output_json=final_resp_json,
        )

    except Exception as e:
        import traceback
        print("ERROR process_update:", repr(e))
        traceback.print_exc()
        return
