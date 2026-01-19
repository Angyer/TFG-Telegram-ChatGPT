-- 001_agenda_schema.sql
-- Dominio “agenda pádel” + ajuste mínimo en sessions

-- 1) Usuario de negocio (enlazado con Telegram)
CREATE TABLE IF NOT EXISTS app_users (
  id BIGINT NOT NULL AUTO_INCREMENT,
  telegram_user_id BIGINT NOT NULL,
  role ENUM('coach','client','admin') NOT NULL,
  full_name VARCHAR(255) NULL,
  phone VARCHAR(32) NULL,
  email VARCHAR(255) NULL,
  status ENUM('active','blocked') NOT NULL DEFAULT 'active',
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_app_users_telegram (telegram_user_id),
  CONSTRAINT fk_app_users_telegram
    FOREIGN KEY (telegram_user_id) REFERENCES telegram_users(telegram_user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2) Coaches
CREATE TABLE IF NOT EXISTS coaches (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  timezone VARCHAR(64) NOT NULL DEFAULT 'Europe/Madrid',
  default_lesson_minutes INT NOT NULL DEFAULT 60,
  notes LONGTEXT NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_coaches_user (user_id),
  CONSTRAINT fk_coaches_user
    FOREIGN KEY (user_id) REFERENCES app_users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3) Clients
CREATE TABLE IF NOT EXISTS clients (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  level ENUM('beginner','intermediate','advanced') NULL,
  notes LONGTEXT NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_clients_user (user_id),
  CONSTRAINT fk_clients_user
    FOREIGN KEY (user_id) REFERENCES app_users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4) Servicios / tipos de clase
CREATE TABLE IF NOT EXISTS services (
  id BIGINT NOT NULL AUTO_INCREMENT,
  name VARCHAR(128) NOT NULL,
  duration_minutes INT NOT NULL,
  price_cents INT NULL,
  currency CHAR(3) NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_services_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 5) Disponibilidad recurrente (reglas)
CREATE TABLE IF NOT EXISTS availability_rules (
  id BIGINT NOT NULL AUTO_INCREMENT,
  coach_id BIGINT NOT NULL,
  weekday TINYINT NOT NULL,         -- 1=Lun ... 7=Dom (define tu convención y mantenla)
  start_time TIME NOT NULL,
  end_time TIME NOT NULL,
  slot_minutes INT NOT NULL DEFAULT 60,
  valid_from DATE NULL,
  valid_to DATE NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_rules_coach_weekday (coach_id, weekday),
  CONSTRAINT fk_rules_coach
    FOREIGN KEY (coach_id) REFERENCES coaches(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 6) Excepciones (bloqueos/huecos extra)
CREATE TABLE IF NOT EXISTS availability_exceptions (
  id BIGINT NOT NULL AUTO_INCREMENT,
  coach_id BIGINT NOT NULL,
  type ENUM('blocked','extra') NOT NULL,
  start_at DATETIME(6) NOT NULL,
  end_at DATETIME(6) NOT NULL,
  reason VARCHAR(255) NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_ex_coach_start (coach_id, start_at),
  CONSTRAINT fk_ex_coach
    FOREIGN KEY (coach_id) REFERENCES coaches(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 7) Reservas / clases
CREATE TABLE IF NOT EXISTS bookings (
  id BIGINT NOT NULL AUTO_INCREMENT,
  coach_id BIGINT NOT NULL,
  client_id BIGINT NOT NULL,
  service_id BIGINT NULL,
  start_at DATETIME(6) NOT NULL,  -- Recomendación: guardar en UTC
  end_at DATETIME(6) NOT NULL,
  status ENUM('tentative','confirmed','cancelled','completed','no_show') NOT NULL DEFAULT 'confirmed',
  created_by_user_id BIGINT NULL,
  cancelled_by_user_id BIGINT NULL,
  cancelled_at DATETIME(6) NULL,
  cancel_reason VARCHAR(255) NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_bookings_coach_time (coach_id, start_at),
  INDEX idx_bookings_client_time (client_id, start_at),
  CONSTRAINT fk_bookings_coach FOREIGN KEY (coach_id) REFERENCES coaches(id),
  CONSTRAINT fk_bookings_client FOREIGN KEY (client_id) REFERENCES clients(id),
  CONSTRAINT fk_bookings_service FOREIGN KEY (service_id) REFERENCES services(id),
  CONSTRAINT fk_bookings_created_by FOREIGN KEY (created_by_user_id) REFERENCES app_users(id),
  CONSTRAINT fk_bookings_cancelled_by FOREIGN KEY (cancelled_by_user_id) REFERENCES app_users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 8) Ajuste mínimo: sesión -> coach activo
-- (MariaDB no soporta IF NOT EXISTS en ADD COLUMN en todas las versiones; si falla, lo aplicas manualmente una vez)
ALTER TABLE sessions
  ADD COLUMN active_coach_id BIGINT NULL;

ALTER TABLE sessions
  ADD CONSTRAINT fk_sessions_active_coach
    FOREIGN KEY (active_coach_id) REFERENCES coaches(id);

CREATE INDEX idx_sessions_active_coach ON sessions(active_coach_id);
