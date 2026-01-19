-- 002_coach_invites.sql
CREATE TABLE IF NOT EXISTS coach_invites (
  id BIGINT NOT NULL AUTO_INCREMENT,

  -- token en hash (sha256 hex) para no guardar el token en claro
  token_hash CHAR(64) NOT NULL,

  -- metadata
  proposed_full_name VARCHAR(255) NULL,
  proposed_timezone VARCHAR(64) NULL,
  proposed_default_lesson_minutes INT NULL,
  note VARCHAR(255) NULL,

  expires_at DATETIME(6) NULL,
  used_at DATETIME(6) NULL,
  used_by_telegram_user_id BIGINT NULL,

  created_at DATETIME(6) NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uq_coach_invites_token_hash (token_hash),
  INDEX idx_coach_invites_expires_used (expires_at, used_at),

  CONSTRAINT fk_coach_invites_used_by_telegram
    FOREIGN KEY (used_by_telegram_user_id) REFERENCES telegram_users(telegram_user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
