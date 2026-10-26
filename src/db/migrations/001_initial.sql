CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS security_events (
  id BIGSERIAL PRIMARY KEY,
  event_id TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  site_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN (
    'PERSON_DETECTED', 'VEHICLE_DETECTED', 'DOOR_OPENED', 'CAMERA_OFFLINE',
    'PANIC_REQUEST', 'SMOKE_DETECTED'
  )),
  observed_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'processed', 'dead_letter')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  locked_until TIMESTAMPTZ,
  locked_by TEXT,
  processed_at TIMESTAMPTZ,
  last_error TEXT,
  case_id UUID,
  CONSTRAINT uq_event_customer_event UNIQUE (customer_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_events_pending
  ON security_events (available_at, received_at, id)
  WHERE status IN ('pending', 'processing');
CREATE INDEX IF NOT EXISTS idx_events_site_observed
  ON security_events (customer_id, site_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_case_id ON security_events (case_id) WHERE case_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS operational_cases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id TEXT NOT NULL,
  site_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'acknowledged', 'resolved')),
  summary TEXT NOT NULL,
  first_observed_at TIMESTAMPTZ NOT NULL,
  last_observed_at TIMESTAMPTZ NOT NULL,
  event_count INTEGER NOT NULL DEFAULT 0 CHECK (event_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cases_open_site_recent
  ON operational_cases (customer_id, site_id, last_observed_at DESC)
  WHERE status IN ('open', 'acknowledged');

CREATE TABLE IF NOT EXISTS case_events (
  case_id UUID NOT NULL REFERENCES operational_cases(id) ON DELETE CASCADE,
  event_pk BIGINT NOT NULL UNIQUE REFERENCES security_events(id) ON DELETE RESTRICT,
  linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (case_id, event_pk)
);

CREATE TABLE IF NOT EXISTS processing_failures (
  id BIGSERIAL PRIMARY KEY,
  event_pk BIGINT NOT NULL REFERENCES security_events(id) ON DELETE CASCADE,
  attempt INTEGER NOT NULL,
  error TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per HTTP delivery attempt, including retries, so duplicate traffic is observable.
CREATE TABLE IF NOT EXISTS ingestion_attempts (
  id BIGSERIAL PRIMARY KEY,
  customer_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  duplicate BOOLEAN NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ingestion_attempts_received_at ON ingestion_attempts(received_at DESC);
