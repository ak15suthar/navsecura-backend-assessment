# NavSecura Backend Engineering Exercise

A small, runnable TypeScript backend demonstrating durable event ingestion, idempotency, concurrent workers, crash recovery, and simple operational-case correlation.

## Architecture

```mermaid
flowchart LR
  S[Camera / sensor sources] -->|POST /v1/events| API[Node.js + Express API]
  OP[Operator application] -->|Read cases / metrics| API
  API -->|Transaction: insert once + record delivery attempt| DB[(PostgreSQL)]
  DB --> EV[(security_events: source of truth + durable work queue)]
  DB --> CASE[(operational_cases + case_events)]
  DB --> MET[(ingestion_attempts + processing_failures)]
  W1[Worker slot 1] -->|SKIP LOCKED claim| DB
  W2[Worker slot 2..N] -->|SKIP LOCKED claim| DB
  W1 -->|Atomic correlation + event completion transaction| DB
  W2 -->|Atomic correlation + event completion transaction| DB
```

### Why this architecture

- **PostgreSQL is the source of truth.** The original event, processing status, case links, and case state are persisted in the database. The queue is not an in-memory list.
- **The API acknowledges durable acceptance.** It returns `202` after the event and delivery-attempt record commit. If the process crashes before commit, the client can retry. If the response is lost after commit, the retry is recognized as a duplicate.
- **Idempotency is enforced by the database.** `UNIQUE (customer_id, event_id)` protects against simultaneous duplicate requests across API instances. A repeated key with a different event identity returns `409` rather than silently accepting a conflicting event.
- **Workers claim with `FOR UPDATE SKIP LOCKED`.** Concurrent workers claim different queue rows without holding a global queue lock. A claim has a lease; expired `processing` rows become eligible for retry after a worker crash.
- **Case updates and completion commit together.** The case update, event-to-case link, and transition to `processed` happen in one PostgreSQL transaction. A crash before commit rolls them all back; a crash after commit leaves all of them committed. `case_events.event_pk UNIQUE` is an additional guard against duplicate linkage.
- **Per-site advisory locks serialize correlation.** A transaction-scoped advisory lock for `(customer_id, site_id)` prevents two workers from both observing “no active case” and creating separate cases for near-simultaneous events at the same site. This deliberately favors correctness over maximum per-site parallelism.
- **Retries are bounded and observable.** Failures are recorded in `processing_failures`, retried with exponential backoff, and moved to `dead_letter` after `MAX_ATTEMPTS`. A production system should alert on dead-letter growth and provide a controlled replay operation.

## Event flow

1. A source submits `POST /v1/events` with an event ID, customer/site/source, type, timestamp, and optional payload.
2. The API validates the shape and event type. In a database transaction, it inserts the event using `ON CONFLICT DO NOTHING` and records the delivery attempt. Repeated matching events return `200` with `duplicate: true`; new events return `202`.
3. A worker claims a pending event (or a processing event whose lease expired), increments the attempt count, and sets `locked_by` / `locked_until`.
4. In a separate transaction, the worker locks the event, acquires a site-scoped advisory lock, and finds an open/acknowledged case for the same customer/site within the configured correlation window. It updates that case or creates one, links the event, recomputes the linked event count, and marks the event processed.
5. The operator UI reads `/v1/cases`; `/v1/stats` exposes received delivery attempts, stored unique events, duplicates, processed, waiting, processing, and dead-letter counts.

## Correlation rule and limitations

An event joins the most recently updated open/acknowledged case for the same customer and site when its observed timestamp falls within the configured window around the case's observed time range. Otherwise a new case is created. `PANIC_REQUEST` and `SMOKE_DETECTED` replace the case summary with a higher priority message. This is a deterministic baseline, not an inference engine: it can merge unrelated events at a busy site or split one activity whose events fall outside the window. A production rule would likely consider eventtype compatibility, camera zones, source relationships, severity, and operator feedback.

## Requirements

- Node.js 20+
- Docker + Docker Compose

## Run locally

```bash
cp .env.example .env
docker compose up -d postgres
npm install
npm run migrate
npm run dev
```

In another terminal, start the worker:

```bash
npm run worker:dev
```

The API terminal prints a line after it saves each event. The worker terminal prints a line when it claims the event and another when it creates or updates a case. For the example events, expect output similar to:

```text
2026-10-26T06:30:01.000Z [api] Event accepted: 2026-10-26T06:30:01.000Z camera-a PERSON_DETECTED (eventId=EV-10001, site=site-1)
2026-10-26T06:30:01.010Z [worker-...] Processing 2026-10-26T06:30:01.000Z camera-a PERSON_DETECTED (eventId=EV-10001, site=site-1, attempt=1)
2026-10-26T06:30:01.050Z [worker-...] Case created: ... <- camera-a PERSON_DETECTED (eventId=EV-10001)
```

If you only see the `[api]` line, the API received the event but the worker is not processing it; make sure `npm run worker:dev` is running and connected to the same database. If processing fails, the worker logs the error and whether it scheduled a retry or moved the event to the dead-letter state. Restart both `npm run dev` and `npm run worker:dev` after pulling or making code changes, and keep both terminals open while submitting events.

The API listens on `http://localhost:3000` by default. To use a compiled build:

```bash
npm run build
npm start
# in a second terminal
npm run worker
```

## API examples

### Ingest an event

```bash
curl -i http://localhost:3000/v1/events \
  -H 'content-type: application/json' \
  -d '{
    "eventId": "EV-10001",
    "customerId": "customer-1",
    "siteId": "site-1",
    "sourceId": "camera-a",
    "eventType": "PERSON_DETECTED",
    "observedAt": "2026-10-26T10:00:01.000Z",
    "payload": {"zone": "loading-bay"}
  }'
```

Repeat the same request to see `duplicate: true`. Reusing `EV-10001` with a different `sourceId`, `eventType`, `siteId`, or `observedAt` returns `409`.

### Read processing statistics and cases

```bash
curl http://localhost:3000/v1/stats
curl 'http://localhost:3000/v1/cases?customerId=customer-1'
curl http://localhost:3000/health
```

## Tests

Unit tests:

```bash
npm test
```

Integration tests use a real PostgreSQL database and are skipped if `TEST_DATABASE_URL` is not set. Use a separate test database, apply migrations to it, then run:

```bash
export TEST_DATABASE_URL=postgres://navsecura:navsecura@localhost:5432/navsecura_test
# Create navsecura_test first, then apply migrations using the same URL:
DATABASE_URL="$TEST_DATABASE_URL" npm run migrate
npm run test:integration
```

The integration suite covers duplicate ingestion, conflicting id reuse, concurrent worker processing/correlation, and atomic case-link/completion state.

## Assessment questions

### Duplicate event / client retry after API crash

The unique database constraint decides the winner even when requests hit different API instances simultaneously. If the first transaction committed but the HTTP response was lost, the retry finds the stored row, records a duplicate delivery attempt, and returns success without enqueuing another event. If the first transaction did not commit, the retry inserts the event normally. If an event ID is reused for a different identity, the API returns `409`.

### Thousands of events arrive together

The API does minimal synchronous work and persists events before acknowledging. PostgreSQL absorbs a durable backlog, while worker concurrency controls processing pressure. `SKIP LOCKED` lets worker slots claim independent rows. In this exercise, the database is also the queue, which is simple to run and audit. At higher sustained throughput, I would introduce a broker (e.g. Kafka or RabbitMQ) after committing an outbox record transactionally, retaining PostgreSQL as the authoritative store. I would load-test ingestion, indexes, connection pool limits, and queue age rather than claim an unmeasured throughput number.

### Concurrent workers / crash after case update

Only one worker can hold the event row lock in the processing transaction, and the site advisory lock serializes case selection/creation for that site. Case mutations, event linkage, and `status='processed'` commit atomically. A crash before commit rolls back the case update and link; the event remains `processing` until its lease expires, then is retried. A crash after commit leaves the event `processed`, so it is not claimed again. This provides at least once attempts with idempotent effects, not magical exactly once execution across arbitrary external side effects.

### Assumptions

- `(customer_id, event_id)` identifies one logical event.
- Event timestamps are ISO 8601 with an explicit timezone.
- Events at the same customer/site within a five-minute window may belong to one active case.
- Cases are considered active while `open` or `acknowledged`.
- This is a single region exercise and the database is available for ingestion; the API returns an error rather than acknowledging unpersisted events.

### What can still go wrong?

- PostgreSQL outage blocks acceptance; this implementation does not buffer events in a separate durable edge gateway.
- A long-running job can outlive its lease; row locks/transactional checks protect state, but lease renewal would be needed for expensive processing.
- A poisoned event can exhaust retries and require operator inspection/replay.
- The time window correlation rule can create false merges/splits.
- Tenant authorization, source authentication, rate limits, retention policy, payload size governance, and encryption/key management are not implemented here.
- If future processing sends notifications or calls external services, use a transactional outbox and downstream idempotency keys.

### If the system became 100x larger

First measure which limit is real. Likely steps: partition high-volume event tables by time or tenant, tune indexes and connection pools, separate ingestion and worker scaling, add a durable broker/outbox for buffering and consumer groups, and move correlation to partitioned site/tenant streams. Keep tenant boundaries explicit and preserve event idempotency. I would not add microservices or a broker before the workload and operational needs justify them.

### Production monitoring

- API request rate, latency, validation failures, `409` conflicts, `5xx` rate.
- Ingestion attempts vs unique events, duplicate ratio, database insert latency.
- Pending queue depth and oldest pending age, processing duration, retry rate, lease expirations, dead-letter count.
- Worker utilization, DB pool saturation, lock waits, transaction retries, disk/IO, replication lag, and database availability.
- Cases per site, events linked per case, and alert-to-operator acknowledgement time.

### What I would deliberately not build yet

No machine learning correlation, complex rule engine, microservice fleet, event streaming platform, operator websocket push, or elaborate deployment system. The first goal is a small, explainable, testable pipeline with correct persistence and recovery semantics.

## Hardest part

The hardest part is not inserting an event; it is making retries and concurrent processing safe across failure boundaries. I used database constraints for ingestion idempotency, row level queue claiming, a per site advisory lock for the “no existing case” race, and one transaction for case mutation plus event completion. Those mechanisms make the correctness argument concrete and testable.
