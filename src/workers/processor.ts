import { Pool, PoolClient } from 'pg';
import { config } from '../config';
import { caseSummary, EventType } from '../domain';
import { withTransaction } from '../services/eventService';

interface ClaimedEvent {
  id: string;
  event_id: string;
  customer_id: string;
  site_id: string;
  source_id: string;
  event_type: EventType;
  observed_at: Date;
  attempts: number;
}

interface ProcessResult {
  caseId: string;
  action: 'created' | 'updated';
}

export class EventProcessor {
  constructor(private readonly db: Pool, private readonly workerId: string) {}

  private log(message: string): void {
    console.log(`${new Date().toISOString()} [${this.workerId}] ${message}`);
  }

  /** Claim one item atomically. SKIP LOCKED lets independent workers claim different rows. */
  async claimOne(): Promise<ClaimedEvent | null> {
    const result = await this.db.query(
      `WITH candidate AS (
         SELECT id FROM security_events
         WHERE (status = 'pending' AND available_at <= NOW())
            OR (status = 'processing' AND locked_until < NOW())
         ORDER BY available_at, received_at, id
         FOR UPDATE SKIP LOCKED
         LIMIT 1
       )
       UPDATE security_events e
       SET status = 'processing', attempts = e.attempts + 1,
           locked_by = $1, locked_until = NOW() + ($2 * INTERVAL '1 second')
       FROM candidate c WHERE e.id = c.id
       RETURNING e.id, e.event_id, e.customer_id, e.site_id, e.source_id,
                 e.event_type, e.observed_at, e.attempts`,
      [this.workerId, config.jobLeaseSeconds]
    );
    return result.rows[0] ?? null;
  }

  /**
   * All authoritative case mutations and marking the event processed happen in one DB transaction.
   * A crash before commit rolls everything back; a crash after commit leaves the event completed.
   */
  async process(event: ClaimedEvent): Promise<ProcessResult | null> {
    const client = await this.db.connect();
    try {
      return await withTransaction(client, async () => {
        const locked = await client.query(
          `SELECT id, status FROM security_events WHERE id = $1 FOR UPDATE`, [event.id]
        );
        if (!locked.rowCount || locked.rows[0].status === 'processed') return null;

        // Serialize correlation per customer/site, including the no-existing-case path.
        await client.query(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
          [`${event.customer_id}:${event.site_id}`]);

        const match = await client.query(
          `SELECT id FROM operational_cases
           WHERE customer_id = $1 AND site_id = $2
             AND status IN ('open', 'acknowledged')
             AND last_observed_at >= $3::timestamptz - ($4 * INTERVAL '1 second')
             AND first_observed_at <= $3::timestamptz + ($4 * INTERVAL '1 second')
           ORDER BY last_observed_at DESC
           LIMIT 1 FOR UPDATE`,
          [event.customer_id, event.site_id, event.observed_at, config.correlationWindowSeconds]
        );

        let caseId: string;
        let action: ProcessResult['action'];
        if (match.rowCount) {
          caseId = match.rows[0].id;
          action = 'updated';
          await client.query(
            `UPDATE operational_cases
             SET first_observed_at = LEAST(first_observed_at, $2),
                 last_observed_at = GREATEST(last_observed_at, $2),
                 event_count = event_count + 1,
                 summary = CASE WHEN $3 IN ('PANIC_REQUEST', 'SMOKE_DETECTED') THEN $4 ELSE summary END,
                 updated_at = NOW()
             WHERE id = $1`,
            [caseId, event.observed_at, event.event_type, caseSummary(event.event_type)]
          );
        } else {
          const created = await client.query(
            `INSERT INTO operational_cases
               (customer_id, site_id, summary, first_observed_at, last_observed_at, event_count)
             VALUES ($1, $2, $3, $4, $4, 0) RETURNING id`,
            [event.customer_id, event.site_id, caseSummary(event.event_type), event.observed_at]
          );
          caseId = created.rows[0].id;
          action = 'created';
        }

        await client.query(
          `INSERT INTO case_events (case_id, event_pk) VALUES ($1, $2)
           ON CONFLICT (event_pk) DO NOTHING`, [caseId, event.id]
        );
        // Count is derived from link rows to remain correct if this transaction is replayed.
        await client.query(
          `UPDATE operational_cases SET event_count = (
             SELECT COUNT(*)::int FROM case_events WHERE case_id = $1
           ), updated_at = NOW() WHERE id = $1`, [caseId]
        );
        await client.query(
          `UPDATE security_events SET status = 'processed', processed_at = NOW(), case_id = $2,
             locked_by = NULL, locked_until = NULL, last_error = NULL WHERE id = $1`,
          [event.id, caseId]
        );
        return { caseId, action };
      });
    } finally {
      client.release();
    }
  }

  async recordFailure(event: ClaimedEvent, error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : String(error);
    const deadLetter = event.attempts >= config.maxAttempts;
    const delaySeconds = Math.min(300, 2 ** Math.min(event.attempts, 8));
    await this.db.query(
      `WITH failed AS (
         INSERT INTO processing_failures(event_pk, attempt, error) VALUES ($1, $2, $3) RETURNING id
       )
       UPDATE security_events
       SET status = $4,
           available_at = CASE WHEN $4 = 'pending' THEN NOW() + ($5 * INTERVAL '1 second') ELSE available_at END,
           locked_by = NULL, locked_until = NULL, last_error = $3
       WHERE id = $1`,
      [event.id, event.attempts, message, deadLetter ? 'dead_letter' : 'pending', delaySeconds]
    );
    this.log(
      `Event ${event.event_id} failed on attempt ${event.attempts}; ` +
      `${deadLetter ? 'moved to dead letter' : `retry scheduled in ${delaySeconds}s`}: ${message}`
    );
  }

  async runOnce(): Promise<boolean> {
    const event = await this.claimOne();
    if (!event) return false;
    this.log(
      `Processing ${event.observed_at.toISOString()} ${event.source_id} ${event.event_type} ` +
      `(eventId=${event.event_id}, site=${event.site_id}, attempt=${event.attempts})`
    );
    try {
      const result = await this.process(event);
      if (result) {
        this.log(
          `Case ${result.action}: ${result.caseId} <- ${event.source_id} ${event.event_type} ` +
          `(eventId=${event.event_id})`
        );
      } else {
        this.log(`Event ${event.event_id} was already processed; no case changes made`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`${new Date().toISOString()} [${this.workerId}] Event ${event.event_id} processing error: ${message}`);
      await this.recordFailure(event, error);
    }
    return true;
  }
}

export type { ClaimedEvent };
