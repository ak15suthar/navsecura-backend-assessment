import { Pool, PoolClient } from 'pg';
import { IncomingEvent } from '../domain';

export class EventService {
  constructor(private readonly db: Pool) {}

  async ingest(input: IncomingEvent) {
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');
      const result = await client.query(
        `INSERT INTO security_events
           (event_id, customer_id, site_id, source_id, event_type, observed_at, payload)
         VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
         ON CONFLICT (customer_id, event_id) DO NOTHING
         RETURNING id, event_id, customer_id, site_id, source_id, event_type, observed_at, received_at, status`,
        [input.eventId, input.customerId, input.siteId, input.sourceId, input.eventType,
         input.observedAt, JSON.stringify(input.payload ?? {})]
      );
      if (result.rowCount === 1) {
        await client.query('INSERT INTO ingestion_attempts(customer_id, event_id, duplicate) VALUES ($1, $2, FALSE)',
          [input.customerId, input.eventId]);
        await client.query('COMMIT');
        return { accepted: true, duplicate: false, event: result.rows[0] };
      }

      // The unique constraint serializes concurrent submissions of the same logical event.
      const existing = await client.query(
        `SELECT id, event_id, customer_id, site_id, source_id, event_type, observed_at, received_at, status
         FROM security_events WHERE customer_id = $1 AND event_id = $2 FOR SHARE`,
        [input.customerId, input.eventId]
      );
      const row = existing.rows[0];
      if (!row) throw new Error('Idempotency conflict could not be resolved; retry the request');
      const sameIdentity = row.site_id === input.siteId && row.source_id === input.sourceId &&
        row.event_type === input.eventType && new Date(row.observed_at).getTime() === new Date(input.observedAt).getTime();
      if (!sameIdentity) {
        const error = new Error('eventId was already used for a different event') as Error & { statusCode?: number };
        error.statusCode = 409;
        throw error;
      }
      await client.query('INSERT INTO ingestion_attempts(customer_id, event_id, duplicate) VALUES ($1, $2, TRUE)',
        [input.customerId, input.eventId]);
      await client.query('COMMIT');
      return { accepted: true, duplicate: true, event: row };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async getStats() {
    const result = await this.db.query(`
      SELECT (SELECT COUNT(*)::int FROM ingestion_attempts) AS received,
             COUNT(*)::int AS stored,
             (SELECT COUNT(*)::int FROM ingestion_attempts WHERE duplicate = TRUE) AS duplicates,
             COUNT(*) FILTER (WHERE status = 'processed')::int AS processed,
             COUNT(*) FILTER (WHERE status = 'pending')::int AS waiting,
             COUNT(*) FILTER (WHERE status = 'processing')::int AS processing,
             COUNT(*) FILTER (WHERE status = 'dead_letter')::int AS dead_letter
      FROM security_events`);
    return result.rows[0];
  }

  async listCases(customerId?: string) {
    const result = await this.db.query(
      `SELECT c.*, COUNT(ce.event_pk)::int AS linked_events
       FROM operational_cases c LEFT JOIN case_events ce ON ce.case_id = c.id
       WHERE ($1::text IS NULL OR c.customer_id = $1)
       GROUP BY c.id ORDER BY c.updated_at DESC LIMIT 100`, [customerId ?? null]
    );
    return result.rows;
  }
}

export async function withTransaction<T>(client: PoolClient, fn: () => Promise<T>): Promise<T> {
  await client.query('BEGIN');
  try {
    const value = await fn();
    await client.query('COMMIT');
    return value;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  }
}
