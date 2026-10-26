import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import request from 'supertest';
import { Pool } from 'pg';
import { createApp } from '../src/app';
import { EventProcessor } from '../src/workers/processor';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeIfDb = databaseUrl ? describe : describe.skip;

describeIfDb('PostgreSQL event pipeline integration', () => {
  const db = new Pool({ connectionString: databaseUrl });
  const app = createApp(db);
  beforeAll(async () => {
    // Run `npm run migrate` against TEST_DATABASE_URL before this test.
    await db.query('TRUNCATE ingestion_attempts, case_events, processing_failures, security_events, operational_cases RESTART IDENTITY CASCADE');
  });
  afterAll(async () => { await db.end(); });

  const makeEvent = (eventId: string, observedAt = '2026-10-26T10:00:00.000Z') => ({
    eventId, customerId: 'customer-test', siteId: 'site-test', sourceId: `camera-${eventId}`,
    eventType: 'PERSON_DETECTED', observedAt, payload: { test: true }
  });

  it('acknowledges the same event twice without storing two rows', async () => {
    const body = makeEvent('duplicate-1');
    const first = await request(app).post('/v1/events').send(body).expect(202);
    const second = await request(app).post('/v1/events').send(body).expect(200);
    expect(first.body.duplicate).toBe(false);
    expect(second.body.duplicate).toBe(true);
    const count = await db.query('SELECT COUNT(*)::int AS count FROM security_events WHERE customer_id = $1 AND event_id = $2', [body.customerId, body.eventId]);
    expect(count.rows[0].count).toBe(1);
  });

  it('rejects reuse of an event id for a different logical event', async () => {
    const body = makeEvent('reused-id');
    await request(app).post('/v1/events').send(body).expect(202);
    await request(app).post('/v1/events').send({ ...body, sourceId: 'different-source' }).expect(409);
  });

  it('correlates events and remains correct when two workers race', async () => {
    const bodies = [makeEvent('race-a'), makeEvent('race-b', '2026-10-26T10:00:02.000Z')];
    for (const body of bodies) await request(app).post('/v1/events').send(body).expect(202);
    const workerA = new EventProcessor(db, 'test-a');
    const workerB = new EventProcessor(db, 'test-b');
    await Promise.all([workerA.runOnce(), workerB.runOnce()]);
    const rows = await db.query(`SELECT COUNT(DISTINCT id)::int AS cases FROM operational_cases WHERE customer_id = 'customer-test' AND site_id = 'site-test'`);
    expect(rows.rows[0].cases).toBe(1);
    const processed = await db.query(`SELECT COUNT(*)::int AS count FROM security_events WHERE event_id IN ('race-a', 'race-b') AND status = 'processed'`);
    expect(processed.rows[0].count).toBe(2);
  });

  it('reclaims work after a crashed worker lease expires', async () => {
    await request(app).post('/v1/events').send(makeEvent('lease-recovery')).expect(202);
    const crashedWorker = new EventProcessor(db, 'worker-that-crashed');
    const firstClaim = await crashedWorker.claimOne();
    expect(firstClaim?.event_id).toBe('lease-recovery');
    await db.query(`UPDATE security_events SET locked_until = NOW() - INTERVAL '1 second' WHERE event_id = 'lease-recovery'`);
    const replacementWorker = new EventProcessor(db, 'replacement-worker');
    const retryClaim = await replacementWorker.claimOne();
    expect(retryClaim?.event_id).toBe('lease-recovery');
    await replacementWorker.process(retryClaim!);
    const completed = await db.query(`SELECT status FROM security_events WHERE event_id = 'lease-recovery'`);
    expect(completed.rows[0].status).toBe('processed');
  });

  it('uses one transaction for case linking and completion', async () => {
    await request(app).post('/v1/events').send(makeEvent('atomic-1')).expect(202);
    const worker = new EventProcessor(db, 'test-atomic');
    await worker.runOnce();
    const row = await db.query(`SELECT e.status, e.case_id, ce.event_pk FROM security_events e
      LEFT JOIN case_events ce ON ce.event_pk = e.id WHERE e.event_id = 'atomic-1'`);
    expect(row.rows[0].status).toBe('processed');
    expect(row.rows[0].case_id).toBeTruthy();
    expect(row.rows[0].event_pk).toBeTruthy();
  });
});
