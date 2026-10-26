import express, { NextFunction, Request, Response } from 'express';
import { z } from 'zod';
import { Pool } from 'pg';
import { EVENT_TYPES } from './domain';
import { EventService } from './services/eventService';

const eventSchema = z.object({
  eventId: z.string().trim().min(1).max(200),
  customerId: z.string().trim().min(1).max(200),
  siteId: z.string().trim().min(1).max(200),
  sourceId: z.string().trim().min(1).max(200),
  eventType: z.enum(EVENT_TYPES),
  observedAt: z.string().datetime({ offset: true }),
  payload: z.record(z.unknown()).optional()
}).strict();

export function createApp(db: Pool) {
  const app = express();
  const events = new EventService(db);
  app.disable('x-powered-by');
  app.use(express.json({ limit: '256kb' }));

  app.get('/health', async (_req, res, next) => {
    try {
      await db.query('SELECT 1');
      res.json({ status: 'ok', database: 'ok' });
    } catch (error) { next(error); }
  });

  app.post('/v1/events', async (req, res, next) => {
    const parsed = eventSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: 'Invalid event', details: parsed.error.flatten() });
      return;
    }
    try {
      const result = await events.ingest(parsed.data);
      console.log(
        `${new Date().toISOString()} [api] Event ${result.duplicate ? 'duplicate' : 'accepted'}: ` +
        `${parsed.data.observedAt} ${parsed.data.sourceId} ${parsed.data.eventType} ` +
        `(eventId=${parsed.data.eventId}, site=${parsed.data.siteId})`
      );
      res.status(result.duplicate ? 200 : 202).json({
        accepted: result.accepted,
        duplicate: result.duplicate,
        event: result.event
      });
    } catch (error) { next(error); }
  });

  app.get('/v1/stats', async (_req, res, next) => {
    try { res.json(await events.getStats()); } catch (error) { next(error); }
  });

  app.get('/v1/cases', async (req, res, next) => {
    const customerId = typeof req.query.customerId === 'string' ? req.query.customerId : undefined;
    try { res.json({ cases: await events.listCases(customerId) }); } catch (error) { next(error); }
  });

  app.use((error: Error & { statusCode?: number }, _req: Request, res: Response, _next: NextFunction) => {
    if (res.headersSent) return;
    if (error.statusCode === 409) {
      res.status(409).json({ error: error.message, code: 'EVENT_ID_REUSED' });
      return;
    }
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  });
  return app;
}
