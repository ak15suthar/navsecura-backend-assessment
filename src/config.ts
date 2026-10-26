import 'dotenv/config';

function positiveInt(name: string, fallback: number): number {
  const parsed = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(parsed) || parsed < 1) throw new Error(`${name} must be a positive integer`);
  return parsed;
}

export const config = {
  port: positiveInt('PORT', 3000),
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://navsecura:navsecura@localhost:5432/navsecura',
  workerConcurrency: positiveInt('WORKER_CONCURRENCY', 4),
  pollIntervalMs: positiveInt('POLL_INTERVAL_MS', 500),
  jobLeaseSeconds: positiveInt('JOB_LEASE_SECONDS', 30),
  maxAttempts: positiveInt('MAX_ATTEMPTS', 8),
  correlationWindowSeconds: positiveInt('CORRELATION_WINDOW_SECONDS', 300)
};
