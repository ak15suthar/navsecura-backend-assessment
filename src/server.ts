import { createApp } from './app';
import { config } from './config';
import { pool } from './db/pool';

const server = createApp(pool).listen(config.port, () => {
  console.log(`NavSecura API listening on port ${config.port}`);
});

async function shutdown(signal: string) {
  console.log(`${signal} received; shutting down API`);
  server.close(async () => {
    await pool.end();
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 10_000).unref();
}
process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));
