import { randomUUID } from 'node:crypto';
import { config } from './config';
import { pool } from './db/pool';
import { EventProcessor } from './workers/processor';

const workerId = `worker-${randomUUID()}`;
const processor = new EventProcessor(pool, workerId);
let stopping = false;

async function runSlot(slot: number) {
  while (!stopping) {
    try {
      const didWork = await processor.runOnce();
      if (!didWork) await new Promise((resolve) => setTimeout(resolve, config.pollIntervalMs));
    } catch (error) {
      console.error(`Worker slot ${slot} encountered an unexpected error`, error);
      await new Promise((resolve) => setTimeout(resolve, config.pollIntervalMs));
    }
  }
}

async function main() {
  console.log(`Starting ${config.workerConcurrency} worker slots (${workerId})`);
  await Promise.all(Array.from({ length: config.workerConcurrency }, (_, index) => runSlot(index + 1)));
  await pool.end();
}

function stop(signal: string) {
  console.log(`${signal} received; stopping worker after current iteration`);
  stopping = true;
}
process.on('SIGTERM', () => stop('SIGTERM'));
process.on('SIGINT', () => stop('SIGINT'));
void main().catch((error) => { console.error(error); process.exitCode = 1; });
