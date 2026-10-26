import fs from 'node:fs/promises';
import path from 'node:path';
import { pool } from './pool';

async function main() {
  const directory = path.join(__dirname, 'migrations');
  const files = (await fs.readdir(directory)).filter((name) => name.endsWith('.sql')).sort();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`);
    for (const file of files) {
      const existing = await client.query('SELECT 1 FROM schema_migrations WHERE name = $1', [file]);
      if (existing.rowCount) continue;
      const sql = await fs.readFile(path.join(directory, file), 'utf8');
      await client.query(sql);
      await client.query('INSERT INTO schema_migrations(name) VALUES ($1)', [file]);
      console.log(`Applied migration ${file}`);
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
