import { Pool } from 'pg';
import { config } from '../config';

export const pool = new Pool({ connectionString: config.databaseUrl, max: 20, idleTimeoutMillis: 30_000 });
pool.on('error', (error) => console.error('Unexpected idle PostgreSQL client error', error));
