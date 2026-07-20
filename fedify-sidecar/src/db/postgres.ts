import pg from 'pg';
import { postgresConfig } from '../config/v6-config.js';
import { logger } from '../utils/logger.js';

const { Pool } = pg;

export const pool = new Pool({
  connectionString: postgresConfig.url,
});

pool.on('error', (err) => {
  logger.error('PostgreSQL client pool error', { error: err.message });
});

/**
 * Bootstrap database schema: create local_cache_posts table if it doesn't exist.
 */
export async function bootstrapDb(): Promise<void> {
  const query = `
    CREATE TABLE IF NOT EXISTS local_cache_posts (
      id UUID PRIMARY KEY,
      actor_uri TEXT NOT NULL,
      content TEXT NOT NULL,
      post_type TEXT NOT NULL DEFAULT 'note',
      name TEXT,                     -- Title for articles
      summary TEXT,                  -- Summary for articles
      hashtags TEXT[] DEFAULT '{}',
      attachments JSONB DEFAULT '[]',
      synced_to_fuseki BOOLEAN NOT NULL DEFAULT FALSE,
      sync_attempts INTEGER DEFAULT 0,
      last_attempt_at TIMESTAMP,
      error_message TEXT,
      target_protocol VARCHAR(32) DEFAULT 'all',
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );

    ALTER TABLE local_cache_posts ADD COLUMN IF NOT EXISTS target_protocol VARCHAR(32) DEFAULT 'all';

    CREATE INDEX IF NOT EXISTS idx_unsynced_posts 
    ON local_cache_posts(synced_to_fuseki) 
    WHERE synced_to_fuseki = FALSE;
  `;

  try {
    const client = await pool.connect();
    try {
      await client.query(query);
      logger.info('[PostgreSQL] Database bootstrap complete (table local_cache_posts verified)');
    } finally {
      client.release();
    }
  } catch (err: any) {
    logger.error('[PostgreSQL] Database bootstrap failed', { error: err.message });
    throw err;
  }
}
