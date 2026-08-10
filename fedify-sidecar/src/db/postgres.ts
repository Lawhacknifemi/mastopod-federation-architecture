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
      in_reply_to TEXT,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );

    ALTER TABLE local_cache_posts ADD COLUMN IF NOT EXISTS target_protocol VARCHAR(32) DEFAULT 'all';
    ALTER TABLE local_cache_posts ADD COLUMN IF NOT EXISTS in_reply_to TEXT;

    CREATE INDEX IF NOT EXISTS idx_unsynced_posts
    ON local_cache_posts(synced_to_fuseki)
    WHERE synced_to_fuseki = FALSE;

    -- Durable like/repost tracking. Previously these lived only in
    -- in-memory Maps in public-routes.ts (userLikesMap, postLikeCountMap,
    -- userRepostsMap, postRepostCountMap) — real counts and viewer-liked/
    -- reposted state, with no fallback of any kind, wiped by every
    -- process restart. Nothing in this codebase ever actually populates
    -- the OpenSearch-indexed engagement.likeCount/repostCount fields
    -- those maps were nominally layered on top of (see
    -- PublicContentIndexWriter.ts's likeDelta/repostDelta handling —
    -- nothing anywhere produces an event with those fields), so the
    -- in-memory maps were the entire real signal, not just a cache of one.
    -- subject_raw_id is the same post identified across its different URI
    -- representations (objectUri vs at:// URI vs numeric feed id all refer
    -- to the same logical post elsewhere in this file) reduced to one
    -- comparable key — see extractRawId(). Matching on it, not the exact
    -- subject_uri string, is what the old in-memory Maps did too
    -- (a linear fallback scan comparing extractRawId() of every stored
    -- key); this preserves that same tolerance in the durable version
    -- instead of quietly becoming stricter.
    CREATE TABLE IF NOT EXISTS post_engagements (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      actor_uri TEXT NOT NULL,
      subject_uri TEXT NOT NULL,
      subject_raw_id TEXT NOT NULL,
      kind TEXT NOT NULL CHECK (kind IN ('like', 'repost')),
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE (actor_uri, subject_raw_id, kind)
    );

    CREATE INDEX IF NOT EXISTS idx_post_engagements_subject_raw
    ON post_engagements(subject_raw_id, kind);

    CREATE INDEX IF NOT EXISTS idx_post_engagements_actor
    ON post_engagements(actor_uri, kind);
  `;

  try {
    const client = await pool.connect();
    try {
      await client.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`).catch(() => {
        // pgcrypto (for gen_random_uuid()) may already be enabled, or the
        // role may lack CREATE EXTENSION rights on a managed Postgres —
        // either way, fall through and let the CREATE TABLE itself fail
        // loudly below if gen_random_uuid() genuinely isn't available.
      });
      await client.query(query);
      logger.info('[PostgreSQL] Database bootstrap complete (local_cache_posts, post_engagements verified)');
    } finally {
      client.release();
    }
  } catch (err: any) {
    logger.error('[PostgreSQL] Database bootstrap failed', { error: err.message });
    throw err;
  }
}
