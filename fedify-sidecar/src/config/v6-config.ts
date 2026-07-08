/**
 * V6 Configuration
 * 
 * Canonical configuration for the V6 architecture.
 * All values are environment-driven for deployment flexibility.
 */

// ============================================================================
// Server Configuration
// ============================================================================

export const serverConfig = {
  port: parseInt(process.env["SIDECAR_PORT"] || '8080', 10),
  host: process.env["SIDECAR_HOST"] || '0.0.0.0',
  baseUrl: process.env["SIDECAR_BASE_URL"] || 'http://localhost:8080',
};

// ============================================================================
// ActivityPods Integration
// ============================================================================

export const activityPodsConfig = {
  url: process.env["ACTIVITYPODS_URL"] || 'http://localhost:3000',
  token: process.env["ACTIVITYPODS_TOKEN"] || '',
  signingApiUrl: process.env["ACTIVITYPODS_SIGNING_API_URL"] || 
    `${process.env["ACTIVITYPODS_URL"] || 'http://localhost:3000'}/api/internal/signatures/batch`,
  inboxReceiverUrl: process.env["ACTIVITYPODS_INBOX_RECEIVER_URL"] ||
    `${process.env["ACTIVITYPODS_URL"] || 'http://localhost:3000'}/api/internal/inbox/receive`,
};

// ============================================================================
// Redis Configuration (Delivery State, Caching, MRF KV)
// ============================================================================

export const redisConfig = {
  url: process.env["REDIS_URL"] || 'redis://localhost:6379',
  maxConcurrentPerDomain: parseInt(process.env["MAX_CONCURRENT_PER_DOMAIN"] || '5', 10),
  idempotencyTtlMs: parseInt(process.env["IDEMPOTENCY_TTL_MS"] || '86400000', 10), // 24h
  rateLimitWindowMs: parseInt(process.env["RATE_LIMIT_WINDOW_MS"] || '3600000', 10), // 1h
  rateLimitMaxPerWindow: parseInt(process.env["RATE_LIMIT_MAX_PER_WINDOW"] || '1000', 10),
  actorCacheTtlMs: parseInt(process.env["ACTOR_CACHE_TTL_MS"] || '3600000', 10), // 1h
};

// ============================================================================
// PostgreSQL Configuration (Local cache database)
// ============================================================================

export const postgresConfig = {
  url: process.env["DATABASE_URL"] || 'postgresql://sidecar_user:sidecar_password@localhost:5432/fedify_sidecar',
};

// ============================================================================
// RedPanda Configuration (Event Logs)
// ============================================================================

export const redpandaConfig = {
  brokers: (process.env["REDPANDA_BROKERS"] || 'localhost:9092').split(','),
  clientId: process.env["REDPANDA_CLIENT_ID"] || 'fedify-sidecar-v6',
  compression: process.env["REDPANDA_COMPRESSION"] || 'zstd',
  
  // V6 Topic Names (Canonical)
  topics: {
    stream1LocalPublic: process.env["STREAM1_TOPIC"] || 'ap.stream1.local-public.v1',
    stream2RemotePublic: process.env["STREAM2_TOPIC"] || 'ap.stream2.remote-public.v1',
    firehose: process.env["FIREHOSE_TOPIC"] || 'ap.firehose.v1',
    outbound: process.env["OUTBOUND_TOPIC"] || 'ap.outbound.v1',
    inbound: process.env["INBOUND_TOPIC"] || 'ap.inbound.v1',
    mrfRejected: process.env["MRF_REJECTED_TOPIC"] || 'ap.mrf.rejected.v1',
    tombstones: process.env["TOMBSTONES_TOPIC"] || 'ap.tombstones.v1',
  },
  
  // Consumer Groups
  consumerGroups: {
    inbound: process.env["INBOUND_CONSUMER_GROUP"] || 'fedify-inbound-v6',
    outbound: process.env["OUTBOUND_CONSUMER_GROUP"] || 'fedify-outbound-v6',
    indexer: process.env["INDEXER_CONSUMER_GROUP"] || 'fedify-indexer-v6',
  },
};

// ============================================================================
// Inbound Worker Configuration
// ============================================================================

export const inboundWorkerConfig = {
  enabled: process.env["ENABLE_INBOUND_WORKER"] !== 'false',
  concurrency: parseInt(process.env["INBOUND_CONCURRENCY"] || '10', 10),
  requestTimeoutMs: parseInt(process.env["REQUEST_TIMEOUT_MS"] || '30000', 10),
  userAgent: process.env["USER_AGENT"] || 'Fedify-Sidecar/v6',
};

// ============================================================================
// Outbound Worker Configuration
// ============================================================================

export const outboundWorkerConfig = {
  enabled: process.env["ENABLE_OUTBOUND_WORKER"] !== 'false',
  concurrency: parseInt(process.env["OUTBOUND_CONCURRENCY"] || '20', 10),
  maxRetries: parseInt(process.env["MAX_RETRIES"] || '10', 10),
  retryDelayMs: parseInt(process.env["RETRY_DELAY_MS"] || '60000', 10),
  requestTimeoutMs: parseInt(process.env["REQUEST_TIMEOUT_MS"] || '30000', 10),
  userAgent: process.env["USER_AGENT"] || 'Fedify-Sidecar/v6',
};

// ============================================================================
// MRF Configuration
// ============================================================================

export const mrfConfig = {
  enabled: process.env["ENABLE_MRF"] !== 'false',
  blockedDomains: (process.env["MRF_BLOCKED_DOMAINS"] || '').split(',').filter(Boolean),
  policies: {
    signatureValidation: process.env["MRF_POLICY_SIGNATURE_VALIDATION"] !== 'false',
    blockedDomain: process.env["MRF_POLICY_BLOCKED_DOMAIN"] !== 'false',
    suspiciousActivity: process.env["MRF_POLICY_SUSPICIOUS_ACTIVITY"] !== 'false',
  },
};

// ============================================================================
// OpenSearch Configuration (Tier 3 - Optional)
// ============================================================================

export const opensearchConfig = {
  enabled: process.env["ENABLE_OPENSEARCH_INDEXER"] !== 'false',
  node: process.env["OPENSEARCH_URL"] || process.env["OPENSEARCH_NODE"] || 'http://localhost:9200',
  indexName: process.env["OPENSEARCH_INDEX_NAME"] || 'ap-activities',
  auth: process.env["OPENSEARCH_USERNAME"]
    ? {
        username: process.env["OPENSEARCH_USERNAME"],
        password: process.env["OPENSEARCH_PASSWORD"] || '',
      }
    : undefined,
  bulk: {
    flushBytes: parseInt(process.env["OPENSEARCH_BULK_FLUSH_BYTES"] || '5000000', 10),
    flushInterval: parseInt(process.env["OPENSEARCH_BULK_FLUSH_INTERVAL"] || '5000', 10),
  },
};

export const searchBackendConfig = {
  backend:
    process.env['SEARCH_BACKEND'] === 'opensearch' ||
    process.env['SEARCH_BACKEND'] === 'qdrant' ||
    process.env['SEARCH_BACKEND'] === 'dual'
      ? process.env['SEARCH_BACKEND']
      : 'dual',
  qdrantUrl: process.env['QDRANT_URL'] || 'http://localhost:6333',
  qdrantCollectionName: process.env['QDRANT_COLLECTION_NAME'] || 'public-content-v1',
  qdrantVectorSize: parseInt(process.env['QDRANT_VECTOR_SIZE'] || '1024', 10),
};

// ============================================================================
// Logging Configuration
// ============================================================================

export const loggingConfig = {
  level: process.env["LOG_LEVEL"] || 'info',
  format: process.env["LOG_FORMAT"] || 'json',
};

// ============================================================================
// Feature Flags
// ============================================================================

export const featureFlags = {
  enableInboundWorker: process.env["ENABLE_INBOUND_WORKER"] !== 'false',
  enableOutboundWorker: process.env["ENABLE_OUTBOUND_WORKER"] !== 'false',
  enableMrf: process.env["ENABLE_MRF"] !== 'false',
  enableOpensearchIndexer: process.env["ENABLE_OPENSEARCH_INDEXER"] !== 'false',
  searchBackend: searchBackendConfig.backend,
  enableWebfinger: process.env["ENABLE_WEBFINGER"] !== 'false',
  enableActorServing: process.env["ENABLE_ACTOR_SERVING"] !== 'false',
};

// ============================================================================
// Validation
// ============================================================================

export function validateConfig(): string[] {
  const errors: string[] = [];

  if (!activityPodsConfig.url) {
    errors.push('ACTIVITYPODS_URL is required');
  }

  if (!activityPodsConfig.token) {
    errors.push('ACTIVITYPODS_TOKEN is required');
  }

  if (!redisConfig.url) {
    errors.push('REDIS_URL is required');
  }

  if (redpandaConfig.brokers.length === 0) {
    errors.push('REDPANDA_BROKERS is required');
  }

  return errors;
}

/**
 * Log configuration on startup
 */
export function logConfiguration(): void {
  console.log('=== V6 Configuration ===');
  console.log(`Server: ${serverConfig.baseUrl}`);
  console.log(`ActivityPods: ${activityPodsConfig.url}`);
  console.log(`Redis: ${redisConfig.url}`);
  console.log(`RedPanda Brokers: ${redpandaConfig.brokers.join(', ')}`);
  console.log(`Topics:`);
  console.log(`  - Stream1: ${redpandaConfig.topics.stream1LocalPublic}`);
  console.log(`  - Stream2: ${redpandaConfig.topics.stream2RemotePublic}`);
  console.log(`  - Firehose: ${redpandaConfig.topics.firehose}`);
  console.log(`  - Outbound: ${redpandaConfig.topics.outbound}`);
  console.log(`  - Inbound: ${redpandaConfig.topics.inbound}`);
  console.log(`  - MRF Rejected: ${redpandaConfig.topics.mrfRejected}`);
  console.log(`Features:`);
  console.log(`  - Inbound Worker: ${featureFlags.enableInboundWorker}`);
  console.log(`  - Outbound Worker: ${featureFlags.enableOutboundWorker}`);
  console.log(`  - MRF: ${featureFlags.enableMrf}`);
  console.log(`  - OpenSearch Indexer: ${featureFlags.enableOpensearchIndexer}`);
  console.log(`  - WebFinger: ${featureFlags.enableWebfinger}`);
  console.log(`  - Actor Serving: ${featureFlags.enableActorServing}`);
}
