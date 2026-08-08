import { Kafka, logLevel } from "kafkajs";
import { V6_TOPICS } from "./v6-topology.js";

export type DurabilityProfile = "development" | "staging" | "production";

export interface TopicGovernanceOptions {
  brokers: string[];
  clientId: string;
  profile: DurabilityProfile;
  retryLimit: number;
  retryBaseMs: number;
}

interface TopicDefinition {
  topic: string;
  partitions: number;
  replicationFactor: number;
  cleanupPolicy: "delete" | "compact,delete";
  retentionMs: number;
}

interface ProfileSettings {
  replicationFactor: number;
  minInSyncReplicas: number;
}

const PROFILE_SETTINGS: Record<DurabilityProfile, ProfileSettings> = {
  development: { replicationFactor: 1, minInSyncReplicas: 1 },
  staging: { replicationFactor: 2, minInSyncReplicas: 1 },
  production: { replicationFactor: 3, minInSyncReplicas: 2 },
};

const TOPIC_RESOURCE_TYPE = 2;

export function resolveTopicGovernanceOptionsFromEnv(): TopicGovernanceOptions {
  const brokers = (process.env["REDPANDA_BROKERS"] || "localhost:9092")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

  if (brokers.length === 0) {
    throw new Error("REDPANDA_BROKERS must include at least one broker endpoint.");
  }

  return {
    brokers,
    clientId: process.env["REDPANDA_CLIENT_ID"] || "fedify-sidecar-v6",
    profile: resolveProfile(process.env["REDPANDA_TOPIC_BOOTSTRAP_PROFILE"]),
    retryLimit: sanitizeInt(process.env["REDPANDA_TOPIC_BOOTSTRAP_RETRIES"], 5, 1, 12),
    retryBaseMs: sanitizeInt(process.env["REDPANDA_TOPIC_BOOTSTRAP_RETRY_BASE_MS"], 250, 50, 15_000),
  };
}

export async function bootstrapRedpandaTopics(
  options: TopicGovernanceOptions,
): Promise<{
  createdTopics: string[];
  existingTopics: string[];
  reconciledTopics: string[];
  requestedTopicCount: number;
}> {
  const kafka = buildKafka(options);
  const admin = kafka.admin();

  await withExponentialBackoff(options, "connect", () => admin.connect());
  try {
    const topics = buildTopicDefinitions(options.profile);
    const existingTopics = new Set(
      await withExponentialBackoff(options, "list-topics", () => admin.listTopics()),
    );
    const missing = topics.filter((topic) => !existingTopics.has(topic.topic));

    if (missing.length > 0) {
      await withExponentialBackoff(options, "create-topics", () =>
        admin.createTopics({
          waitForLeaders: true,
          topics: missing.map((topic) => ({
            topic: topic.topic,
            numPartitions: topic.partitions,
            replicationFactor: topic.replicationFactor,
            configEntries: buildConfigEntries(topic, options.profile),
          })),
        }),
      );
    }

    const reconcileResult = await reconcileTopicProfiles(admin, options, topics);

    return {
      createdTopics: missing.map((topic) => topic.topic),
      existingTopics: topics.filter((topic) => existingTopics.has(topic.topic)).map((topic) => topic.topic),
      reconciledTopics: reconcileResult,
      requestedTopicCount: topics.length,
    };
  } finally {
    await withExponentialBackoff(options, "disconnect", () => admin.disconnect());
  }
}

async function reconcileTopicProfiles(
  admin: ReturnType<Kafka["admin"]>,
  options: TopicGovernanceOptions,
  topics: TopicDefinition[],
): Promise<string[]> {
  const reconciled = new Set<string>();

  const metadata = await withExponentialBackoff(options, "fetch-topic-metadata", () =>
    admin.fetchTopicMetadata({ topics: topics.map((topic) => topic.topic) }),
  );
  const metadataByTopic = new Map(metadata.topics.map((topic) => [topic.name, topic]));

  const partitionPlans = topics
    .map((topic) => {
      const details = metadataByTopic.get(topic.topic);
      const currentPartitions = details?.partitions.length ?? 0;
      return {
        topic: topic.topic,
        currentPartitions,
        requiredPartitions: topic.partitions,
      };
    })
    .filter((topic) => topic.currentPartitions > 0 && topic.currentPartitions < topic.requiredPartitions)
    .map((topic) => {
      reconciled.add(topic.topic);
      return {
        topic: topic.topic,
        count: topic.requiredPartitions,
      };
    });

  if (partitionPlans.length > 0) {
    await withExponentialBackoff(options, "create-partitions", () =>
      admin.createPartitions({ topicPartitions: partitionPlans }),
    );
  }

  const configResources = topics.map((topic) => {
    reconciled.add(topic.topic);
    return {
      type: TOPIC_RESOURCE_TYPE,
      name: topic.topic,
      configEntries: buildConfigEntries(topic, options.profile),
    };
  });

  await withExponentialBackoff(options, "alter-configs", () =>
    admin.alterConfigs({
      validateOnly: false,
      resources: configResources,
    }),
  );

  return [...reconciled.values()];
}

export async function verifyRedpandaTopics(options: TopicGovernanceOptions): Promise<void> {
  const kafka = buildKafka(options);
  const admin = kafka.admin();

  await withExponentialBackoff(options, "connect", () => admin.connect());
  try {
    const topics = buildTopicDefinitions(options.profile);
    const existingTopics = new Set(
      await withExponentialBackoff(options, "list-topics", () => admin.listTopics()),
    );

    const missingTopics = topics
      .filter((topic) => !existingTopics.has(topic.topic))
      .map((topic) => topic.topic);

    if (missingTopics.length > 0) {
      throw new Error(
        `Missing required topics: ${missingTopics.join(", ")}. Run npm run topics:bootstrap before startup.`,
      );
    }

    const metadata = await withExponentialBackoff(options, "fetch-topic-metadata", () =>
      admin.fetchTopicMetadata({ topics: topics.map((topic) => topic.topic) }),
    );

    const metadataByTopic = new Map(metadata.topics.map((topic) => [topic.name, topic]));
    const configByTopic = await fetchTopicConfigEntries(admin, options, topics.map((topic) => topic.topic));

    const failures: string[] = [];
    for (const topic of topics) {
      const topicMetadata = metadataByTopic.get(topic.topic);
      if (!topicMetadata) {
        failures.push(`${topic.topic}: metadata unavailable`);
        continue;
      }

      if (topicMetadata.partitions.length < topic.partitions) {
        failures.push(
          `${topic.topic}: partitions ${topicMetadata.partitions.length} below required ${topic.partitions}`,
        );
      }

      const replicationFloor = topicMetadata.partitions.reduce((min, partition) => {
        const count = Array.isArray(partition.replicas) ? partition.replicas.length : 0;
        return Math.min(min, count);
      }, Number.POSITIVE_INFINITY);

      if (replicationFloor < topic.replicationFactor) {
        failures.push(
          `${topic.topic}: replication factor floor ${replicationFloor} below required ${topic.replicationFactor}`,
        );
      }

      const configEntries = configByTopic.get(topic.topic) ?? new Map<string, string>();
      const compressionType = configEntries.get("compression.type");
      if (compressionType !== "producer" && compressionType !== "zstd") {
        failures.push(`${topic.topic}: compression.type must be producer or zstd (found ${compressionType ?? "unset"})`);
      }

      const expectedCleanup = normalizeCleanupPolicy(topic.cleanupPolicy);
      const actualCleanup = normalizeCleanupPolicy(configEntries.get("cleanup.policy") ?? "");
      if (expectedCleanup !== actualCleanup) {
        failures.push(
          `${topic.topic}: cleanup.policy must be ${topic.cleanupPolicy} (found ${configEntries.get("cleanup.policy") ?? "unset"})`,
        );
      }

      const minIsrRaw = configEntries.get("min.insync.replicas");
      const minIsr = Number.parseInt(minIsrRaw ?? "", 10);
      const expectedMinIsr = PROFILE_SETTINGS[options.profile].minInSyncReplicas;
      const isUnsetInDevelopment = !minIsrRaw && expectedMinIsr <= 1;
      if (!isUnsetInDevelopment && (!Number.isFinite(minIsr) || minIsr < expectedMinIsr)) {
        failures.push(
          `${topic.topic}: min.insync.replicas must be >= ${expectedMinIsr} (found ${minIsrRaw ?? "unset"})`,
        );
      }

      const retentionRaw = configEntries.get("retention.ms");
      const retentionMs = Number.parseInt(retentionRaw ?? "", 10);
      if (!Number.isFinite(retentionMs) || retentionMs < topic.retentionMs) {
        failures.push(
          `${topic.topic}: retention.ms must be >= ${topic.retentionMs} (found ${retentionRaw ?? "unset"})`,
        );
      }
    }

    if (failures.length > 0) {
      throw new Error(`Redpanda topic governance verification failed: ${failures.join("; ")}`);
    }
  } finally {
    await withExponentialBackoff(options, "disconnect", () => admin.disconnect());
  }
}

function buildKafka(options: TopicGovernanceOptions): Kafka {
  return new Kafka({
    clientId: `${options.clientId}-topic-governance`,
    brokers: options.brokers,
    logLevel: logLevel.WARN,
    retry: {
      initialRetryTime: 100,
      retries: 8,
    },
  });
}

async function fetchTopicConfigEntries(
  admin: ReturnType<Kafka["admin"]>,
  options: TopicGovernanceOptions,
  topics: string[],
): Promise<Map<string, Map<string, string>>> {
  const described = await withExponentialBackoff(options, "describe-configs", () =>
    admin.describeConfigs({
      includeSynonyms: false,
      resources: topics.map((topic) => ({
        type: TOPIC_RESOURCE_TYPE,
        name: topic,
        configNames: [
          "cleanup.policy",
          "retention.ms",
          "compression.type",
          "min.insync.replicas",
        ],
      })),
    }),
  );

  const byTopic = new Map<string, Map<string, string>>();
  for (const resource of described.resources) {
    const configMap = new Map<string, string>();
    for (const entry of resource.configEntries ?? []) {
      if (entry.configName && typeof entry.configValue === "string") {
        configMap.set(entry.configName, entry.configValue);
      }
    }
    byTopic.set(resource.resourceName, configMap);
  }

  return byTopic;
}

function buildConfigEntries(
  topic: TopicDefinition,
  profile: DurabilityProfile,
): Array<{ name: string; value: string }> {
  const profileSettings = PROFILE_SETTINGS[profile];
  const entries: Array<{ name: string; value: string }> = [
    { name: "cleanup.policy", value: topic.cleanupPolicy },
    { name: "retention.ms", value: String(topic.retentionMs) },
    { name: "compression.type", value: "producer" },
    { name: "min.insync.replicas", value: String(profileSettings.minInSyncReplicas) },
  ];

  if (topic.cleanupPolicy.includes("compact")) {
    entries.push({ name: "delete.retention.ms", value: String(24 * 60 * 60 * 1000) });
    entries.push({ name: "min.cleanable.dirty.ratio", value: "0.5" });
  }

  return entries;
}

function buildTopicDefinitions(profile: DurabilityProfile): TopicDefinition[] {
  const profileSettings = PROFILE_SETTINGS[profile];

  const v6Topics = Object.values(V6_TOPICS).map<TopicDefinition>((topic) => ({
    topic: sanitizeTopicName(topic.name),
    partitions: sanitizeInt(String(topic.partitions), topic.partitions, 1, 512),
    replicationFactor: Math.min(topic.replicationFactor, profileSettings.replicationFactor),
    cleanupPolicy: topic.name === "ap.tombstones.v1" ? "compact,delete" : "delete",
    retentionMs: sanitizeInt(String(topic.retentionMs), topic.retentionMs, 60_000, 365 * 24 * 60 * 60 * 1000),
  }));

  const atTopics: TopicDefinition[] = [
    {
      topic: sanitizeTopicName(process.env["PROTOCOL_BRIDGE_AT_COMMIT_TOPIC"] || "at.commit.v1"),
      partitions: 3,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "delete",
      retentionMs: 7 * 24 * 60 * 60 * 1000,
    },
    {
      topic: sanitizeTopicName(process.env["PROTOCOL_BRIDGE_AT_REPO_OP_TOPIC"] || "at.repo.op.v1"),
      partitions: 3,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "delete",
      retentionMs: 7 * 24 * 60 * 60 * 1000,
    },
    {
      topic: sanitizeTopicName(process.env["PROTOCOL_BRIDGE_AT_EGRESS_TOPIC"] || "at.egress.v1"),
      partitions: 3,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "delete",
      retentionMs: 7 * 24 * 60 * 60 * 1000,
    },
    {
      topic: sanitizeTopicName(process.env["PROTOCOL_BRIDGE_AT_VERIFIED_INGRESS_TOPIC"] || "at.ingress.v1"),
      partitions: 3,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "delete",
      retentionMs: 7 * 24 * 60 * 60 * 1000,
    },
    {
      topic: sanitizeTopicName(process.env["AT_EXTERNAL_FIREHOSE_RAW_TOPIC"] || "at.firehose.raw.v1"),
      partitions: 3,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "delete",
      retentionMs: 3 * 24 * 60 * 60 * 1000,
    },
    {
      topic: sanitizeTopicName(process.env["PROTOCOL_BRIDGE_AP_INGRESS_TOPIC"] || "ap.atproto-ingress.v1"),
      partitions: 3,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "delete",
      retentionMs: 7 * 24 * 60 * 60 * 1000,
    },
    {
      topic: sanitizeTopicName("at.verify-failed.v1"),
      partitions: 1,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "delete",
      retentionMs: 30 * 24 * 60 * 60 * 1000,
    },
    {
      topic: sanitizeTopicName("at.identity.v1"),
      partitions: 3,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "compact,delete",
      retentionMs: 30 * 24 * 60 * 60 * 1000,
    },
    {
      topic: sanitizeTopicName("at.account.v1"),
      partitions: 3,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "compact,delete",
      retentionMs: 30 * 24 * 60 * 60 * 1000,
    },
    {
      topic: sanitizeTopicName(process.env["MEDIA_ASSET_TOPIC"] || "media.asset.created.v1"),
      partitions: 3,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "delete",
      retentionMs: 30 * 24 * 60 * 60 * 1000,
    },
  ];

  // DLQ topics — produced to by Redpanda Connect on sink failure.
  // Must be pre-created because auto-topic creation is disabled.
  const dlqTopics: TopicDefinition[] = [
    {
      topic: sanitizeTopicName("ap.firehose.dlq.v1"),
      partitions: 1,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "delete",
      retentionMs: 7 * 24 * 60 * 60 * 1000,
    },
    {
      topic: sanitizeTopicName("ap.mrf.rejected.dlq.v1"),
      partitions: 1,
      replicationFactor: profileSettings.replicationFactor,
      cleanupPolicy: "delete",
      retentionMs: 30 * 24 * 60 * 60 * 1000,
    },
  ];

  const deduped = new Map<string, TopicDefinition>();
  for (const topic of [...v6Topics, ...atTopics, ...dlqTopics]) {
    if (!deduped.has(topic.topic)) {
      deduped.set(topic.topic, topic);
    }
  }

  return [...deduped.values()];
}

async function withExponentialBackoff<T>(
  options: TopicGovernanceOptions,
  operation: string,
  fn: () => Promise<T>,
): Promise<T> {
  let attempt = 0;
  let lastError: unknown;

  while (attempt < options.retryLimit) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      attempt += 1;
      if (attempt >= options.retryLimit) {
        break;
      }
      const delayMs = computeBackoffMs(options.retryBaseMs, attempt);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }

  const message = lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(`Redpanda topic governance failed during ${operation}: ${message}`);
}

function computeBackoffMs(baseMs: number, attempt: number): number {
  const exponential = baseMs * Math.pow(2, attempt - 1);
  const jitter = Math.floor(Math.random() * 150);
  return Math.min(exponential + jitter, 10_000);
}

function resolveProfile(value: string | undefined): DurabilityProfile {
  const configured = (value || "").trim().toLowerCase();
  if (!configured) {
    return process.env["NODE_ENV"] === "production" ? "production" : "development";
  }
  if (configured === "development" || configured === "staging" || configured === "production") {
    return configured;
  }
  throw new Error("REDPANDA_TOPIC_BOOTSTRAP_PROFILE must be one of development, staging, production.");
}

function normalizeCleanupPolicy(value: string): string {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .sort()
    .join(",");
}

function sanitizeTopicName(value: string): string {
  const normalized = value.trim();
  if (!normalized) {
    throw new Error("Topic name must not be empty.");
  }
  if (normalized.length > 249) {
    throw new Error(`Topic name exceeds maximum length: ${normalized}`);
  }
  if (!/^[a-zA-Z0-9._-]+$/.test(normalized)) {
    throw new Error(`Topic name contains invalid characters: ${normalized}`);
  }
  return normalized;
}

function sanitizeInt(value: string | undefined, fallback: number, min: number, max: number): number {
  const candidate = Number.parseInt(value ?? "", 10);
  const parsed = Number.isFinite(candidate) ? candidate : fallback;
  if (parsed < min) {
    return min;
  }
  if (parsed > max) {
    return max;
  }
  return parsed;
}
