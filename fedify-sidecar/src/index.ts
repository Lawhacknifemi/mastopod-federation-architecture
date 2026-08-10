/**
 * Fedify Sidecar for ActivityPods - v5
 * 
 * Main entry point that initializes and starts all services:
 * - Redis Streams for work queues (inbound/outbound)
 * - RedPanda for event logs (Stream1, Stream2, Firehose)
 * - OpenSearch for activity storage and querying
 * - HTTP server for receiving inbound activities
 * - Workers for processing inbound/outbound activities
 * 
 * Key Architecture:
 * - Redis Streams: Work queues with consumer groups, XAUTOCLAIM for recovery
 * - RedPanda: Event logs only (NOT work queues)
 * - ActivityPods: Signing API (keys never leave), inbox forwarding
 */

process.env["KAFKAJS_NO_PARTITIONER_WARNING"] ??= "1";

import Fastify, { type FastifyReply, type FastifyRequest } from "fastify";
import { Redis } from "ioredis";
import {
  RedisStreamsQueue,
  createDefaultConfig as createQueueConfig,
  createInboundEnvelope,
  createOutboxIntent,
  createVerifiedInboundEnvelope,
} from "./queue/sidecar-redis-queue.js";
import { createSigningClient } from "./signing/signing-client.js";
import { createRedPandaProducer } from "./streams/redpanda-producer.js";
import { createSearchIndexerService, SearchIndexerService } from "./search/service/SearchIndexerService.js";
import {
  OpenSearchBootstrapService,
  createOpenSearchBootstrapConfig,
} from "./search/service/OpenSearchBootstrapService.js";
import {
  QdrantBootstrapService,
  createQdrantBootstrapConfig,
} from "./search/service/QdrantBootstrapService.js";
import {
  resolveTopicGovernanceOptionsFromEnv,
  verifyRedpandaTopics,
} from "./streams/redpanda-topic-governance.js";
import { createOutboundWorker, OutboundWorker } from "./delivery/outbound-worker.js";
import { createInboundWorker, InboundWorker } from "./delivery/inbound-worker.js";
import { InboundIdempotencyGuard } from "./delivery/InboundIdempotencyGuard.js";
import { ProviderAnnounceGuard } from "./delivery/ProviderAnnounceGuard.js";
import { ContentFingerprintGuard } from "./delivery/ContentFingerprintGuard.js";
import { RedisDomainReputationStore } from "./delivery/DomainReputationStore.js";
import { SpamEvaluator } from "./mrf/SpamEvaluator.js";
import { registerSpamDomainAdminRoutes } from "./admin/spam/fastify-routes.js";
import { registerKeywordRulesAdminRoutes } from "./admin/spam/keyword-rules-routes.js";
import { prewarmEmbeddingModel } from "./mrf/embedding/EmbeddingModel.js";
import { RemoteSharedInboxCache } from "./delivery/RemoteSharedInboxCache.js";
import {
  createOutboxIntentWorker,
  OutboxIntentWorker,
} from "./delivery/outbox-intent-worker.js";
import {
  createOriginReconciliationWorker,
  OriginReconciliationWorker,
} from "./delivery/origin-reconciliation-worker.js";
import { logger } from "./utils/logger.js";
import { ActivityPodsClient } from "./services/activitypods-client.js";
import { registerAtXrpcRoutes, attachSubscribeReposWebSocket } from "./at-adapter/xrpc/AtXrpcFastifyBridge.js";
import { registerOAuthRoutes } from "./at-adapter/oauth/OAuthFastifyBridge.js";
import { OAuthParStore, OAuthAuthorizationCodeStore, OAuthRefreshTokenStore, OAuthGrantStore, OAuthDpopNonceStore, OAuthConsentChallengeStore, OAuthRateLimitStore } from "./at-adapter/oauth/OAuthRedisStores.js";
import { OAuthAsKeyManager } from "./at-adapter/oauth/OAuthAsKeyManager.js";
import { OAuthClientMetadataFetcher } from "./at-adapter/oauth/OAuthClientMetadataFetcher.js";
import { OAuthAuthorizationServer } from "./at-adapter/oauth/OAuthAuthorizationServer.js";
import { DpopVerifier } from "./at-adapter/oauth/DpopVerifier.js";
import type { OAuthAccessTokenVerifier } from "./at-adapter/oauth/OAuthTokenVerifier.js";
import { OAuthTokenVerifier } from "./at-adapter/oauth/OAuthTokenVerifier.js";
import { CompositeOAuthTokenVerifier } from "./at-adapter/oauth/CompositeOAuthTokenVerifier.js";
import { BackendIntrospectionTokenVerifier } from "./at-adapter/oauth/BackendIntrospectionTokenVerifier.js";
import { OAuthExternalDiscoveryBroker } from "./at-adapter/oauth/OAuthExternalDiscoveryBroker.js";
import { renderOAuthSecurityMetricsLines } from "./at-adapter/oauth/OAuthSecurityMetrics.js";
import {
  getMetrics as renderPrometheusMetrics,
  metrics as promMetrics,
} from "./metrics/index.js";
import { parseOAuthRouteRateLimitsFromEnv } from "./at-adapter/oauth/OAuthRateLimitConfig.js";
// AT adapter — identity
import { RedisIdentityBindingRepository } from "./core-domain/identity/RedisIdentityBindingRepository.js";
// AT adapter — repo / alias
import { RedisAtAliasStore } from "./at-adapter/repo/AtAliasStore.js";
import { RedisAtprotoRepoRegistry } from "./atproto/repo/AtprotoRepoRegistry.js";
import { ActivityPodsAtRecordReader } from "./at-adapter/repo/AtRecordReader.js";
import { DefaultAtCarExporter } from "./at-adapter/repo/AtCarExporter.js";
import { DefaultAtRkeyService } from "./at-adapter/repo/AtRkeyService.js";
import { DefaultAtRecordRefResolver } from "./at-adapter/repo/AtRecordRefResolver.js";
import { DefaultAtTargetAliasResolver } from "./at-adapter/repo/AtTargetAliasResolver.js";
import { DefaultAtCommitBuilder } from "./at-adapter/repo/AtCommitBuilder.js";
import { DefaultAtCommitPersistenceService } from "./at-adapter/repo/AtCommitPersistenceService.js";
// AT adapter — identity / handle
import { DefaultHandleResolutionReader } from "./at-adapter/identity/HandleResolutionReader.js";
import { DefaultAtSubjectResolver } from "./at-adapter/identity/AtSubjectResolver.js";
import { HttpIdentityBindingSyncService } from "./at-adapter/identity/IdentityBindingSyncService.js";
import { IdentityWarmupService } from "./at-adapter/identity/IdentityWarmupService.js";
import { RedisIdentityWarmCursorStore } from "./at-adapter/identity/RedisIdentityWarmCursorStore.js";
// AT adapter — firehose
import { DefaultAtFirehoseSubscriptionManager } from "./at-adapter/firehose/AtFirehoseSubscriptionManager.js";
import { RedisAtFirehoseCursorStore } from "./at-adapter/firehose/AtFirehoseCursorStore.js";
import { DefaultAtFirehoseEventEncoder } from "./at-adapter/firehose/AtFirehoseEventEncoder.js";
import { DefaultAtFirehosePublisher } from "./at-adapter/firehose/AtFirehosePublisher.js";
import { AtFirehoseRuntime } from "./at-adapter/firehose/AtFirehoseRuntime.js";
import { DefaultAtRepoDiffBuilder } from "./at-adapter/repo/AtRepoDiffBuilder.js";
// AT adapter — auth / session
import { DefaultAtSessionTokenService } from "./at-adapter/auth/DefaultAtSessionTokenService.js";
import { DefaultAtAccountResolver } from "./at-adapter/auth/DefaultAtAccountResolver.js";
import { DefaultAtSessionService } from "./at-adapter/auth/DefaultAtSessionService.js";
import { RedisSessionFamilyStateStore } from "./at-adapter/auth/SessionFamilyStateStore.js";
import { createHttpAtPasswordVerifier } from "./at-adapter/auth/HttpAtPasswordVerifier.js";
import { LocalAtPasswordVerifier } from "./at-adapter/auth/LocalAtPasswordVerifier.js";
import { ExternalPdsClient } from "./at-adapter/external/ExternalPdsClient.js";
import { RedisExternalAtSessionStore } from "./at-adapter/external/ExternalAtSessionStore.js";
import { ExternalWriteGateway } from "./at-adapter/external/ExternalWriteGateway.js";
import { ExternalReadGateway } from "./at-adapter/external/ExternalReadGateway.js";
import { DefaultAtBlobStore } from "./at-adapter/blob/AtBlobStore.js";
import { DefaultBlobReferenceMapper } from "./at-adapter/blob/BlobReferenceMapper.js";
import { DefaultAtBlobUploadService } from "./at-adapter/blob/AtBlobUploadService.js";
// AT local fixture signing (dev/test only — activated by AT_LOCAL_FIXTURE=true)
import { LocalAtSigningService } from "./signing/LocalAtSigningService.js";
// AT adapter — projection
import { DefaultAtProjectionPolicy } from "./at-adapter/projection/AtProjectionPolicy.js";
import { DefaultAtProjectionWorker } from "./at-adapter/projection/AtProjectionWorker.js";
import { DefaultProfileRecordSerializer } from "./at-adapter/projection/serializers/ProfileRecordSerializer.js";
import { DefaultPostRecordSerializer } from "./at-adapter/projection/serializers/PostRecordSerializer.js";
import { DefaultStandardDocumentRecordSerializer } from "./at-adapter/projection/serializers/StandardDocumentRecordSerializer.js";
import { DefaultFacetBuilder } from "./at-adapter/projection/serializers/FacetBuilder.js";
import { DefaultEmbedBuilder } from "./at-adapter/projection/serializers/EmbedBuilder.js";
import { DefaultImageEmbedBuilder } from "./at-adapter/projection/serializers/ImageEmbedBuilder.js";
import { StoredAttachmentMediaResolver } from "./at-adapter/projection/serializers/StoredAttachmentMediaResolver.js";
import { DefaultVideoEmbedBuilder } from "./at-adapter/projection/serializers/VideoEmbedBuilder.js";
import { DefaultFollowRecordSerializer } from "./at-adapter/projection/serializers/FollowRecordSerializer.js";
import { DefaultLikeRecordSerializer } from "./at-adapter/projection/serializers/LikeRecordSerializer.js";
import { DefaultRepostRecordSerializer } from "./at-adapter/projection/serializers/RepostRecordSerializer.js";
// AT adapter — writes
import { DefaultAtWriteNormalizer } from "./at-adapter/writes/DefaultAtWriteNormalizer.js";
import { DefaultAtWritePolicyGate } from "./at-adapter/writes/DefaultAtWritePolicyGate.js";
import { DefaultAtWriteGateway } from "./at-adapter/writes/DefaultAtWriteGateway.js";
import { DefaultCanonicalClientWriteService } from "./at-adapter/writes/DefaultCanonicalClientWriteService.js";
import { RedisAtWriteResultStore } from "./at-adapter/writes/AtWriteResultStore.js";
import type { AtWriteResultStore } from "./at-adapter/writes/AtWriteResultStore.js";
// Signing + event contracts
import type { SigningService } from "./core-domain/contracts/SigningContracts.js";
import type { EventPublisher } from "./core-domain/events/CoreIdentityEvents.js";
import { RedpandaEventPublisher } from "./core-domain/events/RedpandaEventPublisher.js";
import { RedisProjectionLedger } from "./protocol-bridge/idempotency/ProjectionLedger.js";
import { ActivityPubToCanonicalTranslator } from "./protocol-bridge/activitypub/ActivityPubToCanonicalTranslator.js";
import { AtprotoToCanonicalTranslator } from "./protocol-bridge/atproto/AtprotoToCanonicalTranslator.js";
import { CanonicalToActivityPubProjector } from "./protocol-bridge/projectors/CanonicalToActivityPubProjector.js";
import { CanonicalToAtprotoProjector } from "./protocol-bridge/projectors/CanonicalToAtprotoProjector.js";
import { AtprotoWriteGatewayPort } from "./protocol-bridge/adapters/AtprotoWriteGatewayPort.js";
import { EventPublisherActivityPubPort } from "./protocol-bridge/adapters/EventPublisherActivityPubPort.js";
import { ActivityPubBridgeIngressClient } from "./protocol-bridge/runtime/ActivityPubBridgeIngressClient.js";
import { ActivityPubBridgeOutboundResolverClient } from "./protocol-bridge/runtime/ActivityPubBridgeOutboundResolverClient.js";
import { ActivityPubBridgeActivityResolverClient } from "./protocol-bridge/runtime/ActivityPubBridgeActivityResolverClient.js";
import { ActivityPubBridgeMediaClient } from "./protocol-bridge/runtime/ActivityPubBridgeMediaClient.js";
import { ActivityPubBridgeProfileMediaClient } from "./protocol-bridge/runtime/ActivityPubBridgeProfileMediaClient.js";
import { AtprotoAttachmentMediaResolver } from "./protocol-bridge/runtime/AtprotoAttachmentMediaResolver.js";
import { AtprotoProfileMediaResolver } from "./protocol-bridge/runtime/AtprotoProfileMediaResolver.js";
import { AtprotoLinkPreviewThumbResolver } from "./protocol-bridge/runtime/AtprotoLinkPreviewThumbResolver.js";
import { AtIngressRuntime } from "./at-adapter/ingress/AtIngressRuntime.js";
import { buildAtExternalFirehoseBootstrap, parseAtExternalFirehoseSources } from "./at-adapter/ingress/AtExternalFirehoseBootstrap.js";
import {
  isApInteropMediaFixtureEnabled,
  listApInteropMediaFixtureAccesses,
  recordApInteropMediaFixtureAccess,
  resetApInteropMediaFixtureAccesses,
  resolveApInteropMediaFixtureResponse,
} from "./interop/ap/mediaFixtures.js";
import { HttpAtIdentityResolver } from "./at-adapter/ingress/HttpAtIdentityResolver.js";
import { ProductionAtCommitVerifier } from "./at-adapter/ingress/ProductionAtCommitVerifier.js";
import { normalizeActivityPubNoteLinkPreviewMode } from "./protocol-bridge/projectors/activitypub/ActivityPubProjectionPolicy.js";
import {
  normalizeActivityPubDomainRuleList,
} from "./protocol-bridge/projectors/activitypub/ActivityPubDeliveryPolicy.js";
import { ProtocolBridgeRuntime } from "./protocol-bridge/runtime/ProtocolBridgeRuntime.js";
import { createProtocolBridgeContexts } from "./protocol-bridge/runtime/createProtocolBridgeContexts.js";
import { RedisBridgeProfileMediaStore } from "./protocol-bridge/profile/BridgeProfileMedia.js";
import { RedisBridgePostMediaStore } from "./protocol-bridge/post/BridgePostMedia.js";
import { RedisObservedAtIdentityStore } from "./protocol-bridge/identity/ObservedAtIdentityStore.js";
import { AtIdentityObservationService } from "./protocol-bridge/identity/AtIdentityObservationService.js";
import { ApToAtProjectionWorker } from "./protocol-bridge/workers/ApToAtProjectionWorker.js";
import { AtToApProjectionWorker } from "./protocol-bridge/workers/AtToApProjectionWorker.js";
import { CanonicalIntentPublisher, CANONICAL_V1_TOPIC } from "./protocol-bridge/canonical/CanonicalIntentPublisher.js";
import { CanonicalNotificationConsumer } from "./protocol-bridge/notifications/CanonicalNotificationConsumer.js";
import { registerAtIdentityObservabilityFastifyRoutes } from "./admin/at-observability/fastify-routes.js";
// Fedify runtime integration (feature-flagged: ENABLE_FEDIFY_RUNTIME_INTEGRATION=true)
import { FedifyKvAdapter } from "./federation/FedifyKvAdapter.js";
import { createFedifyAdapter, type FedifyFederationAdapter } from "./federation/FedifyFederationAdapter.js";
import { registerFedifyRoutes, registerFedifyActorAlias } from "./federation/FedifyFastifyBridge.js";
import { FollowersSyncService } from "./federation/fep8fcf/FollowersSyncService.js";
import { registerFollowersSyncRoutes } from "./federation/fep8fcf/FollowersSyncFastifyBridge.js";
import { registerBlockedCollectionRoutes } from "./federation/fep-c648/BlockedCollectionFastifyBridge.js";
import { registerMutedCollectionRoutes } from "./federation/MutedCollectionFastifyBridge.js";
import { registerKeyPackagesRoutes } from "./federation/mls/KeyPackagesFastifyBridge.js";
import { registerMlsMessagesRoutes } from "./federation/mls/MlsMessagesFastifyBridge.js";
import { registerHashtagRoutes } from "./federation/HashtagFastifyBridge.js";
import { RepliesBackfillService } from "./federation/replies-backfill/RepliesBackfillService.js";
import { OriginReconciliationService } from "./federation/origin-reconciliation/OriginReconciliationService.js";
import { SidecarLocalSigningService } from "./signing/SidecarLocalSigningService.js";
import {
  evaluateOutboundWebhookBackpressure,
  normalizeAndDedupeOutboundTargets,
  OutboundWebhookValidationError,
  resolveOutboundWebhookBackpressureConfigFromEnv,
} from "./delivery/outbound-webhook.js";
import { registerMrfAdminIntegration } from "./admin/mrf/integration.js";
import { registerModerationBridgeIntegration } from "./admin/moderation/integration.js";
import type { ModerationBridgeStore } from "./admin/moderation/types.js";
import { ActivityPodsModerationCaseStore } from "./admin/moderation/activitypods-case-store.js";
import { ActivityPodsProviderInboxEventClient } from "./admin/moderation/ActivityPodsProviderInboxEventClient.js";
import {
  ActivityPubReportForwardingService,
  DEFAULT_MODERATION_ACTOR_IDENTIFIER,
  buildModerationActorUri,
  buildProviderActorUriSet,
  PROVIDER_ACTOR_IDENTIFIER,
  PROVIDER_ACTOR_LEGACY_IDENTIFIERS,
  ALL_PROVIDER_ACTOR_IDENTIFIERS,
  PROVIDER_ACTOR_INBOX_PATHS,
} from "./admin/moderation/ActivityPubReportForwardingService.js";
import { CanonicalActivityPubReportForwarder } from "./admin/moderation/CanonicalActivityPubReportForwarder.js";
import { AtprotoReportForwardingService } from "./admin/moderation/AtprotoReportForwardingService.js";
import { CanonicalAtprotoReportForwarder } from "./admin/moderation/CanonicalAtprotoReportForwarder.js";
import {
  ApRelaySubscriptionService,
  parseRelayActorUrls,
} from "./federation/relay/ApRelaySubscriptionService.js";
import {
  AtJetstreamService,
  parseJetstreamUrl,
} from "./at-adapter/jetstream/AtJetstreamService.js";
import { Client as OpenSearchNativeClient } from "@opensearch-project/opensearch";
import { DefaultFeedCandidateService } from "./search/queries/FeedCandidateService.js";
import { QdrantFeedCandidateService } from "./search/queries/QdrantFeedCandidateService.js";
import { FeedRegistry } from "./feed/FeedRegistry.js";
import { DefaultPodFeedService } from "./feed/PodFeedService.js";
import { DefaultPodHydrationService } from "./feed/PodHydrationService.js";
import { OpenSearchFeedProvider } from "./feed/OpenSearchFeedProvider.js";
import { OpenSearchHydrator } from "./feed/OpenSearchHydrator.js";
import { QdrantFeedProvider } from "./feed/QdrantFeedProvider.js";
import { QdrantHydrator } from "./feed/QdrantHydrator.js";
import { QdrantDocumentStore } from "./feed/QdrantDocumentStore.js";
import type { FeedDefinition } from "./feed/contracts.js";
import { registerFeedFastifyRoutes, attachFeedStreamWebSocket } from "./feed/fastify-routes.js";
import { registerPublicFeedRoutes } from "./feed/public-routes.js";
import { DurableStreamSubscriptionService, buildCapabilityLookup } from "./feed/DurableStreamSubscriptionService.js";
import { FeedStreamKafkaConsumer } from "./feed/FeedStreamKafkaConsumer.js";
import { UnifiedFeedBridge } from "./feed/UnifiedFeedBridge.js";
import type { DurableStreamCapability } from "./feed/DurableStreamContracts.js";
import { Fep3ab2Runtime } from "./fep3ab2/Fep3ab2Runtime.js";
import {
  buildProviderCapabilities,
  inferProviderProfile,
  renderCapabilitiesResponse,
} from "./capabilities/provider-capabilities.js";
import { evaluateCapabilityGate } from "./capabilities/gates.js";
import { validateProviderCapabilitiesConfig } from "./capabilities/startup-validator.js";
import { buildEntitlementOverridesFromEnv, checkCapabilityLimit } from "./capabilities/entitlement.js";
import type { ProviderProfile } from "./capabilities/types.js";
import { AccountBundleVerifier } from "./entryway/AccountBundleVerifier.js";
import { ActivityPodsAppBootstrapClient } from "./entryway/ActivityPodsAppBootstrapClient.js";
import { ActivityPodsProvisioningClient } from "./entryway/ActivityPodsProvisioningClient.js";
import { RedisAccountRouteStore } from "./entryway/AccountRouteStore.js";
import { buildEntrywayProvidersFromEnv } from "./entryway/config.js";
import { EntrywayProvisioningService } from "./entryway/EntrywayProvisioningService.js";
import { EntrywayRecoveryWorker } from "./entryway/EntrywayRecoveryWorker.js";
import { registerEntrywayFastifyRoutes } from "./entryway/fastify-routes.js";
import { ProviderCapabilitiesPreflight } from "./entryway/ProviderPreflight.js";
import { StaticEntrywayProviderRouter } from "./entryway/ProviderRouter.js";
import { MediaAssetSyncConsumer } from "./media/MediaAssetSyncConsumer.js";
import { evaluateMediaSignalPolicy } from "./media/MediaSignalPolicy.js";
import type { MRFAdminStore } from "./admin/mrf/store.js";
import { applyLocaleHeaders, resolveLocale, t as translateLocale } from "./http/locale.js";
import { bootstrapDb, pool } from "./db/postgres.js";
import { PostSyncWorker } from "./services/PostSyncWorker.js";


function normalizeMrfAdminStoreMode(raw: string | undefined): "memory" | "redis" {
  if (raw === "memory" || raw === "redis") {
    return raw;
  }
  return "redis";
}

type StartupMode = "blocking" | "background";
type StartupPhase = "starting" | "ready" | "failed";

interface StartupTask {
  name: string;
  start(): Promise<void>;
}

interface StartupState {
  mode: StartupMode;
  phase: StartupPhase;
  startedAtMs: number;
  readyAtMs: number | null;
  currentStep: string | null;
  completedSteps: string[];
  failedStep: string | null;
  error: string | null;
}

function resolveStartupMode(raw: string | undefined, nodeEnv: string): StartupMode {
  if (raw === "blocking" || raw === "background") {
    return raw;
  }

  return nodeEnv === "production" ? "blocking" : "background";
}

function resolveProviderProfile(raw: string | undefined, enableXrpcServer: boolean): ProviderProfile {
  if (
    raw === "ap-core" ||
    raw === "ap-scale" ||
    raw === "dual-protocol-standard"
  ) {
    return raw;
  }

  return inferProviderProfile(enableXrpcServer);
}

// ATProto handle / hostname syntax (RFC-1035, ≥2 labels, TLD does not start
// with a digit). See https://atproto.com/specs/handle
const AT_HANDLE_HOSTNAME_RE =
  /^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]([a-z0-9-]{0,61}[a-z0-9])?$/;

// TLDs reserved for testing/development per RFC 6761 / atproto handle spec.
// Handles using these TLDs cannot resolve in real-world environments.
const AT_RESERVED_DEV_TLDS = new Set([
  "test",
  "localhost",
  "local",
  "invalid",
  "example",
  "internal",
  "alt",
  "arpa",
  "onion",
]);

/**
 * Validate the PDS hostname advertised by `com.atproto.server.describeServer`
 * (and used as the available-user-domain). Hard-fails in production on
 * obvious misconfigurations so traffic never reaches a broken server.
 *
 *   • In production: reject reserved dev TLDs and bare "localhost".
 *   • Always: warn on syntactically invalid hostnames.
 *
 * Note: this is the SIDECAR side of the alignment with the backend's
 * APODS_ATPROTO_LOCAL_PDS_BASE_URL. The two must agree on the public
 * host that fronts /xrpc/* for federation to work.
 */
function assertAtPdsHostnameValid(
  hostname: string,
  log: { warn: (msg: string) => void; info: (msg: string) => void }
): void {
  const isProd = (process.env["NODE_ENV"] || "development") === "production";
  const lower = String(hostname || "").trim().toLowerCase();

  log.info(
    `[at-xrpc] effective AT_PDS_HOSTNAME = "${lower}" (NODE_ENV=${process.env["NODE_ENV"] || "development"})`
  );

  if (!lower) {
    throw new Error("AT_PDS_HOSTNAME is empty; refusing to start XRPC server.");
  }

  // Bare hostnames without a dot are only valid for local dev (e.g.
  // "localhost"). Anything else with no dot is rejected outright.
  if (!lower.includes(".") && lower !== "localhost") {
    throw new Error(
      `AT_PDS_HOSTNAME "${lower}" is not a valid hostname (no TLD).`
    );
  }

  if (lower === "localhost") {
    if (isProd) {
      throw new Error(
        `AT_PDS_HOSTNAME = "localhost" is not allowed in production. Set AT_PDS_HOSTNAME (and APODS_ATPROTO_LOCAL_PDS_BASE_URL on the backend) to the public XRPC host.`
      );
    }
    return;
  }

  if (!AT_HANDLE_HOSTNAME_RE.test(lower)) {
    if (isProd) {
      throw new Error(
        `AT_PDS_HOSTNAME "${lower}" does not match atproto hostname syntax.`
      );
    }
    log.warn(`AT_PDS_HOSTNAME "${lower}" does not match atproto hostname syntax (allowed in dev).`);
    return;
  }

  const tld = lower.split(".").pop()!;
  if (AT_RESERVED_DEV_TLDS.has(tld)) {
    if (isProd) {
      throw new Error(
        `AT_PDS_HOSTNAME "${lower}" uses reserved dev TLD ".${tld}"; not allowed in production.`
      );
    }
    log.warn(`AT_PDS_HOSTNAME "${lower}" uses reserved dev TLD ".${tld}" (dev only).`);
  }
}

// ============================================================================
// Configuration
// ============================================================================

const config = {
  version: process.env["VERSION"] || "5.0.0",
  nodeEnv: process.env["NODE_ENV"] || "development",
  startupMode: resolveStartupMode(
    process.env["SIDECAR_STARTUP_MODE"],
    process.env["NODE_ENV"] || "development",
  ),
  port: parseInt(process.env["PORT"] || "8080", 10),
  host: process.env["HOST"] || "0.0.0.0",
  domain: process.env["DOMAIN"] || "localhost",
  sidecarToken: process.env["SIDECAR_TOKEN"] || "",
  providerProfile: process.env["PROVIDER_PROFILE"],
  providerPlan: process.env["PROVIDER_PLAN"] || "standard",
  providerDisplayName: process.env["PROVIDER_DISPLAY_NAME"] || "ActivityPods Provider",
  providerRegion: process.env["PROVIDER_REGION"] || "unknown",
  enableProviderCapabilitiesEndpoint:
    process.env["ENABLE_PROVIDER_CAPABILITIES_ENDPOINT"] !== "false",
  enableAccountProvisioning:
    process.env["ENABLE_ACCOUNT_PROVISIONING"] === "true",
  enableEntryway:
    process.env["ENABLE_ENTRYWAY"] === "true",
  entrywayToken: process.env["ENTRYWAY_TOKEN"] || "",
  entrywayFingerprintSecret: process.env["ENTRYWAY_FINGERPRINT_SECRET"] || "",
  entrywayRedisUrl: process.env["ENTRYWAY_REDIS_URL"] || process.env["REDIS_URL"] || "redis://localhost:6379",
  entrywayRedisPrefix: process.env["ENTRYWAY_REDIS_PREFIX"] || "entryway:accounts",
  entrywayPublicResolve: process.env["ENTRYWAY_PUBLIC_RESOLVE"] === "true",
  entrywayRequestTimeoutMs: Number.parseInt(process.env["ENTRYWAY_REQUEST_TIMEOUT_MS"] || "10000", 10),
  entrywayRequestMaxAttempts: Number.parseInt(process.env["ENTRYWAY_REQUEST_MAX_ATTEMPTS"] || "4", 10),
  entrywayVerificationTimeoutMs: Number.parseInt(process.env["ENTRYWAY_VERIFICATION_TIMEOUT_MS"] || "8000", 10),
  entrywayVerificationMaxAttempts: Number.parseInt(process.env["ENTRYWAY_VERIFICATION_MAX_ATTEMPTS"] || "3", 10),
  entrywayVerificationStrict: process.env["ENTRYWAY_VERIFICATION_STRICT"] !== "false",
  entrywayStaleProvisioningAfterMs: Number.parseInt(
    process.env["ENTRYWAY_STALE_PROVISIONING_AFTER_MS"] || `${10 * 60 * 1000}`,
    10,
  ),
  entrywayRecoveryIntervalMs: Number.parseInt(process.env["ENTRYWAY_RECOVERY_INTERVAL_MS"] || `${5 * 60 * 1000}`, 10),
  entrywayRecoveryInitialDelayMs: Number.parseInt(process.env["ENTRYWAY_RECOVERY_INITIAL_DELAY_MS"] || `${60 * 1000}`, 10),
  entrywayRecoveryBatchLimit: Number.parseInt(process.env["ENTRYWAY_RECOVERY_BATCH_LIMIT"] || "25", 10),
  entrywayAppBootstrapTimeoutMs: Number.parseInt(process.env["ENTRYWAY_APP_BOOTSTRAP_TIMEOUT_MS"] || "10000", 10),
  entrywayAppBootstrapMaxAttempts: Number.parseInt(process.env["ENTRYWAY_APP_BOOTSTRAP_MAX_ATTEMPTS"] || "4", 10),
  accountProvisioningMaxAccountsPerAppPerDay:
    Number.parseInt(process.env["ACCOUNT_PROVISIONING_MAX_ACCOUNTS_PER_APP_PER_DAY"] || "0", 10),
  enableMediaAssetSync:
    process.env["ENABLE_MEDIA_ASSET_SYNC"] !== "false",
  mediaAssetTopic: process.env["MEDIA_ASSET_TOPIC"] || "media.asset.created.v1",
  
  // Feature flags
  enableOutboundWorker: process.env["ENABLE_OUTBOUND_WORKER"] !== "false",
  enableInboundWorker: process.env["ENABLE_INBOUND_WORKER"] !== "false",
  enableOutboxIntentWorker: process.env["ENABLE_OUTBOX_INTENT_WORKER"] !== "false",
  enableOriginReconciliation:
    process.env["ENABLE_ORIGIN_RECONCILIATION"] !== "false" &&
    process.env["ENABLE_FEDIFY_RUNTIME_INTEGRATION"] === "true",
  enableOpenSearchIndexer: process.env["ENABLE_OPENSEARCH_INDEXER"] !== "false",
  searchBackend:
    process.env["SEARCH_BACKEND"] === "opensearch" ||
    process.env["SEARCH_BACKEND"] === "qdrant" ||
    process.env["SEARCH_BACKEND"] === "dual"
      ? process.env["SEARCH_BACKEND"]
      : "dual",
  enableXrpcServer: process.env["ENABLE_XRPC_SERVER"] !== "false",
  enableFedifyRuntimeIntegration:
    process.env["ENABLE_FEDIFY_RUNTIME_INTEGRATION"] === "true",
  enableFollowersSync: process.env["ENABLE_FOLLOWERS_SYNC"] === "true",
  enableProtocolBridgeApToAt: process.env["ENABLE_PROTOCOL_BRIDGE_AP_TO_AT"] === "true",
  // AT→AP is auto-enabled when Jetstream is on, so Bluesky content reaches the AP timeline.
  // Can be explicitly set to "false" to disable even when ENABLE_AT_JETSTREAM=true.
  enableProtocolBridgeAtToAp:
    process.env["ENABLE_PROTOCOL_BRIDGE_AT_TO_AP"] === "true" ||
    (process.env["ENABLE_AT_JETSTREAM"] === "true" &&
      process.env["ENABLE_PROTOCOL_BRIDGE_AT_TO_AP"] !== "false"),
  enableMrfAdminApi: process.env["ENABLE_MRF_ADMIN_API"] === "true",
  enableApInteropMediaFixtures: isApInteropMediaFixtureEnabled(),
  mrfAdminToken: process.env["MRF_ADMIN_TOKEN"] || "",
  mrfAdminStore: normalizeMrfAdminStoreMode(process.env["MRF_ADMIN_STORE"]),
  mrfAdminRedisPrefix: process.env["MRF_ADMIN_REDIS_PREFIX"] || "mrf:admin",
  enableModerationBridgeApi:
    process.env["ENABLE_MODERATION_BRIDGE_API"] === "true" ||
    process.env["ENABLE_MRF_ADMIN_API"] === "true",
  enableActivityPubReportForwarder:
    process.env["ENABLE_ACTIVITYPUB_REPORT_FORWARDER"] !== "false" &&
    (process.env["ENABLE_MODERATION_BRIDGE_API"] === "true"
      || process.env["ENABLE_MRF_ADMIN_API"] === "true"),
  enableAtprotoReportForwarder:
    process.env["ENABLE_ATPROTO_REPORT_FORWARDER"] !== "false" &&
    (process.env["ENABLE_MODERATION_BRIDGE_API"] === "true"
      || process.env["ENABLE_MRF_ADMIN_API"] === "true"),
  // Provider actor exists whenever the moderation bridge API is on, regardless
  // of whether outbound AP report forwarding is also enabled.  Serves the
  // actor document, inbox, and outbox independently of the forwarding pipeline.
  // Set ENABLE_PROVIDER_ACTOR=false to disable explicitly.
  enableProviderActor:
    process.env["ENABLE_PROVIDER_ACTOR"] !== "false" &&
    (process.env["ENABLE_MODERATION_BRIDGE_API"] === "true"
      || process.env["ENABLE_MRF_ADMIN_API"] === "true"),
  moderationBridgeRedisPrefix:
    process.env["MODERATION_BRIDGE_REDIS_PREFIX"] || "moderation:bridge",
  moderationLabelerDid:
    process.env["MODERATION_LABELER_DID"] || `did:web:${process.env["DOMAIN"] || "localhost"}`,
  moderationLabelerSigningKeyHex: process.env["MODERATION_LABELER_SIGNING_KEY_HEX"] || "",
  moderationAtAdminXrpcBaseUrl: process.env["MODERATION_AT_ADMIN_XRPC_BASE_URL"] || "",
  moderationAtAdminBearerToken: process.env["MODERATION_AT_ADMIN_BEARER_TOKEN"] || "",
  moderationAtAdminTimeoutMs: Number.parseInt(
    process.env["MODERATION_AT_ADMIN_TIMEOUT_MS"] || "5000",
    10,
  ),

  // AP relay subscription
  // AP_RELAY_ACTOR_URLS: comma-separated list of relay actor URLs to follow.
  // Each URL must use https and follow the ActivityRelay / LitePub relay
  // protocol (actor document served at the URL, inbox URL embedded within).
  //
  // Common relay actor URL formats:
  //   https://relay.example.com/actor       (ActivityRelay standard)
  //   https://mastodon.social/relay          (Mastodon built-in relay)
  //
  // Pre-configured relay targets (set AP_RELAY_ACTOR_URLS to override):
  //   https://mastodon.social/relay
  //   https://mastodon.online/relay
  //   https://flipboard.social/relay
  //   https://cosocial.ca/relay
  //   https://web.brid.gy/relay
  //   https://sigmoid.social/relay
  //   https://twit.social/relay
  //   https://blacktwitter.io/relay
  //   https://social.coop/relay
  //   https://mstdn.games/relay
  //   https://werd.social/relay
  //   https://Beige.party/relay
  //   https://glammr.us/relay
  //   https://dmv.community/relay
  //   https://mastodon.mit.edu/relay
  //   https://c.im/relay
  //   https://channel.org/relay
  //   https://dair-community.social/relay
  //   https://macaw.social/relay
  //   https://ursal.zone/relay
  //   https://geekdom.social/relay
  //   https://blorbo.social/relay
  //   https://sunny.garden/relay
  apRelayActorUrls: parseRelayActorUrls(
    process.env["AP_RELAY_ACTOR_URLS"] ??
    [
      "https://relay.fedi.buzz/instance/mastodon.social",
      "https://relay.fedi.buzz/instance/mastodon.online",
      "https://relay.fedi.buzz/instance/flipboard.social",
      "https://relay.fedi.buzz/instance/cosocial.ca",
      "https://relay.fedi.buzz/instance/web.brid.gy",
      "https://relay.fedi.buzz/instance/sigmoid.social",
      "https://relay.fedi.buzz/instance/twit.social",
      "https://relay.fedi.buzz/instance/blacktwitter.io",
      "https://relay.fedi.buzz/instance/social.coop",
      "https://relay.fedi.buzz/instance/mstdn.games",
      "https://relay.fedi.buzz/instance/werd.social",
      "https://relay.fedi.buzz/instance/Beige.party",
      "https://relay.fedi.buzz/instance/glammr.us",
      "https://relay.fedi.buzz/instance/dmv.community",
      "https://relay.fedi.buzz/instance/mastodon.mit.edu",
      "https://relay.fedi.buzz/instance/c.im",
      "https://relay.fedi.buzz/instance/channel.org",
      "https://relay.fedi.buzz/instance/dair-community.social",
      "https://relay.fedi.buzz/instance/macaw.social",
      "https://relay.fedi.buzz/instance/ursal.zone",
      "https://relay.fedi.buzz/instance/geekdom.social",
      "https://relay.fedi.buzz/instance/blorbo.social",
      "https://relay.fedi.buzz/instance/sunny.garden",
    ].join(",")
  ),
  // AP_RELAY_LOCAL_ACTOR_URI: the local AP actor that sends Follow activities
  // to relay servers.  Must be an actor whose private key is held by
  // ActivityPods.  Defaults to https://${DOMAIN}/users/relay.
  apRelayLocalActorUri: process.env["AP_RELAY_LOCAL_ACTOR_URI"] || "",
  // How often (ms) to re-check relay subscriptions. Default: 24 h.
  apRelayResubscribeIntervalMs: Number.parseInt(
    process.env["AP_RELAY_RESUBSCRIBE_INTERVAL_MS"] || `${24 * 60 * 60 * 1_000}`,
    10,
  ),

  // Canonical event log (canonical.v1) — durable protocol-neutral record of
  // every translated CanonicalIntent from both AT and AP bridge workers.
  // ENABLE_CANONICAL_EVENT_LOG=true activates publishing from projection workers.
  // ENABLE_CANONICAL_NOTIFICATIONS=true activates the in-app notification consumer.
  // CANONICAL_TOPIC overrides the default topic name (canonical.v1).
  enableCanonicalEventLog:
    process.env["ENABLE_CANONICAL_EVENT_LOG"] !== "false" &&
    (process.env["ENABLE_PROTOCOL_BRIDGE_AP_TO_AT"] === "true" ||
      process.env["ENABLE_PROTOCOL_BRIDGE_AT_TO_AP"] === "true" ||
      process.env["ENABLE_AT_JETSTREAM"] === "true" ||
      process.env["ENABLE_MODERATION_BRIDGE_API"] === "true" ||
      process.env["ENABLE_MRF_ADMIN_API"] === "true" ||
      process.env["ENABLE_ACTIVITYPUB_REPORT_FORWARDER"] === "true" ||
      process.env["ENABLE_ATPROTO_REPORT_FORWARDER"] === "true"),
  enableCanonicalNotifications:
    process.env["ENABLE_CANONICAL_NOTIFICATIONS"] !== "false" &&
    (process.env["ENABLE_PROTOCOL_BRIDGE_AP_TO_AT"] === "true" ||
      process.env["ENABLE_PROTOCOL_BRIDGE_AT_TO_AP"] === "true" ||
      process.env["ENABLE_AT_JETSTREAM"] === "true"),
  canonicalTopic: process.env["CANONICAL_TOPIC"] || CANONICAL_V1_TOPIC,

  // ATProto Jetstream (lightweight JSON firehose from Bluesky infrastructure).
  // ENABLE_AT_JETSTREAM=true activates ingestion.
  // JETSTREAM_URL overrides the default endpoint (wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post).
  // JETSTREAM_MAX_EVENTS: if set, service exits cleanly after N published events (useful for smoke tests).
  enableAtJetstream: process.env["ENABLE_AT_JETSTREAM"] === "true",
  atJetstreamUrl: parseJetstreamUrl(process.env["JETSTREAM_URL"]),
  atJetstreamPublishTopic:
    process.env["AT_JETSTREAM_PUBLISH_TOPIC"] ||
    process.env["PROTOCOL_BRIDGE_AT_VERIFIED_INGRESS_TOPIC"] ||
    "at.ingress.v1",
  atJetstreamMaxEvents: process.env["JETSTREAM_MAX_EVENTS"]
    ? Number.parseInt(process.env["JETSTREAM_MAX_EVENTS"], 10)
    : undefined,

  // Phase 7: AT session token secret (min 32 chars).
  // Required when ENABLE_XRPC_SERVER is true.
  atSessionSecret: process.env["AT_SESSION_SECRET"] || "",

  // Phase 7: PDS hostname advertised in server.describeServer
  atPdsHostname: process.env["AT_PDS_HOSTNAME"] || process.env["DOMAIN"] || "localhost",

  // ATProto OAuth settings
  enableAtprotoOauth: process.env["ENABLE_ATPROTO_OAUTH"] !== "false",
  atOauthIssuer: process.env["AT_OAUTH_ISSUER"] || "http://localhost:8080",
  atOauthAuthorizationServerOrigin:
    process.env["AT_OAUTH_AUTHORIZATION_SERVER_ORIGIN"] || "http://localhost:8080",
  atOauthResourceServerOrigin:
    process.env["AT_OAUTH_RESOURCE_SERVER_ORIGIN"] || "http://localhost:8080",
  atOauthClientMetadataTimeoutMs: Number.parseInt(
    process.env["AT_OAUTH_CLIENT_METADATA_TIMEOUT_MS"] || "6000",
    10
  ),
  atOauthClientMetadataMaxAttempts: Number.parseInt(
    process.env["AT_OAUTH_CLIENT_METADATA_MAX_ATTEMPTS"] || "4",
    10
  ),
  atOauthExternalDiscoveryTimeoutMs: Number.parseInt(
    process.env["AT_OAUTH_EXTERNAL_DISCOVERY_TIMEOUT_MS"] || "6000",
    10
  ),
  atOauthExternalDiscoveryMaxAttempts: Number.parseInt(
    process.env["AT_OAUTH_EXTERNAL_DISCOVERY_MAX_ATTEMPTS"] || "4",
    10
  ),
  atOauthAllowLocalhostHttpDiscovery: process.env["AT_OAUTH_ALLOW_LOCALHOST_HTTP_DISCOVERY"] === "true",
  atOauthBackendIntrospectionTimeoutMs: Number.parseInt(
    process.env["AT_OAUTH_BACKEND_INTROSPECTION_TIMEOUT_MS"] || "3000",
    10
  ),
  atOauthBackendIntrospectionMaxAttempts: Number.parseInt(
    process.env["AT_OAUTH_BACKEND_INTROSPECTION_MAX_ATTEMPTS"] || "3",
    10
  ),

  // Phase 7: durable write-result correlation settings
  atWriteResultTtlSec: Number.parseInt(process.env["AT_WRITE_RESULT_TTL_SEC"] || "120", 10),
  atWriteResultKeyPrefix: process.env["AT_WRITE_RESULT_KEY_PREFIX"] || "at:write-result",
  atWriteResultChannelPrefix: process.env["AT_WRITE_RESULT_CHANNEL_PREFIX"] || "at:write-result:ch",

  // Local fixture mode — for development / integration testing ONLY.
  // When true: uses LocalAtPasswordVerifier (no ActivityPods auth call) and
  // LocalAtSigningService (secp256k1 signing from Redis-stored fixture keys).
  // NEVER set in production.  Requires provision-test-fixture.ts to have been run.
  atLocalFixture: process.env["AT_LOCAL_FIXTURE"] === "true",

  identityWarmIntervalMs: Number.parseInt(process.env["IDENTITY_WARM_INTERVAL_MS"] || "30000", 10),
  identityWarmBatchLimit: Number.parseInt(process.env["IDENTITY_WARM_BATCH_LIMIT"] || "100", 10),
  externalAtSessionTtlSeconds: Number.parseInt(
    process.env["EXTERNAL_AT_SESSION_TTL_SECONDS"] || `${60 * 60 * 12}`,
    10
  ),
  externalPdsTimeoutMs: Number.parseInt(process.env["EXTERNAL_PDS_TIMEOUT_MS"] || "8000", 10),
  externalPdsMaxAttempts: Number.parseInt(process.env["EXTERNAL_PDS_MAX_ATTEMPTS"] || "5", 10),
  didDocCacheTtlSeconds: Number.parseInt(process.env["DID_DOC_CACHE_TTL_SECONDS"] || "300", 10),
  atOauthRouteRateLimits: parseOAuthRouteRateLimitsFromEnv(process.env),
  protocolBridgeConsumerGroupId:
    process.env["PROTOCOL_BRIDGE_CONSUMER_GROUP_ID"] || "fedify-sidecar-protocol-bridge",
  protocolBridgeApSourceTopic:
    process.env["PROTOCOL_BRIDGE_AP_SOURCE_TOPIC"] ||
    process.env["STREAM1_TOPIC"] ||
    "ap.stream1.local-public.v1",
  protocolBridgeAtCommitTopic:
    process.env["PROTOCOL_BRIDGE_AT_COMMIT_TOPIC"] || "at.commit.v1",
  protocolBridgeAtVerifiedIngressTopic:
    process.env["PROTOCOL_BRIDGE_AT_VERIFIED_INGRESS_TOPIC"] || "at.ingress.v1",
  protocolBridgeApIngressTopic:
    process.env["PROTOCOL_BRIDGE_AP_INGRESS_TOPIC"] || "ap.atproto-ingress.v1",
  atFirehoseConsumerGroupId:
    process.env["AT_FIREHOSE_CONSUMER_GROUP_ID"] || "fedify-sidecar-at-firehose",
  atFirehoseCursorMaxEvents: Number.parseInt(
    process.env["AT_FIREHOSE_CURSOR_MAX_EVENTS"] || "10000",
    10,
  ),
  atExternalFirehoseConsumerGroupId:
    process.env["AT_EXTERNAL_FIREHOSE_CONSUMER_GROUP_ID"] || "fedify-sidecar-at-firehose-external",
  atExternalFirehoseRawTopic:
    process.env["AT_EXTERNAL_FIREHOSE_RAW_TOPIC"] || "at.firehose.raw.v1",
  enableAtExternalFirehose: process.env["ENABLE_AT_EXTERNAL_FIREHOSE"] === "true",
  protocolBridgeLedgerTtlSec: Number.parseInt(
    process.env["PROTOCOL_BRIDGE_LEDGER_TTL_SEC"] || `${60 * 60 * 24 * 14}`,
    10,
  ),
  protocolBridgeIngressTimeoutMs: Number.parseInt(
    process.env["PROTOCOL_BRIDGE_INGRESS_TIMEOUT_MS"] || "10000",
    10,
  ),
  protocolBridgeOutboundResolutionTimeoutMs: Number.parseInt(
    process.env["PROTOCOL_BRIDGE_OUTBOUND_RESOLUTION_TIMEOUT_MS"] || "10000",
    10,
  ),
  protocolBridgeActivityResolutionTimeoutMs: Number.parseInt(
    process.env["PROTOCOL_BRIDGE_ACTIVITY_RESOLUTION_TIMEOUT_MS"] || "10000",
    10,
  ),
  protocolBridgeProfileMediaTimeoutMs: Number.parseInt(
    process.env["PROTOCOL_BRIDGE_PROFILE_MEDIA_TIMEOUT_MS"] || "10000",
    10,
  ),
  protocolBridgeProfileMediaMaxBytes: Number.parseInt(
    process.env["PROTOCOL_BRIDGE_PROFILE_MEDIA_MAX_BYTES"] || `${5 * 1024 * 1024}`,
    10,
  ),
  protocolBridgeAttachmentMediaTimeoutMs: Number.parseInt(
    process.env["PROTOCOL_BRIDGE_ATTACHMENT_MEDIA_TIMEOUT_MS"] || "20000",
    10,
  ),
  protocolBridgeAttachmentMediaMaxBytes: Number.parseInt(
    process.env["PROTOCOL_BRIDGE_ATTACHMENT_MEDIA_MAX_BYTES"] || `${50 * 1024 * 1024}`,
    10,
  ),
  protocolBridgeProfileMediaTtlSec: Number.parseInt(
    process.env["PROTOCOL_BRIDGE_PROFILE_MEDIA_TTL_SEC"] || `${60 * 60 * 24}`,
    10,
  ),
  protocolBridgePostMediaTtlSec: Number.parseInt(
    process.env["PROTOCOL_BRIDGE_POST_MEDIA_TTL_SEC"] || `${60 * 60 * 24}`,
    10,
  ),
  protocolBridgeApNoteLinkPreviewMode: normalizeActivityPubNoteLinkPreviewMode(
    process.env["PROTOCOL_BRIDGE_AP_NOTE_LINK_PREVIEW_MODE"],
  ),
  protocolBridgeApNoteLinkPreviewRichDomains: normalizeActivityPubDomainRuleList(
    process.env["PROTOCOL_BRIDGE_AP_NOTE_LINK_PREVIEW_RICH_DOMAINS"],
  ),
  protocolBridgeApNoteLinkPreviewDisabledDomains: normalizeActivityPubDomainRuleList(
    process.env["PROTOCOL_BRIDGE_AP_NOTE_LINK_PREVIEW_DISABLED_DOMAINS"],
  ),
};

const outboundWebhookBackpressureConfig = resolveOutboundWebhookBackpressureConfigFromEnv();

// ============================================================================
// Global State
// ============================================================================
let queue: RedisStreamsQueue | null = null;
let outboundWorker: OutboundWorker | null = null;
let inboundWorker: InboundWorker | null = null;
let outboxIntentWorker: OutboxIntentWorker | null = null;
let originReconciliationWorker: OriginReconciliationWorker | null = null;
let opensearchIndexer: SearchIndexerService | null = null;
let atRedisClient: Redis | null = null;
let writeResultStore: AtWriteResultStore | null = null;
let identityWarmupService: IdentityWarmupService | null = null;
let atEventPublisher: RedpandaEventPublisher | null = null;
let canonicalPublisher: CanonicalIntentPublisher | undefined;
let atFirehoseRuntime: AtFirehoseRuntime | null = null;
let atExternalFirehoseRuntime: AtIngressRuntime | null = null;
let protocolBridgeRuntime: ProtocolBridgeRuntime | null = null;
let mediaAssetSyncConsumer: MediaAssetSyncConsumer | null = null;
let searchIndexerRedis: Redis | null = null;
let mrfAdminRedisClient: Redis | null = null;
let mrfAdminStore: MRFAdminStore | null = null;
let domainReputationStore: RedisDomainReputationStore | null = null;
let spamEvaluator: SpamEvaluator | null = null;
let moderationBridgeRedisClient: Redis | null = null;
let moderationBridgeStore: ModerationBridgeStore | null = null;
let identityRepo: RedisIdentityBindingRepository | null = null;
let fedifyAdapter: FedifyFederationAdapter | null = null;
let activityPubReportForwardingService: ActivityPubReportForwardingService | null = null;
let canonicalActivityPubReportForwarder: CanonicalActivityPubReportForwarder | null = null;
let atprotoReportForwardingService: AtprotoReportForwardingService | null = null;
let canonicalAtprotoReportForwarder: CanonicalAtprotoReportForwarder | null = null;
let apRelaySubscriptionService: ApRelaySubscriptionService | null = null;
let atJetstreamService: AtJetstreamService | null = null;
let streamSubscriptionService: DurableStreamSubscriptionService | null = null;
let feedStreamKafkaConsumer: FeedStreamKafkaConsumer | null = null;
let unifiedFeedBridge: UnifiedFeedBridge | null = null;
let fep3ab2Runtime: Fep3ab2Runtime | null = null;
let entrywayRedisClient: Redis | null = null;
let entrywayRecoveryWorker: EntrywayRecoveryWorker | null = null;
let postSyncWorker: PostSyncWorker | null = null;
let isShuttingDown = false;
const startupState: StartupState = {
  mode: config.startupMode,
  phase: "starting",
  startedAtMs: Date.now(),
  readyAtMs: null,
  currentStep: null,
  completedSteps: [],
  failedStep: null,
  error: null,
};

function completeStartupTask(taskName: string): void {
  if (!startupState.completedSteps.includes(taskName)) {
    startupState.completedSteps.push(taskName);
  }
  startupState.currentStep = null;
}

function snapshotStartupState() {
  const now = Date.now();

  return {
    mode: startupState.mode,
    phase: startupState.phase,
    currentStep: startupState.currentStep,
    completedSteps: [...startupState.completedSteps],
    failedStep: startupState.failedStep,
    error: startupState.error,
    durationMs: (startupState.readyAtMs ?? now) - startupState.startedAtMs,
    startedAt: new Date(startupState.startedAtMs).toISOString(),
    readyAt: startupState.readyAtMs ? new Date(startupState.readyAtMs).toISOString() : null,
  };
}

async function runStartupTasks(tasks: StartupTask[]): Promise<void> {
  for (const task of tasks) {
    startupState.currentStep = task.name;
    logger.info("Startup task started", {
      task: task.name,
      mode: startupState.mode,
    });

    try {
      await task.start();
      completeStartupTask(task.name);
      logger.info("Startup task completed", { task: task.name });
    } catch (error: any) {
      startupState.phase = "failed";
      startupState.failedStep = task.name;
      startupState.error = error?.message || String(error);
      throw error;
    }
  }
}

function markStartupReady(): void {
  startupState.phase = "ready";
  startupState.readyAtMs = Date.now();
  startupState.currentStep = null;
  startupState.failedStep = null;
  startupState.error = null;
}

/**
 * Populate Prometheus capability health gauges from the live capabilities
 * document.  Called once after all startup tasks complete.
 *   1  = enabled
 *   0  = disabled
 *  -1  = degraded (reserved for future runtime checks)
 */
function populateCapabilityHealthGauges(doc: import("./capabilities/types.js").ProviderCapabilitiesDocument): void {
  for (const cap of doc.capabilities) {
    const value = cap.status === "enabled" ? 1 : 0;
    promMetrics.capabilityHealthGauge.set({ capability: cap.id }, value);
  }
}

// ============================================================================
// Main Application
// ============================================================================

async function main() {
  logger.info("Starting Fedify Sidecar for ActivityPods", {
    version: config.version,
    nodeEnv: config.nodeEnv,
    startupMode: config.startupMode,
  });

  const activityPubOutboundDeliveryPolicy = {
    defaultNoteLinkPreviewMode: config.protocolBridgeApNoteLinkPreviewMode,
    richNoteLinkPreviewDomains: config.protocolBridgeApNoteLinkPreviewRichDomains,
    disabledNoteLinkPreviewDomains: config.protocolBridgeApNoteLinkPreviewDisabledDomains,
  } as const;
  const providerProfile = resolveProviderProfile(
    config.providerProfile,
    config.enableXrpcServer,
  );
  const providerCapabilitiesDocument = buildProviderCapabilities({
    providerId: config.domain,
    providerDisplayName: config.providerDisplayName,
    providerRegion: config.providerRegion,
    profile: providerProfile,
    plan: config.providerPlan,
    enableInboundWorker: config.enableInboundWorker,
    enableOutboundWorker: config.enableOutboundWorker,
    enableOpenSearchIndexer: config.enableOpenSearchIndexer,
    enableXrpcServer: config.enableXrpcServer,
    enableMediaPipeline: config.enableMediaAssetSync,
    enableMrf: config.enableMrfAdminApi,
    atprotoEnabled: config.enableXrpcServer,
    firehoseRetentionDays: Number.parseInt(process.env["FIREHOSE_RETENTION_DAYS"] || "30", 10),
    includeAtDisabledEntries: true,
    enableAccountProvisioning: config.enableAccountProvisioning,
    accountProvisioningMaxAccountsPerAppPerDay:
      Number.isFinite(config.accountProvisioningMaxAccountsPerAppPerDay) &&
      config.accountProvisioningMaxAccountsPerAppPerDay > 0
        ? config.accountProvisioningMaxAccountsPerAppPerDay
        : undefined,
    enableCanonicalEventLog: config.enableCanonicalEventLog,
    enableUnifiedFeed: true,
  });
  const providerCapabilitiesResponse = renderCapabilitiesResponse(providerCapabilitiesDocument);
  const capabilityGate = (capabilityId: string) =>
    evaluateCapabilityGate(providerCapabilitiesDocument, capabilityId);
  const providerValidation = validateProviderCapabilitiesConfig(providerCapabilitiesDocument, {
    profile: providerProfile,
    // Redis and Redpanda have safe localhost defaults; treat as present unless
    // the operator has explicitly disabled them.
    hasRedisUrl: true,
    hasRedpandaBrokers: true,
    // Signing endpoint / ActivityPods URL have no safe default in production.
    hasSigningEndpoint: !!(process.env["ACTIVITYPODS_URL"]) || config.atLocalFixture,
    hasSigningToken: !!(process.env["ACTIVITYPODS_TOKEN"]) || config.atLocalFixture,
    // OpenSearch also has a localhost default but treat missing URL as advisory.
    hasOpenSearchUrl: true,
    hasActivityPodsUrl: !!(process.env["ACTIVITYPODS_URL"]) || config.atLocalFixture,
    hasActivityPodsToken: !!(process.env["ACTIVITYPODS_TOKEN"]) || config.atLocalFixture,
    enableMrf: config.enableMrfAdminApi,
  });

  for (const issue of providerValidation.issues) {
    const logMessage = "Provider capability validation issue";
    if (issue.severity === "fatal") {
      logger.error({
        ruleId: issue.ruleId,
        code: issue.code,
        message: issue.message,
        details: issue.details,
      }, logMessage);
    } else {
      logger.warn({
        ruleId: issue.ruleId,
        code: issue.code,
        message: issue.message,
        details: issue.details,
      }, logMessage);
    }
  }

  if (!providerValidation.ok) {
    throw new Error("Provider capability validation failed");
  }

  const redpandaProducerRequired =
    config.enableOutboxIntentWorker
    || config.enableOutboundWorker
    || config.enableInboundWorker;
  const kafkaBackplaneRequired =
    redpandaProducerRequired
    || config.enableOpenSearchIndexer
    || config.enableXrpcServer
    || config.enableProtocolBridgeApToAt
    || config.enableProtocolBridgeAtToAp
    || config.enableAtJetstream
    || config.enableAtExternalFirehose
    || config.enableCanonicalEventLog
    || config.enableCanonicalNotifications
    || config.enableMediaAssetSync;
  const startupTasks: StartupTask[] = [];

  const enforceTopicGovernance = process.env["REDPANDA_ENFORCE_TOPIC_GOVERNANCE"] !== "false";

  if (config.enableXrpcServer && !config.atLocalFixture) {
    if (!process.env["ACTIVITYPODS_URL"]) {
      throw new Error("ENABLE_XRPC_SERVER requires ACTIVITYPODS_URL when AT_LOCAL_FIXTURE is false");
    }
    if (!process.env["ACTIVITYPODS_TOKEN"]) {
      throw new Error("ENABLE_XRPC_SERVER requires ACTIVITYPODS_TOKEN when AT_LOCAL_FIXTURE is false");
    }
    if (!process.env["EXTERNAL_AT_SESSION_KEY_HEX"]) {
      throw new Error("ENABLE_XRPC_SERVER requires EXTERNAL_AT_SESSION_KEY_HEX when AT_LOCAL_FIXTURE is false");
    }
  }

  if (config.enableEntryway) {
    if (!config.enableAccountProvisioning) {
      throw new Error("ENABLE_ENTRYWAY requires ENABLE_ACCOUNT_PROVISIONING=true");
    }
    if (!config.entrywayToken) {
      throw new Error("ENABLE_ENTRYWAY requires ENTRYWAY_TOKEN");
    }
    if (config.entrywayFingerprintSecret.length < 32) {
      throw new Error("ENABLE_ENTRYWAY requires ENTRYWAY_FINGERPRINT_SECRET with at least 32 characters");
    }
  }

  // Fixture mode banner — unmistakable, fires before any service connections
  if (config.atLocalFixture) {
    let fixtureAccountIds: string[] = [];
    try {
      const raw = process.env['AT_LOCAL_FIXTURE_CREDS'];
      const creds = raw
        ? (JSON.parse(raw) as Record<string, unknown>)
        : { 'http://localhost:3000/atproto365133': '(default)' };
      fixtureAccountIds = Object.keys(creds);
    } catch {
      fixtureAccountIds = ['(parse error — using defaults)'];
    }
    const accountList = fixtureAccountIds.map((id) => `    • ${id}`).join('\n');
    process.stderr.write(
      `\n${'='.repeat(72)}\n` +
      `  WARNING: AT_LOCAL_FIXTURE=true — LOCAL FIXTURE MODE ENABLED\n` +
      `  LocalAtPasswordVerifier + LocalAtSigningService are ACTIVE.\n` +
      `  ActivityPods auth and signing endpoints are BYPASSED.\n` +
      `  NEVER deploy with this flag set.\n` +
      `  Fixture accounts (${fixtureAccountIds.length}):\n` +
      accountList + '\n' +
      `${'='.repeat(72)}\n\n`,
    );
  }

  // Register shutdown handlers
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("uncaughtException", (error) => {
    logger.error("Uncaught exception", { error: error.message, stack: error.stack });
    shutdown("uncaughtException");
  });
  process.on("unhandledRejection", (reason) => {
    logger.error("Unhandled rejection", { reason });
    shutdown("unhandledRejection");
  });

  try {
    if (kafkaBackplaneRequired && enforceTopicGovernance) {
      const topicGovernanceOptions = resolveTopicGovernanceOptionsFromEnv();
      startupTasks.push({
        name: "verify Redpanda topic governance",
        start: async () => {
          await verifyRedpandaTopics(topicGovernanceOptions);
          logger.info("Redpanda topic governance verification passed", {
            profile: topicGovernanceOptions.profile,
            brokers: topicGovernanceOptions.brokers,
          });
        },
      });
    }

    // Initialize Redis Streams queue
    const queueConfig = createQueueConfig();
    queue = new RedisStreamsQueue(queueConfig);
    await queue.connect();
    logger.info("Redis Streams queue connected");

    // Initialize Signing client
    const signingClient = createSigningClient();
    logger.info("Signing client initialized");

    // FEP-8fcf Followers Sync Service (optional, feature-flagged)
    const followersSyncService: FollowersSyncService | undefined = config.enableFollowersSync
      ? new FollowersSyncService({
          domain: config.domain,
          activityPodsUrl: process.env["ACTIVITYPODS_URL"] ?? "http://localhost:3000",
          activityPodsToken: process.env["ACTIVITYPODS_TOKEN"] ?? "",
          requestTimeoutMs: Number.parseInt(process.env["REQUEST_TIMEOUT_MS"] || "30000", 10),
          userAgent: process.env["USER_AGENT"] || "Fedify-Sidecar/5.0 (ActivityPods)",
          digestCacheTtlSeconds: Number.parseInt(
            process.env["FOLLOWERS_SYNC_DIGEST_TTL_SECONDS"] || "300", 10,
          ),
          onStaleRemoteEntry: async (localActorUri, remoteActorUri) => {
            // FEP-8fcf §3.3 case 2: remote collection claims a local actor follows
            // the sender, but ActivityPods has no record of it.  Log structured
            // context for operator visibility.  Sending an Undo Follow outbound
            // is deferred pending per-user signing infrastructure.
            logger.warn("[fep8fcf] stale remote follower entry detected — manual review recommended", {
              domain: config.domain,
              localActorUri,
              remoteActorUri,
            });
          },
        })
      : undefined;
    if (followersSyncService) {
      logger.info("FEP-8fcf followers sync service enabled");
    }

    const repliesBackfillService: RepliesBackfillService | undefined =
      config.enableFedifyRuntimeIntegration
        ? new RepliesBackfillService(signingClient, queue, {
            signerActorUri: config.apRelayLocalActorUri || `https://${config.domain}/users/relay`,
            requestTimeoutMs: Number.parseInt(process.env["REQUEST_TIMEOUT_MS"] || "30000", 10),
            userAgent: process.env["USER_AGENT"] || "Fedify-Sidecar/5.0 (ActivityPods)",
          })
        : undefined;
    const originReconciliationService: OriginReconciliationService | undefined =
      config.enableOriginReconciliation
        ? new OriginReconciliationService({
            queue,
            domain: config.domain,
            initialDelayMs: Number.parseInt(
              process.env["ORIGIN_RECONCILIATION_INITIAL_DELAY_MS"] || "30000",
              10,
            ),
            activationWindowMs: Number.parseInt(
              process.env["ORIGIN_RECONCILIATION_WINDOW_MS"] || `${30 * 60 * 1000}`,
              10,
            ),
            maxAttempts: Number.parseInt(
              process.env["ORIGIN_RECONCILIATION_MAX_ATTEMPTS"] || "5",
              10,
            ),
          })
        : undefined;
    if (repliesBackfillService) {
      logger.info("Mastodon-compatible replies backfill service enabled");
    }
    if (originReconciliationService) {
      logger.info("Origin reconciliation service enabled");
    }

    // Provider actor URI — built whenever the provider actor is enabled,
    // independent of whether outbound AP report forwarding is also enabled.
    // The provider actor serves an inbox, outbox, and actor document regardless
    // of the forwarding pipeline being active.
    const providerActorUri = config.enableProviderActor
      ? buildModerationActorUri(config.domain, PROVIDER_ACTOR_IDENTIFIER)
      : null;

    // Alias used by downstream blocks that reference moderationActorUri
    // (logger, onModerationReportFailed callback).  Always equals providerActorUri.
    const moderationActorUri = providerActorUri;
    const moderationReportCaseStore =
      (config.enableActivityPubReportForwarder || config.enableAtprotoReportForwarder)
      && process.env["ACTIVITYPODS_URL"]
      && process.env["ACTIVITYPODS_TOKEN"]
        ? new ActivityPodsModerationCaseStore({
            baseUrl: process.env["ACTIVITYPODS_URL"],
            bearerToken: process.env["ACTIVITYPODS_TOKEN"],
            timeoutMs: 5_000,
            retries: 3,
            retryBaseMs: 100,
            retryMaxMs: 2_000,
          })
        : null;

    if (
      config.enableActivityPubReportForwarder
      && moderationReportCaseStore
      && config.enableOutboxIntentWorker
      && config.enableCanonicalEventLog
      && config.enableFedifyRuntimeIntegration
    ) {
      activityPubReportForwardingService = new ActivityPubReportForwardingService(
        queue,
        moderationReportCaseStore,
        {
          domain: config.domain,
          moderationActorIdentifier: PROVIDER_ACTOR_IDENTIFIER,
          userAgent: process.env["USER_AGENT"] || "Fedify-Sidecar/1.0 (ActivityPods)",
          fetchTimeoutMs: Number.parseInt(
            process.env["ACTIVITYPUB_REPORT_FETCH_TIMEOUT_MS"] || "10000",
            10,
          ),
          fetchRetries: Number.parseInt(
            process.env["ACTIVITYPUB_REPORT_FETCH_RETRIES"] || "2",
            10,
          ),
          fetchRetryBaseMs: Number.parseInt(
            process.env["ACTIVITYPUB_REPORT_FETCH_RETRY_BASE_MS"] || "250",
            10,
          ),
          fetchRetryMaxMs: Number.parseInt(
            process.env["ACTIVITYPUB_REPORT_FETCH_RETRY_MAX_MS"] || "2500",
            10,
          ),
        },
        {
          info: (message, meta) => logger.info(meta || {}, message),
          warn: (message, meta) => logger.warn(meta || {}, message),
          error: (message, meta) => logger.error(meta || {}, message),
        },
      );
      logger.info("ActivityPub report forwarding service initialized", {
        moderationActorUri,
      });
    } else if (config.enableActivityPubReportForwarder) {
      logger.warn("ActivityPub report forwarding requested but prerequisites were not met", {
        hasActivityPodsUrl: Boolean(process.env["ACTIVITYPODS_URL"]),
        hasActivityPodsToken: Boolean(process.env["ACTIVITYPODS_TOKEN"]),
        enableOutboxIntentWorker: config.enableOutboxIntentWorker,
        enableCanonicalEventLog: config.enableCanonicalEventLog,
        enableFedifyRuntimeIntegration: config.enableFedifyRuntimeIntegration,
      });
    }

    if (
      config.enableAtprotoReportForwarder
      && moderationReportCaseStore
      && config.enableCanonicalEventLog
    ) {
      atprotoReportForwardingService = new AtprotoReportForwardingService(
        moderationReportCaseStore,
        {
          requestTimeoutMs: Number.parseInt(
            process.env["ATPROTO_REPORT_REQUEST_TIMEOUT_MS"] || "10000",
            10,
          ),
          requestRetries: Number.parseInt(
            process.env["ATPROTO_REPORT_REQUEST_RETRIES"] || "2",
            10,
          ),
          requestRetryBaseMs: Number.parseInt(
            process.env["ATPROTO_REPORT_REQUEST_RETRY_BASE_MS"] || "250",
            10,
          ),
          requestRetryMaxMs: Number.parseInt(
            process.env["ATPROTO_REPORT_REQUEST_RETRY_MAX_MS"] || "2500",
            10,
          ),
        },
        {
          info: (message, meta) => logger.info(meta || {}, message),
          warn: (message, meta) => logger.warn(meta || {}, message),
          error: (message, meta) => logger.error(meta || {}, message),
        },
      );
      logger.info("ATProto report forwarding service initialized");
    } else if (config.enableAtprotoReportForwarder) {
      logger.warn("ATProto report forwarding requested but prerequisites were not met", {
        hasActivityPodsUrl: Boolean(process.env["ACTIVITYPODS_URL"]),
        hasActivityPodsToken: Boolean(process.env["ACTIVITYPODS_TOKEN"]),
        enableCanonicalEventLog: config.enableCanonicalEventLog,
      });
    }

    // Initialize RedPanda producer only when worker/indexer features need it.
    let redpanda: any = null;
    if (redpandaProducerRequired) {
      redpanda = createRedPandaProducer();
      startupTasks.push({
        name: "connect RedPanda producer",
        start: async () => {
          await redpanda.connect();
          logger.info("RedPanda producer connected");
        },
      });
    } else {
      logger.info("RedPanda producer skipped (worker delivery pipeline disabled)");
    }

    // Initialize Fedify 2.x runtime adapter (feature-flagged)
    if (config.enableFedifyRuntimeIntegration) {
      const fedifyRedis = new Redis(process.env["REDIS_URL"] ?? "redis://localhost:6379");
      fedifyRedis.on("error", (err: Error) =>
        logger.error("Fedify KV Redis error", { error: err.message }),
      );
      const kv = new FedifyKvAdapter(fedifyRedis);

      // Local signing service for sidecar-owned service actors (e.g. relay).
      // Key pairs are generated on first use and persisted in Redis.
      const localSigningRedis = new Redis(process.env["REDIS_URL"] ?? "redis://localhost:6379");
      localSigningRedis.on("error", (err: Error) =>
        logger.error("Local signing Redis error", { error: err.message }),
      );
      const providerActorKeyAliases = new Map<string, string>(
        PROVIDER_ACTOR_LEGACY_IDENTIFIERS.map((id) => [id, PROVIDER_ACTOR_IDENTIFIER]),
      );
      const localSigningService = new SidecarLocalSigningService(localSigningRedis, {
        keyAliases: providerActorKeyAliases,
      });
      const sidecarServiceActors = ["relay"];
      if (config.enableProviderActor) {
        // Register canonical "provider" and legacy "moderation" so the actor
        // dispatcher serves both /users/provider and /users/moderation with
        // locally-managed key pairs, independent of whether outbound forwarding
        // is also enabled.
        for (const id of ALL_PROVIDER_ACTOR_IDENTIFIERS) {
          sidecarServiceActors.push(id);
        }
      }

      fedifyAdapter = createFedifyAdapter(
        kv,
        {
          domain: config.domain,
          activityPodsUrl: process.env["ACTIVITYPODS_URL"] ?? "http://localhost:3000",
          activityPodsToken: process.env["ACTIVITYPODS_TOKEN"] ?? "",
          requestTimeoutMs: Number.parseInt(process.env["REQUEST_TIMEOUT_MS"] || "30000", 10),
          userAgent: process.env["USER_AGENT"] || "Fedify-Sidecar/1.0 (ActivityPods)",
          localSigningService,
          sidecarServiceActors,
          ...(activityPubReportForwardingService
            ? {
                onModerationReportDelivered: async ({ meta, targetDomain, statusCode }) => {
                  await activityPubReportForwardingService!.markDelivered(meta, {
                    targetDomain,
                    statusCode,
                  });
                },
                onModerationReportFailed: async ({
                  meta,
                  targetDomain,
                  targetInbox,
                  statusCode,
                  error,
                  responseBody,
                  attempt,
                }) => {
                  await activityPubReportForwardingService!.markFailed(meta.caseId, meta, {
                    error,
                    targetDomain,
                    targetInbox,
                    statusCode,
                    responseBody,
                    attempt,
                    moderationActorUri: moderationActorUri ?? undefined,
                  });
                },
              }
            : {}),
          enqueueVerifiedInbox: async (delivery) => {
            if (!queue) {
              throw new Error("Redis queue not initialized");
            }

            const envelope = createVerifiedInboundEnvelope({
              path: delivery.path,
              body: delivery.body,
              remoteIp: delivery.remoteIp,
              verifiedActorUri: delivery.verifiedActorUri,
              verifiedAt: delivery.verifiedAt,
            });
            await queue.enqueueInbound(envelope);
            return { envelopeId: envelope.envelopeId };
          },
        },
        logger,
      );
      logger.info("Fedify runtime adapter initialized", { domain: config.domain });
    }

    // Initialize AP relay subscription service (requires Fedify + outbox intent worker)
    let relayLocalActorPath: string | null = null;
    if (
      config.enableFedifyRuntimeIntegration &&
      config.enableOutboxIntentWorker &&
      config.apRelayActorUrls.length > 0
    ) {
      const relayLocalActorUri =
        config.apRelayLocalActorUri || `https://${config.domain}/users/relay`;
      // Derive the path portion (e.g. "/users/relay") for the inbound bypass.
      try {
        relayLocalActorPath = new URL(relayLocalActorUri).pathname;
      } catch {
        // ignore malformed URI
      }
      const relayRedis = new Redis(process.env["REDIS_URL"] ?? "redis://localhost:6379");
      relayRedis.on("error", (err: Error) =>
        logger.error("Relay subscription Redis error", { error: err.message }),
      );
      apRelaySubscriptionService = new ApRelaySubscriptionService(
        relayRedis,
        queue!,
        {
          relayActorUrls: config.apRelayActorUrls,
          localActorUri: relayLocalActorUri,
          domain: config.domain,
          resubscribeIntervalMs: config.apRelayResubscribeIntervalMs,
          userAgent: process.env["USER_AGENT"] || "Fedify-Sidecar/1.0 (ActivityPods)",
        },
        logger,
      );
      startupTasks.push({
        name: "start AP relay subscription service",
        start: async () => {
          await apRelaySubscriptionService!.start();
          logger.info("AP relay subscription service started", {
            relays: config.apRelayActorUrls,
            localActorUri: relayLocalActorUri,
          });
        },
      });
    } else if (config.apRelayActorUrls.length > 0 && !config.enableFedifyRuntimeIntegration) {
      logger.warn(
        "AP_RELAY_ACTOR_URLS configured but ENABLE_FEDIFY_RUNTIME_INTEGRATION is false — relay subscription disabled",
      );
    }

    // Initialize OpenSearch indexer (dedicated search indexer service)
    if (config.enableOpenSearchIndexer) {
      searchIndexerRedis = new Redis(process.env["REDIS_URL"] ?? "redis://localhost:6379");
      searchIndexerRedis.on("error", (err: Error) =>
        logger.error("Search indexer Redis client error", { error: err.message }),
      );
      opensearchIndexer = createSearchIndexerService({
        redis: searchIndexerRedis as any,
        searchBackend: config.searchBackend as 'opensearch' | 'qdrant' | 'dual',
      });
      if (config.searchBackend === "opensearch" || config.searchBackend === "dual") {
        startupTasks.push({
          name: "bootstrap OpenSearch indices and pipelines",
          start: async () => {
            const bootstrapConfig = createOpenSearchBootstrapConfig();
            const bootstrap = new OpenSearchBootstrapService(bootstrapConfig);
            try {
              await bootstrap.bootstrap();
            } finally {
              await bootstrap.close();
            }
          },
        });
      }
      if (config.searchBackend === "qdrant" || config.searchBackend === "dual") {
        startupTasks.push({
          name: "bootstrap Qdrant collection and payload indexes",
          start: async () => {
            const bootstrap = new QdrantBootstrapService(createQdrantBootstrapConfig());
            await bootstrap.bootstrap();
          },
        });
      }
      startupTasks.push({
        name: "start search indexer",
        start: async () => {
          await opensearchIndexer!.initialize();
          opensearchIndexer!.start().catch(err => {
            logger.error("Search indexer error", {
              backend: config.searchBackend,
              error: err.message,
            });
          });
          logger.info("Search indexer started", {
            backend: config.searchBackend,
          });
        },
      });
    }

    // Initialize PostgreSQL cache database schema
    startupTasks.push({
      name: "bootstrap postgres database schema",
      start: async () => {
        await bootstrapDb();
      },
    });

    // Start background post sync worker
    postSyncWorker = new PostSyncWorker();
    startupTasks.push({
      name: "start post sync worker",
      start: async () => {
        postSyncWorker!.start();
        logger.info("PostSyncWorker started");
      },
    });

    // Initialize outbound worker
    if (config.enableOutboundWorker) {
      outboundWorker = createOutboundWorker(queue, signingClient, redpanda, {
        capabilityGate,
        fedifyRuntimeIntegrationEnabled: config.enableFedifyRuntimeIntegration,
        ...(fedifyAdapter ? { adapter: fedifyAdapter } : {}),
        ...(followersSyncService ? {
          followersSyncService,
          domain: config.domain,
        } : {}),
      });
      startupTasks.push({
        name: "start outbound worker",
        start: async () => {
          outboundWorker!.start().catch(err => {
            logger.error("Outbound worker error", { error: err.message });
          });
          logger.info("Outbound worker started");
        },
      });
    }

    if (config.enableOutboxIntentWorker) {
      // Dedicated Redis connection for the sharedInbox cache so its error
      // handler never pollutes the queue or idempotency Redis clients.
      const sharedInboxCacheRedis = new Redis(process.env["REDIS_URL"] ?? "redis://localhost:6379");
      sharedInboxCacheRedis.on("error", (err: Error) =>
        logger.error("SharedInbox cache Redis error", { error: err.message }),
      );
      const sharedInboxCache = new RemoteSharedInboxCache(
        sharedInboxCacheRedis,
        process.env["USER_AGENT"] || "Fedify-Sidecar/1.0 (ActivityPods)",
      );
      logger.info("Remote sharedInbox cache initialized");

      outboxIntentWorker = createOutboxIntentWorker(queue, redpanda, {
        activityPubOutboundDeliveryPolicy,
        sharedInboxCache,
      });
      startupTasks.push({
        name: "start outbox intent worker",
        start: async () => {
          outboxIntentWorker!.start().catch((err) => {
            logger.error("Outbox intent worker error", { error: err.message });
          });
          logger.info("Outbox intent worker started");
        },
      });
    }

    if (config.enableOriginReconciliation) {
      originReconciliationWorker = createOriginReconciliationWorker(queue, signingClient, {
        signerActorUri: config.apRelayLocalActorUri || `https://${config.domain}/users/relay`,
      });
      startupTasks.push({
        name: "start origin reconciliation worker",
        start: async () => {
          originReconciliationWorker!.start().catch((err) => {
            logger.error("Origin reconciliation worker error", { error: err.message });
          });
          logger.info("Origin reconciliation worker started");
        },
      });
    }

    // Initialize inbound worker
    if (config.enableInboundWorker) {
      const sidecarActorPaths = new Set<string>();
      if (relayLocalActorPath) {
        sidecarActorPaths.add(relayLocalActorPath);
      }
      if (config.enableProviderActor) {
        // Register sender-URI paths for all provider actor identifiers.
        // The inbound worker uses these to detect activities whose *sender*
        // is a provider actor (e.g. relay-reflected or loopback) and fast-path
        // them to Stream2 without forwarding to ActivityPods.
        for (const id of ALL_PROVIDER_ACTOR_IDENTIFIERS) {
          sidecarActorPaths.add(`/users/${id}`);
        }
      }

      // Durable Redis idempotency guard — one dedicated connection so its
      // error handler never pollutes other Redis clients.
      const idempotencyRedis = new Redis(process.env["REDIS_URL"] ?? "redis://localhost:6379");
      idempotencyRedis.on("error", (err: Error) =>
        logger.error("Inbound idempotency Redis error", { error: err.message }),
      );
      const inboundIdempotencyGuard = new InboundIdempotencyGuard(idempotencyRedis);
      logger.info("Inbound idempotency guard initialized");

      // Provider-level Announce aggregator — deduplicates boosts by (actor, object)
      // within a 24-hour window, on top of the activity-ID idempotency guard.
      const announceAggregatorRedis = new Redis(process.env["REDIS_URL"] ?? "redis://localhost:6379");
      announceAggregatorRedis.on("error", (err: Error) =>
        logger.error("Announce aggregator Redis error", { error: err.message }),
      );
      const announceAggregator = new ProviderAnnounceGuard(announceAggregatorRedis);
      logger.info("Provider-level Announce aggregator initialized");

      const contentFingerprintRedis = new Redis(process.env["REDIS_URL"] ?? "redis://localhost:6379");
      contentFingerprintRedis.on("error", (err: Error) =>
        logger.error("Content fingerprint Redis error", { error: err.message }),
      );
      const contentFingerprintStore = new ContentFingerprintGuard(contentFingerprintRedis);
      logger.info("Content fingerprint spam guard initialized");

      const domainReputationRedis = new Redis(process.env["REDIS_URL"] ?? "redis://localhost:6379");
      domainReputationRedis.on("error", (err: Error) =>
        logger.error("Domain reputation Redis error", { error: err.message }),
      );
      domainReputationStore = new RedisDomainReputationStore(domainReputationRedis);
      logger.info("Domain reputation store initialized");

      spamEvaluator = new SpamEvaluator(
        () => mrfAdminStore,
        contentFingerprintStore,
        domainReputationStore,
      );

      // Provider inbox event client — forwards non-Flag provider-directed AP
      // activities (Undo{Flag}, Accept, Reject, generic) to ActivityPods so the
      // operator can act on incoming federation signals.  Only active when the
      // provider actor is enabled and ACTIVITYPODS_URL + ACTIVITYPODS_TOKEN are set.
      const providerInboxEventClient = config.enableProviderActor &&
        process.env["ACTIVITYPODS_URL"] && process.env["ACTIVITYPODS_TOKEN"]
        ? new ActivityPodsProviderInboxEventClient({
            baseUrl: process.env["ACTIVITYPODS_URL"],
            bearerToken: process.env["ACTIVITYPODS_TOKEN"],
          })
        : undefined;

      if (providerInboxEventClient) {
        logger.info("Provider inbox event client initialized");
      }

      const providerActorUriSet = config.enableProviderActor
        ? buildProviderActorUriSet(config.domain)
        : undefined;

      inboundWorker = createInboundWorker(queue, redpanda, {
        capabilityGate,
        fedifyRuntimeIntegrationEnabled: config.enableFedifyRuntimeIntegration,
        ...(fedifyAdapter ? { adapter: fedifyAdapter } : {}),
        ...(sidecarActorPaths.size > 0 ? { sidecarActorPaths } : {}),
        inboundIdempotencyGuard,
        announceAggregator,
        ...(spamEvaluator ? { spamEvaluator } : {}),
        ...(process.env["MEMORY_AP_WEBHOOK_URL"] ? {
          apRemoteWebhookUrl: process.env["MEMORY_AP_WEBHOOK_URL"],
          apRemoteWebhookSecret: process.env["AP_BRIDGE_SECRET"] ?? "",
        } : {}),
        getMrfAdminStore: () => mrfAdminStore,
        getModerationBridgeStore: () => moderationBridgeStore,
        resolveWebIdForActorUri: async (actorUri: string) => {
          if (!identityRepo) return null;
          const binding = await identityRepo.getByActivityPubActorUri(actorUri);
          return binding?.webId ?? null;
        },
        ...(followersSyncService ? {
          followersSyncService,
          followersSyncSigningClient: signingClient,
          domain: config.domain,
        } : {}),
        ...(repliesBackfillService ? { repliesBackfillService } : {}),
        ...(originReconciliationService ? { originReconciliationService } : {}),
        ...(providerActorUriSet ? {
          providerActorUris: providerActorUriSet,
          providerActorInboxPaths: PROVIDER_ACTOR_INBOX_PATHS,
        } : {}),
        ...(providerInboxEventClient ? { providerInboxEventClient } : {}),
      });
      startupTasks.push({
        name: "start inbound worker",
        start: async () => {
          inboundWorker!.start().catch(err => {
            logger.error("Inbound worker error", { error: err.message });
          });
          logger.info("Inbound worker started");
        },
      });
    }

    if (config.enableMediaAssetSync) {
      mediaAssetSyncConsumer = new MediaAssetSyncConsumer(
        {
          brokers: (process.env["REDPANDA_BROKERS"] || "localhost:9092")
            .split(",")
            .map((broker) => broker.trim())
            .filter(Boolean),
          clientId: process.env["REDPANDA_CLIENT_ID"] || "fedify-sidecar",
          consumerGroupId: `${config.protocolBridgeConsumerGroupId}-media-assets`,
          mediaAssetTopic: config.mediaAssetTopic,
          activityPodsBaseUrl: process.env["ACTIVITYPODS_URL"] || "http://localhost:3000",
          activityPodsBearerToken: process.env["ACTIVITYPODS_TOKEN"] || "",
        },
        {
          info: (message, meta) => logger.info(meta || {}, message),
          warn: (message, meta) => logger.warn(meta || {}, message),
          error: (message, meta) => logger.error(meta || {}, message),
        },
        async (event) => {
          const bindingUrl = event.bindings?.activitypub?.url;
          const activityId = bindingUrl || event.asset.canonicalUrl;
          return evaluateMediaSignalPolicy(mrfAdminStore, {
            activityId,
            originHost: bindingUrl ? new URL(bindingUrl).host : undefined,
            actorId: event.asset.ownerId,
            visibility: "public",
            signals: event.signals ?? null,
          });
        },
      );
      startupTasks.push({
        name: "start media asset sync consumer",
        start: async () => {
          await mediaAssetSyncConsumer!.start();
          logger.info("Media asset sync consumer started", {
            topic: config.mediaAssetTopic,
          });
        },
      });
    }

    // Create HTTP server
    const app = Fastify({
      logger: false,
      trustProxy: true,
      bodyLimit: 1024 * 1024,  // 1MB
    });
    let xrpcServerForWebSocket: any = null;

    app.addHook("onRequest", async (request, reply) => {
      const locale = resolveLocale(request.headers["accept-language"]);
      applyLocaleHeaders(reply, locale);
    });

    // Raw body parser for signature verification
    app.addContentTypeParser(
      ["application/activity+json", "application/ld+json", "application/json"],
      { parseAs: "string" },
      (req, body, done) => {
        done(null, body);
      }
    );

    // Health check endpoint
    app.get("/health", async () => {
      return {
        status: "ok",
        version: config.version,
        uptime: process.uptime(),
        startup: snapshotStartupState(),
        capabilities: providerCapabilitiesDocument.capabilities.map((cap) => ({
          id: cap.id,
          status: cap.status,
        })),
      };
    });

    if (config.enableProviderCapabilitiesEndpoint) {
      app.get("/.well-known/provider-capabilities", async (request, reply) => {
        const currentEtag = `"${providerCapabilitiesResponse.etag}"`;
        const ifNoneMatch = request.headers["if-none-match"];
        const normalizedIfNoneMatch = typeof ifNoneMatch === "string"
          ? ifNoneMatch.split(",").map((value) => value.trim())
          : [];

        if (normalizedIfNoneMatch.includes(currentEtag) || normalizedIfNoneMatch.includes(providerCapabilitiesResponse.etag)) {
          reply.code(304);
          reply.header("etag", currentEtag);
          reply.header("cache-control", "public, max-age=60, stale-while-revalidate=300");
          return reply.send();
        }

        reply.header("content-type", "application/json; charset=utf-8");
        reply.header("cache-control", "public, max-age=60, stale-while-revalidate=300");
        reply.header("etag", currentEtag);
        return reply.send(providerCapabilitiesResponse.body);
      });
    }

    if (config.enableEntryway) {
      const entrywayProviders = buildEntrywayProvidersFromEnv(process.env, {
        activityPodsUrl: process.env["ACTIVITYPODS_URL"],
        activityPodsToken: process.env["ACTIVITYPODS_TOKEN"],
        appClientId: process.env["ENTRYWAY_APP_CLIENT_ID"],
        origin: process.env["ENTRYWAY_APP_ORIGIN"] || `https://${config.domain}`,
        redirectUri: process.env["ENTRYWAY_REDIRECT_URI"],
      });

      if (entrywayProviders.length === 0) {
        throw new Error("ENABLE_ENTRYWAY requires at least one configured Entryway provider");
      }

      entrywayRedisClient = new Redis(config.entrywayRedisUrl);
      entrywayRedisClient.on("error", (err: Error) =>
        logger.error("Entryway Redis error", { error: err.message }),
      );

      const entrywayProviderClient = new ActivityPodsProvisioningClient({
        timeoutMs: config.entrywayRequestTimeoutMs,
        retryPolicy: {
          maxAttempts: config.entrywayRequestMaxAttempts,
          baseDelayMs: 150,
          maxDelayMs: 3_000,
        },
        userAgent: process.env["USER_AGENT"] || "ActivityPods-Entryway/1.0",
      });
      const entrywayService = new EntrywayProvisioningService({
        store: new RedisAccountRouteStore(entrywayRedisClient, config.entrywayRedisPrefix),
        providerRouter: new StaticEntrywayProviderRouter(entrywayProviders),
        providerClient: entrywayProviderClient,
        providerPreflight: new ProviderCapabilitiesPreflight(entrywayProviderClient),
        appBootstrapper: new ActivityPodsAppBootstrapClient({
          timeoutMs: config.entrywayAppBootstrapTimeoutMs,
          retryPolicy: {
            maxAttempts: config.entrywayAppBootstrapMaxAttempts,
            baseDelayMs: 150,
            maxDelayMs: 3_000,
          },
          userAgent: process.env["USER_AGENT"] || "ActivityPods-Entryway/1.0",
        }),
        verifier: new AccountBundleVerifier({
          timeoutMs: config.entrywayVerificationTimeoutMs,
          retryPolicy: {
            maxAttempts: config.entrywayVerificationMaxAttempts,
            baseDelayMs: 150,
            maxDelayMs: 2_500,
          },
          strict: config.entrywayVerificationStrict,
        }),
        fingerprintSecret: config.entrywayFingerprintSecret,
        staleProvisioningAfterMs: config.entrywayStaleProvisioningAfterMs,
      });

      registerEntrywayFastifyRoutes(app, {
        service: entrywayService,
        entrywayToken: config.entrywayToken,
        allowPublicResolve: config.entrywayPublicResolve,
      });

      entrywayRecoveryWorker = new EntrywayRecoveryWorker(
        entrywayService,
        {
          intervalMs: config.entrywayRecoveryIntervalMs,
          initialDelayMs: config.entrywayRecoveryInitialDelayMs,
          batchLimit: config.entrywayRecoveryBatchLimit,
        },
        {
          info: (message, meta) => logger.info(meta || {}, message),
          warn: (message, meta) => logger.warn(meta || {}, message),
          error: (message, meta) => logger.error(meta || {}, message),
        },
      );
      entrywayRecoveryWorker.start();

      logger.info("Entryway routes enabled", {
        providers: entrywayProviders.map((provider) => provider.providerId),
        publicResolve: config.entrywayPublicResolve,
        strictVerification: config.entrywayVerificationStrict,
        recoveryIntervalMs: config.entrywayRecoveryIntervalMs,
      });
    }

    // Readiness check endpoint
    app.get("/ready", async (_request, reply) => {
      if (!queue) {
        reply.code(503);
        return {
          status: "not_ready",
          reason: "Queue not initialized",
          startup: snapshotStartupState(),
        };
      }

      const startup = snapshotStartupState();
      if (startup.phase !== "ready") {
        reply.code(503);
        return {
          status: startup.phase === "failed" ? "startup_failed" : "starting",
          startup,
        };
      }
      
      const [
        outboundPending,
        inboundPending,
        outboxIntentPending,
        originReconcilePending,
        dlqInboundLen,
        dlqOutboundLen,
        dlqOutboxIntentLen,
        dlqOriginReconcileLen,
      ] = await Promise.all([
        queue.getPendingCount("outbound"),
        queue.getPendingCount("inbound"),
        queue.getPendingCount("outbox_intent"),
        queue.getPendingCount("origin_reconcile"),
        queue.getDlqLength("inbound"),
        queue.getDlqLength("outbound"),
        queue.getDlqLength("outbox_intent"),
        queue.getDlqLength("origin_reconcile"),
      ]);

      return {
        status: "ready",
        startup,
        queues: {
          outbound: { pending: outboundPending },
          inbound: { pending: inboundPending },
          outboxIntent: { pending: outboxIntentPending },
          originReconcile: { pending: originReconcilePending },
          dlq: {
            inbound: dlqInboundLen,
            outbound: dlqOutboundLen,
            outboxIntent: dlqOutboxIntentLen,
            originReconcile: dlqOriginReconcileLen,
          },
        },
        workers: {
          outbound: config.enableOutboundWorker,
          inbound: config.enableInboundWorker,
          outboxIntent: config.enableOutboxIntentWorker,
          originReconcile: config.enableOriginReconciliation,
          opensearch: config.enableOpenSearchIndexer,
        },
      };
    });

    // Metrics endpoint (Prometheus format)
    app.get("/metrics", async () => {
      if (!queue) {
        return "# Queue not initialized\n";
      }
      
      const [
        outboundPending,
        inboundPending,
        outboxIntentPending,
        originReconcilePending,
        outboundLength,
        inboundLength,
        outboxIntentLength,
        originReconcileLength,
      ] = await Promise.all([
        queue.getPendingCount("outbound"),
        queue.getPendingCount("inbound"),
        queue.getPendingCount("outbox_intent"),
        queue.getPendingCount("origin_reconcile"),
        queue.getStreamLength("outbound"),
        queue.getStreamLength("inbound"),
        queue.getStreamLength("outbox_intent"),
        queue.getStreamLength("origin_reconcile"),
      ]);
      const [dlqInboundLength, dlqOutboundLength, dlqOutboxIntentLength, dlqOriginReconcileLength] = await Promise.all([
        queue.getDlqLength("inbound"),
        queue.getDlqLength("outbound"),
        queue.getDlqLength("outbox_intent"),
        queue.getDlqLength("origin_reconcile"),
      ]);

      promMetrics.queueDepth.set({ topic: "outbound" }, outboundLength);
      promMetrics.queueDepth.set({ topic: "inbound" }, inboundLength);
      promMetrics.queueDepth.set({ topic: "outbox_intent" }, outboxIntentLength);
      promMetrics.queueDepth.set({ topic: "origin_reconcile" }, originReconcileLength);
      const prometheusMetrics = (await renderPrometheusMetrics()).trimEnd();

      return [
        prometheusMetrics,
        `# HELP fedify_outbound_pending Number of pending outbound jobs`,
        `# TYPE fedify_outbound_pending gauge`,
        `fedify_outbound_pending ${outboundPending}`,
        `# HELP fedify_inbound_pending Number of pending inbound envelopes`,
        `# TYPE fedify_inbound_pending gauge`,
        `fedify_inbound_pending ${inboundPending}`,
        `# HELP fedify_outbox_intent_pending Number of pending outbox intents`,
        `# TYPE fedify_outbox_intent_pending gauge`,
        `fedify_outbox_intent_pending ${outboxIntentPending}`,
        `# HELP fedify_origin_reconcile_pending Number of pending origin reconciliation jobs`,
        `# TYPE fedify_origin_reconcile_pending gauge`,
        `fedify_origin_reconcile_pending ${originReconcilePending}`,
        `# HELP fedify_outbound_stream_length Total outbound stream length`,
        `# TYPE fedify_outbound_stream_length gauge`,
        `fedify_outbound_stream_length ${outboundLength}`,
        `# HELP fedify_inbound_stream_length Total inbound stream length`,
        `# TYPE fedify_inbound_stream_length gauge`,
        `fedify_inbound_stream_length ${inboundLength}`,
        `# HELP fedify_outbox_intent_stream_length Total outbox intent stream length`,
        `# TYPE fedify_outbox_intent_stream_length gauge`,
        `fedify_outbox_intent_stream_length ${outboxIntentLength}`,
        `# HELP fedify_origin_reconcile_stream_length Total origin reconciliation stream length`,
        `# TYPE fedify_origin_reconcile_stream_length gauge`,
        `fedify_origin_reconcile_stream_length ${originReconcileLength}`,
        `# HELP fedify_dlq_inbound_length Inbound DLQ entries (signature failures, validation errors)`,
        `# TYPE fedify_dlq_inbound_length gauge`,
        `fedify_dlq_inbound_length ${dlqInboundLength}`,
        `# HELP fedify_dlq_outbound_length Outbound DLQ entries (exhausted/deferred delivery jobs)`,
        `# TYPE fedify_dlq_outbound_length gauge`,
        `fedify_dlq_outbound_length ${dlqOutboundLength}`,
        `# HELP fedify_dlq_outbox_intent_length Outbox-intent DLQ entries`,
        `# TYPE fedify_dlq_outbox_intent_length gauge`,
        `fedify_dlq_outbox_intent_length ${dlqOutboxIntentLength}`,
        `# HELP fedify_dlq_origin_reconcile_length Origin reconciliation DLQ entries`,
        `# TYPE fedify_dlq_origin_reconcile_length gauge`,
        `fedify_dlq_origin_reconcile_length ${dlqOriginReconcileLength}`,
        `# HELP fedify_uptime_seconds Uptime in seconds`,
        `# TYPE fedify_uptime_seconds gauge`,
        `fedify_uptime_seconds ${Math.floor(process.uptime())}`,
        ...renderOAuthSecurityMetricsLines(),
      ].filter((line) => line.length > 0).join("\n") + "\n";
    });

    const openSearchReader =
      config.searchBackend === "opensearch"
        ? new OpenSearchNativeClient(
            process.env["OPENSEARCH_USERNAME"]
              ? {
                  node: process.env["OPENSEARCH_URL"] ?? "http://localhost:9200",
                  auth: {
                    username: process.env["OPENSEARCH_USERNAME"],
                    password: process.env["OPENSEARCH_PASSWORD"] ?? "",
                  },
                  ssl: {
                    rejectUnauthorized: process.env["OPENSEARCH_SSL_VERIFY"] !== "false",
                  },
                }
              : {
                  node: process.env["OPENSEARCH_URL"] ?? "http://localhost:9200",
                  ssl: {
                    rejectUnauthorized: process.env["OPENSEARCH_SSL_VERIFY"] !== "false",
                  },
                },
          )
        : null;

    const qdrantReadConfig = {
      baseUrl: process.env["QDRANT_URL"] ?? "http://localhost:6333",
      apiKey: process.env["QDRANT_API_KEY"],
      collectionName: process.env["QDRANT_COLLECTION_NAME"] ?? "public-content-v1",
      requestTimeoutMs: parseInt(process.env["QDRANT_REQUEST_TIMEOUT_MS"] ?? "5000", 10),
    };

    const feedDefinitions: FeedDefinition[] = [
      {
        id: "urn:activitypods:feed:public-discovery:v1",
        kind: "discovery",
        visibility: "public",
        title: "Public Discovery",
        description: "Public relay and canonical-discovery feed skeletons",
        sourcePolicy: {
          includeStream1: false,
          includeStream2: true,
          includeCanonical: true,
          includeFirehose: false,
          includeUnified: true,
        },
        rankingPolicy: { mode: "ranked", providerHint: "search-feed-candidates" },
        hydrationShape: "card",
        realtimeCapable: true,
        supportsSse: true,
        supportsWebSocket: true,
        experimental: false,
        providerId: "search.candidates.v1",
      },
      {
        id: "urn:activitypods:feed:graph-personalized:v1",
        kind: "graph",
        visibility: "authenticated",
        title: "Graph Personalized",
        description: "Viewer-scoped graph feed skeletons",
        sourcePolicy: {
          includeStream1: true,
          includeStream2: true,
          includeCanonical: true,
          includeFirehose: false,
          includeUnified: true,
        },
        rankingPolicy: { mode: "blended", providerHint: "search-feed-candidates" },
        hydrationShape: "card",
        realtimeCapable: true,
        supportsSse: true,
        supportsWebSocket: true,
        experimental: false,
        providerId: "search.candidates.v1",
      },
      {
        id: "urn:activitypods:feed:topic:v1",
        kind: "topic",
        visibility: "public",
        title: "Topic",
        description: "Topic-filtered public feed skeletons",
        sourcePolicy: {
          includeStream1: false,
          includeStream2: true,
          includeCanonical: true,
          includeFirehose: false,
          includeUnified: true,
        },
        rankingPolicy: { mode: "blended", providerHint: "search-feed-candidates" },
        hydrationShape: "card",
        realtimeCapable: true,
        supportsSse: true,
        supportsWebSocket: true,
        experimental: false,
        providerId: "search.candidates.v1",
      },
    ];

    const feedRegistry = new FeedRegistry(feedDefinitions);
    const feedProvider =
      config.searchBackend === "opensearch"
        ? new OpenSearchFeedProvider(
            openSearchReader as any,
            new DefaultFeedCandidateService(openSearchReader as any),
          )
        : new QdrantFeedProvider(
            qdrantReadConfig,
            new QdrantFeedCandidateService(new QdrantDocumentStore(qdrantReadConfig)),
          );
    const feedService = new DefaultPodFeedService(
      feedRegistry,
      new Map([["search.candidates.v1", feedProvider]]),
      {
        maxAttempts: 3,
        initialDelayMs: 150,
        maxDelayMs: 2_000,
      },
    );
    const hydrationService = new DefaultPodHydrationService(
      new Map([
        [
          "default",
          config.searchBackend === "opensearch"
            ? new OpenSearchHydrator(openSearchReader as any)
            : new QdrantHydrator(qdrantReadConfig),
        ],
      ]),
      {
        concurrency: 4,
        maxAttempts: 3,
        initialDelayMs: 150,
        maxDelayMs: 2_000,
      },
    );
    // Build stream capabilities by scanning feed definitions for SSE/WS support per source.
    const sourceSseSupport = new Map<string, boolean>();
    const sourceWsSupport = new Map<string, boolean>();
    const sourceToPolicy: Array<[string, keyof typeof feedDefinitions[0]["sourcePolicy"]]> = [
      ["stream1", "includeStream1"],
      ["stream2", "includeStream2"],
      ["canonical", "includeCanonical"],
      ["firehose", "includeFirehose"],
      ["unified", "includeUnified"],
    ];
    for (const def of feedDefinitions) {
      for (const [source, policyKey] of sourceToPolicy) {
        if (def.sourcePolicy[policyKey]) {
          if (def.supportsSse) sourceSseSupport.set(source, true);
          if (def.supportsWebSocket) sourceWsSupport.set(source, true);
        }
      }
    }
    const streamCapabilities: DurableStreamCapability[] = (["stream1", "stream2", "canonical", "firehose", "unified"] as const)
      .filter((s) => sourceSseSupport.has(s) || sourceWsSupport.has(s))
      .map((s) => ({
        stream: s,
        supportsSse: sourceSseSupport.get(s) ?? false,
        supportsWebSocket: sourceWsSupport.get(s) ?? false,
        requiresAuthentication: false,
        replayCapable: false,
      }));
    streamSubscriptionService = new DurableStreamSubscriptionService({
      sidecarToken: config.sidecarToken,
      capabilityLookup: buildCapabilityLookup(streamCapabilities),
    });
    streamSubscriptionService.start();

    feedStreamKafkaConsumer = new FeedStreamKafkaConsumer(
      {
        brokers: (process.env["REDPANDA_BROKERS"] || "localhost:9092")
          .split(",")
          .map((b) => b.trim())
          .filter(Boolean),
        clientId: "fedify-feed-stream-consumer",
        groupId: process.env["FEED_STREAM_CONSUMER_GROUP_ID"] || "fedify-feed-stream-consumer-v1",
        stream1Topic: process.env["STREAM1_TOPIC"] || "ap.stream1.local-public.v1",
        stream2Topic: process.env["STREAM2_TOPIC"] || "ap.stream2.remote-public.v1",
        canonicalTopic: config.canonicalTopic,
        firehoseTopic: process.env["FIREHOSE_TOPIC"] || "ap.firehose.v1",
      },
      streamSubscriptionService,
    );
    void feedStreamKafkaConsumer.start().catch((err) => {
      logger.error("FeedStreamKafkaConsumer failed to start", {
        error: err instanceof Error ? err.message : String(err),
      });
    });

    unifiedFeedBridge = new UnifiedFeedBridge(
      {
        brokers: (process.env["REDPANDA_BROKERS"] || "localhost:9092")
          .split(",")
          .map((b) => b.trim())
          .filter(Boolean),
        clientId: "fedify-unified-feed-bridge",
        groupId: process.env["UNIFIED_FEED_BRIDGE_GROUP_ID"] || "fedify-unified-feed-bridge-v1",
        canonicalTopic: config.canonicalTopic,
        stream2Topic: process.env["STREAM2_TOPIC"] || "ap.stream2.remote-public.v1",
      },
      streamSubscriptionService,
    );
    void unifiedFeedBridge.start().catch((err) => {
      logger.error("UnifiedFeedBridge failed to start", {
        error: err instanceof Error ? err.message : String(err),
      });
    });

    const entitlementOverrides = buildEntitlementOverridesFromEnv(process.env);
    const checkStreamEntitlement = (
      transport: "sse" | "websocket",
      currentCount: number,
    ) => {
      const field = transport === "sse" ? "maxSseConnections" : "maxWsConnections";
      const result = checkCapabilityLimit(
        providerCapabilitiesDocument.entitlements.plan,
        "ap.feeds.realtime",
        field,
        currentCount,
        entitlementOverrides,
      );
      return { allowed: result.allowed, effectiveLimit: result.effectiveLimit };
    };

    const activityPodsClient = new ActivityPodsClient(process.env["ACTIVITYPODS_URL"] ?? "http://localhost:3000");

    registerFeedFastifyRoutes(app, {
      sidecarToken: config.sidecarToken,
      feedRegistry,
      feedService,
      hydrationService,
      viewershipHistoryClient: process.env["ACTIVITYPODS_INTERNAL_API_KEY"] ? activityPodsClient : undefined,
      streamSubscriptionService,
      capabilityGate,
      checkStreamEntitlement,
    });

    registerPublicFeedRoutes(app, {
      feedService,
      hydrationService,
      // postSyncWorker is created later, during background startup — a
      // getter reads the module variable at request time instead of
      // capturing its (still-null) value at this registration point.
      getPostSyncWorker: () => postSyncWorker,
    });

    const enableFep3ab2Streaming = (process.env["ENABLE_FEP_3AB2_STREAMING"] ?? "true") !== "false";
    if (enableFep3ab2Streaming) {
      const activityPodsBaseUrl = process.env["ACTIVITYPODS_URL"] || "";
      const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";
      const ticketSecret = process.env["FEP_3AB2_TICKET_SECRET"] || config.sidecarToken;
      if (activityPodsBaseUrl && activityPodsToken && ticketSecret) {
        const allowedOrigins = String(process.env["FEP_3AB2_ALLOWED_ORIGINS"] || "")
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean);
        const sameSiteRaw = String(process.env["FEP_3AB2_COOKIE_SAMESITE"] || "Lax").trim();
        const cookieSameSite = sameSiteRaw === "Strict" || sameSiteRaw === "None" ? sameSiteRaw : "Lax";
        fep3ab2Runtime = new Fep3ab2Runtime({
          app,
          streamSubscriptionService,
          redisUrl: process.env["REDIS_URL"] ?? "redis://localhost:6379",
          activityPodsBaseUrl,
          activityPodsToken,
          ticketSecret,
          publicBaseUrl: process.env["FEP_3AB2_PUBLIC_BASE_URL"],
          allowedOrigins,
          ticketTtlSec: Number.parseInt(process.env["FEP_3AB2_TICKET_TTL_SEC"] || "900", 10),
          heartbeatIntervalMs: Number.parseInt(process.env["FEP_3AB2_HEARTBEAT_INTERVAL_MS"] || "20000", 10),
          cookieName: process.env["FEP_3AB2_COOKIE_NAME"] || "ap_stream_ticket",
          cookiePath: process.env["FEP_3AB2_COOKIE_PATH"] || "/streaming",
          cookieSameSite,
          cookieSecure: process.env["FEP_3AB2_COOKIE_SECURE"] === "false" ? false : undefined,
          cookieDomain: process.env["FEP_3AB2_COOKIE_DOMAIN"] || undefined,
          prefix: process.env["FEP_3AB2_REDIS_PREFIX"] || "fep3ab2",
          privateRealtimeChannel:
            process.env["FEP_3AB2_PRIVATE_REALTIME_CHANNEL"] || "fep3ab2:private-events",
          replayTtlSec: Number.parseInt(process.env["FEP_3AB2_REPLAY_TTL_SEC"] || "900", 10),
          replayMaxEvents: Number.parseInt(process.env["FEP_3AB2_REPLAY_MAX_EVENTS"] || "500", 10),
          replayMaxIndexSize: Number.parseInt(process.env["FEP_3AB2_REPLAY_MAX_INDEX_SIZE"] || "10000", 10),
          maxPendingReplayPublishes:
            Number.parseInt(process.env["FEP_3AB2_MAX_PENDING_REPLAY_PUBLISHES"] || "2048", 10),
          maxStreamBufferBytes:
            Number.parseInt(process.env["FEP_3AB2_MAX_STREAM_BUFFER_BYTES"] || "1048576", 10),
        });
        await fep3ab2Runtime.start();
      } else {
        logger.warn(
          "FEP-3ab2 streaming routes were not started because ACTIVITYPODS_URL or ACTIVITYPODS_TOKEN is missing",
        );
      }
    }

    if (config.enableMrfAdminApi) {
      const registration = await registerMrfAdminIntegration({
        app,
        logger,
        enabled: config.enableMrfAdminApi,
        adminToken: config.mrfAdminToken,
        storeMode: config.mrfAdminStore,
        redisUrl: process.env["REDIS_URL"] ?? "redis://localhost:6379",
        redisPrefix: config.mrfAdminRedisPrefix,
      });
      mrfAdminRedisClient = registration.redisClient;
      mrfAdminStore = registration.store;
    }

    // Domain reputation admin routes — registered independently of MRF admin so
    // the domain blocklist is always manageable even when MRF is in dry-run mode.
    if (config.mrfAdminToken && domainReputationStore) {
      registerSpamDomainAdminRoutes(app, domainReputationStore, config.mrfAdminToken);
    }

    // Keyword rules admin routes — CRUD over the keyword-filter module's rules list.
    if (config.mrfAdminToken && mrfAdminStore) {
      registerKeywordRulesAdminRoutes(app, mrfAdminStore, config.mrfAdminToken);
    }

    // Prewarm MiniLM-L6 embedding model in the background if any semantic keyword
    // rules are active, so the first inbound message does not pay the cold-load cost.
    // Best-effort: failure only means the first semantic evaluation incurs the delay.
    if (mrfAdminStore) {
      void mrfAdminStore.getModuleConfig("keyword-filter")
        .then((cfg) => {
          if (!cfg?.enabled) return;
          const rules = (cfg.config as { rules?: Array<{ semantic?: boolean }> }).rules ?? [];
          if (rules.some((r) => r.semantic === true)) {
            prewarmEmbeddingModel();
            logger.info("Prewarming MiniLM-L6 embedding model (semantic keyword rules active)");
          }
        })
        .catch((err: unknown) => {
          logger.warn("Could not inspect keyword-filter config during startup prewarm", {
            error: err instanceof Error ? err.message : String(err),
          });
        });
    }

    // Shared inbox endpoint
    const enqueueRawInboundRequest = async (
      request: FastifyRequest,
      reply: FastifyReply,
      path: string,
    ): Promise<void> => {
      const locale = resolveLocale(request.headers["accept-language"]);
      if (!queue) {
        reply.status(503).send({
          error: "Service unavailable",
          message: translateLocale(locale, "common.serviceUnavailable"),
        });
        return;
      }

      const envelope = createInboundEnvelope({
        method: "POST",
        path,
        headers: normalizeHeaders(request.headers),
        body: request.body as string,
        remoteIp: request.ip,
      });

      await queue.enqueueInbound(envelope);
      reply.status(202).send({ accepted: true, envelopeId: envelope.envelopeId });
    };

    const enqueueVerifiedBenchmarkInboundRequest = async (
      request: FastifyRequest,
      reply: FastifyReply,
      path: string,
    ): Promise<void> => {
      const locale = resolveLocale(request.headers["accept-language"]);
      if (!queue) {
        reply.status(503).send({
          error: "Service unavailable",
          message: translateLocale(locale, "common.serviceUnavailable"),
        });
        return;
      }

      const authHeader = (request.headers["authorization"] as string) || "";
      const [scheme, token] = authHeader.split(" ");
      if (scheme !== "Bearer" || token !== config.sidecarToken) {
        reply.status(401).send({
          error: "Unauthorized",
          message: translateLocale(locale, "common.unauthorized"),
        });
        return;
      }

      let activity: Record<string, unknown>;
      let serializedBody: string;
      if (typeof request.body === "string") {
        try {
          activity = JSON.parse(request.body) as Record<string, unknown>;
          serializedBody = request.body;
        } catch {
          reply.status(400).send({
            error: "Invalid JSON body",
            message: translateLocale(locale, "common.invalidJsonBody"),
          });
          return;
        }
      } else if (request.body && typeof request.body === "object" && !Array.isArray(request.body)) {
        activity = request.body as Record<string, unknown>;
        serializedBody = JSON.stringify(activity);
      } else {
        reply.status(400).send({
          error: "Invalid JSON body",
          message: translateLocale(locale, "common.invalidJsonBody"),
        });
        return;
      }

      const verifiedActorUri = typeof activity["actor"] === "string" ? activity["actor"] : null;
      if (!verifiedActorUri) {
        reply.status(400).send({
          error: "Missing string actor field",
          message: translateLocale(locale, "common.missingStringActorField"),
        });
        return;
      }

      const envelope = createVerifiedInboundEnvelope({
        path,
        body: serializedBody,
        remoteIp: request.ip,
        verifiedActorUri,
        headers: {
          "x-sidecar-benchmark": "1",
        },
      });

      await queue.enqueueInbound(envelope);
      reply.status(202).send({ accepted: true, envelopeId: envelope.envelopeId });
    };

    const isAuthorizedSidecarInternalRequest = (request: FastifyRequest): boolean => {
      const authHeader = (request.headers["authorization"] as string) || "";
      const [scheme, token] = authHeader.split(" ");
      return scheme === "Bearer" && token === config.sidecarToken;
    };

    if (config.enableApInteropMediaFixtures) {
      app.route({
        method: ["GET", "HEAD"],
        url: "/interop-fixtures/:fixtureName",
        handler: async (request, reply) => {
          const { fixtureName } = request.params as { fixtureName: string };
          const response = resolveApInteropMediaFixtureResponse(
            fixtureName,
            typeof request.headers.range === "string" ? request.headers.range : undefined,
          );
          if (!response) {
            const locale = resolveLocale(request.headers["accept-language"]);
            reply.status(404).send({
              error: "Not Found",
              message: translateLocale(locale, "common.notFound"),
            });
            return;
          }

          recordApInteropMediaFixtureAccess(fixtureName, {
            method: request.method,
            receivedAt: Date.now(),
            userAgent: typeof request.headers["user-agent"] === "string"
              ? request.headers["user-agent"]
              : undefined,
            remoteAddress: typeof request.headers["x-forwarded-for"] === "string"
              ? request.headers["x-forwarded-for"]
              : request.ip,
            range: typeof request.headers.range === "string" ? request.headers.range : undefined,
            accept: typeof request.headers.accept === "string" ? request.headers.accept : undefined,
          });

          for (const [headerName, headerValue] of Object.entries(response.headers)) {
            reply.header(headerName, headerValue);
          }
          reply.status(response.statusCode);
          reply.send(request.method === "HEAD" ? undefined : response.body);
        },
      });

      app.get("/internal/interop/fixtures/:fixtureName/accesses", async (request, reply) => {
        const locale = resolveLocale(request.headers["accept-language"]);
        if (!isAuthorizedSidecarInternalRequest(request)) {
          reply.status(401).send({
            error: "Unauthorized",
            message: translateLocale(locale, "common.unauthorized"),
          });
          return;
        }

        const { fixtureName } = request.params as { fixtureName: string };
        const accesses = listApInteropMediaFixtureAccesses(fixtureName);
        reply.send({
          fixtureName,
          count: accesses.length,
          accesses,
        });
      });

      app.delete("/internal/interop/fixtures/:fixtureName/accesses", async (request, reply) => {
        const locale = resolveLocale(request.headers["accept-language"]);
        if (!isAuthorizedSidecarInternalRequest(request)) {
          reply.status(401).send({
            error: "Unauthorized",
            message: translateLocale(locale, "common.unauthorized"),
          });
          return;
        }

        const { fixtureName } = request.params as { fixtureName: string };
        resetApInteropMediaFixtureAccesses(fixtureName);
        reply.status(204).send();
      });
    }

    // Shared inbox uses Fedify as the primary verifier/router when enabled.
    // Actor-specific inboxes stay on the sidecar-native verifier so the sidecar
    // never needs local private keys to satisfy Fedify's authenticated
    // document-loading path.
    if (!fedifyAdapter) {
      app.post("/inbox", async (request, reply) => {
        await enqueueRawInboundRequest(request, reply, "/inbox");
      });
    }

    // /sharedInbox is an explicit per-pod shared inbox URL that actor documents
    // may advertise.  It normalises to "/inbox" before queuing so ActivityPods
    // always receives the canonical shared-inbox path for recipient resolution.
    // This route is unconditional — when Fedify is active it handles "/inbox"
    // directly; "/sharedInbox" goes through the sidecar-native raw path whose
    // inbound worker performs its own signature verification (fail-closed).
    app.post("/sharedInbox", async (request, reply) => {
      await enqueueRawInboundRequest(request, reply, "/inbox");
    });

    app.post("/users/:username/inbox", async (request, reply) => {
      const { username } = request.params as { username: string };
      await enqueueRawInboundRequest(request, reply, `/users/${username}/inbox`);
    });

    // Internal benchmark-only ingress that enqueues a trusted envelope while
    // preserving the downstream target inbox path for ActivityPods forwarding.
    app.post("/internal/bench/users/:username/inbox", async (request, reply) => {
      const { username } = request.params as { username: string };
      await enqueueVerifiedBenchmarkInboundRequest(
        request,
        reply,
        `/users/${username}/inbox`,
      );
    });

    app.post("/internal/bench/:username/inbox", async (request, reply) => {
      const { username } = request.params as { username: string };
      await enqueueVerifiedBenchmarkInboundRequest(
        request,
        reply,
        `/${username}/inbox`,
      );
    });

    // ── Provider actor well-known paths ────────────────────────────────────────
    // Registered BEFORE the legacy /:username/inbox wildcard so that the literal
    // /actor path is not accidentally captured by that wildcard.
    if (config.enableProviderActor) {
      // GET /actor — Mastodon instance-actor compatibility alias.
      // Served directly (not via redirect) so strict AP implementations that
      // do not follow redirects on actor fetches still receive the document.
      // The actor id field always points to the canonical /users/provider URI.
      // When Fedify is not active, fall back to a 301 redirect (same semantics
      // as before; the adapter is always present in production when the provider
      // actor is enabled, because enableProviderActor requires either
      // ENABLE_MODERATION_BRIDGE_API or ENABLE_MRF_ADMIN_API which in turn
      // require the Fedify runtime).
      if (fedifyAdapter) {
        registerFedifyActorAlias(app, fedifyAdapter, "/actor", `/users/${PROVIDER_ACTOR_IDENTIFIER}`);
      } else {
        app.get("/actor", async (_request, reply) => {
          reply.redirect(`/users/${PROVIDER_ACTOR_IDENTIFIER}`, 301);
        });
      }

      // POST /actor/inbox — explicit route for POST /actor/inbox.
      // Some servers may construct the inbox URL from /actor directly rather
      // than following the canonical /users/provider document.  Normalises to
      // /actor/inbox in the queue so the inbound worker can classify it via
      // PROVIDER_ACTOR_INBOX_PATHS.
      app.post("/actor/inbox", async (request, reply) => {
        await enqueueRawInboundRequest(request, reply, "/actor/inbox");
      });
    }
    // ───────────────────────────────────────────────────────────────────────────

    // Legacy compatibility alias. Canonical actor documents advertise
    // /users/:identifier/inbox, but we keep this fallback route to avoid
    // breaking older callers during the Fedify ingress migration.
    app.post("/:username/inbox", async (request, reply) => {
      const { username } = request.params as { username: string };
      await enqueueRawInboundRequest(request, reply, `/${username}/inbox`);
    });

    // Outbound webhook — receives delivery work from ActivityPods
    app.post("/webhook/outbox", async (request, reply) => {
      const requestStartedAt = Date.now();
      const authHeader = (request.headers["authorization"] as string) || "";
      const [scheme, token] = authHeader.split(" ");
      if (scheme !== "Bearer" || token !== config.sidecarToken) {
        promMetrics.outboundWebhookRequestsTotal.inc({ status: "unauthorized" });
        reply.status(401).send({ error: "Unauthorized" });
        return;
      }
      let body: Record<string, unknown> | null = null;
      if (typeof request.body === "string") {
        try {
          const parsed = JSON.parse(request.body) as unknown;
          if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
            body = parsed as Record<string, unknown>;
          }
        } catch {
          body = null;
        }
      } else if (request.body && typeof request.body === "object" && !Array.isArray(request.body)) {
        body = request.body as Record<string, unknown>;
      }

      const actorUri = body?.["actorUri"];
      const activity = body?.["activity"];
      const remoteTargets = body?.["remoteTargets"];
      if (
        typeof actorUri !== "string"
        || actorUri.length === 0
        || !activity
        || typeof activity !== "object"
        || Array.isArray(activity)
        || !Array.isArray(remoteTargets)
      ) {
        promMetrics.outboundWebhookRequestsTotal.inc({ status: "invalid" });
        reply.status(400).send({ error: "Bad Request" });
        return;
      }

      const bodyRecord = body as Record<string, unknown>;
      const activityRecord = activity as Record<string, unknown>;
      const metaValue = bodyRecord["meta"];
      const normalizedMeta = metaValue && typeof metaValue === "object" && !Array.isArray(metaValue)
        ? metaValue
        : undefined;

      if (!queue || !config.enableOutboxIntentWorker) {
        promMetrics.outboundWebhookRequestsTotal.inc({ status: "unavailable" });
        reply.status(503).send({ error: "Service unavailable" });
        return;
      }

      try {
        const normalizedTargets = normalizeAndDedupeOutboundTargets(
          remoteTargets,
          outboundWebhookBackpressureConfig,
        );
        promMetrics.outboundWebhookTargetCount.observe(normalizedTargets.inputTargetCount);
        if (normalizedTargets.invalidTargetCount > 0) {
          promMetrics.outboundWebhookTargetsDedupedTotal.inc(
            { reason: "invalid" },
            normalizedTargets.invalidTargetCount,
          );
        }
        if (normalizedTargets.duplicateTargetCount > 0) {
          promMetrics.outboundWebhookTargetsDedupedTotal.inc(
            { reason: "duplicate" },
            normalizedTargets.duplicateTargetCount,
          );
        }

        const activityIdValue = bodyRecord["activityId"];
        const activityId = typeof activityIdValue === "string" && activityIdValue.trim().length > 0
          ? activityIdValue
          : typeof activityRecord["id"] === "string" && activityRecord["id"].trim().length > 0
            ? activityRecord["id"]
            : null;
        if (!activityId) {
          throw new OutboundWebhookValidationError(
            "OUTBOUND_ACTIVITY_ID_MISSING",
            400,
            "activityId is required when activity.id is not present.",
          );
        }

        const [outboundPending, outboundLength, outboxIntentPending, outboxIntentLength] = await Promise.all([
          queue.getPendingCount("outbound"),
          queue.getStreamLength("outbound"),
          queue.getPendingCount("outbox_intent"),
          queue.getStreamLength("outbox_intent"),
        ]);
        const backpressure = evaluateOutboundWebhookBackpressure(
          {
            pendingCount: Math.max(outboundPending, outboxIntentPending),
            streamLength: Math.max(outboundLength, outboxIntentLength),
          },
          outboundWebhookBackpressureConfig,
        );

        if (backpressure.reject) {
          promMetrics.outboundWebhookRequestsTotal.inc({ status: "backpressure" });
          promMetrics.outboundWebhookBackpressureRejectionsTotal.inc({
            reason: backpressure.reason ?? "pending",
          });
          if (backpressure.retryAfterSeconds) {
            reply.header("retry-after", backpressure.retryAfterSeconds.toString());
          }
          reply.status(503).send({
            error: "Outbound delivery queue is under backpressure",
            reason: backpressure.reason,
            retryAfterSeconds: backpressure.retryAfterSeconds,
          });
          return;
        }

        const bridgeValue = bodyRecord["bridge"];
        const activityPubHints =
          bridgeValue && typeof bridgeValue === "object" && !Array.isArray(bridgeValue)
            ? (bridgeValue as Record<string, unknown>)["activityPubHints"]
            : undefined;
        const bridgeHints = activityPubHints && typeof activityPubHints === "object" && !Array.isArray(activityPubHints)
          ? activityPubHints as Record<string, unknown>
          : undefined;
        const intent = createOutboxIntent({
          activityId,
          actorUri,
          activity: JSON.stringify(activityRecord),
          targets: normalizedTargets.targets,
          ...(normalizedMeta ? { meta: normalizedMeta } : {}),
          ...(bridgeHints ? { bridgeHints } : {}),
        });

        await queue.enqueueOutboxIntent(intent);

        const queueingLatencySeconds = (Date.now() - requestStartedAt) / 1000;
        promMetrics.outboundWebhookQueueingLatency.observe(queueingLatencySeconds);
        promMetrics.outboundWebhookRequestsTotal.inc({ status: "accepted" });

        reply.status(202).send({
          accepted: true,
          intentId: intent.intentId,
          jobCount: normalizedTargets.targets.length,
          inputTargetCount: normalizedTargets.inputTargetCount,
          duplicateTargetCount: normalizedTargets.duplicateTargetCount,
          invalidTargetCount: normalizedTargets.invalidTargetCount,
          queueingLatencyMs: Math.round(queueingLatencySeconds * 1000),
        });
      } catch (error) {
        if (error instanceof OutboundWebhookValidationError) {
          promMetrics.outboundWebhookRequestsTotal.inc({ status: "invalid" });
          reply.status(error.statusCode).send({
            error: error.message,
            code: error.code,
          });
          return;
        }
        promMetrics.outboundWebhookRequestsTotal.inc({ status: "error" });
        logger.error("Outbound webhook processing failed", {
          error: error instanceof Error ? error.message : String(error),
        });
        const locale = resolveLocale(request.headers["accept-language"]);
        reply.status(500).send({
          error: "Internal server error",
          message: translateLocale(locale, "common.internalServerError"),
        });
      }
    });

    // -----------------------------------------------------------------------
    // Fedify 2.x HTTP routes (WebFinger, NodeInfo, actor documents)
    // Only registered when the runtime integration flag is on.
    // -----------------------------------------------------------------------
    // FEP-8fcf partial followers collection endpoint — must be registered
    // before the Fedify catch-all so it takes priority over Fedify's own
    // /users/:id/followers dispatcher.
    if (followersSyncService) {
      registerFollowersSyncRoutes(app, {
        service: followersSyncService,
        domain: config.domain,
        userAgent: process.env["USER_AGENT"] || "Fedify-Sidecar/5.0 (ActivityPods)",
        requestTimeoutMs: Number.parseInt(process.env["REQUEST_TIMEOUT_MS"] || "30000", 10),
      });
      logger.info("FEP-8fcf followers_synchronization endpoint registered");
    }

    // FEP-eb48 hashtag tag document endpoint — must be before the Fedify
    // catch-all to ensure /tags/:tag routes are handled here.
    registerHashtagRoutes(app, {
      domain: config.domain,
      searchBaseUrl: process.env["HASHTAG_SEARCH_BASE_URL"] || undefined,
    });
    logger.info("FEP-eb48 hashtag document endpoint registered (/tags/:tag)");

    // FEP-c648 blocked collection endpoint — registered before the Fedify
    // catch-all so /users/:id/blocked takes priority.
    registerBlockedCollectionRoutes(app, {
      activityPodsUrl: process.env["ACTIVITYPODS_URL"] ?? "http://activitypods:3000",
      activityPodsToken: process.env["ACTIVITYPODS_TOKEN"] ?? "",
      domain: config.domain,
      userAgent: process.env["USER_AGENT"] || "Fedify-Sidecar/5.0 (ActivityPods)",
      requestTimeoutMs: Number.parseInt(process.env["REQUEST_TIMEOUT_MS"] || "30000", 10),
    });
    logger.info("FEP-c648 blocked collection endpoint registered");

    registerMutedCollectionRoutes(app, {
      activityPodsUrl: process.env["ACTIVITYPODS_URL"] ?? "http://activitypods:3000",
      activityPodsToken: process.env["ACTIVITYPODS_TOKEN"] ?? "",
      domain: config.domain,
      userAgent: process.env["USER_AGENT"] || "Fedify-Sidecar/5.0 (ActivityPods)",
      requestTimeoutMs: Number.parseInt(process.env["REQUEST_TIMEOUT_MS"] || "30000", 10),
    });
    logger.info("Muted collection endpoint registered");

    registerKeyPackagesRoutes(app, {
      activityPodsUrl: process.env["ACTIVITYPODS_URL"] ?? "http://activitypods:3000",
      activityPodsToken: process.env["ACTIVITYPODS_TOKEN"] ?? "",
      domain: config.domain,
      userAgent: process.env["USER_AGENT"] || "Fedify-Sidecar/5.0 (ActivityPods)",
      requestTimeoutMs: Number.parseInt(process.env["REQUEST_TIMEOUT_MS"] || "30000", 10),
    });
    logger.info("AP E2EE MLS key packages endpoint registered");

    registerMlsMessagesRoutes(app, {
      activityPodsUrl: process.env["ACTIVITYPODS_URL"] ?? "http://activitypods:3000",
      activityPodsToken: process.env["ACTIVITYPODS_TOKEN"] ?? "",
      domain: config.domain,
      userAgent: process.env["USER_AGENT"] || "Fedify-Sidecar/5.0 (ActivityPods)",
      requestTimeoutMs: Number.parseInt(process.env["REQUEST_TIMEOUT_MS"] || "30000", 10),
    });
    logger.info("AP E2EE MLS messages endpoint registered");

    if (fedifyAdapter) {
      registerFedifyRoutes(app, fedifyAdapter);
      logger.info(
        "Fedify HTTP routes registered (WebFinger, NodeInfo, actor dispatch, verified inbox ingress)",
      );
    }

    // -----------------------------------------------------------------------
    // Phase 7: AT XRPC Server
    // Wire all /xrpc/* routes and the subscribeRepos WebSocket endpoint onto
    // the already-listening Fastify app.
    //
    // All concrete dependencies are now wired.  The signing client (already
    // instantiated above) is adapted to the SigningService interface so it can
    // be consumed by DefaultAtCommitBuilder without a separate HTTP adapter.
    // -----------------------------------------------------------------------
    if (config.enableXrpcServer) {
      try {
        const { DefaultAtXrpcServer } = await import("./at-adapter/xrpc/AtXrpcServer.js");

        // ---- Shared Redis client for AT adapter stores ----
        const atRedis = new Redis(process.env["REDIS_URL"] ?? "redis://localhost:6379");
        atRedisClient = atRedis;
        atRedis.on("error", (err: Error) =>
          logger.error("AT Redis client error", { error: err.message }),
        );

        // ---- Identity binding repository ----
        identityRepo = new RedisIdentityBindingRepository(atRedis);
        const observedAtIdentityStore = new RedisObservedAtIdentityStore(atRedis);
        const observedAtIdentityResolver = new HttpAtIdentityResolver({
          timeoutMs: config.externalPdsTimeoutMs,
          maxAttempts: config.externalPdsMaxAttempts,
          failedResolutionCacheTtlMs: 60_000,
          redisCache: atRedis,
          redisCacheTtlSeconds: config.didDocCacheTtlSeconds,
        });
        const atIdentityObservationService = new AtIdentityObservationService(
          observedAtIdentityStore,
          identityRepo,
          observedAtIdentityResolver,
          {
            warn: (message, meta) => logger.warn(meta || {}, message),
          },
        );

        if (config.mrfAdminToken) {
          registerAtIdentityObservabilityFastifyRoutes(app, {
            adminToken: config.mrfAdminToken,
            store: observedAtIdentityStore,
          });
        }

        // ---- Repo / alias stores ----
        const aliasStore   = new RedisAtAliasStore(atRedis);
        const repoRegistry = new RedisAtprotoRepoRegistry(atRedis);

        // ---- Handle resolution ----
        const handleResolutionReader = new DefaultHandleResolutionReader(identityRepo);

        // ---- Record reader + CAR exporter ----
        // getRecord/listRecords now read ActivityPods' real Fuseki-backed
        // atproto.getRecord/listRecords actions directly, not aliasStore/
        // repoRegistry (Redis, populated only by the dead legacy Kafka
        // bridge — see ActivityPodsAtRecordReader's own comment). Repo CAR
        // export (com.atproto.sync.getRepo, via carExporter below) has the
        // same underlying staleness but is a separate, larger fix — it
        // needs a full repo snapshot, not single-record lookups — and is
        // out of scope for this pass.
        const recordReader = new ActivityPodsAtRecordReader(
          handleResolutionReader,
          process.env["ACTIVITYPODS_URL"] ?? "http://localhost:3000",
        );
        const carExporter = new DefaultAtCarExporter(repoRegistry);

        // ---- Firehose ----
        const firehoseCursorStore   = new RedisAtFirehoseCursorStore(atRedis, {
          maxEvents: config.atFirehoseCursorMaxEvents,
        });
        const firehoseSubscriptions = new DefaultAtFirehoseSubscriptionManager(firehoseCursorStore);
        const firehosePublisher     = new DefaultAtFirehosePublisher(
          new DefaultAtFirehoseEventEncoder(),
          firehoseCursorStore,
          firehoseSubscriptions,
          new DefaultAtRepoDiffBuilder(),
        );

        // ---- Session service ----
        const sessionSecret =
          config.atSessionSecret ||
          "dev-session-secret-at-least-32-characters";
        const sessionEndpointEnabled = true;
        let sessionService: any = undefined;
        let accountResolverForSession: any = undefined;
        let passwordVerifierForSession: any = undefined;
        let oauthTokenVerifier: OAuthAccessTokenVerifier | undefined = undefined;
        let identityBindingSyncService: HttpIdentityBindingSyncService | undefined = undefined;
        const externalPdsClient = new ExternalPdsClient({
          timeoutMs: config.externalPdsTimeoutMs,
          maxAttempts: config.externalPdsMaxAttempts,
        });
        const externalAtSessionStore = new RedisExternalAtSessionStore(
          atRedis,
          process.env["EXTERNAL_AT_SESSION_KEY_HEX"] ?? "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
          config.externalAtSessionTtlSeconds,
        );
        const externalWriteGateway = new ExternalWriteGateway(
          externalPdsClient,
          externalAtSessionStore,
          process.env["AT_OAUTH_CLIENT_ID"] ?? config.atOauthIssuer.replace(/\/$/, '') + '/oauth/atproto-sidecar.client.json',
        );
        const externalReadGateway = new ExternalReadGateway(externalPdsClient);
        if (sessionEndpointEnabled) {
          const sessionFamilyStateStore = new RedisSessionFamilyStateStore(atRedis);
          const tokenService    = new DefaultAtSessionTokenService({
            secret: sessionSecret,
            sessionStateStore: sessionFamilyStateStore,
          });
          identityBindingSyncService = config.atLocalFixture
            ? undefined
            : new HttpIdentityBindingSyncService({
                backendBaseUrl: process.env["ACTIVITYPODS_URL"]!,
                bearerToken: process.env["ACTIVITYPODS_TOKEN"]!,
                identityBindingRepository: identityRepo,
                repoRegistry,
                logger,
              });

          if (identityBindingSyncService) {
          logger.info("AT identity sync enabled", {
              backendBaseUrl: process.env["ACTIVITYPODS_URL"],
            });
          }

          const accountResolver = new DefaultAtAccountResolver(
            identityRepo,
            identityBindingSyncService,
            logger,
          );

          // Local fixture mode: bypass ActivityPods auth (dev/test only)
          const passwordVerifier = config.atLocalFixture
            ? new LocalAtPasswordVerifier()
            : createHttpAtPasswordVerifier({
                baseUrl: process.env["ACTIVITYPODS_URL"] ?? "http://localhost:3000",
                token:   process.env["ACTIVITYPODS_TOKEN"] ?? "",
              });

          accountResolverForSession  = accountResolver;
          passwordVerifierForSession = passwordVerifier;
          sessionService = new DefaultAtSessionService(
            accountResolver,
            passwordVerifier,
            tokenService,
            externalPdsClient,
            externalAtSessionStore,
          );
        }

        // ---- Signing adapter (SigningClient → SigningService) ----
        // Local fixture mode: use in-process secp256k1 signing from Redis-stored keys.
        // Production mode:    proxy signing calls to the ActivityPods signing API.
        const signingServiceAdapter: SigningService = config.atLocalFixture
          ? new LocalAtSigningService(atRedis)
          : {
              signAtprotoCommit:  (req) => signingClient.signAtprotoCommit(req),
              signPlcOperation:   (req) => signingClient.signAtprotoPlcOp(req),
              getAtprotoPublicKey: (req) => signingClient.getAtprotoPublicKey(req),
              generateApSigningKey: () => { throw new Error("generateApSigningKey not available via sidecar"); },
              generateAtSigningKey: () => { throw new Error("generateAtSigningKey not available via sidecar"); },
              getApPublicKey:       () => { throw new Error("getApPublicKey not available via sidecar"); },
            };

        // ---- Event publisher adapter ----
        // All AT-native writes and ingress audit events should hit the same
        // RedPanda-backed event bus implementation so we keep a single durable
        // audit path and avoid silent write-side drops.
        atEventPublisher = new RedpandaEventPublisher({
          brokers: (process.env["REDPANDA_BROKERS"] || "localhost:9092")
            .split(",")
            .map((broker) => broker.trim())
            .filter(Boolean),
          clientId: `${process.env["REDPANDA_CLIENT_ID"] || "fedify-sidecar"}-events`,
          compression: (process.env["REDPANDA_COMPRESSION"] || "zstd") as
            | "none"
            | "gzip"
            | "snappy"
            | "lz4"
            | "zstd",
          connectionTimeoutMs: Number.parseInt(
            process.env["REDPANDA_CONNECTION_TIMEOUT"] || "10000",
            10,
          ),
          requestTimeoutMs: Number.parseInt(
            process.env["REDPANDA_REQUEST_TIMEOUT"] || "30000",
            10,
          ),
          source: "fedify-sidecar.at-native",
        });
        const eventPublisherAdapter: EventPublisher = atEventPublisher;

        atFirehoseRuntime = new AtFirehoseRuntime({
          config: {
            brokers: (process.env["REDPANDA_BROKERS"] || "localhost:9092")
              .split(",")
              .map((broker) => broker.trim())
              .filter(Boolean),
            clientId: process.env["REDPANDA_CLIENT_ID"] || "fedify-sidecar",
            consumerGroupId: config.atFirehoseConsumerGroupId,
            commitTopic: "at.commit.v1",
            identityTopic: "at.identity.v1",
            accountTopic: "at.account.v1",
          },
          publisher: firehosePublisher,
          logger: {
            info: (message, meta) => logger.info(meta || {}, message),
            warn: (message, meta) => logger.warn(meta || {}, message),
            error: (message, meta) => logger.error(meta || {}, message),
          },
        });

        // ---- Projection worker ----
        const blobStore             = new DefaultAtBlobStore();
        const blobReferenceMapper   = new DefaultBlobReferenceMapper();
        const blobUploadService     = new DefaultAtBlobUploadService(blobStore, blobReferenceMapper);
        const profileMediaStore     = new RedisBridgeProfileMediaStore(atRedis, {
          ttlSeconds: config.protocolBridgeProfileMediaTtlSec,
        });
        const postMediaStore        = new RedisBridgePostMediaStore(atRedis, {
          ttlSeconds: config.protocolBridgePostMediaTtlSec,
        });
        const bridgeRasterImageClient = new ActivityPubBridgeProfileMediaClient({
          activityPodsBaseUrl: process.env["ACTIVITYPODS_URL"] || "http://localhost:3000",
          bearerToken: process.env["ACTIVITYPODS_TOKEN"] || "",
          timeoutMs: config.protocolBridgeProfileMediaTimeoutMs,
          maxMediaBytes: config.protocolBridgeProfileMediaMaxBytes,
        });
        const bridgeAttachmentMediaClient = new ActivityPubBridgeMediaClient({
          activityPodsBaseUrl: process.env["ACTIVITYPODS_URL"] || "http://localhost:3000",
          bearerToken: process.env["ACTIVITYPODS_TOKEN"] || "",
          timeoutMs: config.protocolBridgeAttachmentMediaTimeoutMs,
          maxMediaBytes: config.protocolBridgeAttachmentMediaMaxBytes,
        });
        const profileMediaResolver  = config.enableProtocolBridgeApToAt
          ? new AtprotoProfileMediaResolver(
              profileMediaStore,
              bridgeRasterImageClient,
              blobUploadService,
              {
                warn: (message, meta) => logger.warn(meta || {}, message),
                error: (message, meta) => logger.error(meta || {}, message),
              },
            )
          : { resolveAvatarBlob: async () => null, resolveBannerBlob: async () => null };
        const attachmentMediaResolver = new StoredAttachmentMediaResolver(postMediaStore, {
          warn: (message, meta) => logger.warn(meta || {}, message),
        });
        const rkeyService          = new DefaultAtRkeyService();
        const recordRefResolver    = new DefaultAtRecordRefResolver(aliasStore);
        const subjectResolver      = new DefaultAtSubjectResolver(identityRepo);
        const targetAliasResolver  = new DefaultAtTargetAliasResolver(aliasStore);
        const commitBuilder        = new DefaultAtCommitBuilder(signingServiceAdapter);
        const persistenceService   = new DefaultAtCommitPersistenceService(
          aliasStore,
          eventPublisherAdapter,
          atRedis,
        );

        const projectionWorker = new DefaultAtProjectionWorker(
          new DefaultAtProjectionPolicy(),
          identityRepo,
          repoRegistry,
          new DefaultProfileRecordSerializer(),
          new DefaultPostRecordSerializer(),
          new DefaultStandardDocumentRecordSerializer(),
          rkeyService,
          aliasStore,
          commitBuilder,
          persistenceService,
          eventPublisherAdapter,
          {
            mediaResolver:       profileMediaResolver,
            facetBuilder:        new DefaultFacetBuilder(),
            embedBuilder:        new DefaultEmbedBuilder(
              new DefaultImageEmbedBuilder(blobUploadService, attachmentMediaResolver),
              new DefaultVideoEmbedBuilder(blobUploadService, attachmentMediaResolver),
            ),
            recordRefResolver,
            subjectResolver,
            targetAliasResolver,
            followSerializer:    new DefaultFollowRecordSerializer(),
            likeSerializer:      new DefaultLikeRecordSerializer(),
            repostSerializer:    new DefaultRepostRecordSerializer(),
          },
        );

        // ---- Write gateway ----
        const resultStore   = new RedisAtWriteResultStore({
          redis: atRedis,
          resultTtlSec: Number.isFinite(config.atWriteResultTtlSec)
            ? config.atWriteResultTtlSec
            : 120,
          keyPrefix: config.atWriteResultKeyPrefix,
          channelPrefix: config.atWriteResultChannelPrefix,
        });
        writeResultStore = resultStore;
        const writeService  = new DefaultCanonicalClientWriteService({
          projectionWorker,
          aliasStore,
          resultStore,
          identityRepo,
          profileMediaStore,
          postMediaStore,
        });
        const writeGateway  = new DefaultAtWriteGateway({
          normalizer:  new DefaultAtWriteNormalizer(),
          policyGate:  new DefaultAtWritePolicyGate(identityRepo, aliasStore),
          writeService,
          resultStore,
          identityBindingSyncService,
          logger,
        });

        if (
          config.enableProtocolBridgeApToAt ||
          config.enableProtocolBridgeAtToAp
        ) {
          const { translationContext, projectionContext } = createProtocolBridgeContexts(
            identityRepo,
            aliasStore,
            {
              localPdsOrigin: config.atOauthResourceServerOrigin,
              ...(config.enableProtocolBridgeApToAt
                ? {
                    activityResolver: new ActivityPubBridgeActivityResolverClient({
                      activityPodsBaseUrl: process.env["ACTIVITYPODS_URL"] || "http://localhost:3000",
                      bearerToken: process.env["ACTIVITYPODS_TOKEN"] || "",
                      timeoutMs: config.protocolBridgeActivityResolutionTimeoutMs,
                    }),
                  }
                : {}),
            },
          );
          const projectionLedger = new RedisProjectionLedger(atRedis, {
            ttlSeconds: config.protocolBridgeLedgerTtlSec,
          });
          const allowAllPolicy = {
            evaluate: async () => ({ allowed: true }),
          };
          const bridgeAtWritePort = new AtprotoWriteGatewayPort(
            writeGateway,
            accountResolverForSession,
            config.enableProtocolBridgeApToAt
              ? {
                  attachmentMediaResolver: new AtprotoAttachmentMediaResolver(
                    bridgeAttachmentMediaClient,
                    blobUploadService,
                    {
                      warn: (message, meta) => logger.warn(meta || {}, message),
                      error: (message, meta) => logger.error(meta || {}, message),
                    },
                  ),
                  linkPreviewThumbResolver: new AtprotoLinkPreviewThumbResolver(
                    bridgeRasterImageClient,
                    blobUploadService,
                    {
                      warn: (message, meta) => logger.warn(meta || {}, message),
                      error: (message, meta) => logger.error(meta || {}, message),
                    },
                  ),
                  logger: {
                    warn: (message, meta) => logger.warn(meta || {}, message),
                  },
                }
              : undefined,
          );
          const bridgeApPublishPort = config.enableProtocolBridgeAtToAp
            ? new EventPublisherActivityPubPort(
                eventPublisherAdapter,
                {
                  outboundResolver: new ActivityPubBridgeOutboundResolverClient({
                    activityPodsBaseUrl: process.env["ACTIVITYPODS_URL"] || "http://localhost:3000",
                    bearerToken: process.env["ACTIVITYPODS_TOKEN"] || "",
                    timeoutMs: config.protocolBridgeOutboundResolutionTimeoutMs,
                  }),
                  deliveryPolicy: activityPubOutboundDeliveryPolicy,
                },
              )
            : undefined;
          const bridgeApIngressForwarder = config.enableProtocolBridgeAtToAp
            ? new ActivityPubBridgeIngressClient({
                activityPodsBaseUrl: process.env["ACTIVITYPODS_URL"] || "http://localhost:3000",
                bearerToken: process.env["ACTIVITYPODS_TOKEN"] || "",
                timeoutMs: config.protocolBridgeIngressTimeoutMs,
              })
            : undefined;

          canonicalPublisher = config.enableCanonicalEventLog && atEventPublisher
            ? new CanonicalIntentPublisher(atEventPublisher, config.canonicalTopic)
            : undefined;

          const canonicalNotificationConsumer = config.enableCanonicalNotifications && atEventPublisher
            ? new CanonicalNotificationConsumer(
                {
                  brokers: (process.env["REDPANDA_BROKERS"] || "localhost:9092")
                    .split(",")
                    .map((broker) => broker.trim())
                    .filter(Boolean),
                  clientId: process.env["REDPANDA_CLIENT_ID"] || "fedify-sidecar",
                  consumerGroupId: `${config.protocolBridgeConsumerGroupId}-canonical-notifications`,
                  canonicalTopic: config.canonicalTopic,
                  activityPodsBaseUrl: process.env["ACTIVITYPODS_URL"] || "http://localhost:3000",
                  activityPodsBearerToken: process.env["ACTIVITYPODS_TOKEN"] || "",
                },
                {
                  info: (message, meta) => logger.info(meta || {}, message),
                  warn: (message, meta) => logger.warn(meta || {}, message),
                  error: (message, meta) => logger.error(meta || {}, message),
                },
              )
            : undefined;

          protocolBridgeRuntime = new ProtocolBridgeRuntime({
            config: {
              brokers: (process.env["REDPANDA_BROKERS"] || "localhost:9092")
                .split(",")
                .map((broker) => broker.trim())
                .filter(Boolean),
              clientId: process.env["REDPANDA_CLIENT_ID"] || "fedify-sidecar",
              consumerGroupId: config.protocolBridgeConsumerGroupId,
              apSourceTopic: config.protocolBridgeApSourceTopic,
              atCommitTopic: config.protocolBridgeAtCommitTopic,
              atVerifiedIngressTopic: config.protocolBridgeAtVerifiedIngressTopic,
              apIngressTopic: config.protocolBridgeApIngressTopic,
              enableApToAt: config.enableProtocolBridgeApToAt,
              enableAtToAp: config.enableProtocolBridgeAtToAp,
            },
            translationContext,
            apToAtWorker: config.enableProtocolBridgeApToAt
              ? new ApToAtProjectionWorker(
                  new ActivityPubToCanonicalTranslator(),
                  new CanonicalToAtprotoProjector(),
                  allowAllPolicy,
                  projectionLedger,
                  bridgeAtWritePort,
                  projectionContext,
                  undefined,
                  canonicalPublisher,
                )
              : undefined,
            atToApWorker: config.enableProtocolBridgeAtToAp
              ? new AtToApProjectionWorker(
                  new AtprotoToCanonicalTranslator(),
                  new CanonicalToActivityPubProjector({
                    noteLinkPreviewMode: config.protocolBridgeApNoteLinkPreviewMode,
                  }),
                  allowAllPolicy,
                  projectionLedger,
                  bridgeApPublishPort!,
                  projectionContext,
                  undefined,
                  canonicalPublisher,
                  atIdentityObservationService,
                )
              : undefined,
            apIngressForwarder: bridgeApIngressForwarder,
            canonicalNotificationConsumer,
            logger: {
              info: (message, meta) => logger.info(meta || {}, message),
              warn: (message, meta) => logger.warn(meta || {}, message),
              error: (message, meta) => logger.error(meta || {}, message),
            },
          });
        }

        // ---- OAuth Authorization Server + metadata/route bridge ----
        if (config.enableAtprotoOauth) {
          const oauthParStore = new OAuthParStore(atRedis);
          const oauthCodeStore = new OAuthAuthorizationCodeStore(atRedis);
          const oauthRefreshStore = new OAuthRefreshTokenStore(atRedis);
          const oauthGrantStore = new OAuthGrantStore(atRedis);
          const oauthNonceStore = new OAuthDpopNonceStore(atRedis);
          const oauthConsentChallengeStore = new OAuthConsentChallengeStore(atRedis);
          const oauthRateLimitStore = new OAuthRateLimitStore(atRedis);
          const oauthKeyManager = new OAuthAsKeyManager(atRedis);
          const oauthClientMetadataFetcher = new OAuthClientMetadataFetcher(
            config.atOauthClientMetadataTimeoutMs,
            config.atOauthClientMetadataMaxAttempts,
            config.atOauthAllowLocalhostHttpDiscovery,
          );
          const oauthExternalDiscoveryBroker = new OAuthExternalDiscoveryBroker({
            timeoutMs: config.atOauthExternalDiscoveryTimeoutMs,
            maxAttempts: config.atOauthExternalDiscoveryMaxAttempts,
            allowLocalhostHttp: config.atOauthAllowLocalhostHttpDiscovery,
          });

          const oauthAuthorizationServer = new OAuthAuthorizationServer({
            issuer: config.atOauthIssuer,
            authorizationServerOrigin: config.atOauthAuthorizationServerOrigin,
            resourceServerOrigin: config.atOauthResourceServerOrigin,
            keyManager: oauthKeyManager,
            clientMetadataFetcher: oauthClientMetadataFetcher,
            parStore: oauthParStore,
            codeStore: oauthCodeStore,
            refreshStore: oauthRefreshStore,
            grantStore: oauthGrantStore,
            nonceStore: oauthNonceStore,
          });
          await oauthAuthorizationServer.initialize();

          const dpopVerifier = new DpopVerifier(oauthNonceStore);

          const localOauthTokenVerifier = new OAuthTokenVerifier({
            issuer: config.atOauthIssuer,
            resourceServerOrigin: config.atOauthResourceServerOrigin,
            keyManager: oauthKeyManager,
            dpopVerifier,
            nonceFactory: () => oauthAuthorizationServer.mintDpopNonce(),
          });

          oauthTokenVerifier = localOauthTokenVerifier;

          if (process.env["ACTIVITYPODS_URL"] && process.env["ACTIVITYPODS_TOKEN"]) {
            const backendOauthTokenVerifier = new BackendIntrospectionTokenVerifier({
              introspectionUrl: `${process.env["ACTIVITYPODS_URL"].replace(/\/$/, '')}/api/internal/oauth/introspect`,
              introspectionBearerToken: process.env["ACTIVITYPODS_TOKEN"],
              dpopVerifier,
              nonceFactory: () => oauthAuthorizationServer.mintDpopNonce(),
              identityBindings: identityRepo,
              timeoutMs: config.atOauthBackendIntrospectionTimeoutMs,
              maxAttempts: config.atOauthBackendIntrospectionMaxAttempts,
            });

            oauthTokenVerifier = new CompositeOAuthTokenVerifier([
              localOauthTokenVerifier,
              backendOauthTokenVerifier,
            ]);
          }

          registerOAuthRoutes(app, {
            authorizationServer: oauthAuthorizationServer,
            dpopVerifier,
            sessionService,
            externalDiscoveryBroker: oauthExternalDiscoveryBroker,
            consentChallengeStore: oauthConsentChallengeStore,
            rateLimitStore: oauthRateLimitStore,
            rateLimits: config.atOauthRouteRateLimits,
          });

          logger.info("AT OAuth routes registered", {
            issuer: config.atOauthIssuer,
            authorizationServerOrigin: config.atOauthAuthorizationServerOrigin,
            resourceServerOrigin: config.atOauthResourceServerOrigin,
            routeRateLimits: config.atOauthRouteRateLimits,
          });
        }

        // ---- Internal AT session mint (trusted backend-only path) ----
        // Used by ActivityPods dashboard/services to obtain a short-lived
        // AT access token for managed accounts without requiring an app password.
        app.post('/api/internal/atproto/session', async (request, reply) => {
          const authHeader = (request.headers.authorization as string | undefined) ?? '';
          const [scheme, token] = authHeader.split(' ');
          if (scheme !== 'Bearer' || token !== (process.env['ACTIVITYPODS_TOKEN'] || '')) {
            reply.status(401).send({ error: 'Unauthorized' });
            return;
          }

          if (!sessionService) {
            reply.status(503).send({ error: 'Session service unavailable' });
            return;
          }

          let rawBody: Record<string, unknown> | undefined;
          if (typeof request.body === 'string') {
            try {
              const parsed = JSON.parse(request.body);
              if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                rawBody = parsed as Record<string, unknown>;
              }
            } catch {
              rawBody = undefined;
            }
          } else if (request.body && typeof request.body === 'object' && !Array.isArray(request.body)) {
            rawBody = request.body as Record<string, unknown>;
          }

          const canonicalAccountId = typeof rawBody?.['canonicalAccountId'] === 'string'
            ? rawBody['canonicalAccountId'].trim()
            : '';

          if (!canonicalAccountId) {
            reply.status(400).send({ error: 'canonicalAccountId is required' });
            return;
          }

          if (!identityRepo) {
            reply.status(503).send({ error: 'Identity repository unavailable' });
            return;
          }

          const binding = await identityRepo.getByCanonicalAccountId(canonicalAccountId);
          if (!binding || !binding.atprotoDid || !binding.atprotoHandle) {
            reply.status(404).send({ error: 'ATProto identity binding not found' });
            return;
          }

          const isExternal = binding.atprotoManaged === false || binding.atprotoSource === 'external';
          if (isExternal) {
            reply.status(403).send({ error: 'External accounts require delegated credentials' });
            return;
          }

          const accessJwt = await sessionService.mintAccessToken({
            canonicalAccountId,
            did: binding.atprotoDid,
            handle: binding.atprotoHandle,
            scope: 'full',
          });

          reply.status(200).send({
            accessJwt,
            did: binding.atprotoDid,
            handle: binding.atprotoHandle,
          });
        });

        // ---- Assemble XRPC server ----
        // Deployment alignment self-check: surface obvious misconfigs of
        // the PDS hostname advertised in com.atproto.server.describeServer
        // before the server starts taking traffic.
        assertAtPdsHostnameValid(config.atPdsHostname, logger);

        const xrpcServer = new DefaultAtXrpcServer({
          recordReader,
          carExporter,
          blobStore,
          handleResolutionReader,
          firehoseSubscriptions,
          repoRegistry,
          activityPodsUrl: process.env["ACTIVITYPODS_URL"] ?? "http://localhost:3000",
          identityRepo,
          serverConfig: {
            hostname:           config.atPdsHostname,
            inviteCodeRequired: false,
            acceptsNewAccounts: false,
          },
          sessionService,
          accountResolver: accountResolverForSession,
          passwordVerifier: passwordVerifierForSession,
          writeGateway,
          externalWriteGateway,
          externalReadGateway,
        });

        registerAtXrpcRoutes(app, {
          xrpcServer,
          sessionService,
          oauthTokenVerifier,
          capabilityGate,
        });
        xrpcServerForWebSocket = xrpcServer;

        if (config.enableModerationBridgeApi) {
          const moderationRegistration = await registerModerationBridgeIntegration({
            app,
            logger,
            enabled: config.enableModerationBridgeApi,
            adminToken: config.mrfAdminToken,
            storeMode: config.mrfAdminStore,
            redisUrl: process.env["REDIS_URL"] ?? "redis://localhost:6379",
            redisPrefix: config.moderationBridgeRedisPrefix,
            identityBindingRepository: identityRepo,
            labelerDid: config.moderationLabelerDid,
            labelerSigningKeyHex: config.moderationLabelerSigningKeyHex || undefined,
            atAdminXrpcBaseUrl: config.moderationAtAdminXrpcBaseUrl || undefined,
            atAdminBearerToken: config.moderationAtAdminBearerToken || undefined,
            atAdminTimeoutMs: Number.isFinite(config.moderationAtAdminTimeoutMs)
              ? Math.max(1_000, config.moderationAtAdminTimeoutMs)
              : 5_000,
            activityPodsBaseUrl: process.env["ACTIVITYPODS_URL"] || undefined,
            activityPodsBearerToken: process.env["ACTIVITYPODS_TOKEN"] || undefined,
            activityPodsTimeoutMs: 5_000,
            activityPodsRetries: 3,
            activityPodsRetryBaseMs: 100,
            activityPodsRetryMaxMs: 2_000,
            internalBridgeToken: process.env["ACTIVITYPODS_TOKEN"] || undefined,
            canonicalPublisher,
            activityPubReportForwardingService: activityPubReportForwardingService || undefined,
            atprotoReportForwardingService: atprotoReportForwardingService || undefined,
          });
          moderationBridgeRedisClient = moderationRegistration.redisClient;
          moderationBridgeStore = moderationRegistration.store;
        }

        if (activityPubReportForwardingService && config.enableActivityPubReportForwarder) {
          canonicalActivityPubReportForwarder = new CanonicalActivityPubReportForwarder(
            {
              brokers: (process.env["REDPANDA_BROKERS"] || "localhost:9092")
                .split(",")
                .map((broker) => broker.trim())
                .filter(Boolean),
              clientId: process.env["REDPANDA_CLIENT_ID"] || "fedify-sidecar",
              consumerGroupId: `${config.protocolBridgeConsumerGroupId}-canonical-ap-report-forwarder`,
              canonicalTopic: config.canonicalTopic,
            },
            activityPubReportForwardingService,
            {
              info: (message, meta) => logger.info(meta || {}, message),
              warn: (message, meta) => logger.warn(meta || {}, message),
              error: (message, meta) => logger.error(meta || {}, message),
            },
          );
          startupTasks.push({
            name: "start canonical ActivityPub report forwarder",
            start: async () => {
              await canonicalActivityPubReportForwarder!.start();
              logger.info("Canonical ActivityPub report forwarder started", {
                moderationActorUri,
                canonicalTopic: config.canonicalTopic,
              });
            },
          });
        }

        if (atprotoReportForwardingService && config.enableAtprotoReportForwarder) {
          canonicalAtprotoReportForwarder = new CanonicalAtprotoReportForwarder(
            {
              brokers: (process.env["REDPANDA_BROKERS"] || "localhost:9092")
                .split(",")
                .map((broker) => broker.trim())
                .filter(Boolean),
              clientId: process.env["REDPANDA_CLIENT_ID"] || "fedify-sidecar",
              consumerGroupId: `${config.protocolBridgeConsumerGroupId}-canonical-atproto-report-forwarder`,
              canonicalTopic: config.canonicalTopic,
            },
            atprotoReportForwardingService,
            {
              info: (message, meta) => logger.info(meta || {}, message),
              warn: (message, meta) => logger.warn(meta || {}, message),
              error: (message, meta) => logger.error(meta || {}, message),
            },
          );
          startupTasks.push({
            name: "start canonical ATProto report forwarder",
            start: async () => {
              await canonicalAtprotoReportForwarder!.start();
              logger.info("Canonical ATProto report forwarder started", {
                canonicalTopic: config.canonicalTopic,
              });
            },
          });
        }

        if (!config.atLocalFixture) {
          identityWarmupService = new IdentityWarmupService({
            backendBaseUrl: process.env["ACTIVITYPODS_URL"]!,
            bearerToken: process.env["ACTIVITYPODS_TOKEN"]!,
            identityBindingRepository: identityRepo,
            cursorStore: new RedisIdentityWarmCursorStore(atRedis),
            repoRegistry,
            logger,
            intervalMs: config.identityWarmIntervalMs,
            batchLimit: config.identityWarmBatchLimit,
          });
        }

        startupTasks.push({
          name: "start AT runtime backplane",
          start: async () => {
            await atEventPublisher!.connect();
            await atFirehoseRuntime!.start();

            logger.info("AT firehose runtime enabled", {
              consumerGroupId: config.atFirehoseConsumerGroupId,
              commitTopic: "at.commit.v1",
              identityTopic: "at.identity.v1",
              accountTopic: "at.account.v1",
              cursorMaxEvents: config.atFirehoseCursorMaxEvents,
            });

            if (config.enableAtExternalFirehose) {
              try {
                const externalFirehoseSources = parseAtExternalFirehoseSources(
                  process.env["AT_EXTERNAL_FIREHOSE_SOURCES"],
                );
                const externalIdentityResolver = new HttpAtIdentityResolver({
                  fetchImpl: fetch,
                  timeoutMs: config.externalPdsTimeoutMs,
                  maxAttempts: config.externalPdsMaxAttempts,
                  failedResolutionCacheTtlMs: 60_000,
                  ...(atRedisClient ? {
                    redisCache: atRedisClient,
                    redisCacheTtlSeconds: config.didDocCacheTtlSeconds,
                  } : {}),
                });
                const externalCommitVerifier = new ProductionAtCommitVerifier({
                  identityResolver: externalIdentityResolver,
                  repoRegistry,
                });
                const externalIngressBootstrap = buildAtExternalFirehoseBootstrap({
                  runtimeConfig: {
                    brokers: (process.env["REDPANDA_BROKERS"] || "localhost:9092")
                      .split(",")
                      .map((broker) => broker.trim())
                      .filter(Boolean),
                    clientId: process.env["REDPANDA_CLIENT_ID"] || "fedify-sidecar",
                    consumerGroupId: config.atExternalFirehoseConsumerGroupId,
                    rawTopic: config.atExternalFirehoseRawTopic,
                    sources: externalFirehoseSources,
                  },
                  redis: atRedis as any,
                  eventPublisher: eventPublisherAdapter,
                  repoRegistry,
                  commitVerifier: externalCommitVerifier,
                  logger: {
                    info: (message, meta) => logger.info(meta || {}, message),
                    warn: (message, meta) => logger.warn(meta || {}, message),
                    error: (message, meta) => logger.error(meta || {}, message),
                  },
                  identityResolverOptions: {
                    timeoutMs: config.externalPdsTimeoutMs,
                    maxAttempts: config.externalPdsMaxAttempts,
                    failedResolutionCacheTtlMs: 60_000,
                  },
                  syncRebuilderOptions: {
                    fetchImpl: fetch,
                    timeoutMs: config.externalPdsTimeoutMs,
                    maxAttempts: config.externalPdsMaxAttempts,
                  },
                  ...(spamEvaluator ? { spamEvaluator } : {}),
                });

                if (externalIngressBootstrap.kind === "ready") {
                  await externalIngressBootstrap.runtime.start();
                  atExternalFirehoseRuntime = externalIngressBootstrap.runtime;

                  logger.info(
                    {
                      enableAtExternalFirehose: true,
                      sourceCount: externalIngressBootstrap.sources.length,
                      rawTopic: config.atExternalFirehoseRawTopic,
                      consumerGroupId: config.atExternalFirehoseConsumerGroupId,
                    },
                    "External AT firehose intake started",
                  );
                } else {
                  logger.warn(
                    {
                      enableAtExternalFirehose: true,
                      sourceCount: externalIngressBootstrap.sources.length,
                      rawTopic: config.atExternalFirehoseRawTopic,
                      consumerGroupId: config.atExternalFirehoseConsumerGroupId,
                      bootstrapStatus: externalIngressBootstrap.kind,
                      bootstrapReason: externalIngressBootstrap.reason,
                    },
                    `External AT firehose intake requested but not started: ${externalIngressBootstrap.message}`,
                  );
                }
              } catch (error: any) {
                logger.error(
                  {
                    enableAtExternalFirehose: true,
                    error: error?.message || String(error),
                  },
                  "External AT firehose intake requested but configuration validation failed",
                );
              }
            }

            if (protocolBridgeRuntime) {
              await protocolBridgeRuntime.start();

              logger.info("Protocol bridge runtime enabled", {
                enableApToAt: config.enableProtocolBridgeApToAt,
                enableAtToAp: config.enableProtocolBridgeAtToAp,
                apSourceTopic: config.protocolBridgeApSourceTopic,
                atCommitTopic: config.protocolBridgeAtCommitTopic,
                atVerifiedIngressTopic: config.protocolBridgeAtVerifiedIngressTopic,
                apIngressTopic: config.protocolBridgeApIngressTopic,
              });
            }

            if (identityWarmupService) {
              identityWarmupService.start();

              logger.info("AT identity warmup enabled", {
                backendBaseUrl: process.env["ACTIVITYPODS_URL"],
                intervalMs: config.identityWarmIntervalMs,
                batchLimit: config.identityWarmBatchLimit,
              });
            }

            if (config.enableAtJetstream) {
              atJetstreamService = new AtJetstreamService(
                atEventPublisher!,
                {
                  url: config.atJetstreamUrl,
                  publishTopic: config.atJetstreamPublishTopic,
                  maxEvents: config.atJetstreamMaxEvents,
                },
                {
                  info: (message, meta) => logger.info(meta ?? {}, message),
                  warn: (message, meta) => logger.warn(meta ?? {}, message),
                  error: (message, meta) => logger.error(meta ?? {}, message),
                },
              );
              atJetstreamService.onMaxEvents(() => {
                logger.info("Jetstream maxEvents reached — initiating graceful shutdown");
                process.emit("SIGTERM");
              });
              atJetstreamService.start();

              logger.info("AT Jetstream intake started", {
                url: config.atJetstreamUrl,
                publishTopic: config.atJetstreamPublishTopic,
                maxEvents: config.atJetstreamMaxEvents ?? null,
              });
            }
          },
        });

        if (config.atLocalFixture) {
          logger.warn(
            "AT_LOCAL_FIXTURE=true — LocalAtPasswordVerifier and LocalAtSigningService are active. " +
            "This mode bypasses ActivityPods auth and uses Redis-stored fixture keys. " +
            "NEVER use in production."
          );
        }

        logger.info("AT XRPC server routes registered", {
          hostname:              config.atPdsHostname,
          writeEndpointsEnabled: true,
          sessionEndpointEnabled,
          localFixtureMode:      config.atLocalFixture,
        });
      } catch (err: any) {
        logger.error({
          error: err.message,
          stack: err.stack,
        }, "Failed to initialise AT XRPC server");
      }
    }

    if (xrpcServerForWebSocket) {
      attachSubscribeReposWebSocket(app, xrpcServerForWebSocket);
    }
    attachFeedStreamWebSocket(app, streamSubscriptionService, {
      capabilityGate,
      checkStreamEntitlement,
    });

    // Start HTTP server only after all HTTP and WebSocket routes are registered.
    if (config.startupMode === "blocking") {
      await runStartupTasks(startupTasks);
    }

    await app.listen({ port: config.port, host: config.host });

    logger.info(`Fedify Sidecar listening on ${config.host}:${config.port}`);
    logger.info(`Metrics available at http://${config.host}:${config.port}/metrics`);
    logger.info("Configuration summary", {
      domain: config.domain,
      providerProfile,
      startupMode: config.startupMode,
      enableOutboundWorker: config.enableOutboundWorker,
      enableInboundWorker: config.enableInboundWorker,
      enableOpenSearchIndexer: config.enableOpenSearchIndexer,
      enableMediaAssetSync: config.enableMediaAssetSync,
      enableEntryway: config.enableEntryway,
    });

    if (config.startupMode === "background") {
      logger.info("HTTP server is listening while background startup continues", {
        pendingStartupTasks: startupTasks.map((task) => task.name),
      });

      void (async () => {
        try {
          await runStartupTasks(startupTasks);
          markStartupReady();
          populateCapabilityHealthGauges(providerCapabilitiesDocument);
          logger.info("Background startup completed", {
            startup: snapshotStartupState(),
          });
        } catch (error: any) {
          logger.error(
            {
              error: error?.message || String(error),
              startup: snapshotStartupState(),
            },
            "Background startup failed",
          );
        }
      })();
    } else {
      markStartupReady();
      populateCapabilityHealthGauges(providerCapabilitiesDocument);
      logger.info("Startup completed", {
        startup: snapshotStartupState(),
      });
    }

  } catch (error: any) {
    logger.error({ error: error.message, stack: error.stack }, "Failed to start Fedify Sidecar");
    process.exit(1);
  }
}

/**
 * Normalize headers to lowercase keys
 */
function normalizeHeaders(headers: Record<string, any>): Record<string, string> {
  const normalized: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (typeof value === "string") {
      normalized[key.toLowerCase()] = value;
    } else if (Array.isArray(value)) {
      normalized[key.toLowerCase()] = value[0];
    }
  }
  return normalized;
}

/**
 * Graceful shutdown handler
 */
async function shutdown(signal: string): Promise<void> {
  if (isShuttingDown) {
    logger.warn("Shutdown already in progress");
    return;
  }

  isShuttingDown = true;
  logger.info(`Received ${signal}, starting graceful shutdown...`);

  const shutdownTimeout = setTimeout(() => {
    logger.error("Shutdown timeout exceeded, forcing exit");
    process.exit(1);
  }, 30000);

  try {
    // Stop relay subscription service
    if (apRelaySubscriptionService) {
      apRelaySubscriptionService.shutdown();
      logger.info("AP relay subscription service stopped");
    }

    if (streamSubscriptionService) {
      streamSubscriptionService.shutdown();
      logger.info("Feed stream subscription service stopped");
    }

    if (feedStreamKafkaConsumer) {
      await feedStreamKafkaConsumer.shutdown();
      logger.info("Feed stream Kafka consumer stopped");
    }

    if (unifiedFeedBridge) {
      await unifiedFeedBridge.shutdown();
      logger.info("Unified feed bridge stopped");
    }

    if (fep3ab2Runtime) {
      await fep3ab2Runtime.shutdown();
      logger.info("FEP-3ab2 runtime stopped");
    }

    if (entrywayRecoveryWorker) {
      await entrywayRecoveryWorker.stop();
      entrywayRecoveryWorker = null;
      logger.info("Entryway recovery worker stopped");
    }

    if (entrywayRedisClient) {
      await entrywayRedisClient.quit().catch(() => entrywayRedisClient!.disconnect());
      entrywayRedisClient = null;
      logger.info("Entryway Redis client disconnected");
    }

    if (atJetstreamService) {
      atJetstreamService.shutdown();
      logger.info("AT Jetstream service stopped");
    }

    if (canonicalActivityPubReportForwarder) {
      await canonicalActivityPubReportForwarder.stop();
      canonicalActivityPubReportForwarder = null;
      logger.info("Canonical ActivityPub report forwarder stopped");
    }

    if (canonicalAtprotoReportForwarder) {
      await canonicalAtprotoReportForwarder.stop();
      canonicalAtprotoReportForwarder = null;
      logger.info("Canonical ATProto report forwarder stopped");
    }

    if (atFirehoseRuntime) {
      await atFirehoseRuntime.stop();
      atFirehoseRuntime = null;
      logger.info("AT firehose runtime stopped");
    }

    if (atExternalFirehoseRuntime) {
      await atExternalFirehoseRuntime.stop();
      atExternalFirehoseRuntime = null;
      logger.info("External AT firehose runtime stopped");
    }

    if (protocolBridgeRuntime) {
      await protocolBridgeRuntime.stop();
      protocolBridgeRuntime = null;
      logger.info("Protocol bridge runtime stopped");
    }

    // Stop workers first
    if (outboxIntentWorker) {
      await outboxIntentWorker.stop();
      logger.info("Outbox intent worker stopped");
    }

    if (originReconciliationWorker) {
      await originReconciliationWorker.stop();
      logger.info("Origin reconciliation worker stopped");
    }

    if (outboundWorker) {
      await outboundWorker.stop();
      logger.info("Outbound worker stopped");
    }

    if (inboundWorker) {
      await inboundWorker.stop();
      logger.info("Inbound worker stopped");
    }

    if (mediaAssetSyncConsumer) {
      await mediaAssetSyncConsumer.stop();
      mediaAssetSyncConsumer = null;
      logger.info("Media asset sync consumer stopped");
    }

    if (opensearchIndexer) {
      await opensearchIndexer.stop();
      logger.info("OpenSearch indexer stopped");
    }

    // Disconnect queue
    if (queue) {
      await queue.disconnect();
      logger.info("Redis Streams queue disconnected");
    }

    // Close write-result store (drains pending waiters + quits subscriber)
    if (writeResultStore) {
      await writeResultStore.close();
      logger.info("Write result store closed");
    }

    if (postSyncWorker) {
      await postSyncWorker.stop();
      postSyncWorker = null;
      logger.info("Post sync worker stopped");
    }

    // Close PostgreSQL pool
    try {
      await pool.end();
      logger.info("PostgreSQL client pool closed");
    } catch (dbErr: any) {
      logger.error("Error closing PostgreSQL pool", { error: dbErr.message });
    }

    if (identityWarmupService) {
      await identityWarmupService.stop();
      identityWarmupService = null;
      logger.info("Identity warmup service stopped");
    }

    if (searchIndexerRedis) {
      await searchIndexerRedis.quit().catch(() => searchIndexerRedis!.disconnect());
      searchIndexerRedis = null;
      logger.info("Search indexer Redis client disconnected");
    }

    if (mrfAdminRedisClient) {
      await mrfAdminRedisClient.quit().catch(() => mrfAdminRedisClient!.disconnect());
      mrfAdminRedisClient = null;
      logger.info("MRF admin Redis client disconnected");
    }

    if (moderationBridgeRedisClient) {
      await moderationBridgeRedisClient.quit().catch(() => moderationBridgeRedisClient!.disconnect());
      moderationBridgeRedisClient = null;
      logger.info("Moderation bridge Redis client disconnected");
    }

    // Quit shared AT Redis client
    if (atRedisClient) {
      await atRedisClient.quit().catch(() => atRedisClient!.disconnect());
      logger.info("AT Redis client disconnected");
    }

    if (atEventPublisher) {
      await atEventPublisher.disconnect();
      atEventPublisher = null;
      logger.info("AT event publisher disconnected");
    }

    clearTimeout(shutdownTimeout);
    logger.info("Graceful shutdown completed");
    process.exit(0);
  } catch (error: any) {
    logger.error("Error during shutdown", { error: error.message });
    clearTimeout(shutdownTimeout);
    process.exit(1);
  }
}

// Start the application
main();
