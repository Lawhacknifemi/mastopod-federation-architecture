import { z } from "zod";

const CONTROL_CHARACTERS = /[\u0000-\u001F\u007F]/;
const AT_URI_PREFIX = "at://";
const DID_PREFIX = "did:";

function nonControlTrimmedString(max: number, label: string) {
  return z
    .string()
    .trim()
    .min(1, `${label} is required`)
    .max(max, `${label} must be at most ${max} characters`)
    .refine((value) => !CONTROL_CHARACTERS.test(value), `${label} must not contain control characters`);
}

function isSafeHttpUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    return (
      (parsed.protocol === "http:" || parsed.protocol === "https:") &&
      !parsed.username &&
      !parsed.password &&
      parsed.href.length <= 2048
    );
  } catch {
    return false;
  }
}

function isSafeDid(value: string): boolean {
  return value.startsWith(DID_PREFIX) && !CONTROL_CHARACTERS.test(value) && value.length <= 2048;
}

function isSafeAtUri(value: string): boolean {
  return value.startsWith(AT_URI_PREFIX) && !CONTROL_CHARACTERS.test(value) && value.length <= 2048;
}

function isIsoDateTime(value: string): boolean {
  return !Number.isNaN(Date.parse(value));
}

export const FeedSourceSchema = z.enum(["stream1", "stream2", "canonical", "firehose", "unified"]);
export type FeedSource = z.infer<typeof FeedSourceSchema>;

export const FeedKindSchema = z.enum(["graph", "discovery", "topic", "locality", "notifications", "custom"]);
export type FeedKind = z.infer<typeof FeedKindSchema>;

export const FeedVisibilitySchema = z.enum(["public", "authenticated", "internal"]);
export type FeedVisibility = z.infer<typeof FeedVisibilitySchema>;

export const HydrationShapeSchema = z.enum(["card", "activity", "thread-item", "full"]);
export type HydrationShape = z.infer<typeof HydrationShapeSchema>;

export const RankingModeSchema = z.enum(["chronological", "ranked", "blended"]);
export type RankingMode = z.infer<typeof RankingModeSchema>;

export const OpaqueIdentifierSchema = nonControlTrimmedString(512, "identifier");
export const FeedIdSchema = nonControlTrimmedString(256, "feedId");
export const ViewerIdSchema = nonControlTrimmedString(2048, "viewerId");

export const SafeHttpUrlSchema = z
  .string()
  .trim()
  .min(1, "URL is required")
  .max(2048, "URL must be at most 2048 characters")
  .refine((value) => isSafeHttpUrl(value), "URL must be an absolute http(s) URL without credentials");

export const SafeResourceUriSchema = z
  .string()
  .trim()
  .min(1, "resource URI is required")
  .max(2048, "resource URI must be at most 2048 characters")
  .refine(
    (value) => isSafeHttpUrl(value) || isSafeAtUri(value) || isSafeDid(value),
    "resource URI must be a safe http(s), at://, or did: URI",
  );

export const IsoDateTimeSchema = z
  .string()
  .trim()
  .min(1, "timestamp is required")
  .max(64, "timestamp must be at most 64 characters")
  .refine((value) => isIsoDateTime(value), "timestamp must be a valid ISO-8601 string");

export const FeedSourcePolicySchema = z
  .object({
    includeStream1: z.boolean().default(false),
    includeStream2: z.boolean().default(false),
    includeCanonical: z.boolean().default(false),
    includeFirehose: z.boolean().default(false),
    includeUnified: z.boolean().default(false),
  })
  .superRefine((value, ctx) => {
    if (
      !value.includeStream1 &&
      !value.includeStream2 &&
      !value.includeCanonical &&
      !value.includeFirehose &&
      !value.includeUnified
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "feed source policy must enable at least one source",
      });
    }
  });

export const RankingPolicySchema = z.object({
  mode: RankingModeSchema,
  providerHint: nonControlTrimmedString(128, "providerHint").optional(),
});

export const FeedDefinitionSchema = z.object({
  id: FeedIdSchema,
  kind: FeedKindSchema,
  visibility: FeedVisibilitySchema.default("public"),
  title: nonControlTrimmedString(120, "title").optional(),
  description: nonControlTrimmedString(512, "description").optional(),
  sourcePolicy: FeedSourcePolicySchema,
  rankingPolicy: RankingPolicySchema,
  hydrationShape: HydrationShapeSchema,
  realtimeCapable: z.boolean().default(false),
  supportsSse: z.boolean().default(false),
  supportsWebSocket: z.boolean().default(false),
  experimental: z.boolean().default(false),
  providerId: nonControlTrimmedString(128, "providerId"),
});

export const PublicFeedDefinitionSchema = FeedDefinitionSchema.omit({ providerId: true });

export const FeedRequestFiltersSchema = z.object({
  tags: z.array(nonControlTrimmedString(64, "tag")).max(32, "tags must contain at most 32 values").optional(),
  langs: z.array(nonControlTrimmedString(16, "language")).max(16, "langs must contain at most 16 values").optional(),
  authors: z.array(SafeResourceUriSchema).max(64, "authors must contain at most 64 values").optional(),
});

export const FeedRequestSchema = z.object({
  feedId: FeedIdSchema,
  viewerId: ViewerIdSchema.optional(),
  limit: z.number().int().min(1).max(100),
  cursor: nonControlTrimmedString(512, "cursor").optional(),
  filters: FeedRequestFiltersSchema.optional(),
  excludeViewed: z.boolean().optional(),
});

export const FeedHintsSchema = z.object({
  reason: nonControlTrimmedString(64, "reason").optional(),
  rankBucket: nonControlTrimmedString(64, "rankBucket").optional(),
}).strict();

export const FeedSkeletonSchema = z.object({
  stableId: OpaqueIdentifierSchema,
  canonicalUri: SafeResourceUriSchema.optional(),
  activityPubObjectId: SafeHttpUrlSchema.optional(),
  source: FeedSourceSchema,
  score: z.number().finite().min(0).max(1_000_000).optional(),
  publishedAt: IsoDateTimeSchema.optional(),
  authorId: SafeResourceUriSchema.optional(),
  hints: FeedHintsSchema.optional(),
}).superRefine((value, ctx) => {
  if (!value.canonicalUri && !value.activityPubObjectId) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "feed skeleton must include canonicalUri or activityPubObjectId",
      path: ["canonicalUri"],
    });
  }
});

export const FeedResponseSchema = z.object({
  items: z.array(FeedSkeletonSchema).max(100),
  cursor: nonControlTrimmedString(512, "cursor").optional(),
  capabilities: z.object({
    hydrationRequired: z.literal(true),
    realtimeAvailable: z.boolean(),
    supportsSse: z.boolean(),
    supportsWebSocket: z.boolean(),
  }),
});

export const HydrationItemInputSchema = z.object({
  stableId: OpaqueIdentifierSchema.optional(),
  canonicalUri: SafeResourceUriSchema.optional(),
  activityPubObjectId: SafeHttpUrlSchema.optional(),
  source: FeedSourceSchema.optional(),
}).superRefine((value, ctx) => {
  if (!value.stableId && !value.canonicalUri && !value.activityPubObjectId) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "hydration item must include at least one identifier",
    });
  }
});

export const HydrationRequestSchema = z.object({
  viewerId: ViewerIdSchema.optional(),
  items: z.array(HydrationItemInputSchema).max(100),
  shape: HydrationShapeSchema.default("card"),
});

export const HydratedActorSchema = z.object({
  id: SafeResourceUriSchema,
  displayName: nonControlTrimmedString(200, "displayName").optional(),
  handle: nonControlTrimmedString(200, "handle").optional(),
  avatarUrl: SafeHttpUrlSchema.optional(),
  isLocal: z.boolean().optional(),
});

export const HydratedMediaSchema = z.object({
  url: SafeHttpUrlSchema,
  mimeType: nonControlTrimmedString(128, "mimeType").optional(),
  width: z.number().int().min(1).max(20_000).optional(),
  height: z.number().int().min(1).max(20_000).optional(),
});

export const HydratedObjectSchema = z.object({
  id: SafeResourceUriSchema,
  type: nonControlTrimmedString(128, "type"),
  publishedAt: IsoDateTimeSchema.optional(),
  url: SafeHttpUrlSchema.optional(),
  content: z
    .object({
      text: z.string().trim().max(32_768).optional(),
      html: z.string().trim().max(131_072).optional(),
      summary: z.string().trim().max(2_048).optional(),
    })
    .strict()
    .optional(),
  author: HydratedActorSchema.optional(),
  media: z.array(HydratedMediaSchema).max(32).optional(),
  engagement: z
    .object({
      likeCount: z.number().int().min(0).max(1_000_000_000).optional(),
      shareCount: z.number().int().min(0).max(1_000_000_000).optional(),
      replyCount: z.number().int().min(0).max(1_000_000_000).optional(),
    })
    .strict()
    .optional(),
  provenance: z
    .object({
      source: FeedSourceSchema,
      discoveredVia: nonControlTrimmedString(64, "discoveredVia").optional(),
    })
    .strict()
    .optional(),
  // Which protocol(s) this content actually exists on — used by
  // GET /at/feed's ?source= filter (see public-routes.ts). Without this
  // declared here, Zod silently strips it on parse and every item falls
  // back to being treated as present on both networks, making the filter
  // a no-op — see OpenSearchHydrator.ts's toHydratedObject for the write side.
  protocolPresence: z.array(z.enum(["ap", "at"])).optional(),
});

export const HydrationOmittedReasonSchema = z.enum([
  "not_found",
  "deleted",
  "blocked",
  "viewer_not_allowed",
  "invalid_request",
  "unsupported_source",
  "temporarily_unavailable",
]);

export const HydrationOmittedSchema = z.object({
  id: OpaqueIdentifierSchema,
  reason: HydrationOmittedReasonSchema,
});

export const HydrationResultSchema = z.object({
  items: z.array(HydratedObjectSchema).max(100),
  omitted: z.array(HydrationOmittedSchema).max(100).optional(),
});

export type FeedDefinition = z.infer<typeof FeedDefinitionSchema>;
export type PublicFeedDefinition = z.infer<typeof PublicFeedDefinitionSchema>;
export type FeedRequest = z.infer<typeof FeedRequestSchema>;
export type FeedSkeleton = z.infer<typeof FeedSkeletonSchema>;
export type FeedResponse = z.infer<typeof FeedResponseSchema>;
export type HydrationRequest = z.infer<typeof HydrationRequestSchema>;
export type HydrationItemInput = z.infer<typeof HydrationItemInputSchema>;
export type HydratedObject = z.infer<typeof HydratedObjectSchema>;
export type HydrationResult = z.infer<typeof HydrationResultSchema>;
export type HydrationOmittedReason = z.infer<typeof HydrationOmittedReasonSchema>;

export function toPublicFeedDefinition(definition: FeedDefinition): PublicFeedDefinition {
  return PublicFeedDefinitionSchema.parse(definition);
}
