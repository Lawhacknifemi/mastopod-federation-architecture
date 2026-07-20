import type { PublicContentDocument } from "../search/models/PublicContentDocument.js";
import type { PodHydrator, HydrationSourceRequest } from "./PodHydrationService.js";
import type { FeedSource, HydratedObject, HydrationResult } from "./contracts.js";
import { OpenSearchDocumentStore, type OpenSearchSearchClient } from "./OpenSearchDocumentStore.js";
import type { SearchDocumentStore } from "./SearchDocumentStore.js";
import type { SearchReadRetryPolicy } from "./OpenSearchRetry.js";

export class SearchHydrator implements PodHydrator {
  constructor(private readonly documentStore: SearchDocumentStore) {}

  async hydrate(input: HydrationSourceRequest): Promise<HydrationResult> {
    const stableIds = new Set<string>();
    const canonicalUris = new Set<string>();
    const activityPubObjectIds = new Set<string>();

    for (const item of input.items) {
      if (item.stableId) stableIds.add(item.stableId);
      if (item.canonicalUri && (isSafeUrl(item.canonicalUri) || isSafeAtUri(item.canonicalUri))) canonicalUris.add(item.canonicalUri);
      if (item.activityPubObjectId && isSafeUrl(item.activityPubObjectId)) activityPubObjectIds.add(item.activityPubObjectId);
    }

    const docs = await this.documentStore.fetchByIdentifiers({ stableIds, canonicalUris, activityPubObjectIds });
    const docsByStableId = new Map<string, PublicContentDocument>();
    const docsByCanonicalUri = new Map<string, PublicContentDocument>();
    const docsByApObjectId = new Map<string, PublicContentDocument>();

    for (const doc of docs) {
      docsByStableId.set(doc.stableDocId, doc);
      if (isSafeUrl(doc.canonicalContentId) || isSafeAtUri(doc.canonicalContentId)) docsByCanonicalUri.set(doc.canonicalContentId, doc);
      if (isSafeUrl(doc.ap?.objectUri)) docsByApObjectId.set(doc.ap.objectUri, doc);
    }

    const hydrated: HydratedObject[] = [];
    const omitted: Array<{ id: string; reason: "not_found" | "invalid_request" }> = [];

    for (const item of input.items) {
      const lookupId = item.stableId ?? item.canonicalUri ?? item.activityPubObjectId ?? "unknown";
      const doc =
        (item.stableId ? docsByStableId.get(item.stableId) : undefined)
        ?? (item.canonicalUri ? docsByCanonicalUri.get(item.canonicalUri) : undefined)
        ?? (item.activityPubObjectId ? docsByApObjectId.get(item.activityPubObjectId) : undefined);

      if (!doc) {
        omitted.push({ id: lookupId, reason: "not_found" });
        continue;
      }

      const hydratedItem = toHydratedObject(doc, item.source ?? guessSource(doc));
      if (!hydratedItem) {
        omitted.push({ id: lookupId, reason: "invalid_request" });
        continue;
      }

      hydrated.push(hydratedItem);
    }

    return omitted.length > 0 ? { items: hydrated, omitted } : { items: hydrated };
  }
}

export class OpenSearchHydrator extends SearchHydrator {

  constructor(
    osClient: OpenSearchSearchClient,
    options: { indexName?: string; retryPolicy?: Partial<SearchReadRetryPolicy>; requestTimeoutMs?: number } = {},
  ) {
    super(new OpenSearchDocumentStore(osClient, options));
  }
}

function toHydratedObject(doc: PublicContentDocument, source: FeedSource): HydratedObject | null {
  const id = isSafeUrl(doc.ap?.objectUri)
    ? doc.ap.objectUri
    : isSafeAtUri(doc.at?.uri)
      ? doc.at.uri
      : (isSafeUrl(doc.canonicalContentId) || isSafeAtUri(doc.canonicalContentId))
        ? doc.canonicalContentId
        : null;
  if (!id) return null;

  const authorId = isSafeUrl(doc.author.apUri)
    ? doc.author.apUri
    : isSafeDid(doc.author.did)
      ? doc.author.did
      : null;

  const text = sanitizeText(doc.text);
  const summary = text.length > 280 ? `${text.slice(0, 279)}…` : text;

  return {
    id,
    type: "Note",
    publishedAt: normalizeIso(doc.createdAt) ?? undefined,
    content: { text, summary },
    author: authorId
      ? {
          id: authorId,
          displayName: sanitizeOptionalText(doc.author.displayName),
          handle: sanitizeOptionalText(doc.author.handle),
        }
      : undefined,
    engagement: {
      likeCount: clampCount(doc.engagement?.likeCount),
      shareCount: clampCount(doc.engagement?.repostCount),
      replyCount: clampCount(doc.engagement?.replyCount),
    },
    provenance: { source, discoveredVia: doc.sourceKind },
  };
}

function sanitizeText(value: string): string {
  return value.replace(/[\u0000-\u001F\u007F]/g, "").trim().slice(0, 32768);
}

function sanitizeOptionalText(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const cleaned = sanitizeText(value).slice(0, 200);
  return cleaned.length > 0 ? cleaned : undefined;
}

function normalizeIso(value: string | undefined): string | null {
  if (!value) return null;
  return Number.isNaN(Date.parse(value)) ? null : new Date(value).toISOString();
}

function clampCount(value: number | undefined): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  return Math.max(0, Math.min(1000000000, Math.trunc(value)));
}

function isSafeUrl(value: string | undefined): value is string {
  if (!value) return false;
  try {
    const parsed = new URL(value);
    return ["http:", "https:"].includes(parsed.protocol) && !parsed.username && !parsed.password;
  } catch {
    return false;
  }
}

function isSafeAtUri(value: string | undefined): value is string {
  return Boolean(value && value.startsWith("at://") && !/[\u0000-\u001F\u007F]/.test(value));
}

function isSafeDid(value: string | undefined): value is string {
  return Boolean(value && value.startsWith("did:") && !/[\u0000-\u001F\u007F]/.test(value));
}

function guessSource(doc: PublicContentDocument): FeedSource {
  return doc.sourceKind === "local" ? "stream1" : "stream2";
}
