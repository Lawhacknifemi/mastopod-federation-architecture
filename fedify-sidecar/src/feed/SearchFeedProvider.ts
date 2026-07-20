import type { FeedCandidateService, FeedCandidateRequest, FeedCursor } from "../search/queries/FeedCandidateService.js";
import type { PublicContentDocument } from "../search/models/PublicContentDocument.js";
import type { PodFeedProvider, ResolvedFeedRequest } from "./PodFeedService.js";
import type { FeedResponse, FeedSource } from "./contracts.js";
import type { SearchDocumentStore } from "./SearchDocumentStore.js";

export class SearchFeedProvider implements PodFeedProvider {
  constructor(
    private readonly documentStore: SearchDocumentStore,
    private readonly candidateService: FeedCandidateService,
  ) {}

  async getFeed(input: ResolvedFeedRequest): Promise<FeedResponse> {
    const candidateRequest = this.toCandidateRequest(input);
    const candidateResult = await this.candidateService.getCandidates(candidateRequest);

    const candidateIds = candidateResult.candidates.map((candidate) => candidate.stableDocId);
    if (candidateIds.length === 0) {
      return {
        items: [],
        cursor: encodeCursor(candidateResult.nextCursor),
        capabilities: {
          hydrationRequired: true,
          realtimeAvailable: false,
          supportsSse: false,
          supportsWebSocket: false,
        },
      };
    }

    const documentsById = await this.documentStore.fetchByStableIds(candidateIds);
    const filterLangs = input.request.filters?.langs ?? [];
    const filterLangSet = new Set(filterLangs.map((lang) => lang.toLowerCase()));

    const items = candidateResult.candidates
      .map((candidate) => {
        const doc = documentsById.get(candidate.stableDocId);
        if (!doc) {
          return null;
        }
        if (!matchesLangFilter(doc, filterLangSet)) {
          return null;
        }

        const canonicalUri = selectCanonicalUri(doc);
        const activityPubObjectId = selectActivityPubObjectId(doc);
        if (!canonicalUri && !activityPubObjectId) {
          return null;
        }

        const source = selectFeedSource(doc, input.definition.sourcePolicy);
        if (!source) {
          return null;
        }

        return {
          stableId: doc.stableDocId,
          canonicalUri: canonicalUri ?? undefined,
          activityPubObjectId: activityPubObjectId ?? undefined,
          source,
          score: Number.isFinite(candidate.score) ? candidate.score : undefined,
          publishedAt: normalizeIso(doc.createdAt) ?? undefined,
          authorId: selectAuthorId(doc) ?? undefined,
          hints: {
            reason: normalizeHint(candidate.bucket),
            rankBucket: normalizeHint(candidate.bucket),
          },
        };
      })
      .filter((item): item is NonNullable<typeof item> => item !== null);

    return {
      items,
      cursor: encodeCursor(candidateResult.nextCursor),
      capabilities: {
        hydrationRequired: true,
        realtimeAvailable: false,
        supportsSse: false,
        supportsWebSocket: false,
      },
    };
  }

  private toCandidateRequest(input: ResolvedFeedRequest): FeedCandidateRequest {
    const decodedCursor = decodeCursor(input.request.cursor);
    const feedType = toCandidateFeedType(input.definition.kind);
    return {
      viewerCanonicalId: input.request.viewerId ?? "anonymous",
      feedType,
      limit: input.request.limit,
      ...(decodedCursor ? { cursor: decodedCursor } : {}),
      ...(input.request.filters?.tags ? { interests: input.request.filters.tags } : {}),
      ...(input.request.filters?.authors ? { followedIds: input.request.filters.authors } : {}),
    };
  }
}

function toCandidateFeedType(kind: string): "home" | "custom" | "topic" {
  if (kind === "graph") {
    return "home";
  }
  if (kind === "topic") {
    return "topic";
  }
  return "custom";
}

function decodeCursor(raw: string | undefined): FeedCursor | undefined {
  if (!raw) {
    return undefined;
  }

  try {
    const decoded = Buffer.from(raw, "base64url").toString("utf8");
    const parsed = JSON.parse(decoded) as FeedCursor;
    if (parsed && typeof parsed === "object") {
      return parsed;
    }
    return undefined;
  } catch {
    return undefined;
  }
}

function encodeCursor(cursor: FeedCursor | undefined): string | undefined {
  if (!cursor || Object.keys(cursor).length === 0) {
    return undefined;
  }

  try {
    return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
  } catch {
    return undefined;
  }
}

function matchesLangFilter(doc: PublicContentDocument, filterLangSet: Set<string>): boolean {
  if (filterLangSet.size === 0) {
    return true;
  }
  const docLangs = (doc.langs ?? []).map((lang) => lang.toLowerCase());
  return docLangs.some((lang) => filterLangSet.has(lang));
}

function isSafeUrl(value: string | undefined): value is string {
  if (!value) {
    return false;
  }
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

function selectCanonicalUri(doc: PublicContentDocument): string | null {
  if (isSafeUrl(doc.canonicalContentId) || isSafeAtUri(doc.canonicalContentId)) {
    return doc.canonicalContentId;
  }
  if (isSafeUrl(doc.ap?.objectUri)) {
    return doc.ap.objectUri;
  }
  if (isSafeAtUri(doc.at?.uri)) {
    return doc.at.uri;
  }
  return null;
}

function selectActivityPubObjectId(doc: PublicContentDocument): string | null {
  return isSafeUrl(doc.ap?.objectUri) ? doc.ap.objectUri : null;
}

function selectAuthorId(doc: PublicContentDocument): string | null {
  if (isSafeUrl(doc.author.apUri)) {
    return doc.author.apUri;
  }
  if (isSafeDid(doc.author.did)) {
    return doc.author.did;
  }
  return null;
}

function normalizeIso(value: string | undefined): string | null {
  if (!value) {
    return null;
  }
  return Number.isNaN(Date.parse(value)) ? null : new Date(value).toISOString();
}

function selectFeedSource(
  doc: PublicContentDocument,
  sourcePolicy: {
    includeStream1: boolean;
    includeStream2: boolean;
    includeCanonical: boolean;
    includeFirehose: boolean;
    includeUnified: boolean;
  },
): FeedSource | null {
  if (doc.sourceKind === "local" && sourcePolicy.includeStream1) {
    return "stream1";
  }
  if (doc.sourceKind === "remote" && sourcePolicy.includeStream2) {
    return "stream2";
  }
  if (sourcePolicy.includeCanonical) {
    return "canonical";
  }
  if (sourcePolicy.includeFirehose) {
    return "firehose";
  }
  if (sourcePolicy.includeUnified) {
    return "unified";
  }
  return null;
}

function normalizeHint(value: string): string {
  const cleaned = value.replace(/[^a-zA-Z0-9_+-]/g, "-").slice(0, 64);
  return cleaned.length > 0 ? cleaned : "candidate";
}
