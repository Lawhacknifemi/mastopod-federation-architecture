import type { FastifyInstance, FastifyRequest } from "fastify";
import type { DefaultPodFeedService } from "./PodFeedService.js";
import type { DefaultPodHydrationService } from "./PodHydrationService.js";
import crypto from "node:crypto";
import { pool } from "../db/postgres.js";

export interface PublicFeedRouteDeps {
  feedService: DefaultPodFeedService;
  hydrationService: DefaultPodHydrationService;
  /**
   * Returns the running PostSyncWorker, or null if it hasn't started yet.
   * A getter (not the instance itself) because this module's routes are
   * registered before the worker is created during background startup —
   * capturing the instance directly here would freeze on its initial
   * null value. See POST /at/create-post, which calls triggerNow() on it
   * after caching a post so sync doesn't wait for the next poll tick.
   */
  getPostSyncWorker: () => { triggerNow: () => void } | null;
}

/**
 * Resolves a signed-in/signed-up account's real atprotoDid/atprotoHandle
 * via ActivityPods' internal identity-projection API, for embedding in the
 * /at/auth/login and /at/auth/signup responses.
 *
 * Fails open (returns nulls) rather than failing the whole login/signup —
 * the DID is a nice-to-have for the frontend session, not a login
 * precondition. This is the credentials-based counterpart to
 * atproto-identity-api.service.js's GET /.well-known/atproto-identity
 * (used by the separate OIDC sign-in fallback in oidc.ts) — that route
 * needs a browser-held OIDC token, which this server-to-server call
 * doesn't have, so it goes through the internal bearer-token API instead.
 */
async function fetchAtprotoIdentity(
  activityPodsUrl: string,
  activityPodsToken: string,
  canonicalAccountId: string
): Promise<{ atprotoDid: string | null; atprotoHandle: string | null }> {
  try {
    const res = await fetch(
      `${activityPodsUrl}/api/internal/identity/by-canonical-account-id?canonicalAccountId=${encodeURIComponent(canonicalAccountId)}`,
      { headers: { Authorization: `Bearer ${activityPodsToken}` } }
    );
    if (!res.ok) return { atprotoDid: null, atprotoHandle: null };
    const projection = (await res.json()) as any;
    return {
      atprotoDid: projection.atprotoDid || null,
      atprotoHandle: projection.atprotoHandle || null
    };
  } catch {
    return { atprotoDid: null, atprotoHandle: null };
  }
}

/**
 * Resolves the real AT record CID for a given ActivityPub object URI, via
 * ActivityPods' GET /record-cid (backed by the canonicalRefId correlation
 * atproto.createRecord stores). Used by POST /at/reply to build a proper
 * strongRef {uri, cid} for the ATProto side of a reply, instead of a
 * placeholder CID a real relay/AppView has no way to verify.
 *
 * Returns null (not an error) when the parent's author has no DID, or no
 * AT record was ever created for that URI (e.g. it's AP-only) — callers
 * should treat that as "post as standalone on AT", not retry.
 */
async function fetchAtRecordCid(
  activityPodsUrl: string,
  activityPodsToken: string,
  parentActorUri: string,
  canonicalRefId: string
): Promise<string | null> {
  try {
    const res = await fetch(
      `${activityPodsUrl}/api/internal/social/record-cid?actorUri=${encodeURIComponent(parentActorUri)}&canonicalRefId=${encodeURIComponent(canonicalRefId)}`,
      { headers: { Authorization: `Bearer ${activityPodsToken}` } }
    );
    if (!res.ok) return null;
    const body = (await res.json()) as any;
    return body.cid || null;
  } catch {
    return null;
  }
}

/**
 * Resolves which network(s) a post actually exists on — 'all',
 * 'activitypods', or 'atproto' — by looking up its own target_protocol in
 * local_cache_posts (every post and reply made through this system gets a
 * row there with the value it was actually created with; see
 * POST /at/create-post and POST /at/reply's own insert below).
 *
 * Used by POST /at/reply to cap a reply's actual delivery to whatever
 * network(s) its parent is really on — a reply to an ActivityPub-only
 * post should never spawn a standalone record on ATProto (or vice
 * versa), even if the replier asked for "all". Returns null if the
 * parent isn't found (e.g. it was created by something other than this
 * system's own endpoints) — callers should treat that as "unknown,
 * don't additionally constrain" rather than block the reply.
 */
async function resolveParentScope(parentUri: string): Promise<"all" | "activitypods" | "atproto" | null> {
  try {
    const parentId = new URL(parentUri).pathname.split("/").filter(Boolean).pop();
    if (!parentId) return null;
    const { rows } = await pool.query(
      `SELECT target_protocol FROM local_cache_posts WHERE id = $1 LIMIT 1`,
      [parentId]
    );
    const value = rows[0]?.target_protocol;
    return value === "activitypods" || value === "atproto" ? value : value === "all" ? "all" : null;
  } catch {
    return null;
  }
}

function parseRequestBody(body: unknown): any {
  if (typeof body === "string") {
    try {
      return JSON.parse(body);
    } catch {
      return {};
    }
  }
  return body || {};
}

function stringToNumericId(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash << 5) - hash + str.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

// In-memory persistent like, reply & repost tracking maps
const userLikesMap = new Map<string, Set<string>>();
const postLikeCountMap = new Map<string, number>();
const postReplyCountMap = new Map<string, number>();
const userRepostsMap = new Map<string, Set<string>>();
const postRepostCountMap = new Map<string, number>();

function extractRawId(uri: string): string | undefined {
  if (!uri) return undefined;
  const lastSegment = uri.split("/").pop() || "";
  return lastSegment.replace(/-/g, "").toLowerCase();
}

function isPostLikedByUser(webId: string | undefined, postUri: string): boolean {
  if (!webId || !postUri) return false;
  const userLikes = userLikesMap.get(webId);
  if (!userLikes) return false;
  if (userLikes.has(postUri)) return true;

  const rawId = extractRawId(postUri);
  if (!rawId) return false;
  for (const uri of userLikes) {
    if (extractRawId(uri) === rawId) return true;
  }
  return false;
}

function getPostLikeCount(postUri: string, baseCount: number): number {
  if (!postUri) return Math.max(0, baseCount);
  let offset = postLikeCountMap.get(postUri) || 0;
  if (offset === 0) {
    const rawId = extractRawId(postUri);
    if (rawId) {
      for (const [key, count] of postLikeCountMap.entries()) {
        if (extractRawId(key) === rawId) {
          offset = count;
          break;
        }
      }
    }
  }
  return Math.max(0, baseCount + offset);
}

function isPostRepostedByUser(webId: string | undefined, postUri: string): boolean {
  if (!webId || !postUri) return false;
  const userReposts = userRepostsMap.get(webId);
  if (!userReposts) return false;
  if (userReposts.has(postUri)) return true;

  const rawId = extractRawId(postUri);
  if (!rawId) return false;
  for (const uri of userReposts) {
    if (extractRawId(uri) === rawId) return true;
  }
  return false;
}

function getPostRepostCount(postUri: string, baseCount: number): number {
  if (!postUri) return Math.max(0, baseCount);
  let offset = postRepostCountMap.get(postUri) || 0;
  if (offset === 0) {
    const rawId = extractRawId(postUri);
    if (rawId) {
      for (const [key, count] of postRepostCountMap.entries()) {
        if (extractRawId(key) === rawId) {
          offset = count;
          break;
        }
      }
    }
  }
  return Math.max(0, baseCount + offset);
}

function getPostReplyCount(postUri: string, baseCount: number, dbCountsMap?: Map<string, number>): number {
  const inMemoryOffset = postReplyCountMap.get(postUri) || 0;
  const dbCount = dbCountsMap ? (dbCountsMap.get(postUri) || 0) : 0;
  return Math.max(0, baseCount + inMemoryOffset + dbCount);
}

function mapToUnifiedFeedItem(item: any, viewerWebId?: string, dbCountsMap?: Map<string, number>): any {
  const isAtProto = item.id.startsWith("at://");
  
  let hasMedia = false;
  let attachments: any[] = [];
  if (item.media && Array.isArray(item.media)) {
    hasMedia = item.media.length > 0;
    attachments = item.media.map((m: any, index: number) => ({
      id: String(index),
      type: m.mimeType?.startsWith("video") ? "Video" : "Image",
      mediaType: m.mimeType || "image/jpeg",
      url: m.url,
      previewUrl: m.url
    }));
  }

  const numericId = stringToNumericId(item.id);
  const authorId = stringToNumericId(item.author?.id || "");

  const hashtags: string[] = [];
  if (item.content?.text) {
    const hashRegex = /#(\w+)/g;
    let match;
    while ((match = hashRegex.exec(item.content.text)) !== null) {
      if (match[1]) {
        hashtags.push(match[1]);
      }
    }
  }

  const baseLikeCount = item.engagement?.likeCount || 0;
  const finalLikeCount = getPostLikeCount(item.id, baseLikeCount);
  const baseReplyCount = item.engagement?.replyCount || 0;
  const finalReplyCount = getPostReplyCount(item.id, baseReplyCount, dbCountsMap);
  const viewerHasLiked = isPostLikedByUser(viewerWebId, item.id) || !!item.engagement?.viewerHasLiked;
  const finalRepostCount = getPostRepostCount(item.id, item.engagement?.shareCount || 0);
  const viewerHasReposted = isPostRepostedByUser(viewerWebId, item.id) || (item.engagement?.viewerHasReposted ?? false);

  return {
    id: numericId,
    content: item.content?.text || item.content?.html || "",
    hashtags,
    postType: item.type?.toLowerCase() === "article" ? "article" : "note",
    title: item.content?.summary || null,
    summary: item.content?.summary || null,
    canonicalUrl: item.url || item.id,
    hasMedia,
    createdAt: item.publishedAt || new Date().toISOString(),
    isPublic: true,
    authorId,
    authorName: item.author?.displayName || item.author?.handle || "User",
    authorWebId: item.author?.id || "",
    authorProviderEndpoint: item.author?.id ? new URL(item.author.id).origin : "",
    authorAvatar: item.author?.avatarUrl || null,
    source: isAtProto ? "atproto" : "activitypods",
    atUri: isAtProto ? item.id : null,
    objectUri: !isAtProto ? item.id : null,
    replyParentUri: item.replyToStableId || null,
    replyRootUri: item.replyToStableId || null,
    likeCount: finalLikeCount,
    replyCount: finalReplyCount,
    repostCount: finalRepostCount,
    viewerHasLiked,
    viewerHasReposted,
    quoteCount: 0,
    type: "post"
  };
}

function mapToAtPost(item: any): any {
  const isAtProto = item.id.startsWith("at://");
  const numericId = stringToNumericId(item.id);
  
  return {
    id: numericId,
    authorDid: item.author?.id || "",
    rkey: isAtProto ? item.id.split("/").pop() || "" : "",
    atUri: isAtProto ? item.id : "",
    cid: null,
    content: item.content?.text || "",
    isPublic: true,
    facets: null,
    embeds: null,
    replyParentUri: null,
    createdAt: item.publishedAt || new Date().toISOString(),
    ingestedAt: item.publishedAt || new Date().toISOString(),
    sourceRelay: null,
    authorHandle: item.author?.handle || item.author?.displayName || null
  };
}

async function introspectToken(token: string): Promise<{ active: boolean; webId?: string }> {
  const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
  const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";
  
  try {
    const response = await fetch(`${activityPodsUrl}/api/internal/oauth/introspect`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${activityPodsToken}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ token })
    });

    if (!response.ok) {
      return { active: false };
    }

    const data = await response.json() as any;
    if (data && data.active) {
      return { active: true, webId: data.sub || data.canonical_account_id };
    }
  } catch (e) {
    console.error("Introspection request failed:", e);
  }
  return { active: false };
}

async function getAuthedWebId(req: FastifyRequest): Promise<string | undefined> {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return undefined;
  }
  const token = authHeader.substring(7).trim();

  // Try verifying as local HMAC token first (bypassing OIDC)
  const hmacWebId = verifyHmacToken(token);
  if (hmacWebId) {
    return hmacWebId;
  }

  const intro = await introspectToken(token);
  return intro.active ? intro.webId : undefined;
}

function verifyHmacToken(token: string): string | null {
  const secretKey = process.env["ACTIVITYPODS_TOKEN"] || "";
  if (!secretKey) return null;

  try {
    const parts = token.split(".");
    if (parts.length !== 2) return null;
    const base64Payload = parts[0];
    const signature = parts[1];
    if (!base64Payload || !signature) return null;

    const expectedSignature = crypto
      .createHmac("sha256", secretKey)
      .update(base64Payload)
      .digest("base64url");

    if (signature !== expectedSignature) {
      return null;
    }

    const payloadStr = Buffer.from(base64Payload, "base64url").toString("utf8");
    const payload = JSON.parse(payloadStr);

    if (!payload.actorUri || typeof payload.exp !== "number") {
      return null;
    }

    if (payload.exp < Date.now()) {
      return null;
    }

    return payload.actorUri;
  } catch (err) {
    return null;
  }
}

async function findRepostUri(webId: string, subjectUri: string): Promise<string | undefined> {
  const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
  const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";

  try {
    const response = await fetch(
      `${activityPodsUrl}/api/internal/social/outbox?actorUri=${encodeURIComponent(webId)}&page=1&pageSize=100`,
      {
        headers: {
          "Authorization": `Bearer ${activityPodsToken}`
        }
      }
    );
    if (!response.ok) return undefined;
    const data = await response.json() as any;
    const items = data.items || [];
    for (const item of items) {
      if (item.type === "Announce" && (item.object === subjectUri || (typeof item.object === "object" && item.object?.id === subjectUri))) {
        return item.id || item["@id"];
      }
    }
  } catch (e) {
    console.error("Failed to lookup outbox for repost removal:", e);
  }
  return undefined;
}


async function queryReplies(rootStableId: string, limit: number): Promise<any[]> {
  const searchBackend = process.env["SEARCH_BACKEND"] || "opensearch";
  
  if (searchBackend === "qdrant") {
    const qdrantUrl = (process.env["QDRANT_URL"] || "http://localhost:6333").replace(/\/$/, "");
    const collection = process.env["QDRANT_COLLECTION_NAME"] || "public-content-v1";
    const apiKey = process.env["QDRANT_API_KEY"];

    const response = await fetch(`${qdrantUrl}/collections/${encodeURIComponent(collection)}/points/scroll`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(apiKey ? { "api-key": apiKey } : {})
      },
      body: JSON.stringify({
        filter: {
          must: [
            { key: "isDeleted", match: { value: false } },
            { key: "replyToStableId", match: { value: rootStableId } }
          ]
        },
        limit
      })
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Qdrant scroll failed: ${text}`);
    }

    const data = await response.json() as any;
    return (data.result?.points || []).map((p: any) => p.payload);
  } else {
    const osUrl = (process.env["OPENSEARCH_URL"] || "http://localhost:9200").replace(/\/$/, "");
    const indexName = process.env["OPENSEARCH_INDEX_NAME"] || "public-content-v1";
    
    const authHeader: Record<string, string> = {};
    if (process.env["OPENSEARCH_USERNAME"] && process.env["OPENSEARCH_PASSWORD"]) {
      const auth = Buffer.from(`${process.env["OPENSEARCH_USERNAME"]}:${process.env["OPENSEARCH_PASSWORD"]}`).toString("base64");
      authHeader["Authorization"] = `Basic ${auth}`;
    }

    const response = await fetch(`${osUrl}/${indexName}/_search`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...authHeader
      },
      body: JSON.stringify({
        size: limit,
        query: {
          bool: {
            filter: [
              { term: { isDeleted: false } },
              { term: { replyToStableId: rootStableId } }
            ]
          }
        }
      })
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`OpenSearch search failed: ${text}`);
    }

    const data = await response.json() as any;
    const hits = data.hits?.hits || [];
    return hits.map((h: any) => h._source);
  }
}

export function registerPublicFeedRoutes(app: FastifyInstance, deps: PublicFeedRouteDeps): void {
  // GET /at/feed - Unified feed (AT + ActivityPods)
  app.get("/at/feed", async (req, reply) => {
    const webId = await getAuthedWebId(req);
    const query = req.query as any;

    let feedId = "urn:activitypods:feed:public-discovery:v1";
    if (query.hashtag) {
      feedId = "urn:activitypods:feed:topic:v1";
    } else if (webId) {
      feedId = "urn:activitypods:feed:graph-personalized:v1";
    }

    try {
      const feedRequest = {
        feedId,
        viewerId: webId,
        limit: query.limit ? parseInt(query.limit as string, 10) : 20,
        cursor: query.cursor as string | undefined,
        filters: query.hashtag ? { tags: [query.hashtag as string] } : undefined,
        excludeViewed: query.excludeViewed === "true"
      };

      const skeleton = await deps.feedService.getFeed(feedRequest);
      const hydrated = await deps.hydrationService.hydrate({
        viewerId: webId,
        items: skeleton.items,
        shape: "card"
      });

      // Fetch DB reply counts grouped by in_reply_to
      const dbReplyCountsMap = new Map<string, number>();
      try {
        const { rows: replyCountRows } = await pool.query(
          `SELECT in_reply_to, COUNT(*) as cnt FROM local_cache_posts WHERE in_reply_to IS NOT NULL GROUP BY in_reply_to`
        );
        for (const r of replyCountRows) {
          if (r.in_reply_to) {
            dbReplyCountsMap.set(r.in_reply_to, parseInt(r.cnt, 10));
          }
        }
      } catch (err) {
        console.error("Failed to query DB reply counts:", err);
      }

      const mappedItems: any[] = [];
      for (const item of hydrated.items) {
        const presence = (item as any).protocolPresence || (item.id.startsWith("at://") ? ["at"] : ["ap", "at"]);
        if (query.source === "atproto" && !presence.includes("at")) continue;
        if (query.source === "activitypods" && !presence.includes("ap")) continue;

        const mapped = mapToUnifiedFeedItem(item, webId, dbReplyCountsMap);
        // Exclude direct replies from the top-level timeline feed
        if (mapped.replyParentUri || (item as any).replyToStableId) continue;

        mappedItems.push(mapped);
      }

      // Query unsynced posts from local cache database
      const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
      let unsyncedItems: any[] = [];
      try {
        const { rows } = await pool.query(
          `SELECT id, actor_uri, content, post_type, name, summary, hashtags, attachments, target_protocol, in_reply_to, created_at
           FROM local_cache_posts
           WHERE in_reply_to IS NULL
           ORDER BY created_at DESC`
        );
        
        const rawUnsynced = rows.map((row: any) => {
          const authorName = row.actor_uri.split("/").pop() || "user";
          const objectUri = `${activityPodsUrl}/${authorName}/data/${row.post_type === "article" ? "article" : "note"}/${row.id}`;
          const isAtProtoOnly = row.target_protocol === "atproto";
          const itemSource = isAtProtoOnly ? "atproto" : "activitypods";
          const atUri = `at://did:plc:${authorName}/app.bsky.feed.post/${row.id.replace(/-/g, '')}`;
          const postId = isAtProtoOnly ? atUri : objectUri;
          
          return {
            id: postId,
            content: row.content,
            postType: row.post_type,
            title: row.name || null,
            summary: row.summary || null,
            canonicalUrl: objectUri,
            createdAt: row.created_at.toISOString(),
            isPublic: true,
            authorId: null,
            authorName: authorName,
            authorWebId: row.actor_uri,
            authorProviderEndpoint: new URL(row.actor_uri).origin,
            authorAvatar: null,
            source: itemSource,
            atUri: (row.target_protocol === "atproto" || row.target_protocol === "all") ? atUri : null,
            objectUri: objectUri,
            replyParentUri: row.in_reply_to || null,
            replyRootUri: row.in_reply_to || null,
            type: "post",
            protocolPresence: isAtProtoOnly ? ["at"] : (row.target_protocol === "activitypods" ? ["ap"] : ["ap", "at"]),
            hashtags: row.hashtags || [],
            attachments: Array.isArray(row.attachments) ? row.attachments : [],
            likeCount: getPostLikeCount(postId, 0),
            replyCount: getPostReplyCount(postId, 0, dbReplyCountsMap),
            repostCount: getPostRepostCount(postId, 0),
            viewerHasLiked: isPostLikedByUser(webId, postId),
            viewerHasReposted: isPostRepostedByUser(webId, postId)
          };
        });

        // Filter unsynced items based on hashtag / source query
        unsyncedItems = rawUnsynced.filter((item: any) => {
          if (query.source === "atproto" && !item.protocolPresence.includes("at")) return false;
          if (query.source === "activitypods" && !item.protocolPresence.includes("ap")) return false;

          if (query.hashtag) {
            const hashtagClean = (query.hashtag as string).toLowerCase().replace(/^#/, "");
            const tagMatch = item.hashtags.some((h: string) => h.toLowerCase().replace(/^#/, "") === hashtagClean);
            if (!tagMatch) return false;
          }
          return true;
        });
      } catch (dbErr) {
        console.error("Failed to query unsynced posts from DB:", dbErr);
      }

      // Merge and sort
      let finalItems = [...unsyncedItems, ...mappedItems];
      
      // Deduplicate by objectUri or id
      const seen = new Set<string>();
      finalItems = finalItems.filter((item: any) => {
        const uri = item.objectUri || item.id;
        if (seen.has(uri)) return false;
        seen.add(uri);
        return true;
      });

      // Sort by createdAt descending
      finalItems.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());

      reply.send({
        items: finalItems,
        nextCursor: skeleton.cursor || null,
        hasMore: !!skeleton.cursor
      });
    } catch (err) {
      console.error("Failed to query public feed:", err);
      reply.code(500).send({ error: "internal_error", message: String(err) });
    }
  });

  // GET /at/posts - Public ATProto posts
  app.get("/at/posts", async (req, reply) => {
    const webId = await getAuthedWebId(req);
    const query = req.query as any;

    const feedId = webId 
      ? "urn:activitypods:feed:graph-personalized:v1" 
      : "urn:activitypods:feed:public-discovery:v1";

    try {
      const feedRequest = {
        feedId,
        viewerId: webId,
        limit: query.limit ? parseInt(query.limit as string, 10) : 20,
        cursor: query.cursor as string | undefined
      };

      const skeleton = await deps.feedService.getFeed(feedRequest);
      const hydrated = await deps.hydrationService.hydrate({
        viewerId: webId,
        items: skeleton.items,
        shape: "card"
      });

      const mappedPosts: any[] = [];
      for (const item of hydrated.items) {
        if (!item.id.startsWith("at://")) continue;
        mappedPosts.push(mapToAtPost(item));
      }

      reply.send(mappedPosts);
    } catch (err) {
      console.error("Failed to query public posts:", err);
      reply.code(500).send({ error: "internal_error", message: String(err) });
    }
  });

  // GET /at/thread - Thread context
  app.get("/at/thread", async (req, reply) => {
    const webId = await getAuthedWebId(req);
    const query = req.query as any;
    const rootUri = query.rootUri as string;

    if (!rootUri) {
      reply.code(400).send({ error: "invalid_request", message: "Missing rootUri" });
      return;
    }

    try {
      const isAtProto = rootUri.startsWith("at://");
      const rootHydrateRes = await deps.hydrationService.hydrate({
        viewerId: webId,
        items: [
          isAtProto 
            ? { canonicalUri: rootUri }
            : { activityPubObjectId: rootUri }
        ],
        shape: "card"
      });

      let rootItemMapped: any = null;
      let rootStableId = rootUri;

      if (rootHydrateRes.items && rootHydrateRes.items[0]) {
        rootItemMapped = mapToUnifiedFeedItem(rootHydrateRes.items[0], webId);
        rootStableId = rootHydrateRes.items[0].id || rootUri;
      } else {
        // Fallback: Query local_cache_posts for root item
        try {
          const { rows } = await pool.query(
            `SELECT id, actor_uri, content, post_type, name, summary, hashtags, attachments, target_protocol, in_reply_to, created_at
             FROM local_cache_posts
             WHERE id::text = $1 OR actor_uri LIKE '%' || $1 OR $1 LIKE '%' || id::text`,
            [rootUri.split('/').pop()]
          );
          if (rows.length > 0) {
            const row = rows[0];
            const authorName = row.actor_uri.split("/").pop() || "user";
            const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
            const objectUri = `${activityPodsUrl}/${authorName}/data/${row.post_type === "article" ? "article" : "note"}/${row.id}`;
            const isAtProtoOnly = row.target_protocol === "atproto";
            const atUri = `at://did:plc:${authorName}/app.bsky.feed.post/${row.id.replace(/-/g, '')}`;
            const postId = isAtProtoOnly ? atUri : objectUri;
            rootStableId = objectUri;
            rootItemMapped = {
              id: stringToNumericId(postId),
              content: row.content,
              postType: row.post_type,
              title: row.name || null,
              summary: row.summary || null,
              canonicalUrl: objectUri,
              createdAt: row.created_at.toISOString(),
              isPublic: true,
              authorId: null,
              authorName: authorName,
              authorWebId: row.actor_uri,
              authorProviderEndpoint: new URL(row.actor_uri).origin,
              authorAvatar: null,
              source: isAtProtoOnly ? "atproto" : "activitypods",
              atUri: (row.target_protocol === "atproto" || row.target_protocol === "all") ? atUri : null,
              objectUri: objectUri,
              replyParentUri: row.in_reply_to || null,
              replyRootUri: row.in_reply_to || null,
              type: "post",
              protocolPresence: isAtProtoOnly ? ["at"] : (row.target_protocol === "activitypods" ? ["ap"] : ["ap", "at"]),
              hashtags: row.hashtags || [],
              attachments: Array.isArray(row.attachments) ? row.attachments : [],
              likeCount: getPostLikeCount(postId, 0),
              replyCount: getPostReplyCount(postId, 0),
              viewerHasLiked: isPostLikedByUser(webId, postId)
            };
          }
        } catch (err) {
          console.error("Failed to query root item from local_cache_posts:", err);
        }
      }

      const mappedReplies: any[] = [];

      // 1. Hydrate OpenSearch / vector replies
      try {
        const replyDocs = await queryReplies(rootStableId, 100);
        if (replyDocs.length > 0) {
          const hydrationItems = replyDocs.map((doc: any) => {
            const isAt = doc.at?.uri ? true : false;
            return isAt 
              ? { canonicalUri: doc.at.uri }
              : { activityPubObjectId: doc.ap.objectUri };
          });

          const hydratedReplies = await deps.hydrationService.hydrate({
            viewerId: webId,
            items: hydrationItems,
            shape: "card"
          });

          mappedReplies.push(...hydratedReplies.items.map(r => mapToUnifiedFeedItem(r, webId)));
        }
      } catch (err) {
        console.warn("Failed to query vector replies:", err);
      }
      
      // 2. Query local cache replies for thread
      try {
        const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
        const { rows } = await pool.query(
          `SELECT id, actor_uri, content, post_type, name, summary, hashtags, attachments, target_protocol, in_reply_to, created_at
           FROM local_cache_posts
           WHERE in_reply_to = $1 
              OR in_reply_to = $2
              OR ($3 != '' AND in_reply_to = $3)
              OR ($4 != '' AND in_reply_to = $4)
              OR ($5 != '' AND in_reply_to = $5)
           ORDER BY created_at ASC`,
          [
            rootUri,
            rootStableId,
            rootItemMapped?.objectUri || '',
            rootItemMapped?.atUri || '',
            String(rootItemMapped?.id || '')
          ]
        );
        for (const row of rows) {
          const authorName = row.actor_uri.split("/").pop() || "user";
          const objectUri = `${activityPodsUrl}/${authorName}/data/note/${row.id}`;
          const isAtProtoOnly = row.target_protocol === "atproto";
          const atUri = `at://did:plc:${authorName}/app.bsky.feed.post/${row.id.replace(/-/g, '')}`;
          const postId = isAtProtoOnly ? atUri : objectUri;

          mappedReplies.push({
            id: stringToNumericId(postId),
            content: row.content,
            postType: row.post_type,
            title: null,
            summary: null,
            canonicalUrl: objectUri,
            createdAt: row.created_at.toISOString(),
            isPublic: true,
            authorId: null,
            authorName: authorName,
            authorWebId: row.actor_uri,
            authorProviderEndpoint: new URL(row.actor_uri).origin,
            authorAvatar: null,
            source: isAtProtoOnly ? "atproto" : "activitypods",
            atUri: isAtProtoOnly ? atUri : null,
            objectUri: objectUri,
            replyParentUri: row.in_reply_to,
            replyRootUri: row.in_reply_to,
            type: "post",
            likeCount: getPostLikeCount(postId, 0),
            viewerHasLiked: isPostLikedByUser(webId, postId)
          });
        }
      } catch (dbErr) {
        console.error("Failed to query thread local replies:", dbErr);
      }

      // Deduplicate replies by objectUri or id
      const seenReplies = new Set<string>();
      const uniqueReplies = mappedReplies.filter(r => {
        const key = r.objectUri || r.atUri || String(r.id);
        if (seenReplies.has(key)) return false;
        seenReplies.add(key);
        return true;
      });

      reply.send({
        rootUri,
        root: rootItemMapped,
        items: uniqueReplies,
        nextCursor: null,
        hasMore: false,
        replyCount: uniqueReplies.length,
        participantCount: new Set(uniqueReplies.map(r => r.authorWebId)).size,
        lastActivityAt: uniqueReplies.length > 0 
          ? new Date(Math.max(...uniqueReplies.map(r => new Date(r.createdAt).getTime()))).toISOString()
          : rootItemMapped?.createdAt || new Date().toISOString()
      });
    } catch (err) {
      console.error("Failed to query thread context:", err);
      reply.code(500).send({ error: "internal_error", message: String(err) });
    }
  });

  // Stories stub endpoints
  app.get("/at/stories", async (req, reply) => {
    reply.send({ groups: [] });
  });

  app.post("/at/stories", async (req, reply) => {
    reply.code(503).send({ error: "Story writes are not supported" });
  });

  app.delete("/at/stories", async (req, reply) => {
    reply.send({ ok: true });
  });

  app.post("/at/stories/viewed", async (req, reply) => {
    reply.send({ ok: true, recorded: 0 });
  });

  // POST /at/reposts - Repost a post (Announce)
  app.post("/at/reposts", async (req, reply) => {
    const webId = await getAuthedWebId(req);
    if (!webId) {
      reply.code(401).send({ error: "unauthorized", message: "Authentication required" });
      return;
    }

    const body = parseRequestBody(req.body);
    const subjectUri = body.objectUri || body.atUri;
    if (!subjectUri) {
      reply.code(400).send({ error: "invalid_request", message: "Missing atUri or objectUri" });
      return;
    }

    // Previously always implicitly "all" (AP outbox + whatever the bridge
    // auto-mirrored) with no way to choose — same capping rule as
    // replies now applies: a repost can't reach a network its subject
    // doesn't actually exist on.
    const requestedTargetProtocol =
      body.targetProtocol === "atproto" || body.targetProtocol === "at"
        ? "atproto"
        : body.targetProtocol === "activitypods" || body.targetProtocol === "ap"
          ? "activitypods"
          : "all";

    const subjectScope = await resolveParentScope(subjectUri);
    let normalizedTargetProtocol: "all" | "activitypods" | "atproto";
    if (!subjectScope || subjectScope === "all") {
      normalizedTargetProtocol = requestedTargetProtocol;
    } else if (requestedTargetProtocol === "all" || requestedTargetProtocol === subjectScope) {
      normalizedTargetProtocol = subjectScope;
    } else {
      reply.code(400).send({
        error: "invalid_request",
        message: `This post only exists on ${subjectScope === "activitypods" ? "ActivityPub" : "ATProto"} — can't repost it on ${requestedTargetProtocol === "activitypods" ? "ActivityPub" : "ATProto"}.`
      });
      return;
    }

    // Maintain in-memory repost state
    if (!userRepostsMap.has(webId)) {
      userRepostsMap.set(webId, new Set());
    }
    userRepostsMap.get(webId)!.add(subjectUri);
    const currentCount = postRepostCountMap.get(subjectUri) || 0;
    postRepostCountMap.set(subjectUri, currentCount + 1);

    const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
    const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";

    let apRepostUri: string | undefined;
    let atDelivered = false;

    if (normalizedTargetProtocol === "all" || normalizedTargetProtocol === "activitypods") {
      try {
        const response = await fetch(`${activityPodsUrl}/api/internal/social/outbox`, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${activityPodsToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            actorUri: webId,
            activity: {
              "@context": "https://www.w3.org/ns/activitystreams",
              "type": "Announce",
              "actor": webId,
              "object": subjectUri,
              // Without this, activitypub.activity.isPublic() (which
              // just checks `to` for the Public URI) returns false, and
              // ActivityPods' own unrelated sharing/announcer.js service
              // — which reuses the Announce activity TYPE for a totally
              // different "share access to a resource" feature — matches
              // on any non-public Announce and rejects it outright
              // ("Only the creator has the right to share the object"),
              // since the reposter isn't the original post's creator.
              // Every repost was silently failing (deleted right after
              // creation — outbox.js rolls back the activity when a
              // side-effect handler throws) until this was added.
              "to": ["https://www.w3.org/ns/activitystreams#Public"]
            },
            ...(normalizedTargetProtocol === "activitypods" ? { targetProtocol: "activitypods" } : {})
          })
        });

        if (!response.ok) {
          const text = await response.text();
          console.warn(`ActivityPods outbox post non-200: ${response.status} ${text}`);
        } else {
          const result = await response.json() as any;
          apRepostUri = result.activityUri;
        }
      } catch (err) {
        console.error("Repost outbox dispatch error:", err);
      }
    }

    // Strictly "atproto" only, not "all" — same reasoning as replies: when
    // the AP branch above ran, that outbox call already triggers the
    // native bridge's automatic AT mirror, so also calling
    // atproto-record here would double-write the repost as two
    // independent AT records.
    if (normalizedTargetProtocol === "atproto") {
      try {
        const subjectUrl = new URL(subjectUri);
        const subjectActorUri = `${subjectUrl.protocol}//${subjectUrl.host}/${subjectUrl.pathname.replace(/^\//, "").split("/")[0]}`;
        const subjectCid = await fetchAtRecordCid(activityPodsUrl, activityPodsToken, subjectActorUri, subjectUri);

        if (subjectCid) {
          const res = await fetch(`${activityPodsUrl}/api/internal/social/atproto-record`, {
            method: "POST",
            headers: {
              "Authorization": `Bearer ${activityPodsToken}`,
              "Content-Type": "application/json"
            },
            body: JSON.stringify({
              actorUri: webId,
              collection: "app.bsky.feed.repost",
              record: {
                $type: "app.bsky.feed.repost",
                subject: { uri: subjectUri, cid: subjectCid },
                createdAt: new Date().toISOString()
              }
            })
          });
          atDelivered = res.ok;
          if (!res.ok) {
            console.error("Repost AT delivery failed:", res.status, await res.text().catch(() => ""));
          }
        } else {
          // Shouldn't normally happen — capping above should have
          // already rejected this case — but resolveParentScope returns
          // null (not a hard guarantee) when the subject isn't found in
          // local_cache_posts at all, so this is a real defensive path,
          // not dead code.
          console.warn(`Repost AT delivery skipped — no AT record found for ${subjectUri}`);
        }
      } catch (err) {
        console.error("Repost AT delivery error:", err);
      }
    }

    reply.send({
      ok: true,
      subjectUri,
      repostUri: apRepostUri || `${webId}/activities/repost-${Date.now()}`,
      reposted: true,
      atDelivered
    });
  });

  // POST /at/reposts/remove - Undo a repost (Undo Announce)
  // NOTE: same as DELETE /at/like — only undoes the ActivityPub-side
  // Announce. No AT-side undo yet (would need the created repost
  // record's rkey tracked somewhere durable); auth here was already
  // correct before this pass, unlike like's.
  app.post("/at/reposts/remove", async (req, reply) => {
    const webId = await getAuthedWebId(req);
    if (!webId) {
      reply.code(401).send({ error: "unauthorized", message: "Authentication required" });
      return;
    }

    const body = parseRequestBody(req.body);
    const subjectUri = body.objectUri || body.atUri;
    if (!subjectUri) {
      reply.code(400).send({ error: "invalid_request", message: "Missing atUri or objectUri" });
      return;
    }

    // Maintain in-memory repost state
    if (userRepostsMap.has(webId)) {
      userRepostsMap.get(webId)!.delete(subjectUri);
    }
    const currentCount = postRepostCountMap.get(subjectUri) || 0;
    postRepostCountMap.set(subjectUri, Math.max(0, currentCount - 1));

    let repostUri = body.repostUri;
    if (!repostUri) {
      repostUri = await findRepostUri(webId, subjectUri);
    }

    try {
      const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
      const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";

      if (repostUri) {
        // Post Undo to outbox via internal-social-api
        await fetch(`${activityPodsUrl}/api/internal/social/outbox`, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${activityPodsToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            actorUri: webId,
            activity: {
              "@context": "https://www.w3.org/ns/activitystreams",
              "type": "Undo",
              "actor": webId,
              "object": repostUri
            }
          })
        });
      }

      reply.send({
        ok: true,
        subjectUri,
        reposted: false
      });
    } catch (err) {
      console.error("Undo repost non-fatal outbox dispatch warning:", err);
      reply.send({
        ok: true,
        subjectUri,
        reposted: false
      });
    }
  });

  // POST /at/moderation/author - Block/mute author (stub)
  app.post("/at/moderation/author", async (req, reply) => {
    reply.send({ ok: true });
  });

  // POST /at/subscribe - Subscribe to ATProto relay/PDS source (stub)
  app.post("/at/subscribe", async (req, reply) => {
    const body = parseRequestBody(req.body);
    reply.send({
      success: true,
      message: "Subscribed successfully",
      sourceId: body?.sourceId || "default-source"
    });
  });

  // GET /at/status - Query ATProto relay/PDS firehose ingestion status (stub)
  app.get("/at/status", async (req, reply) => {
    reply.send({
      sources: [],
      stats: {
        totalAtPosts: 0,
        totalAtIdentities: 0,
        totalAtRecords: 0
      }
    });
  });

  // GET /at/records - Query ATProto records (stub)
  app.get("/at/records", async (req, reply) => {
    reply.send([]);
  });

  // POST /at/create-post - Proxy create ActivityPub Note/Article to user's outbox
  // The sidecar authenticates the caller via its HMAC token, then caches the post
  // in PostgreSQL and returns immediately. A background worker periodically syncs it to ActivityPods.
  app.post("/at/create-post", async (req, reply) => {
    const webId = await getAuthedWebId(req);
    if (!webId) {
      reply.code(401).send({ error: "unauthorized", message: "Authentication required" });
      return;
    }

    const body = parseRequestBody(req.body);
    const { content, postType = "note", name, summary, hashtags = [], attachments = [], targetProtocol = "all" } = body;

    if (!content && attachments.length === 0) {
      reply.code(400).send({ error: "invalid_request", message: "Post content or attachment is required" });
      return;
    }

    const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
    const postId = crypto.randomUUID();
    const username = webId.split("/").pop() || "user";
    const objectUri = `${activityPodsUrl}/${username}/data/${postType === "article" ? "article" : "note"}/${postId}`;
    const normalizedTargetProtocol = (targetProtocol === "atproto" || targetProtocol === "activitypods") ? targetProtocol : "all";

    try {
      // Save post metadata to local PG cache
      await pool.query(
        `INSERT INTO local_cache_posts (
          id, actor_uri, content, post_type, name, summary, hashtags, attachments, target_protocol, synced_to_fuseki
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, FALSE)`,
        [
          postId,
          webId,
          content || "",
          postType,
          name || null,
          summary || null,
          hashtags,
          JSON.stringify(attachments),
          normalizedTargetProtocol,
        ]
      );

      reply.code(201).send({ ok: true, objectUri, targetProtocol: normalizedTargetProtocol });

      // Kick the sync worker now instead of leaving this post to wait for
      // the next 5s poll tick. Fire-and-forget, after the response is
      // already sent — the worker's own isTickActive guard keeps this to
      // the same one-write-in-flight-at-a-time behavior as the periodic
      // poll, so this doesn't add Fuseki write concurrency, it just
      // removes idle waiting when nothing else is in flight.
      deps.getPostSyncWorker()?.triggerNow();
    } catch (err: any) {
      console.error("[create-post] Failed to cache post in PostgreSQL:", err);
      reply.code(500).send({ error: "internal_error", message: `Failed to queue post: ${err.message}` });
    }
  });

  // POST /at/auth/login - Direct credentials login via internal-social-api
  app.post("/at/auth/login", async (req, reply) => {
    const body = parseRequestBody(req.body);
    const { username, password } = body;
    if (!username || !password) {
      reply.code(400).send({ error: "invalid_request", message: "Username and password are required" });
      return;
    }

    const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
    const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";
    const canonicalAccountId = username.startsWith("http://") || username.startsWith("https://")
      ? username
      : `${activityPodsUrl}/${username}`;

    try {
      const response = await fetch(`${activityPodsUrl}/api/internal/social/auth/verify`, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${activityPodsToken}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          canonicalAccountId,
          password
        })
      });

      if (!response.ok) {
        if (response.status === 401 || response.status === 404) {
          reply.code(401).send({ error: "authentication_failed", message: "Invalid username or password" });
          return;
        }
        const text = await response.text();
        throw new Error(`ActivityPods verify failed: ${response.status} ${text}`);
      }

      const result = await response.json() as any;
      if (result.ok && result.token) {
        // Fetch actor profile info and the real atprotoDid in parallel —
        // two independent lookups, not a dependency chain, so this adds
        // no latency beyond whichever one is slower.
        const [actorResponse, identity] = await Promise.all([
          fetch(`${activityPodsUrl}/api/internal/social/actor?actorUri=${encodeURIComponent(canonicalAccountId)}`, {
            headers: { "Authorization": `Bearer ${activityPodsToken}` }
          }),
          fetchAtprotoIdentity(activityPodsUrl, activityPodsToken, canonicalAccountId)
        ]);

        let userProfile: any = {};
        if (actorResponse.ok) {
          userProfile = await actorResponse.json();
        }

        reply.send({
          token: result.token,
          user: {
            webId: canonicalAccountId,
            name: userProfile.name || username,
            handle: userProfile.preferredUsername || username,
            atprotoDid: identity.atprotoDid,
            atprotoHandle: identity.atprotoHandle
          }
        });
      } else {
        reply.code(401).send({ error: "authentication_failed", message: "Invalid credentials" });
      }
    } catch (err) {
      console.error("Login failed:", err);
      reply.code(500).send({ error: "internal_error", message: String(err) });
    }
  });

  // POST /at/auth/signup - Direct user registration
  app.post("/at/auth/signup", async (req, reply) => {
    const body = parseRequestBody(req.body);
    const { username, email, password } = body;
    if (!username || !password) {
      reply.code(400).send({ error: "invalid_request", message: "Username and password are required" });
      return;
    }

    const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
    const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";

    try {
      const response = await fetch(`${activityPodsUrl}/auth/signup`, {
        method: "POST",
        headers: {
          "Accept": "application/json",
          "Content-Type": "application/json",
          "Authorization": `Bearer ${activityPodsToken}`
        },
        body: JSON.stringify({
          username,
          email,
          password
        })
      });

      if (!response.ok) {
        const text = await response.text();
        let parsed: any = {};
        try {
          parsed = JSON.parse(text);
        } catch {}
        reply.code(response.status).send({
          error: parsed.error || "registration_failed",
          message: parsed.message || `Registration failed with HTTP ${response.status}`
        });
        return;
      }

      const result = await response.json() as any;
      const canonicalAccountId = result.webId || result.canonicalAccountId || `${activityPodsUrl}/${username}`;

      // Automatically log the user in after registration
      const loginResponse = await fetch(`${activityPodsUrl}/api/internal/social/auth/verify`, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${activityPodsToken}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          canonicalAccountId,
          password
        })
      });

      if (loginResponse.ok) {
        const loginResult = await loginResponse.json() as any;
        if (loginResult.ok && loginResult.token) {
          // The DID is minted synchronously as part of the signup hook
          // that already completed above (see auth.js's after.signup),
          // so it's safe to look up immediately here — no race.
          const identity = await fetchAtprotoIdentity(activityPodsUrl, activityPodsToken, canonicalAccountId);
          reply.send({
            token: loginResult.token,
            user: {
              webId: canonicalAccountId,
              name: username,
              handle: username,
              atprotoDid: identity.atprotoDid,
              atprotoHandle: identity.atprotoHandle
            }
          });
          return;
        }
      }

      reply.send({
        ok: true,
        canonicalAccountId
      });
    } catch (err) {
      console.error("Signup failed:", err);
      reply.code(500).send({ error: "internal_error", message: String(err) });
    }
  });

  // POST /at/like - Like a post
  app.post("/at/like", async (req, reply) => {
    const authedWebId = await getAuthedWebId(req);
    if (!authedWebId) {
      reply.code(401).send({ error: "unauthorized", message: "Authentication required" });
      return;
    }

    const body = parseRequestBody(req.body);
    const { postUri, targetProtocol = "all" } = body;
    // Always the authenticated caller — the previous version read
    // actorUri from the request body (defaulting to a hardcoded test
    // account when auth failed, and client-overridable even when it
    // didn't), the same impersonation gap /at/reply had.
    const actorUri = authedWebId;

    if (!postUri) {
      reply.code(400).send({ error: "invalid_request", message: "postUri is required" });
      return;
    }

    const requestedTargetProtocol =
      targetProtocol === "atproto" || targetProtocol === "at"
        ? "atproto"
        : targetProtocol === "activitypods" || targetProtocol === "ap"
          ? "activitypods"
          : "all";

    // Same capping rule as replies/reposts: a like can't reach a network
    // its subject doesn't actually exist on.
    const subjectScope = await resolveParentScope(postUri);
    let normalizedTargetProtocol: "all" | "activitypods" | "atproto";
    if (!subjectScope || subjectScope === "all") {
      normalizedTargetProtocol = requestedTargetProtocol;
    } else if (requestedTargetProtocol === "all" || requestedTargetProtocol === subjectScope) {
      normalizedTargetProtocol = subjectScope;
    } else {
      reply.code(400).send({
        error: "invalid_request",
        message: `This post only exists on ${subjectScope === "activitypods" ? "ActivityPub" : "ATProto"} — can't like it on ${requestedTargetProtocol === "activitypods" ? "ActivityPub" : "ATProto"}.`
      });
      return;
    }

    // Record like in local in-memory state
    if (!userLikesMap.has(authedWebId)) {
      userLikesMap.set(authedWebId, new Set());
    }
    const userSet = userLikesMap.get(authedWebId)!;
    if (!userSet.has(postUri)) {
      userSet.add(postUri);
      const currentOffset = postLikeCountMap.get(postUri) || 0;
      postLikeCountMap.set(postUri, currentOffset + 1);
    }

    const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
    const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";
    let atDelivered = false;

    try {
      // 1. ActivityPub, via the real outbox path — same one every other
      // create endpoint in this file uses.
      if (normalizedTargetProtocol === "all" || normalizedTargetProtocol === "activitypods") {
        const likeActivity = {
          "@context": "https://www.w3.org/ns/activitystreams",
          type: "Like",
          actor: actorUri,
          object: postUri,
          to: ["https://www.w3.org/ns/activitystreams#Public"]
        };

        try {
          const res = await fetch(`${activityPodsUrl}/api/internal/social/outbox`, {
            method: "POST",
            headers: {
              "Authorization": `Bearer ${activityPodsToken}`,
              "Content-Type": "application/json"
            },
            body: JSON.stringify({
              actorUri,
              activity: likeActivity,
              ...(normalizedTargetProtocol === "activitypods" ? { targetProtocol: "activitypods" } : {})
            })
          });
          if (!res.ok) {
            console.error("Outbox Like failed:", res.status, await res.text().catch(() => ""));
          }
        } catch (err) {
          console.error("Outbox Like error:", err);
        }
      }

      // 2. ATProto, via the real atproto-record endpoint with the
      // subject's real CID — replaces the old call to a route
      // ActivityPods never mounted (/atproto/... 404'd every time),
      // a hardcoded fake DID, and a placeholder subject CID. Strictly
      // "atproto" only, not "all" — see /at/reply and /at/reposts for
      // why: the AP branch above already triggers the native bridge's
      // automatic mirror for "all", so calling this too would
      // double-write the like as two independent AT records.
      if (normalizedTargetProtocol === "atproto") {
        try {
          const postUrl = new URL(postUri);
          const postActorUri = `${postUrl.protocol}//${postUrl.host}/${postUrl.pathname.replace(/^\//, "").split("/")[0]}`;
          const subjectCid = await fetchAtRecordCid(activityPodsUrl, activityPodsToken, postActorUri, postUri);

          if (subjectCid) {
            const res = await fetch(`${activityPodsUrl}/api/internal/social/atproto-record`, {
              method: "POST",
              headers: {
                "Authorization": `Bearer ${activityPodsToken}`,
                "Content-Type": "application/json"
              },
              body: JSON.stringify({
                actorUri,
                collection: "app.bsky.feed.like",
                record: {
                  $type: "app.bsky.feed.like",
                  subject: { uri: postUri, cid: subjectCid },
                  createdAt: new Date().toISOString()
                }
              })
            });
            atDelivered = res.ok;
            if (!res.ok) {
              console.error("ATProto Like failed:", res.status, await res.text().catch(() => ""));
            }
          } else {
            console.warn(`Like AT delivery skipped — no AT record found for ${postUri}`);
          }
        } catch (err) {
          console.error("ATProto Like error:", err);
        }
      }

      reply.send({ ok: true, liked: true, postUri, atDelivered });
    } catch (err) {
      console.error("Like failed:", err);
      reply.code(500).send({ error: "internal_error", message: String(err) });
    }
  });

  // DELETE /at/like - Unlike a post
  // NOTE: this only undoes the ActivityPub-side Like. There's no AT-side
  // undo yet — that needs the created like record's rkey tracked
  // somewhere durable (atproto.createRecord's return has it, but nothing
  // persists it today), which is a real, separate gap from the
  // auth/CID/capping fixes made alongside this. Flagged, not fixed here.
  app.delete("/at/like", async (req, reply) => {
    const authedWebId = await getAuthedWebId(req);
    if (!authedWebId) {
      reply.code(401).send({ error: "unauthorized", message: "Authentication required" });
      return;
    }

    const body = parseRequestBody(req.body);
    const { postUri, targetProtocol = "all" } = body;
    const actorUri = authedWebId;

    if (!postUri) {
      reply.code(400).send({ error: "invalid_request", message: "postUri is required" });
      return;
    }

    // Remove like from local state
    const userSet = userLikesMap.get(authedWebId);
    if (userSet && userSet.has(postUri)) {
      userSet.delete(postUri);
      const currentOffset = postLikeCountMap.get(postUri) || 0;
      postLikeCountMap.set(postUri, Math.max(-10, currentOffset - 1));
    }

    const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
    const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";

    try {
      if (targetProtocol === "all" || targetProtocol === "activitypods" || targetProtocol === "ap") {
        const undoActivity = {
          "@context": "https://www.w3.org/ns/activitystreams",
          type: "Undo",
          actor: actorUri,
          object: {
            type: "Like",
            actor: actorUri,
            object: postUri
          },
          to: ["https://www.w3.org/ns/activitystreams#Public"]
        };

        await fetch(`${activityPodsUrl}/api/internal/social/outbox`, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${activityPodsToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({ actorUri, activity: undoActivity })
        }).catch(err => console.error("Outbox Undo Like error:", err));
      }

      reply.send({ ok: true, liked: false, postUri });
    } catch (err) {
      console.error("Unlike failed:", err);
      reply.code(500).send({ error: "internal_error", message: String(err) });
    }
  });

  // POST /at/reply - Reply / Comment on a post
  app.post("/at/reply", async (req, reply) => {
    const authedWebId = await getAuthedWebId(req);
    if (!authedWebId) {
      reply.code(401).send({ error: "unauthorized", message: "Authentication required" });
      return;
    }

    const body = parseRequestBody(req.body);
    const { parentUri, text, targetProtocol = "all" } = body;
    // The actor is always the authenticated caller — never client-supplied.
    // The previous version read actorUri from the request body (defaulting
    // to a hardcoded test account when absent, which the frontend always
    // hit in practice since it never sent one), so every reply was posted
    // as that fixed fake actor regardless of who was actually signed in.
    const actorUri = authedWebId;

    if (!parentUri || !text) {
      reply.code(400).send({ error: "invalid_request", message: "parentUri and text are required" });
      return;
    }

    const requestedTargetProtocol =
      targetProtocol === "atproto" || targetProtocol === "at"
        ? "atproto"
        : targetProtocol === "activitypods" || targetProtocol === "ap"
          ? "activitypods"
          : "all";

    // A reply can never reach a network its parent doesn't actually exist
    // on — replying "all" to an ActivityPub-only post must not spawn a
    // standalone, unthreaded record on ATProto (or vice versa). Cap the
    // requested protocol to the parent's real scope; if they don't
    // overlap at all (e.g. an ATProto-only reply to an ActivityPub-only
    // post), that's not a degraded case to silently handle — it's an
    // impossible request, and we say so rather than doing nothing.
    const parentScope = await resolveParentScope(parentUri);
    let normalizedTargetProtocol: "all" | "activitypods" | "atproto";
    if (!parentScope || parentScope === "all") {
      normalizedTargetProtocol = requestedTargetProtocol;
    } else if (requestedTargetProtocol === "all" || requestedTargetProtocol === parentScope) {
      normalizedTargetProtocol = parentScope;
    } else {
      reply.code(400).send({
        error: "invalid_request",
        message: `This post only exists on ${parentScope === "activitypods" ? "ActivityPub" : "ATProto"} — can't reply to it on ${requestedTargetProtocol === "activitypods" ? "ActivityPub" : "ATProto"}.`
      });
      return;
    }

    const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
    const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";

    try {
      const timestamp = new Date().toISOString();
      const replyId = crypto.randomUUID();
      const username = actorUri.split("/").pop() || "user";
      const replyNoteId = `${activityPodsUrl}/${username}/data/note/${replyId}`;

      // Increment parent reply count in local state
      const currentOffset = postReplyCountMap.get(parentUri) || 0;
      postReplyCountMap.set(parentUri, currentOffset + 1);

      // Save reply to local_cache_posts purely so GET /at/feed's reply-count
      // / thread queries can see it — actual delivery happens synchronously
      // below, not via PostSyncWorker. Marked synced immediately: leaving
      // this FALSE (as before) meant PostSyncWorker's own poll query would
      // pick the row up ~seconds later and repost it as an unrelated,
      // non-reply top-level Note (its SELECT doesn't even include
      // in_reply_to), duplicating every reply as a phantom standalone post.
      await pool.query(
        `INSERT INTO local_cache_posts (
          id, actor_uri, content, post_type, name, summary, hashtags, attachments, target_protocol, in_reply_to, synced_to_fuseki
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE)`,
        [
          replyId,
          actorUri,
          text,
          "note",
          null,
          null,
          [],
          JSON.stringify([]),
          normalizedTargetProtocol,
          parentUri
        ]
      ).catch(err => console.error("Failed to insert reply into local_cache_posts:", err));

      let apDelivered = false;
      // atDelivered reflects only the explicit atproto-post call below,
      // which fires for "atproto"-only replies. For "all", the AT side is
      // delivered implicitly via the native bridge's automatic mirror of
      // the AP outbox call — real, but not confirmed by this endpoint,
      // same as /at/create-post's response never confirms it either.
      let atDelivered = false;

      // 1. ActivityPub reply, via the real, proven outbox path (same one
      // /at/create-post uses) — not a hand-rolled fetch to a made-up shape.
      if (normalizedTargetProtocol === "all" || normalizedTargetProtocol === "activitypods") {
        const noteActivity = {
          "@context": "https://www.w3.org/ns/activitystreams",
          type: "Create",
          actor: actorUri,
          to: ["https://www.w3.org/ns/activitystreams#Public"],
          object: {
            "@id": replyNoteId,
            type: "Note",
            attributedTo: actorUri,
            content: text,
            inReplyTo: parentUri,
            published: timestamp,
            to: ["https://www.w3.org/ns/activitystreams#Public"]
          }
        };

        try {
          const res = await fetch(`${activityPodsUrl}/api/internal/social/outbox`, {
            method: "POST",
            headers: {
              "Authorization": `Bearer ${activityPodsToken}`,
              "Content-Type": "application/json"
            },
            body: JSON.stringify({
              actorUri,
              activity: noteActivity,
              ...(normalizedTargetProtocol === "activitypods" ? { targetProtocol: "activitypods" } : {})
            })
          });
          apDelivered = res.ok;
          if (!res.ok) {
            console.error("Reply AP delivery failed:", res.status, await res.text().catch(() => ""));
          }
        } catch (err) {
          console.error("Reply AP delivery error:", err);
        }
      }

      // 2. ATProto reply, via the real atproto-post endpoint (same one
      // used for ATProto-only top-level posts) — not the old call to a
      // route ActivityPods never actually mounted (/atproto/... 404'd
      // every time) with a hardcoded fake DID.
      //
      // Strictly "atproto" only, not "all": when the AP branch above ran
      // (targetProtocol "all"), that outbox call already triggers the
      // native bridge's automatic AT mirror (same as every top-level
      // "all" post — see PostSyncWorker.syncPost, which never calls this
      // endpoint for "all" either), so also calling atproto-post here
      // would double-write the same reply as two independent AT records.
      // First-pass testing against real Fuseki data is what caught this.
      if (normalizedTargetProtocol === "atproto") {
        try {
          // A reply strongRef needs the parent's real record CID, not
          // just its URI — resolve it via the parent author's own
          // canonicalRefId-tagged AT record. If nothing resolves (parent
          // was never mirrored to AT — e.g. it's AP-only), replyToCid is
          // omitted and postAtprotoOnly posts this as a standalone
          // record rather than shipping a strongRef with a fake CID.
          let parentCid: string | null = null;
          try {
            const parentUrl = new URL(parentUri);
            const parentActorUri = `${parentUrl.protocol}//${parentUrl.host}/${parentUrl.pathname.replace(/^\//, "").split("/")[0]}`;
            parentCid = await fetchAtRecordCid(activityPodsUrl, activityPodsToken, parentActorUri, parentUri);
          } catch (err) {
            console.error("Reply parent CID resolution error:", err);
          }

          const res = await fetch(`${activityPodsUrl}/api/internal/social/atproto-post`, {
            method: "POST",
            headers: {
              "Authorization": `Bearer ${activityPodsToken}`,
              "Content-Type": "application/json"
            },
            body: JSON.stringify({
              actorUri,
              content: text,
              replyToUri: parentUri,
              ...(parentCid ? { replyToCid: parentCid } : {}),
              // Lets a future reply to THIS reply resolve its real CID too.
              canonicalRefId: replyNoteId
            })
          });
          atDelivered = res.ok;
          if (!res.ok) {
            console.error("Reply AT delivery failed:", res.status, await res.text().catch(() => ""));
          }
        } catch (err) {
          console.error("Reply AT delivery error:", err);
        }
      }

      reply.send({
        ok: true,
        replyUri: replyNoteId,
        parentUri,
        text,
        apDelivered,
        atDelivered
      });
    } catch (err) {
      console.error("Reply failed:", err);
      reply.code(500).send({ error: "internal_error", message: String(err) });
    }
  });

  // POST /at/replies/resolve - Resolve reply policy for a post
  app.post("/at/replies/resolve", async (req, reply) => {
    const body = parseRequestBody(req.body);
    const objectUri = body.objectUri || body.parentUri || body.uri || "";

    reply.send({
      objectUri,
      mayReply: true,
      requiresApproval: false,
      canReplyIsSet: true,
      authorityUri: null,
      policyLabel: "Public",
      reason: "Public post allows replies"
    });
  });
}
