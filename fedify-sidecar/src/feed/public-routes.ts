import type { FastifyInstance, FastifyRequest } from "fastify";
import type { DefaultPodFeedService } from "./PodFeedService.js";
import type { DefaultPodHydrationService } from "./PodHydrationService.js";
import crypto from "node:crypto";
import { pool } from "../db/postgres.js";

export interface PublicFeedRouteDeps {
  feedService: DefaultPodFeedService;
  hydrationService: DefaultPodHydrationService;
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

function mapToUnifiedFeedItem(item: any): any {
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
    replyParentUri: null,
    replyRootUri: null,
    likeCount: item.engagement?.likeCount || 0,
    repostCount: item.engagement?.shareCount || 0,
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

      const mappedItems: any[] = [];
      for (const item of hydrated.items) {
        const presence = item.protocolPresence || (item.id.startsWith("at://") ? ["at"] : ["ap", "at"]);
        if (query.source === "atproto" && !presence.includes("at")) continue;
        if (query.source === "activitypods" && !presence.includes("ap")) continue;

        mappedItems.push(mapToUnifiedFeedItem(item));
      }

      // Query unsynced posts from local cache database
      const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
      let unsyncedItems: any[] = [];
      try {
        const { rows } = await pool.query(
          `SELECT id, actor_uri, content, post_type, name, summary, hashtags, attachments, target_protocol, created_at
           FROM local_cache_posts
           ORDER BY created_at DESC`
        );
        
        const rawUnsynced = rows.map((row: any) => {
          const authorName = row.actor_uri.split("/").pop() || "user";
          const objectUri = `${activityPodsUrl}/${authorName}/data/${row.post_type === "article" ? "article" : "note"}/${row.id}`;
          const isAtProtoOnly = row.target_protocol === "atproto";
          const itemSource = isAtProtoOnly ? "atproto" : "activitypods";
          const atUri = `at://did:plc:${authorName}/app.bsky.feed.post/${row.id.replace(/-/g, '')}`;
          
          return {
            id: isAtProtoOnly ? atUri : objectUri,
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
            type: "post",
            protocolPresence: isAtProtoOnly ? ["at"] : (row.target_protocol === "activitypods" ? ["ap"] : ["ap", "at"]),
            hashtags: row.hashtags || [],
            attachments: Array.isArray(row.attachments) ? row.attachments : []
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

      const rootItem = rootHydrateRes.items[0];
      if (!rootItem) {
        reply.code(404).send({ error: "not_found", message: "Thread root not found" });
        return;
      }

      const rootStableId = rootItem.id;
      const replyDocs = await queryReplies(rootStableId, 100);

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

      const mappedReplies = hydratedReplies.items.map(mapToUnifiedFeedItem);
      const rootItemMapped = mapToUnifiedFeedItem(rootItem);

      reply.send({
        rootUri,
        root: rootItemMapped,
        items: mappedReplies,
        nextCursor: null,
        hasMore: false,
        replyCount: mappedReplies.length,
        participantCount: new Set(mappedReplies.map(r => r.authorWebId)).size,
        lastActivityAt: mappedReplies.length > 0 
          ? new Date(Math.max(...mappedReplies.map(r => new Date(r.createdAt).getTime()))).toISOString()
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

    try {
      const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
      const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";

      // Post Announce to outbox via internal-social-api
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
            "object": subjectUri
          }
        })
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`ActivityPods outbox post failed: ${response.status} ${text}`);
      }

      const result = await response.json() as any;
      const activityUri = result.activityUri;

      reply.send({
        ok: true,
        subjectUri,
        repostUri: activityUri || `${webId}/activities/repost-${Date.now()}`,
        reposted: true
      });
    } catch (err) {
      console.error("Repost failed:", err);
      reply.code(500).send({ error: "internal_error", message: String(err) });
    }
  });

  // POST /at/reposts/remove - Undo a repost (Undo Announce)
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

    let repostUri = body.repostUri;
    if (!repostUri) {
      repostUri = await findRepostUri(webId, subjectUri);
    }

    if (!repostUri) {
      reply.code(404).send({ error: "not_found", message: "Repost activity not found to undo" });
      return;
    }

    try {
      const activityPodsUrl = (process.env["ACTIVITYPODS_URL"] || "http://activitypods:3000").replace(/\/$/, "");
      const activityPodsToken = process.env["ACTIVITYPODS_TOKEN"] || "";

      // Post Undo to outbox via internal-social-api
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
            "type": "Undo",
            "actor": webId,
            "object": repostUri
          }
        })
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`ActivityPods outbox post failed: ${response.status} ${text}`);
      }

      reply.send({
        ok: true,
        subjectUri,
        repostUri,
        reposted: false
      });
    } catch (err) {
      console.error("Undo repost failed:", err);
      reply.code(500).send({ error: "internal_error", message: String(err) });
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
        // Fetch actor profile info
        const actorResponse = await fetch(`${activityPodsUrl}/api/internal/social/actor?actorUri=${encodeURIComponent(canonicalAccountId)}`, {
          headers: {
            "Authorization": `Bearer ${activityPodsToken}`
          }
        });

        let userProfile: any = {};
        if (actorResponse.ok) {
          userProfile = await actorResponse.json();
        }

        reply.send({
          token: result.token,
          user: {
            webId: canonicalAccountId,
            name: userProfile.name || username,
            handle: userProfile.preferredUsername || username
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
          reply.send({
            token: loginResult.token,
            user: {
              webId: canonicalAccountId,
              name: username,
              handle: username
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
}
