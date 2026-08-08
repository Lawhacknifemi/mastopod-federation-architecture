import { pool } from "../db/postgres.js";
import { activityPodsConfig } from "../config/v6-config.js";
import { logger } from "../utils/logger.js";

export class PostSyncWorker {
  private timer: NodeJS.Timeout | null = null;
  private isRunning: boolean = false;
  private isTickActive: boolean = false;
  private retriggerPending: boolean = false;
  private readonly intervalMs: number;

  constructor(intervalMs: number = 5000) {
    this.intervalMs = intervalMs;
  }

  /**
   * Ask for an immediate sync attempt instead of waiting for the next
   * periodic tick — called right after a new post is cached so the
   * common case doesn't pay for a fixed poll delay.
   *
   * Deliberately preserves the same one-write-in-flight-at-a-time
   * discipline the periodic poll already has (see tick()'s isTickActive
   * guard): if nothing is running, this starts a tick now; if a tick is
   * already in flight, it just flags that another pass is needed the
   * moment the current one finishes, rather than firing a second
   * concurrent tick. Total writes against Fuseki/ActivityPods per post
   * are unchanged either way — this only removes idle waiting when the
   * worker has nothing else to do.
   */
  triggerNow(): void {
    if (!this.isRunning) {
      return;
    }
    if (this.isTickActive) {
      this.retriggerPending = true;
      return;
    }
    void this.tick();
  }

  /**
   * Start the worker
   */
  start(): void {
    if (this.isRunning) {
      logger.warn("[PostSyncWorker] Service is already running");
      return;
    }

    this.isRunning = true;
    logger.info(`[PostSyncWorker] Service starting (interval: ${this.intervalMs}ms)`);

    this.timer = setInterval(() => {
      void this.tick();
    }, this.intervalMs);
  }

  /**
   * Stop the worker
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    logger.info("[PostSyncWorker] Service stopped");

    // Wait for active tick to complete if one is running
    while (this.isTickActive) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  /**
   * Single polling tick to process unsynced posts
   */
  private async tick(): Promise<void> {
    if (this.isTickActive) {
      return;
    }

    this.isTickActive = true;

    try {
      // Find up to 10 unsynced posts, skipping poisoned records (sync_attempts >= 5)
      const { rows } = await pool.query(
        `SELECT id, actor_uri, content, post_type, name, summary, hashtags, attachments, target_protocol, sync_attempts
         FROM local_cache_posts
         WHERE synced_to_fuseki = FALSE AND sync_attempts < 5
         ORDER BY created_at ASC
         LIMIT 10`
      );

      if (rows.length > 0) {
        logger.debug(`[PostSyncWorker] Found ${rows.length} unsynced posts to sync`);
      }

      for (const row of rows) {
        if (!this.isRunning) {
          break; // Stop processing if the worker is shutting down
        }
        await this.syncPost(row);
      }
    } catch (err: any) {
      logger.error("[PostSyncWorker] Tick processing error", { error: err.message });
    } finally {
      this.isTickActive = false;

      // A triggerNow() call arrived while this tick was running — run
      // once more immediately rather than making that post wait for the
      // periodic timer. Still only one tick active at a time.
      if (this.retriggerPending) {
        this.retriggerPending = false;
        if (this.isRunning) {
          void this.tick();
        }
      }
    }
  }

  /**
   * Synchronize a single post to the ActivityPods outbox
   */
  private async syncPost(row: any): Promise<void> {
    // ATProto-only posts never touch the ActivityPub outbox — they go
    // straight to ActivityPods' /atproto-post endpoint, which creates an
    // app.bsky.feed.post record directly with no AP resource/activity at
    // all. This used to just mark the row synced and return without
    // calling anything, which silently dropped the post on both networks.
    if (row.target_protocol === "atproto") {
      await this.syncAtprotoOnlyPost(row);
      return;
    }

    const activityPodsUrl = activityPodsConfig.url.replace(/\/$/, "");
    const activityPodsToken = activityPodsConfig.token;
    const webId = row.actor_uri;

    const username = webId.split("/").pop() || "user";
    const objectUri = `${activityPodsUrl}/${username}/data/${row.post_type === "article" ? "article" : "note"}/${row.id}`;

    // Format tags
    const tags = (row.hashtags || []).map((tag: string) => ({
      type: "Hashtag",
      name: tag.startsWith("#") ? tag : `#${tag}`,
      href: `${activityPodsUrl}/tags/${tag.replace(/^#/, "")}`,
    }));

    // Format attachments
    const attachments = Array.isArray(row.attachments) ? row.attachments : [];
    const apAttachments = attachments.map((att: any) => ({
      type: att.type || "Image",
      mediaType: att.mediaType,
      url: att.url,
      name: att.name || undefined,
    }));

    // Build standard Create activity matching ActivityPods outbox contract
    const activity = {
      "@context": "https://www.w3.org/ns/activitystreams",
      type: "Create",
      actor: webId,
      to: ["https://www.w3.org/ns/activitystreams#Public"],
      object: {
        "@id": objectUri,
        type: row.post_type === "article" ? "Article" : "Note",
        attributedTo: webId,
        content: row.content || "",
        to: ["https://www.w3.org/ns/activitystreams#Public"],
        ...(row.name ? { name: row.name } : {}),
        ...(row.summary ? { summary: row.summary } : {}),
        ...(tags.length > 0 ? { tag: tags } : {}),
        ...(apAttachments.length > 0 ? { attachment: apAttachments } : {}),
      },
    };

    try {
      const response = await fetch(`${activityPodsUrl}/api/internal/social/outbox`, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${activityPodsToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ actorUri: webId, activity, targetProtocol: row.target_protocol }),
      });

      if (!response.ok) {
        const text = await response.text().catch(() => "");
        if (response.status === 400 && text.includes("already exist")) {
          logger.info(`[PostSyncWorker] Resource ${row.id} already exists in ActivityPods, marking synced`, { postId: row.id });
        } else {
          throw new Error(`HTTP ${response.status}: ${text}`);
        }
      }

      // Sync successful - update record in DB
      await pool.query(
        `UPDATE local_cache_posts
         SET synced_to_fuseki = TRUE,
             updated_at = NOW(),
             error_message = NULL
         WHERE id = $1`,
        [row.id]
      );

      logger.info(`[PostSyncWorker] Successfully synced post ${row.id} to ActivityPods`, {
        postId: row.id,
        actor: webId,
        objectUri
      });
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      logger.error(`[PostSyncWorker] Failed to sync post ${row.id}`, {
        postId: row.id,
        attempts: row.sync_attempts + 1,
        error: errorMsg,
      });

      // Update sync attempts counter and save error message
      await pool.query(
        `UPDATE local_cache_posts
         SET sync_attempts = sync_attempts + 1,
             last_attempt_at = NOW(),
             error_message = $1,
             updated_at = NOW()
         WHERE id = $2`,
        [errorMsg, row.id]
      );
    }
  }

  /**
   * Sync a post whose target_protocol is strictly "atproto" — calls
   * ActivityPods' /atproto-post endpoint, which creates an
   * app.bsky.feed.post record directly (no AP resource/activity created).
   */
  private async syncAtprotoOnlyPost(row: any): Promise<void> {
    const activityPodsUrl = activityPodsConfig.url.replace(/\/$/, "");
    const activityPodsToken = activityPodsConfig.token;
    const webId = row.actor_uri;

    const tags = (row.hashtags || []).map((tag: string) => ({
      type: "Hashtag",
      name: tag.startsWith("#") ? tag : `#${tag}`,
      href: `${activityPodsUrl}/tags/${tag.replace(/^#/, "")}`,
    }));

    const attachments = Array.isArray(row.attachments) ? row.attachments : [];

    try {
      const response = await fetch(`${activityPodsUrl}/api/internal/social/atproto-post`, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${activityPodsToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          actorUri: webId,
          content: row.content || "",
          tags: tags.length > 0 ? tags : undefined,
          attachments: attachments.length > 0 ? attachments : undefined,
        }),
      });

      if (!response.ok) {
        const text = await response.text().catch(() => "");
        throw new Error(`HTTP ${response.status}: ${text}`);
      }

      await pool.query(
        `UPDATE local_cache_posts
         SET synced_to_fuseki = TRUE,
             updated_at = NOW(),
             error_message = NULL
         WHERE id = $1`,
        [row.id]
      );

      logger.info(`[PostSyncWorker] Successfully synced ATProto-only post ${row.id}`, {
        postId: row.id,
        actor: webId,
      });
    } catch (err: any) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      logger.error(`[PostSyncWorker] Failed to sync ATProto-only post ${row.id}`, {
        postId: row.id,
        attempts: row.sync_attempts + 1,
        error: errorMsg,
      });

      await pool.query(
        `UPDATE local_cache_posts
         SET sync_attempts = sync_attempts + 1,
             last_attempt_at = NOW(),
             error_message = $1,
             updated_at = NOW()
         WHERE id = $2`,
        [errorMsg, row.id]
      );
    }
  }
}
