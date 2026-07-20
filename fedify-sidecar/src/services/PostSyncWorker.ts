import { pool } from "../db/postgres.js";
import { activityPodsConfig } from "../config/v6-config.js";
import { logger } from "../utils/logger.js";

export class PostSyncWorker {
  private timer: NodeJS.Timeout | null = null;
  private isRunning: boolean = false;
  private isTickActive: boolean = false;
  private readonly intervalMs: number;

  constructor(intervalMs: number = 5000) {
    this.intervalMs = intervalMs;
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
    }
  }

  /**
   * Synchronize a single post to the ActivityPods outbox
   */
  private async syncPost(row: any): Promise<void> {
    // If target protocol is strictly ATProto, mark synced without sending to ActivityPods outbox
    if (row.target_protocol === "atproto") {
      await pool.query(
        `UPDATE local_cache_posts
         SET synced_to_fuseki = TRUE,
             updated_at = NOW(),
             error_message = NULL
         WHERE id = $1`,
        [row.id]
      );
      logger.info(`[PostSyncWorker] ATProto-targeted post ${row.id} processed`, { postId: row.id });
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
        body: JSON.stringify({ actorUri: webId, activity }),
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
}
