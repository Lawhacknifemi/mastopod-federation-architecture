/**
 * ActivityPods Outbox Event Emitter Service
 * 
 * This Moleculer service emits events when activities are committed to outboxes.
 * The sidecar listens to these events to:
 * 1. Produce to RedPanda Stream1 (for public activities)
 * 2. Create delivery jobs in Redis (for remote federation)
 * 
 * This replaces the "watch outboxes via Solid Notifications" approach with
 * a more reliable event-driven pattern.
 */
"use strict";

const { ulid } = require("ulid");
const {
  getActivityActorUri,
  resolveDeliveryTargets,
} = require("./activitypub-recipient-resolution");

const {
  getSearchableBy,
  isSearchableBy,
  AS_PUBLIC,
} = require('../utils/search-consent');

const {
  extractHashtagsFromText,
} = require('../utils/hashtags');

module.exports = {
  name: "outbox-emitter",

  dependencies: ["activitypub.outbox"],

  settings: {
    // Sidecar webhook URL for event delivery
    sidecarWebhookUrl: process.env.SIDECAR_WEBHOOK_URL || "http://fedify-sidecar:8080/webhook/outbox",
    sidecarToken: process.env.SIDECAR_TOKEN || "",

    // Retry settings for webhook delivery
    webhookRetries: Number(process.env.WEBHOOK_RETRIES) || 3,
    webhookTimeoutMs: Number(process.env.WEBHOOK_TIMEOUT_MS) || 5000,
  },

  events: {
    /**
     * Listen for outbox activity commits.
     * This event is emitted by ActivityPods when an activity is successfully
     * committed to an actor's outbox.
     */
    "activitypub.outbox.posted": {
	                async handler(ctx) {
        const { activity } = ctx.params;
        const actorUri = getActivityActorUri(activity);

        // Resolve remote delivery targets (not provided by ActivityPods in this event)
        let deliveryTargets = [];
        if (actorUri) {
          try {
            const result = await ctx.call('outbox-emitter.resolveDeliveryTargets', { actorUri, activity });
            deliveryTargets = result.targets || [];
          } catch (err) {
            this.logger.warn("Failed to resolve delivery targets", { actorUri, error: err.message });
          }
        }

        // Build the event payload matching the Stream1 schema
        const event = {
          schema: "ap.outbox.committed.v1",
          eventId: ulid(),
          timestamp: new Date().toISOString(),

          // Source
          actorUri,
          podDataset: ctx.meta?.podDataset,

          // Activity
          activityId: activity.id || activity["@id"],
          objectId: this.extractObjectId(activity),
          activityType: activity.type || activity["@type"],
          activity,
          bridge: ctx.meta?.protocolBridge || null,

          // Delivery targets (resolved above)
          deliveryTargets,

          // Metadata
          meta: {
            isPublicIndexable: this.isPublicIndexable(activity),
            isDeleteOrTombstone: this.isDeleteOrTombstone(activity),
            visibility: this.determineVisibility(activity),
            searchConsent: this.buildSearchConsent(activity),
            hashtags: this.extractMetadataHashtags(activity),
          },
        };

        // Emit internal event for local consumers
        ctx.emit("outbox.event.ready", event);

        // Deliver to sidecar webhook
        await this.deliverToSidecar(ctx, event);
      },
    },
  },

  actions: {
    /**
     * Manually emit an outbox event (for testing/reconciliation).
     */
    emitEvent: {
      params: {
        actorUri: { type: "string" },
        activity: { type: "object" },
        deliveryTargets: { type: "array", optional: true },
      },
      async handler(ctx) {
        const { actorUri, activity, deliveryTargets } = ctx.params;

        const event = {
          schema: "ap.outbox.committed.v1",
          eventId: ulid(),
          timestamp: new Date().toISOString(),
          actorUri,
          activityId: activity.id || activity["@id"],
          objectId: this.extractObjectId(activity),
          activityType: activity.type || activity["@type"],
          activity,
          bridge: ctx.meta?.protocolBridge || null,
          deliveryTargets: deliveryTargets || [],
          meta: {
            isPublicIndexable: this.isPublicIndexable(activity),
            isDeleteOrTombstone: this.isDeleteOrTombstone(activity),
            visibility: this.determineVisibility(activity),
            searchConsent: this.buildSearchConsent(activity),
            hashtags: this.extractMetadataHashtags(activity),
          },
        };

        await this.deliverToSidecar(ctx, event);
        return { success: true, eventId: event.eventId };
      },
    },

    /**
     * Resolve delivery targets for an activity.
     * This is called by the sidecar if it needs to resolve targets itself.
     */
    resolveDeliveryTargets: {
      params: {
        actorUri: { type: "string" },
        activity: { type: "object" },
      },
	      async handler(ctx) {
	        const { actorUri, activity } = ctx.params;

	        const activityActorUri = getActivityActorUri(activity);
	        if (!activityActorUri || activityActorUri !== actorUri) {
	          throw new Error("activity.actor does not match actorUri");
	        }

	        const isLocal = await ctx.call("activitypub.actor.isLocal", { actorUri });
	        if (!isLocal) {
	          throw new Error("actorUri is not hosted locally");
	        }

	        const actor = await ctx.call("activitypub.actor.get", { actorUri });
	        if (!actor) {
	          throw new Error("Local actor could not be resolved");
	        }

	        const targets = await resolveDeliveryTargets({
	          ctx,
	          actorUri,
	          actor,
	          activity,
	          logger: this.logger,
	        });

	        return { targets };
	      },
	    },
	  },

  methods: {
    /**
     * Deliver event to sidecar webhook.
     */
    async deliverToSidecar(ctx, event) {
      const url = this.settings.sidecarWebhookUrl;

      // Build the webhook payload in the shape the sidecar expects.
      // The internal event schema differs from the sidecar's webhook contract.
      const payload = {
        actorUri: event.actorUri,
        activityId: event.activityId,
        activity: event.activity,
        bridge: event.bridge || undefined,
        remoteTargets: event.deliveryTargets,  // sidecar validates body.remoteTargets
        meta: event.meta,
      };

      const headers = {
        "Content-Type": "application/json",
        "X-Event-Id": event.eventId,
        "X-Event-Schema": event.schema,
      };
      // Only add Authorization header if a token is configured — omitting an
      // empty Bearer token avoids confusing the sidecar's auth middleware.
      if (this.settings.sidecarToken) {
        headers["Authorization"] = `Bearer ${this.settings.sidecarToken}`;
      }

      const body = JSON.stringify(payload);

      try {
        await this.retryWithBackoff(async () => {
          const response = await fetch(url, {
            method: "POST",
            headers,
            body,
            signal: AbortSignal.timeout(this.settings.webhookTimeoutMs),
          });

          if (response.ok) {
            this.logger.debug("Delivered event to sidecar", {
              eventId: event.eventId,
              activityId: event.activityId,
            });
            return;
          }

          const error = new Error(`Sidecar webhook returned ${response.status}`);
          // 429 and 5xx are transient; 4xx (except 429) are permanent.
          error.retryable = response.status === 429 || response.status >= 500;
          throw error;
        }, {
          maxRetries: Math.max(0, this.settings.webhookRetries - 1),
          baseDelayMs: 250,
          maxDelayMs: 10000,
          retryIf: (err) => err.retryable !== false,
        });
      } catch (err) {
        this.logger.error("Failed to deliver event to sidecar after retries", {
          eventId: event.eventId,
          activityId: event.activityId,
          error: err.message,
        });
      }
    },

    /**
     * Exponential backoff with full jitter.
     * Retries fn() up to opts.maxRetries times.  Between each attempt the
     * delay is chosen uniformly at random in [0, min(maxDelayMs, baseDelayMs *
     * 2^attempt)] — the "full jitter" strategy recommended by AWS to avoid
     * thundering-herd problems.
     *
     * @param {() => Promise<void>} fn
     * @param {{ maxRetries: number, baseDelayMs: number, maxDelayMs: number, retryIf?: (err: Error) => boolean }} opts
     */
    async retryWithBackoff(fn, opts) {
      const { maxRetries, baseDelayMs, maxDelayMs, retryIf } = opts;
      let lastError;
      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
          return await fn();
        } catch (err) {
          lastError = err;
          if (attempt === maxRetries) break;
          if (retryIf && !retryIf(err)) break;
          const cap = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt));
          const delay = Math.random() * cap;
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
      throw lastError;
    },

    /**
     * Extract object ID from activity.
     */
    extractObjectId(activity) {
      const object = activity.object;
      if (!object) return null;
      if (typeof object === "string") return object;
      return object.id || object["@id"] || null;
    },

    /**
     * Check if activity is publicly indexable.
     * Uses FEP-268d searchableBy consent when present; falls back to
     * audience-based check (as:Public in to/cc) when no explicit consent is set.
     */
    isPublicIndexable(activity) {
      const obj =
        activity.object && typeof activity.object === 'object'
          ? activity.object
          : activity;
      return isSearchableBy(obj, AS_PUBLIC);
    },

    /**
     * Build a search consent descriptor for the activity's inner object.
     * Shape: { raw: string[], isPublic: boolean, explicitlySet: boolean }
     */
    buildSearchConsent(activity) {
      const obj =
        activity.object && typeof activity.object === 'object'
          ? activity.object
          : activity;
      const raw = getSearchableBy(obj);
      return {
        raw,
        isPublic: isSearchableBy(obj, AS_PUBLIC),
        explicitlySet: raw.length > 0,
      };
    },

    /**
     * Extract hashtags for dual-protocol metadata (Facets).
     */
    extractMetadataHashtags(activity) {
      const obj =
        activity.object && typeof activity.object === 'object'
          ? activity.object
          : activity;
      return extractHashtagsFromText(obj.content || "");
    },

    /**
     * Check if activity is a delete or tombstone.
     */
    isDeleteOrTombstone(activity) {
      const type = activity.type || activity["@type"];
      return type === "Delete" || type === "Tombstone" || 
             (type === "Undo" && activity.object?.type === "Announce");
    },

    /**
     * Determine visibility level.
     */
    determineVisibility(activity) {
      const publicAddress = "https://www.w3.org/ns/activitystreams#Public";
      const to = Array.isArray(activity.to) ? activity.to : [activity.to];
      const cc = Array.isArray(activity.cc) ? activity.cc : [activity.cc];

      if (to.includes(publicAddress) || to.includes("as:Public")) {
        return "public";
      }
      if (cc.includes(publicAddress) || cc.includes("as:Public")) {
        return "unlisted";
      }
      if (to.some(r => r?.endsWith("/followers"))) {
        return "followers";
      }
      return "direct";
    },

  },
};
