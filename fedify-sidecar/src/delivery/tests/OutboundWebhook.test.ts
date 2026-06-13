import { describe, expect, it } from "vitest";
import {
  evaluateOutboundWebhookBackpressure,
  normalizeAndDedupeOutboundTargets,
  OutboundWebhookValidationError,
  resolveOutboundWebhookBackpressureConfigFromEnv,
} from "../outbound-webhook.js";

describe("normalizeAndDedupeOutboundTargets", () => {
  it("dedupes repeated shared inbox targets and skips invalid entries", () => {
    const result = normalizeAndDedupeOutboundTargets(
      [
        {
          inboxUrl: "https://mastodon.example/users/alice/inbox",
          sharedInboxUrl: "https://mastodon.example/inbox",
          targetDomain: "mastodon.example",
        },
        {
          inboxUrl: "https://mastodon.example/users/bob/inbox",
          sharedInboxUrl: "https://mastodon.example/inbox",
          targetDomain: "mastodon.example",
        },
        {
          inboxUrl: "http://10.0.0.5/inbox",
          targetDomain: "10.0.0.5",
        },
      ],
      { maxTargetsPerRequest: 100 },
    );

    expect(result.targets).toEqual([
      {
        inboxUrl: "https://mastodon.example/users/alice/inbox",
        sharedInboxUrl: "https://mastodon.example/inbox",
        deliveryUrl: "https://mastodon.example/inbox",
        targetDomain: "mastodon.example",
      },
    ]);
    expect(result.inputTargetCount).toBe(3);
    expect(result.duplicateTargetCount).toBe(1);
    expect(result.invalidTargetCount).toBe(1);
  });

  it("returns empty target results when remoteTargets is empty", () => {
    const result = normalizeAndDedupeOutboundTargets([], { maxTargetsPerRequest: 100 });
    expect(result.targets).toEqual([]);
    expect(result.inputTargetCount).toBe(0);
    expect(result.duplicateTargetCount).toBe(0);
    expect(result.invalidTargetCount).toBe(0);
  });

  it("rejects requests that exceed the configured maximum target count", () => {
    expect(() =>
      normalizeAndDedupeOutboundTargets(
        [
          { inboxUrl: "https://one.example/inbox" },
          { inboxUrl: "https://two.example/inbox" },
        ],
        { maxTargetsPerRequest: 1 },
      ),
    ).toThrowError(OutboundWebhookValidationError);

    try {
      normalizeAndDedupeOutboundTargets(
        [
          { inboxUrl: "https://one.example/inbox" },
          { inboxUrl: "https://two.example/inbox" },
        ],
        { maxTargetsPerRequest: 1 },
      );
    } catch (error) {
      expect(error).toBeInstanceOf(OutboundWebhookValidationError);
      expect((error as OutboundWebhookValidationError).statusCode).toBe(413);
    }
  });
});

describe("evaluateOutboundWebhookBackpressure", () => {
  it("rejects when pending jobs exceed the configured threshold", () => {
    const result = evaluateOutboundWebhookBackpressure(
      {
        pendingCount: 200,
        streamLength: 50,
      },
      {
        maxPending: 200,
        maxQueueDepth: 500,
        retryAfterSeconds: 5,
        maxTargetsPerRequest: 100,
      },
    );

    expect(result).toEqual({
      reject: true,
      reason: "pending",
      retryAfterSeconds: 5,
    });
  });

  it("rejects when queue depth exceeds the configured threshold", () => {
    const result = evaluateOutboundWebhookBackpressure(
      {
        pendingCount: 10,
        streamLength: 500,
      },
      {
        maxPending: 200,
        maxQueueDepth: 500,
        retryAfterSeconds: 7,
        maxTargetsPerRequest: 100,
      },
    );

    expect(result).toEqual({
      reject: true,
      reason: "queue_depth",
      retryAfterSeconds: 7,
    });
  });

  it("does not reject on queue depth alone when there is no pending backlog", () => {
    const result = evaluateOutboundWebhookBackpressure(
      {
        pendingCount: 0,
        streamLength: 500,
      },
      {
        maxPending: 200,
        maxQueueDepth: 500,
        retryAfterSeconds: 7,
        maxTargetsPerRequest: 100,
      },
    );

    expect(result).toEqual({ reject: false });
  });

  it("does not reject on stream length when queue-depth gate is disabled", () => {
    const result = evaluateOutboundWebhookBackpressure(
      {
        pendingCount: 10,
        streamLength: 500_000,
      },
      {
        maxPending: 200,
        maxQueueDepth: 0,
        retryAfterSeconds: 7,
        maxTargetsPerRequest: 100,
      },
    );

    expect(result).toEqual({ reject: false });
  });
});

describe("resolveOutboundWebhookBackpressureConfigFromEnv", () => {
  it("defaults queue-depth gate to disabled to avoid stream-history false positives", () => {
    const previous = process.env["OUTBOUND_WEBHOOK_MAX_QUEUE_DEPTH"];
    delete process.env["OUTBOUND_WEBHOOK_MAX_QUEUE_DEPTH"];

    const config = resolveOutboundWebhookBackpressureConfigFromEnv();
    expect(config.maxQueueDepth).toBe(0);

    if (previous === undefined) {
      delete process.env["OUTBOUND_WEBHOOK_MAX_QUEUE_DEPTH"];
    } else {
      process.env["OUTBOUND_WEBHOOK_MAX_QUEUE_DEPTH"] = previous;
    }
  });

  it("allows explicit queue-depth thresholds via env when desired", () => {
    const previous = process.env["OUTBOUND_WEBHOOK_MAX_QUEUE_DEPTH"];
    process.env["OUTBOUND_WEBHOOK_MAX_QUEUE_DEPTH"] = "75000";

    const config = resolveOutboundWebhookBackpressureConfigFromEnv();
    expect(config.maxQueueDepth).toBe(75_000);

    if (previous === undefined) {
      delete process.env["OUTBOUND_WEBHOOK_MAX_QUEUE_DEPTH"];
    } else {
      process.env["OUTBOUND_WEBHOOK_MAX_QUEUE_DEPTH"] = previous;
    }
  });
});
