vi.mock("../../utils/logger.js", () => {
  const noop = () => {};
  const logger = { info: noop, warn: noop, error: noop, debug: noop };
  return { logger, default: logger };
});

import { describe, expect, it, vi } from "vitest";
import {
  OutboxIntentWorker,
  type OutboxIntentWorkerConfig,
} from "../outbox-intent-worker.js";
import type { OutboxIntent } from "../../queue/sidecar-redis-queue.js";
import { DEFAULT_ACTIVITYPUB_OUTBOUND_DELIVERY_POLICY } from "../../protocol-bridge/projectors/activitypub/ActivityPubDeliveryPolicy.js";

class TestOutboxIntentWorker extends OutboxIntentWorker {
  async runIntent(messageId: string, intent: OutboxIntent): Promise<void> {
    return this.processIntent(messageId, intent);
  }
}

function makeIntent(overrides: Partial<OutboxIntent> = {}): OutboxIntent {
  return {
    intentId: "intent-001",
    activityId: "https://example.com/activities/1",
    actorUri: "https://example.com/users/alice",
    activity: JSON.stringify({
      id: "https://example.com/activities/1",
      type: "Create",
      actor: "https://example.com/users/alice",
      object: {
        id: "https://example.com/objects/1",
        type: "Note",
        content: "Hello",
      },
    }),
    targets: [
      {
        inboxUrl: "https://remote.example/users/bob/inbox",
        sharedInboxUrl: "https://remote.example/inbox",
        deliveryUrl: "https://remote.example/inbox",
        targetDomain: "remote.example",
      },
    ],
    createdAt: Date.now() - 100,
    attempt: 0,
    maxAttempts: 8,
    notBeforeMs: 0,
    meta: {
      isPublicActivity: true,
      isPublicIndexable: true,
      visibility: "public",
    },
    ...overrides,
  };
}

function makeQueue(overrides: Record<string, unknown> = {}) {
  return {
    consumeOutboxIntents: async function* () {},
    getOutboxIntentState: vi.fn().mockResolvedValue({}),
    markOutboxIntentEventLogPublished: vi.fn().mockResolvedValue(undefined),
    enqueueOutboundBatchForIntent: vi.fn().mockResolvedValue({ enqueued: true, jobCount: 1 }),
    markOutboxIntentCompleted: vi.fn().mockResolvedValue(undefined),
    enqueueOutboxIntent: vi.fn().mockResolvedValue("retry-msg-1"),
    ack: vi.fn().mockResolvedValue(undefined),
    moveToDlq: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as any;
}

function makeRedpanda(overrides: Record<string, unknown> = {}) {
  return {
    publishToStream1: vi.fn().mockResolvedValue(undefined),
    publishTombstone: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as any;
}

function makeConfig(overrides: Partial<OutboxIntentWorkerConfig> = {}): OutboxIntentWorkerConfig {
  return {
    concurrency: 1,
    outboundJobMaxAttempts: 10,
    activityPubOutboundDeliveryPolicy: DEFAULT_ACTIVITYPUB_OUTBOUND_DELIVERY_POLICY,
    ...overrides,
  };
}

describe("OutboxIntentWorker", () => {
  it("publishes the event log and atomically fans out outbound jobs on success", async () => {
    const queue = makeQueue();
    const redpanda = makeRedpanda();
    const worker = new TestOutboxIntentWorker(queue, redpanda, makeConfig());
    const intent = makeIntent();

    await worker.runIntent("msg-001", intent);

    expect(redpanda.publishToStream1).toHaveBeenCalledTimes(1);
    expect(redpanda.publishToStream1).toHaveBeenCalledWith(
      expect.objectContaining({
        actorUri: intent.actorUri,
        outboxIntentId: intent.intentId,
      }),
    );
    expect(queue.markOutboxIntentEventLogPublished).toHaveBeenCalledWith(intent.intentId);
    expect(queue.enqueueOutboundBatchForIntent).toHaveBeenCalledTimes(1);
    expect(queue.enqueueOutboundBatchForIntent).toHaveBeenCalledWith(
      intent.intentId,
      expect.arrayContaining([
        expect.objectContaining({
          activityId: intent.activityId,
          actorUri: intent.actorUri,
          targetInbox: "https://remote.example/inbox",
          targetDomain: "remote.example",
          maxAttempts: 10,
        }),
      ]),
    );
    expect(queue.markOutboxIntentCompleted).toHaveBeenCalledWith(intent.intentId);
    expect(queue.ack).toHaveBeenCalledWith("outbox_intent", "msg-001");
    expect(queue.enqueueOutboxIntent).not.toHaveBeenCalled();
    expect(queue.moveToDlq).not.toHaveBeenCalled();
  });

  it("requeues the intent with backoff after a transient event-log failure", async () => {
    const queue = makeQueue();
    const redpanda = makeRedpanda({
      publishToStream1: vi.fn().mockRejectedValue(new Error("broker unavailable")),
    });
    const worker = new TestOutboxIntentWorker(queue, redpanda, makeConfig());
    const intent = makeIntent();

    const before = Date.now();
    await worker.runIntent("msg-002", intent);

    expect(queue.ack).toHaveBeenCalledWith("outbox_intent", "msg-002");
    expect(queue.enqueueOutboxIntent).toHaveBeenCalledTimes(1);
    const retryIntent = queue.enqueueOutboxIntent.mock.calls[0]?.[0] as OutboxIntent | undefined;
    expect(retryIntent).toBeDefined();
    if (!retryIntent) {
      throw new Error("Expected retry intent to be enqueued");
    }
    expect(retryIntent.intentId).toBe(intent.intentId);
    expect(retryIntent.attempt).toBe(1);
    expect(retryIntent.lastError).toContain("broker unavailable");
    expect(retryIntent.notBeforeMs).toBeGreaterThan(before);
    expect(queue.moveToDlq).not.toHaveBeenCalled();
    expect(queue.enqueueOutboundBatchForIntent).not.toHaveBeenCalled();
  });

  it("acks duplicate completed intents without re-publishing or re-enqueueing", async () => {
    const completedAt = Date.now() - 10;
    const queue = makeQueue({
      getOutboxIntentState: vi.fn().mockResolvedValue({ completedAt }),
    });
    const redpanda = makeRedpanda();
    const worker = new TestOutboxIntentWorker(queue, redpanda, makeConfig());
    const intent = makeIntent({ createdAt: completedAt - 10 });

    await worker.runIntent("msg-003", intent);

    expect(queue.ack).toHaveBeenCalledWith("outbox_intent", "msg-003");
    expect(redpanda.publishToStream1).not.toHaveBeenCalled();
    expect(queue.enqueueOutboundBatchForIntent).not.toHaveBeenCalled();
    expect(queue.markOutboxIntentCompleted).not.toHaveBeenCalled();
  });

  it("still publishes public activities to Stream1 when search indexing is disabled", async () => {
    const queue = makeQueue();
    const redpanda = makeRedpanda();
    const worker = new TestOutboxIntentWorker(queue, redpanda, makeConfig());
    const intent = makeIntent({
      meta: {
        isPublicActivity: true,
        isPublicIndexable: false,
        visibility: "public",
      },
    });

    await worker.runIntent("msg-004", intent);

    expect(redpanda.publishToStream1).toHaveBeenCalledTimes(1);
    expect(queue.enqueueOutboundBatchForIntent).toHaveBeenCalledTimes(1);
  });

  it("processes public activity with empty targets list, publishing event log and marking completed", async () => {
    const queue = makeQueue();
    const redpanda = makeRedpanda();
    const worker = new TestOutboxIntentWorker(queue, redpanda, makeConfig());
    const intent = makeIntent({
      targets: [],
    });

    await worker.runIntent("msg-005", intent);

    expect(redpanda.publishToStream1).toHaveBeenCalledTimes(1);
    expect(queue.markOutboxIntentEventLogPublished).toHaveBeenCalledWith(intent.intentId);
    expect(queue.enqueueOutboundBatchForIntent).toHaveBeenCalledWith(intent.intentId, []);
    expect(queue.markOutboxIntentCompleted).toHaveBeenCalledWith(intent.intentId);
    expect(queue.ack).toHaveBeenCalledWith("outbox_intent", "msg-005");
    expect(queue.enqueueOutboxIntent).not.toHaveBeenCalled();
    expect(queue.moveToDlq).not.toHaveBeenCalled();
  });
});
