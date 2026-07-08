import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { PostSyncWorker } from "./PostSyncWorker.js";
import { pool } from "../db/postgres.js";
import { logger } from "../utils/logger.js";

// Mock the postgres pool
vi.mock("../db/postgres.js", () => {
  return {
    pool: {
      query: vi.fn(),
    },
  };
});

describe("PostSyncWorker", () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal("fetch", mockFetch);
    
    // Spy on logger to print errors to stdout
    vi.spyOn(logger, "error").mockImplementation((...args: any[]) => {
      console.log("LOGGER ERROR:", ...args);
    });
    vi.spyOn(logger, "info").mockImplementation((...args: any[]) => {
      console.log("LOGGER INFO:", ...args);
    });
    vi.spyOn(logger, "debug").mockImplementation((...args: any[]) => {
      console.log("LOGGER DEBUG:", ...args);
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("polls unsynced posts and successfully syncs them to ActivityPods", async () => {
    // 1. Mock DB query return value for unsynced posts
    const mockPost = {
      id: "abc-123",
      actor_uri: "https://pod.example/alice",
      content: "Hello world!",
      post_type: "note",
      name: null,
      summary: null,
      hashtags: ["tag1"],
      attachments: [{ url: "https://example.com/img.jpg", type: "Image" }],
      sync_attempts: 0,
    };

    vi.mocked(pool.query).mockResolvedValueOnce({
      rows: [mockPost],
      command: "SELECT",
      rowCount: 1,
      oid: 0,
      fields: [],
    } as any);

    // 2. Mock fetch response for outbox post
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 201,
      json: async () => ({ activityUri: "https://pod.example/alice/data/note/abc-123" }),
    });

    const worker = new PostSyncWorker(1000); // 1s interval
    (worker as any).isRunning = true; // Set isRunning to true so tick processes items
    
    // Call the private tick method directly
    await (worker as any).tick();

    // Verify DB select query was called
    expect(pool.query).toHaveBeenCalledWith(
      expect.stringContaining("SELECT id, actor_uri, content, post_type, name, summary, hashtags, attachments, sync_attempts"),
    );

    // Verify fetch was called with the correct Create activity payload
    expect(mockFetch).toHaveBeenCalledTimes(1);
    
    const calls = mockFetch.mock.calls;
    expect(calls).toBeDefined();
    expect(calls.length).toBe(1);
    
    const firstCall = calls[0];
    expect(firstCall).toBeDefined();
    
    const fetchUrl = firstCall?.[0] as string;
    const fetchInit = firstCall?.[1] as RequestInit;
    
    expect(fetchUrl).toContain("/api/internal/social/outbox");
    expect(fetchInit?.method).toBe("POST");
    
    const body = JSON.parse(fetchInit?.body as string);
    expect(body.actorUri).toBe("https://pod.example/alice");
    expect(body.activity.type).toBe("Create");
    expect(body.activity.object.content).toBe("Hello world!");
    expect(body.activity.object.tag).toEqual([
      {
        type: "Hashtag",
        name: "#tag1",
        href: expect.stringContaining("/tags/tag1"),
      },
    ]);
    expect(body.activity.object.attachment).toEqual([
      {
        type: "Image",
        url: "https://example.com/img.jpg",
      },
    ]);

    // Verify DB update query was called to set synced_to_fuseki = TRUE
    expect(pool.query).toHaveBeenLastCalledWith(
      expect.stringContaining("synced_to_fuseki = TRUE"),
      [mockPost.id]
    );
  });

  it("increments sync_attempts and records the error message when sync fails", async () => {
    const mockPost = {
      id: "abc-123",
      actor_uri: "https://pod.example/alice",
      content: "Hello world!",
      post_type: "note",
      name: null,
      summary: null,
      hashtags: [],
      attachments: [],
      sync_attempts: 1,
    };

    vi.mocked(pool.query).mockResolvedValueOnce({
      rows: [mockPost],
      command: "SELECT",
      rowCount: 1,
      oid: 0,
      fields: [],
    } as any);

    // Mock fetch failure response
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      text: async () => "Internal Server Error",
    });

    const worker = new PostSyncWorker(1000);
    (worker as any).isRunning = true; // Set isRunning to true so tick processes items
    await (worker as any).tick();

    // Verify fetch was called
    expect(mockFetch).toHaveBeenCalledTimes(1);

    // Verify DB update query was called to set sync_attempts = sync_attempts + 1 and error_message
    expect(pool.query).toHaveBeenLastCalledWith(
      expect.stringContaining("sync_attempts = sync_attempts + 1"),
      ["HTTP 500: Internal Server Error", mockPost.id]
    );
  });
});
