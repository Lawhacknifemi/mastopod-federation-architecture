import type { ActivityPodsEmojiDefinition } from "../lexicon/ActivityPodsEmojiLexicon.js";

export type AtRepoCollection =
  | 'app.bsky.actor.profile'
  | 'app.bsky.feed.post'
  | 'site.standard.document'
  | 'app.bsky.graph.follow'
  | 'app.bsky.feed.like'
  | 'app.bsky.feed.repost'
  | 'org.activitypods.emojiReaction';

export interface AtRepoBridgeMetadata {
  canonicalIntentId: string;
  sourceProtocol: 'activitypub' | 'atproto';
  provenance: {
    originProtocol: 'activitypub' | 'atproto';
    originEventId: string;
    originAccountId?: string | null;
    mirroredFromCanonicalIntentId?: string | null;
    projectionMode: 'native' | 'mirrored';
  };
}

export interface AtRecordLocator {
  collection: AtRepoCollection;
  rkey: string;
}

export interface AtRepoOpV1 {
  did: string;
  canonicalAccountId: string;
  opId: string;
  opType: 'create' | 'update' | 'delete';
  collection: AtRepoCollection;
  rkey: string;
  canonicalRefId: string;
  record: unknown | null;
  bridge?: AtRepoBridgeMetadata;
  emittedAt: string;
}

export interface AtCommitV1 {
  did: string;
  canonicalAccountId: string;
  rev: string;
  commitCid: string;
  prevCommitCid: string | null;
  /**
   * The previous commit's MST root ("data" field) — distinct from
   * prevCommitCid (the previous commit BLOCK's own CID). Required for a
   * relay to perform MST inversion verification on non-genesis commits;
   * see AtRepoDiffBuilder.ts's prevData mapping.
   */
  prevDataCid?: string | null;
  repoVersion: 3;
  ops: Array<{
    action: 'create' | 'update' | 'delete';
    collection: string;
    rkey: string;
    canonicalRefId?: string;
    uri?: string;
    cid?: string;
    subjectDid?: string | null;
    subjectUri?: string | null;
    subjectCid?: string | null;
    reactionContent?: string | null;
    reactionEmoji?: ActivityPodsEmojiDefinition | null;
    record?: Record<string, unknown> | null;
    bridge?: AtRepoBridgeMetadata;
  }>;
  emittedAt: string;
  /**
   * Optional base64-encoded CAR v1 slice for this commit, containing the
   * commit block plus changed MST/record blocks, rooted at `commitCid`.
   * When present, AtRepoDiffBuilder uses these bytes directly instead of
   * emitting an empty CAR — populated today by the native ActivityPods
   * AT bridge (helpers/atproto/atproto.service.js), which builds real CAR
   * output via helpers/atproto/carfile.js. Absent for events from paths
   * that don't yet produce real block data (see AtRepoDiffBuilder).
   */
  carSliceBase64?: string;
}

export interface AtEgressV1 {
  did: string;
  canonicalAccountId: string;
  kind: 'profile' | 'post' | 'article' | 'follow' | 'like' | 'repost' | 'emojiReaction';
  canonicalRefId: string;
  atUri?: string;
  cid?: string;
  emittedAt: string;
}
