/**
 * V6.5 Phase 4: AT Firehose Event Encoder
 *
 * Encodes firehose events into wire payloads for the subscribeRepos WebSocket
 * stream.
 *
 * Encoding requirement (ATProto spec):
 *   The subscribeRepos stream uses CBOR-encoded messages, NOT JSON.
 *   Each message is two concatenated CBOR values: header followed by body.
 *   The header is a CBOR map containing:
 *     - header is a CBOR map with an "op" field (1 = message, -1 = error)
 *       and a "t" field containing the event type string.
 *     - body is a second CBOR map with the event fields.
 *
 * Ref: https://atproto.com/specs/event-stream
 */

import * as dagCbor from '@ipld/dag-cbor';
import { CID } from 'multiformats/cid';

export interface FirehoseEventBase {
  seq: number;
  time: string;
}

export interface CommitFirehoseEvent extends FirehoseEventBase {
  $type: '#commit';
  repo: string;
  rev: string;
  since: string | null;
  commit: string;
  tooBig: false;
  blocks: Uint8Array;
  ops: Array<{
    action: 'create' | 'update' | 'delete';
    path: string;
    cid: string | null;
    prev?: string;
  }>;
  blobs: [];
  prevData?: string | null;
}

export interface IdentityFirehoseEvent extends FirehoseEventBase {
  $type: '#identity';
  did: string;
  handle?: string;
}

export interface AccountFirehoseEvent extends FirehoseEventBase {
  $type: '#account';
  did: string;
  active: boolean;
  status?: 'takendown' | 'suspended' | 'deleted' | 'deactivated';
}

export interface AtFirehoseEventEncoder {
  encodeCommit(evt: CommitFirehoseEvent): Uint8Array;
  encodeIdentity(evt: IdentityFirehoseEvent): Uint8Array;
  encodeAccount(evt: AccountFirehoseEvent): Uint8Array;
}

export class DefaultAtFirehoseEventEncoder implements AtFirehoseEventEncoder {
  encodeCommit(evt: CommitFirehoseEvent): Uint8Array {
    // BUGFIX: `commit` and each op's `cid` are spec'd as CID-links
    // (https://atproto.com/specs/event-stream#framing), which DAG-CBOR
    // encodes as CBOR tag 42 (0x2a) around the CID bytes — not as plain
    // text strings. This file previously encoded them as plain strings
    // via raw `cborg`, which official relays reject while trying to parse
    // ops[i].cid ("expected cbor type 'tag' in input"). Converting to real
    // CID objects and encoding the body with @ipld/dag-cbor (which already
    // handles CID objects correctly, see ProductionAtCommitVerifier.ts for
    // the same pattern on the read/verify side) fixes this.
    const body: Record<string, unknown> = {
      ...evt,
      commit: evt.commit ? CID.parse(evt.commit) : evt.commit,
      // prevData is also a CID-link (the previous commit's MST root) — same
      // tag-42 requirement as commit/ops[].cid above. `since` stays a plain
      // string; it's a TID revision, not a CID.
      prevData: evt.prevData ? CID.parse(evt.prevData) : null,
      ops: evt.ops.map(op => ({
        ...op,
        cid: op.cid ? CID.parse(op.cid) : null
      }))
    };
    delete (body as { $type?: string }).$type;
    return this._encode('#commit', body);
  }

  encodeIdentity(evt: IdentityFirehoseEvent): Uint8Array {
    return this._encode('#identity', evt as unknown as Record<string, unknown>);
  }

  encodeAccount(evt: AccountFirehoseEvent): Uint8Array {
    return this._encode('#account', evt as unknown as Record<string, unknown>);
  }

  /**
   * Encodes the event into the ATProto wire format: two concatenated
   * DAG-CBOR objects (header map, then body map). Uses @ipld/dag-cbor
   * (not raw cborg) so any CID objects in the payload (see encodeCommit)
   * are correctly encoded as tag-42 CID-links.
   */
  private _encode<T extends { $type?: string } & object>(type: string, body: T): Uint8Array {
    const header = { op: 1, t: type };
    const { $type: _ignoredType, ...payload } = body;
    return concatBytes(dagCbor.encode(header), dagCbor.encode(payload));
  }
}

function concatBytes(left: Uint8Array, right: Uint8Array): Uint8Array {
  const combined = new Uint8Array(left.length + right.length);
  combined.set(left, 0);
  combined.set(right, left.length);
  return combined;
}
