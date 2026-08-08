/**
 * V6.5 Phase 4: AT Repo Diff Builder
 *
 * Builds a CAR slice from a locally generated AtCommitV1 event for use in
 * the subscribeRepos firehose #commit message.
 *
 * The ATProto sync spec requires that #commit messages include a "blocks"
 * field containing a CAR slice with the commit block and all changed data
 * blocks, with the commit CID as the first root.
 *
 * Phase 4 scope:
 *   - Supports only diffs generated from locally created commits.
 *   - Does not reconstruct arbitrary historical diffs.
 *   - CAR slice is a mock Uint8Array pending real IPLD/CAR library integration.
 *
 * Ref: https://atproto.com/specs/event-stream#commit
 */

import { AtCommitV1 } from '../events/AtRepoEvents.js';

export interface RepoDiffOp {
  action: 'create' | 'update' | 'delete';
  path: string; // collection/rkey
  cid: string | null;
  prev?: string;
}

export interface RepoDiffBuildResult {
  commitCid: string;
  prevData?: string | null;
  ops: RepoDiffOp[];
  carSlice: Uint8Array;
}

export interface AtRepoDiffBuilder {
  buildFromCommit(commit: AtCommitV1): Promise<RepoDiffBuildResult>;
}

export class DefaultAtRepoDiffBuilder implements AtRepoDiffBuilder {
  async buildFromCommit(commit: AtCommitV1): Promise<RepoDiffBuildResult> {
    const ops: RepoDiffOp[] = commit.ops.map(op => ({
      action: op.action,
      path: `${op.collection}/${op.rkey}`,
      cid: op.cid ?? null
    }));

    // If the event already carries real CAR bytes (e.g. from the native
    // ActivityPods AT bridge, which builds a real CAR via
    // helpers/atproto/carfile.js), use them directly rather than emitting
    // an empty CAR slice — a spec-following relay needs real block bytes
    // to verify the commit, not just a frame on the wire.
    if (commit.carSliceBase64) {
      return {
        commitCid: commit.commitCid,
        // BUGFIX: prevData is the previous commit's MST ROOT ("data" field,
      // per https://atproto.com/specs/sync), not the previous COMMIT
      // block's own CID (prevCommitCid/the commit's `prev` chain link) —
      // these are two different CIDs. A relay uses prevData for MST
      // inversion verification of non-genesis commits and rejects the
      // event outright ("missing prevData field") if it's wrong/absent.
      // This conflation predates this session's changes and was never
      // exercised until a real relay verified a real non-genesis commit.
      prevData: commit.prevDataCid ?? null,
        ops,
        carSlice: new Uint8Array(Buffer.from(commit.carSliceBase64, 'base64'))
      };
    }

    // In a real implementation, this would use @atproto/repo to build a CAR slice
    // containing the commit block, changed MST nodes, and new/updated record blocks.
    // For Phase 5, we provide a valid empty CAR slice using @ipld/car.
    const { CarWriter } = require('@ipld/car');
    const { CID } = require('multiformats/cid');
    
    let rootCid;
    try {
      rootCid = CID.parse(commit.commitCid);
    } catch (e) {
      rootCid = CID.parse('bafyreidfzuf5ruicbupabj4fp3cbge3000000000000000000000000000');
    }

    const { writer, out } = CarWriter.create([rootCid]);
    
    const chunks: Uint8Array[] = [];
    const collectPromise = (async () => {
      for await (const chunk of out) {
        chunks.push(chunk);
      }
    })();

    // In a full implementation:
    // await writer.put({ cid: rootCid, bytes: commitBytes });
    // ... write changed blocks ...
    
    await writer.close();
    await collectPromise;

    const totalLength = chunks.reduce((acc, chunk) => acc + chunk.length, 0);
    const carSlice = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      carSlice.set(chunk, offset);
      offset += chunk.length;
    }

    return {
      commitCid: commit.commitCid,
      // BUGFIX: prevData is the previous commit's MST ROOT ("data" field,
      // per https://atproto.com/specs/sync), not the previous COMMIT
      // block's own CID (prevCommitCid/the commit's `prev` chain link) —
      // these are two different CIDs. A relay uses prevData for MST
      // inversion verification of non-genesis commits and rejects the
      // event outright ("missing prevData field") if it's wrong/absent.
      // This conflation predates this session's changes and was never
      // exercised until a real relay verified a real non-genesis commit.
      prevData: commit.prevDataCid ?? null,
      ops,
      carSlice
    };
  }
}
