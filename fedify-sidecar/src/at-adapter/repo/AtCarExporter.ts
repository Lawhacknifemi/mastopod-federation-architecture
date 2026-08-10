/**
 * V6.5 Phase 4: AT CAR Exporter
 *
 * Implements com.atproto.sync.getRepo: exports the full current repo state
 * as a CAR (Content Addressable aRchive) file.
 *
 * Phase 4 scope:
 *   - Full export only; partial "since" export is deferred to Phase 5.
 *   - The commit object is the first root in the CAR header.
 *   - MST nodes and record blocks are included for the current snapshot.
 *
 * In production, this module MUST use a real CAR/IPLD library (e.g. @ipld/car,
 * @atproto/repo).  The current implementation returns a well-formed mock
 * Uint8Array that exercises the interface contract while the MST library
 * integration is completed.
 *
 * Ref: https://atproto.com/specs/repository#car-file-serialization
 */

import { AtprotoRepoRegistry } from '../../atproto/repo/AtprotoRepoRegistry.js';
import { isValidDid } from '../identity/HandleResolutionReader.js';

export interface AtCarExporter {
  exportRepo(did: string, since?: string): Promise<Uint8Array>;
}

/**
 * Real CAR export via ActivityPods' atproto.getCheckout action — the same
 * Repo.getCheckout() method that already builds real CAR bytes for every
 * commit published to the firehose (see AtRepoDiffBuilder.ts's use of
 * carSliceBase64), just exposed for a full-repo request instead of a
 * single-commit delta. Replaces DefaultAtCarExporter below, which always
 * returned an empty CAR regardless of what the repo actually contained.
 */
export class ActivityPodsAtCarExporter implements AtCarExporter {
  constructor(private readonly activityPodsUrl: string) {}

  async exportRepo(did: string, since?: string): Promise<Uint8Array> {
    if (!did || !isValidDid(did)) {
      throw new Error(`Invalid DID: ${did}`);
    }

    // Same Phase 4 scope note as before: incremental "since" export isn't
    // implemented (Repo.getCheckout() always builds from every block
    // currently stored, not a diff against a prior rev).
    if (since !== undefined && since !== '') {
      throw new Error(
        'Partial CAR export via "since" is not supported in this version. ' +
        'Fetch the full repo and use getLatestCommit to detect staleness.'
      );
    }

    const res = await fetch(`${this.activityPodsUrl}/atproto/checkout/${encodeURIComponent(did)}`);
    if (!res.ok) {
      throw new Error(`Repo not found: ${did}`);
    }
    const body = (await res.json()) as any;
    if (!body?.carBase64) {
      throw new Error(`Repo not found: ${did}`);
    }
    return new Uint8Array(Buffer.from(body.carBase64, 'base64'));
  }
}

export class DefaultAtCarExporter implements AtCarExporter {
  constructor(private readonly repoRegistry: AtprotoRepoRegistry) {}

  async exportRepo(did: string, since?: string): Promise<Uint8Array> {
    // Validate DID before any storage access.
    if (!did || !isValidDid(did)) {
      throw new Error(`Invalid DID: ${did}`);
    }

    // Phase 4: partial export is not supported.
    if (since !== undefined && since !== '') {
      throw new Error(
        'Partial CAR export via "since" is not supported in Phase 4. ' +
        'Fetch the full repo and use getLatestCommit to detect staleness.'
      );
    }

    const repoState = await this.repoRegistry.getByDid(did);
    if (!repoState) {
      throw new Error(`Repo not found: ${did}`);
    }

    // In a real implementation, this would use @atproto/repo to traverse the MST
    // and build a complete CAR file. For Phase 5, we provide a valid empty CAR
    // using @ipld/car to satisfy the format requirement without needing the full
    // MST implementation.
    const { CarWriter } = require('@ipld/car');
    const { CID } = require('multiformats/cid');
    
    // Parse the root CID or use a dummy one if not available
    let rootCid;
    try {
      rootCid = CID.parse(repoState.rootCid || 'bafyreidfzuf5ruicbupabj4fp3cbge3000000000000000000000000000');
    } catch (e) {
      rootCid = CID.parse('bafyreidfzuf5ruicbupabj4fp3cbge3000000000000000000000000000');
    }

    const { writer, out } = CarWriter.create([rootCid]);
    
    // Collect the CAR bytes
    const chunks: Uint8Array[] = [];
    const collectPromise = (async () => {
      for await (const chunk of out) {
        chunks.push(chunk);
      }
    })();

    // In a full implementation, we would write blocks here:
    // await writer.put({ cid: rootCid, bytes: commitBytes });
    // ... write MST nodes and records ...
    
    await writer.close();
    await collectPromise;

    // Concatenate chunks
    const totalLength = chunks.reduce((acc, chunk) => acc + chunk.length, 0);
    const result = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }

    return result;
  }
}
