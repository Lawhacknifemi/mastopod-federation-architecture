/**
 * V6.5 Phase 4: com.atproto.sync.getRepo
 *
 * Returns the full CAR export of a hosted AT repository.
 *
 * Security:
 *   - DID is validated before any storage access.
 *   - Only active repos are served; deactivated/taken-down repos return 400.
 *   - Local repos explicitly reject the "since" parameter until incremental
 *     CAR export is implemented.
 *   - External repos proxy the request upstream and preserve an allowlisted
 *     set of response headers.
 *
 * Ref: https://atproto.com/lexicon/com-atproto-sync#comatprotosyncgetrepo
 */

import { AtCarExporter } from '../../repo/AtCarExporter.js';
import { HandleResolutionReader, isValidDid } from '../../identity/HandleResolutionReader.js';
import { XrpcErrors } from '../middleware/XrpcErrorMapper.js';
import type { IdentityBindingRepository } from '../../../core-domain/identity/IdentityBindingRepository.js';
import type { ExternalReadGateway } from '../../external/ExternalReadGateway.js';
import { isExternalAtprotoBinding } from '../../external/ExternalAccountMode.js';

export interface SyncGetRepoRouteDeps {
  carExporter: AtCarExporter;
  handleResolutionReader: HandleResolutionReader;
  identityRepo?: IdentityBindingRepository;
  externalReadGateway?: ExternalReadGateway;
}

export interface SyncGetRepoResult {
  headers: Record<string, string>;
  body: Uint8Array;
}

export class SyncGetRepoRoute {
  constructor(private readonly deps: SyncGetRepoRouteDeps) {}

  async handle(did: string | undefined, since?: string): Promise<SyncGetRepoResult> {
    // 1. Validate input.
    if (!did || !did.trim()) {
      throw XrpcErrors.invalidRequest('did parameter is required');
    }
    const trimmedDid = did.trim();
    if (!isValidDid(trimmedDid)) {
      throw XrpcErrors.invalidDid(trimmedDid);
    }

    // 2. Resolve the DID.
    const resolved = await this.deps.handleResolutionReader.resolveRepoInput(trimmedDid);
    if (!resolved) {
      throw XrpcErrors.repoNotFound(trimmedDid);
    }

    const binding = this.deps.identityRepo
      ? await this.deps.identityRepo.getByAtprotoDid(resolved.did)
      : null;

    if (isExternalAtprotoBinding(binding)) {
      if (!binding?.atprotoPdsEndpoint || !this.deps.externalReadGateway) {
        throw XrpcErrors.repoNotFound(resolved.did);
      }

      const external = await this.deps.externalReadGateway.getRepo(
        binding.atprotoPdsEndpoint,
        resolved.did,
        since?.trim() ? since : undefined
      );

      return {
        headers: externalCarHeaders(external.headers),
        body: external.body,
      };
    }

    // 4. "since" is not supported for local repos in this version.
    if (since !== undefined && since !== '') {
      throw XrpcErrors.invalidRequest(
        'Partial CAR export via "since" is not supported in this version. ' +
        'Fetch the full repo and use getLatestCommit to detect staleness.'
      );
    }

    // 5. Export CAR. carExporter itself is the real existence check now
    // (it queries ActivityPods for the repo's actual latest commit) — the
    // separate repoRegistry pre-check this used to do gated on the same
    // stale Redis store getRecord/listRecords/describeRepo did, and would
    // reject every repo created via the native write path before the
    // export was even attempted.
    let carBytes: Uint8Array;
    try {
      carBytes = await this.deps.carExporter.exportRepo(resolved.did);
    } catch {
      throw XrpcErrors.repoNotFound(resolved.did);
    }

    return {
      headers: { 'Content-Type': 'application/vnd.ipld.car' },
      body: carBytes
    };
  }
}

function externalCarHeaders(headers: Headers): Record<string, string> {
  const responseHeaders: Record<string, string> = {
    'Content-Type': headers.get('content-type') || 'application/vnd.ipld.car',
  };

  const contentLength = headers.get('content-length');
  if (contentLength && /^\d+$/.test(contentLength)) {
    responseHeaders['Content-Length'] = contentLength;
  }

  const repoRev = headers.get('atproto-repo-rev');
  if (repoRev) {
    responseHeaders['Atproto-Repo-Rev'] = repoRev;
  }

  const etag = headers.get('etag');
  if (etag) {
    responseHeaders['ETag'] = etag;
  }

  const cacheControl = headers.get('cache-control');
  if (cacheControl) {
    responseHeaders['Cache-Control'] = cacheControl;
  }

  return responseHeaders;
}
