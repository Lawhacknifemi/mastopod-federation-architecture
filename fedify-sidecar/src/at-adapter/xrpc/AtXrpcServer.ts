/**
 * V6.5 Phase 4: AT XRPC Server
 *
 * Owns HTTP server setup, route registration under /xrpc/*, content
 * negotiation, common security headers, and error mapping.
 *
 * Public endpoints (unauthenticated, per ATProto spec):
 *   GET  /xrpc/com.atproto.sync.getRepo
 *   GET  /xrpc/com.atproto.sync.getLatestCommit
 *   GET  /xrpc/com.atproto.sync.getBlob
 *   GET  /xrpc/com.atproto.repo.getRecord
 *   GET  /xrpc/com.atproto.repo.listRecords
 *   GET  /xrpc/com.atproto.identity.resolveHandle
 *   WS   /xrpc/com.atproto.sync.subscribeRepos
 *
 * Security hardening:
 *   - All responses include standard security headers (X-Content-Type-Options,
 *     X-Frame-Options, Referrer-Policy, Permissions-Policy).
 *   - CORS is restricted to the configured origin list; wildcard is only
 *     used for the public read endpoints as required by the ATProto spec.
 *   - Write/mutation endpoints are NOT registered here (Phase 4 scope).
 *   - Rate limiting is noted as a TODO; implement at the reverse-proxy layer
 *     (nginx/Caddy) or via a middleware library before production deployment.
 *
 * This module does not contain any repo logic.  All business logic is
 * delegated to the injected route handlers.
 *
 * Ref: https://atproto.com/specs/xrpc
 */

import { AtRecordReader } from '../repo/AtRecordReader.js';
import { AtCarExporter } from '../repo/AtCarExporter.js';
import type { AtBlobStore } from '../blob/AtBlobStore.js';
import { HandleResolutionReader } from '../identity/HandleResolutionReader.js';
import { AtFirehoseSubscriptionManager } from '../firehose/AtFirehoseSubscriptionManager.js';
import { AtprotoRepoRegistry } from '../../atproto/repo/AtprotoRepoRegistry.js';
import type { IdentityBindingRepository } from '../../core-domain/identity/IdentityBindingRepository.js';
import { SyncGetRepoRoute } from './routes/SyncGetRepoRoute.js';
import { SyncGetLatestCommitRoute } from './routes/SyncGetLatestCommitRoute.js';
import { SyncGetBlobRoute } from './routes/SyncGetBlobRoute.js';
import { RepoGetRecordRoute } from './routes/RepoGetRecordRoute.js';
import { RepoListRecordsRoute } from './routes/RepoListRecordsRoute.js';
import { IdentityResolveHandleRoute } from './routes/IdentityResolveHandleRoute.js';
import { SubscribeReposRoute } from './routes/SubscribeReposRoute.js';
import { DefaultRepoRevLookup } from './middleware/AtRepoRevHeader.js';
import { mapToXrpcError, XrpcErrors } from './middleware/XrpcErrorMapper.js';
// Phase 7: authenticated write + session routes
import {
  ServerDescribeServerRoute,
  type ServerDescribeServerConfig,
} from './routes/ServerDescribeServerRoute.js';
import { ServerCreateSessionRoute } from './routes/ServerCreateSessionRoute.js';
import { ServerRefreshSessionRoute } from './routes/ServerRefreshSessionRoute.js';
import { RepoCreateRecordRoute } from './routes/RepoCreateRecordRoute.js';
import { RepoPutRecordRoute } from './routes/RepoPutRecordRoute.js';
import { RepoDeleteRecordRoute } from './routes/RepoDeleteRecordRoute.js';
import { RepoDescribeRepoRoute } from './routes/RepoDescribeRepoRoute.js';
import type { AtSessionService, AtAccountResolver, AtPasswordVerifier } from '../auth/AtSessionTypes.js';
import type { AtWriteGateway } from '../writes/AtWriteTypes.js';
import type { AtSessionContext } from '../auth/AtSessionTypes.js';
import type { ExternalWriteGateway } from '../external/ExternalWriteGateway.js';
import type { ExternalReadGateway } from '../external/ExternalReadGateway.js';

// ---------------------------------------------------------------------------
// Dependency injection interface
// ---------------------------------------------------------------------------

export interface AtXrpcServerDeps {
  recordReader: AtRecordReader;
  carExporter: AtCarExporter;
  blobStore?: AtBlobStore;
  handleResolutionReader: HandleResolutionReader;
  firehoseSubscriptions: AtFirehoseSubscriptionManager;
  repoRegistry: AtprotoRepoRegistry;
  /** Used by describeRepo to query ActivityPods' real collections list directly. */
  activityPodsUrl: string;
  /** Optional: enables handleIsCorrect round-trip in describeRepo */
  identityRepo?: IdentityBindingRepository;
  // Phase 7: write + session deps (optional — omit to disable write endpoints)
  serverConfig?: ServerDescribeServerConfig;
  sessionService?: AtSessionService;
  accountResolver?: AtAccountResolver;
  passwordVerifier?: AtPasswordVerifier;
  writeGateway?: AtWriteGateway;
  externalWriteGateway?: ExternalWriteGateway;
  externalReadGateway?: ExternalReadGateway;
}

// ---------------------------------------------------------------------------
// Server interface
// ---------------------------------------------------------------------------

export interface AtXrpcServer {
  start(): Promise<void>;
  stop(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Security headers applied to every HTTP response
// ---------------------------------------------------------------------------

const SECURITY_HEADERS: Record<string, string> = {
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'Referrer-Policy': 'no-referrer',
  'Permissions-Policy': 'geolocation=(), microphone=(), camera=()'
};

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export class DefaultAtXrpcServer implements AtXrpcServer {
  private readonly getRepoRoute: SyncGetRepoRoute;
  private readonly getLatestCommitRoute: SyncGetLatestCommitRoute;
  private readonly getBlobRoute: SyncGetBlobRoute | null;
  private readonly getRecordRoute: RepoGetRecordRoute;
  private readonly listRecordsRoute: RepoListRecordsRoute;
  private readonly resolveHandleRoute: IdentityResolveHandleRoute;
  private readonly subscribeReposRoute: SubscribeReposRoute;
  // Phase 7 routes (null when Phase 7 deps not provided)
  private readonly describeServerRoute: ServerDescribeServerRoute | null;
  private readonly createSessionRoute: ServerCreateSessionRoute | null;
  private readonly refreshSessionRoute: ServerRefreshSessionRoute | null;
  private readonly createRecordRoute: RepoCreateRecordRoute | null;
  private readonly putRecordRoute: RepoPutRecordRoute | null;
  private readonly deleteRecordRoute: RepoDeleteRecordRoute | null;
  private readonly describeRepoRoute: RepoDescribeRepoRoute | null;

  constructor(private readonly deps: AtXrpcServerDeps) {
    const revLookup = new DefaultRepoRevLookup(deps.repoRegistry);

    this.getRepoRoute = new SyncGetRepoRoute({
      carExporter: deps.carExporter,
      handleResolutionReader: deps.handleResolutionReader,
      repoRegistry: deps.repoRegistry,
      identityRepo: deps.identityRepo,
      externalReadGateway: deps.externalReadGateway,
    });

    this.getLatestCommitRoute = new SyncGetLatestCommitRoute(
      deps.repoRegistry,
      deps.handleResolutionReader,
      revLookup,
      deps.identityRepo,
      deps.externalReadGateway
    );

    this.getBlobRoute = deps.blobStore
      ? new SyncGetBlobRoute({
          blobStore: deps.blobStore,
          handleResolutionReader: deps.handleResolutionReader,
        })
      : null;

    this.getRecordRoute = new RepoGetRecordRoute(
      deps.recordReader,
      deps.handleResolutionReader,
      revLookup,
      deps.identityRepo,
      deps.externalReadGateway
    );

    this.listRecordsRoute = new RepoListRecordsRoute(
      deps.recordReader,
      deps.handleResolutionReader,
      revLookup,
      deps.identityRepo,
      deps.externalReadGateway
    );

    this.resolveHandleRoute = new IdentityResolveHandleRoute(
      deps.handleResolutionReader
    );

    this.subscribeReposRoute = new SubscribeReposRoute(
      deps.firehoseSubscriptions
    );

    // Phase 7: wire write/session routes when all deps are present
    this.describeServerRoute = deps.serverConfig
      ? new ServerDescribeServerRoute(deps.serverConfig)
      : null;

    this.createSessionRoute =
      deps.sessionService
        ? new ServerCreateSessionRoute(deps.sessionService)
        : null;

    this.refreshSessionRoute =
      deps.sessionService
        ? new ServerRefreshSessionRoute(deps.sessionService)
        : null;

    this.createRecordRoute = deps.writeGateway
      ? new RepoCreateRecordRoute(
          deps.writeGateway,
          deps.identityRepo,
          deps.externalWriteGateway
        )
      : null;

    this.putRecordRoute = deps.writeGateway
      ? new RepoPutRecordRoute(
          deps.writeGateway,
          deps.identityRepo,
          deps.externalWriteGateway
        )
      : null;

    this.deleteRecordRoute = deps.writeGateway
      ? new RepoDeleteRecordRoute(
          deps.writeGateway,
          deps.identityRepo,
          deps.externalWriteGateway
        )
      : null;

    this.describeRepoRoute = new RepoDescribeRepoRoute(
      deps.activityPodsUrl,
      deps.handleResolutionReader,
      deps.identityRepo,
      deps.externalReadGateway,
    );
  }

  async start(): Promise<void> {
    // Transport is owned by AtXrpcFastifyBridge, which mounts routes directly
    // onto the Fastify instance.  This lifecycle method intentionally does nothing.
  }

  async stop(): Promise<void> {
    // No transport-level teardown needed; Fastify handles graceful shutdown.
  }

  // ---------------------------------------------------------------------------
  // Request dispatch (transport-agnostic, usable in tests)
  // ---------------------------------------------------------------------------

  async handleRequest(
    method: string,
    path: string,
    query: Record<string, string | undefined>,
    clientIp?: string
  ): Promise<{ status: number; headers: Record<string, string>; body: any }> {
    try {
      // TODO (Phase 5): Implement rate limiting middleware here.
      // E.g., check rateLimitStore.increment(clientIp, path)
      // If exceeded, throw XrpcErrors.rateLimitExceeded()
      
      let result: { headers: Record<string, string>; body: any };
      const did = query["did"] as string | undefined;
      const since = query["since"] as string | undefined;
      const cid = query["cid"] as string | undefined;
      const repo = query["repo"] as string | undefined;
      const collection = query["collection"] as string | undefined;
      const rkey = query["rkey"] as string | undefined;
      const cursor = query["cursor"] as string | undefined;
      const reverse = query["reverse"] as string | undefined;
      const handle = query["handle"] as string | undefined;
      const limitValue = query["limit"] as string | undefined;

      if (method === 'GET' && path === '/xrpc/com.atproto.sync.getRepo') {
        result = await this.getRepoRoute.handle(did as string, since);

      } else if (method === 'GET' && path === '/xrpc/com.atproto.sync.getLatestCommit') {
        result = await this.getLatestCommitRoute.handle(did as string);

      } else if (method === 'GET' && path === '/xrpc/com.atproto.sync.getBlob') {
        if (!this.getBlobRoute) {
          return {
            status: 501,
            headers: { ...SECURITY_HEADERS },
            body: { error: 'MethodNotImplemented', message: 'Blob reads are not configured' }
          };
        }
        result = await this.getBlobRoute.handle(did as string, cid as string);

      } else if (method === 'GET' && path === '/xrpc/com.atproto.repo.getRecord') {
        result = await this.getRecordRoute.handle(
          repo as string,
          collection as string,
          rkey as string,
          cid
        );

      } else if (method === 'GET' && path === '/xrpc/com.atproto.repo.listRecords') {
        const limit = limitValue ? parseInt(limitValue, 10) : undefined;
        result = await this.listRecordsRoute.handle(
          repo as string,
          collection as string,
          limit,
          cursor,
          reverse === 'true'
        );

      } else if (method === 'GET' && path === '/xrpc/com.atproto.identity.resolveHandle') {
        result = await this.resolveHandleRoute.handle(handle as string);

      } else if (method === 'GET' && path === '/xrpc/com.atproto.server.describeServer') {
        if (!this.describeServerRoute) {
          return { status: 501, headers: { ...SECURITY_HEADERS }, body: { error: 'MethodNotImplemented', message: 'describeServer not configured' } };
        }
        result = await this.describeServerRoute.handle();

      } else if (method === 'GET' && path === '/xrpc/com.atproto.repo.describeRepo') {
        result = await this.describeRepoRoute!.handle(repo as string);

      } else {
        return {
          status: 501,
          headers: { ...SECURITY_HEADERS },
          body: { error: 'MethodNotImplemented', message: 'Route not found' }
        };
      }

      return {
        status: 200,
        headers: { ...SECURITY_HEADERS, ...result.headers },
        body: result.body
      };

    } catch (err) {
      const mapped = mapToXrpcError(err);
      return {
        status: mapped.status,
        headers: { ...SECURITY_HEADERS, 'Content-Type': 'application/json' },
        body: mapped.body
      };
    }
  }

  // ---------------------------------------------------------------------------
  // Phase 7: Authenticated request dispatch
  // Caller is responsible for extracting the bearer token and resolving the
  // AtSessionContext before calling this method.
  // ---------------------------------------------------------------------------

  async handleAuthenticatedRequest(
    method: string,
    path: string,
    body: Record<string, unknown> | undefined,
    auth: AtSessionContext
  ): Promise<{ status: number; headers: Record<string, string>; body: any }> {
    try {
      let result: { headers: Record<string, string>; body: any };

      if (method === 'POST' && path === '/xrpc/com.atproto.repo.createRecord') {
        if (!this.createRecordRoute) {
          return { status: 501, headers: { ...SECURITY_HEADERS }, body: { error: 'MethodNotImplemented', message: 'Write endpoints not configured' } };
        }
        result = await this.createRecordRoute.handle(body, auth);

      } else if (method === 'POST' && path === '/xrpc/com.atproto.repo.putRecord') {
        if (!this.putRecordRoute) {
          return { status: 501, headers: { ...SECURITY_HEADERS }, body: { error: 'MethodNotImplemented', message: 'Write endpoints not configured' } };
        }
        result = await this.putRecordRoute.handle(body, auth);

      } else if (method === 'POST' && path === '/xrpc/com.atproto.repo.deleteRecord') {
        if (!this.deleteRecordRoute) {
          return { status: 501, headers: { ...SECURITY_HEADERS }, body: { error: 'MethodNotImplemented', message: 'Write endpoints not configured' } };
        }
        result = await this.deleteRecordRoute.handle(body, auth);

      } else {
        return {
          status: 501,
          headers: { ...SECURITY_HEADERS },
          body: { error: 'MethodNotImplemented', message: 'Route not found' }
        };
      }

      return {
        status: 200,
        headers: { ...SECURITY_HEADERS, ...result.headers },
        body: result.body
      };

    } catch (err) {
      const mapped = mapToXrpcError(err);
      return {
        status: mapped.status,
        headers: { ...SECURITY_HEADERS, 'Content-Type': 'application/json' },
        body: mapped.body
      };
    }
  }

  // ---------------------------------------------------------------------------
  // Phase 7: Unauthenticated session creation (special case: no auth token yet)
  // ---------------------------------------------------------------------------

  async handleCreateSession(
    body: Record<string, unknown> | undefined
  ): Promise<{ status: number; headers: Record<string, string>; body: any }> {
    try {
      if (!this.createSessionRoute) {
        return {
          status: 501,
          headers: { ...SECURITY_HEADERS },
          body: { error: 'MethodNotImplemented', message: 'Session endpoints not configured' }
        };
      }
      const result = await this.createSessionRoute.handle(body);
      return {
        status: 200,
        headers: { ...SECURITY_HEADERS, ...result.headers },
        body: result.body
      };
    } catch (err) {
      const mapped = mapToXrpcError(err);
      return {
        status: mapped.status,
        headers: { ...SECURITY_HEADERS, 'Content-Type': 'application/json' },
        body: mapped.body
      };
    }
  }

  async handleRefreshSession(
    authorizationHeader: string | undefined
  ): Promise<{ status: number; headers: Record<string, string>; body: any }> {
    try {
      if (!this.refreshSessionRoute) {
        return {
          status: 501,
          headers: { ...SECURITY_HEADERS },
          body: { error: 'MethodNotImplemented', message: 'Session endpoints not configured' }
        };
      }
      const result = await this.refreshSessionRoute.handle(authorizationHeader);
      return {
        status: 200,
        headers: { ...SECURITY_HEADERS, ...result.headers },
        body: result.body
      };
    } catch (err) {
      const mapped = mapToXrpcError(err);
      return {
        status: mapped.status,
        headers: { ...SECURITY_HEADERS, 'Content-Type': 'application/json' },
        body: mapped.body
      };
    }
  }

  /**
   * Handle an incoming WebSocket connection for subscribeRepos.
   * The caller is responsible for providing the send/close callbacks that
   * wrap the actual WebSocket object.
   */
  async handleWebSocketConnection(
    connectionId: string,
    rawCursor: string | undefined,
    sendFn: (data: Uint8Array) => Promise<void>,
    closeFn: (code?: number, reason?: string) => Promise<void>
  ): Promise<void> {
    await this.subscribeReposRoute.handleConnection(
      connectionId,
      rawCursor,
      sendFn,
      closeFn
    );
  }

  async handleWebSocketDisconnection(connectionId: string): Promise<void> {
    await this.subscribeReposRoute.handleDisconnection(connectionId);
  }
}
