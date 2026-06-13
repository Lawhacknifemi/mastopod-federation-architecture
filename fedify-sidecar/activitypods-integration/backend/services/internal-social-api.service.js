'use strict';

/**
 * Internal Social API — ActivityPods Backend
 *
 * Provides internal HTTP endpoints that your app backend calls to perform
 * social actions on behalf of users, without requiring OIDC from the user.
 *
 * All routes are protected by ACTIVITYPODS_TOKEN (Bearer token).
 * Internally, actions are executed with ctx.meta.webId = actorUri so that
 * ActivityPods enforces the correct per-actor permissions without OIDC.
 *
 * Routes registered under /api/internal/social:
 *
 *   POST /outbox
 *        Body: { actorUri: string; activity: object }
 *        → 201 { activityUri: string; activity: object }
 *        Post any ActivityPub activity to an actor's outbox.
 *        Handles Create(Note), Follow, Like, Announce, Undo, etc.
 *
 *   GET  /actor?actorUri={uri}
 *        → AP actor document (profile)
 *
 *   GET  /inbox?actorUri={uri}&page={n}&pageSize={n}
 *        → { items: object[], totalItems: number, nextPage: number|null }
 *
 *   GET  /outbox?actorUri={uri}&page={n}&pageSize={n}
 *        → { items: object[], totalItems: number, nextPage: number|null }
 *
 *   PUT  /profile
 *        Body: { actorUri: string; displayName?: string; summary?: string }
 *        → 200 { ok: true }
 *        Update an actor's profile fields.
 *
 *   POST /auth/verify
 *        Body: { canonicalAccountId: string; password: string }
 *        → 200 { ok: true; scope: "full" | "app_password_restricted" }
 *        → 401 { ok: false; reason: "invalid_password" }
 *        → 404 { ok: false; reason: "account_not_found" }
 *        Verify a user's password. Used by your app for login.
 */

const crypto = require('crypto');
const { Errors: WebErrors } = require('moleculer-web');
const { MoleculerError } = require('moleculer').Errors;

const AS_CONTEXT = 'https://www.w3.org/ns/activitystreams';
const DEFAULT_PAGE_SIZE = 20;
const MAX_PAGE_SIZE = 100;

module.exports = {
  name: 'internal-social-api',

  dependencies: [
    'api',
    'activitypub.outbox',
    'activitypub.actor',
    'activitypub.collection',
    'auth.account',
  ],

  settings: {
    auth: {
      bearerToken:
        process.env.ACTIVITYPODS_TOKEN ||
        process.env.INTERNAL_API_TOKEN ||
        process.env.SIDECAR_TOKEN ||
        '',
    },
    routePath: '/api/internal/social',
  },

  async started() {
    const bearerToken = this.settings.auth.bearerToken;

    if (!bearerToken) {
      this.logger.warn(
        '[InternalSocialApi] No internal bearer token configured; all requests will be rejected',
      );
    }

    await this.broker.call('api.addRoute', {
      route: {
        name: 'social-internal',
        path: this.settings.routePath,
        authorization: false,
        authentication: false,
        bodyParsers: { json: { strict: false, limit: '256kb' } },
        onBeforeCall: (ctx, _route, req) => {
          const authHeader = (
            req.headers.authorization ||
            req.headers.Authorization ||
            ''
          ).trim();
          const token = this.parseBearerToken(authHeader);
          if (!token) {
            throw new WebErrors.UnAuthorizedError(
              WebErrors.ERR_INVALID_TOKEN,
              null,
              'Unauthorized',
            );
          }

          if (this.safeTokenEquals(bearerToken, token)) {
            ctx.meta.isMasterCall = true;
          } else {
            const actorUri = this.verifyUserToken(token, bearerToken);
            if (!actorUri) {
              throw new WebErrors.UnAuthorizedError(
                WebErrors.ERR_INVALID_TOKEN,
                null,
                'Unauthorized',
              );
            }
            ctx.meta.isMasterCall = false;
            ctx.meta.authenticatedActorUri = actorUri;
          }

          ctx.meta.$responseHeaders = {
            ...(ctx.meta.$responseHeaders || {}),
            'Cache-Control': 'no-store',
            Pragma: 'no-cache',
            'X-Content-Type-Options': 'nosniff',
          };
        },
        aliases: {
          'POST /outbox':      'internal-social-api.postToOutbox',
          'GET  /actor':       'internal-social-api.getActor',
          'GET  /inbox':       'internal-social-api.getInbox',
          'GET  /outbox':      'internal-social-api.getOutbox',
          'PUT  /profile':     'internal-social-api.updateProfile',
          'POST /auth/verify': 'internal-social-api.verifyPassword',
        },
      },
      toBottom: false,
    });

    this.logger.info(
      '[InternalSocialApi] Routes registered under ' + this.settings.routePath,
    );
  },

  actions: {

    // =========================================================================
    // POST /outbox
    // Body: { actorUri: string; activity: object }
    // =========================================================================
    postToOutbox: {
      async handler(ctx) {
        const actorUri = this.requireString(ctx.params?.actorUri, 'actorUri');
        ctx.meta.dataset = this.getDataset(actorUri);
        const activity = ctx.params?.activity;

        if (!ctx.meta.isMasterCall && ctx.meta.authenticatedActorUri !== actorUri) {
          throw new MoleculerError('Forbidden: Token does not match requested actor', 403, 'FORBIDDEN');
        }

        if (!activity || typeof activity !== 'object' || Array.isArray(activity)) {
          throw new MoleculerError('activity must be an object', 400, 'INVALID_INPUT');
        }

        this.validateHttpUrl(actorUri, 'actorUri');

        // Resolve the actor's outbox URI
        const actor = await ctx.call('activitypub.actor.get', {
          actorUri,
          webId: 'system',
        });
        if (!actor) {
          ctx.meta.$statusCode = 404;
          return { error: 'not_found', message: `Actor not found: ${actorUri}` };
        }

        const outboxUri = actor.outbox || actor['as:outbox'];
        if (!outboxUri) {
          ctx.meta.$statusCode = 422;
          return { error: 'no_outbox', message: 'Actor has no outbox' };
        }

        // Post to outbox as the actor (not as system — this enforces actor ownership)
        const posted = await ctx.call('activitypub.outbox.post', {
          collectionUri: outboxUri,
          ...activity,
        }, { meta: { webId: actorUri, dataset: this.getDataset(actorUri) } });

        ctx.meta.$statusCode = 201;
        return {
          activityUri: posted?.id || posted?.['@id'] || null,
          activity: posted,
        };
      },
    },

    // =========================================================================
    // GET /actor?actorUri={uri}
    // =========================================================================
    getActor: {
      async handler(ctx) {
        const actorUri = this.requireString(
          ctx.params?.actorUri ?? ctx.meta.queryString?.actorUri,
          'actorUri',
        );
        ctx.meta.dataset = this.getDataset(actorUri);

        if (!ctx.meta.isMasterCall && ctx.meta.authenticatedActorUri !== actorUri) {
          throw new MoleculerError('Forbidden: Token does not match requested actor', 403, 'FORBIDDEN');
        }

        this.validateHttpUrl(actorUri, 'actorUri');

        const actor = await ctx.call('activitypub.actor.get', {
          actorUri,
          webId: 'system',
        });

        if (!actor) {
          ctx.meta.$statusCode = 404;
          return { error: 'not_found', message: `Actor not found: ${actorUri}` };
        }

        ctx.meta.$statusCode = 200;
        return actor;
      },
    },

    // =========================================================================
    // GET /inbox?actorUri={uri}&page={n}&pageSize={n}
    // =========================================================================
    getInbox: {
      async handler(ctx) {
        const actorUri = this.requireString(
          ctx.params?.actorUri ?? ctx.meta.queryString?.actorUri,
          'actorUri',
        );
        ctx.meta.dataset = this.getDataset(actorUri);

        if (!ctx.meta.isMasterCall && ctx.meta.authenticatedActorUri !== actorUri) {
          throw new MoleculerError('Forbidden: Token does not match requested actor', 403, 'FORBIDDEN');
        }

        this.validateHttpUrl(actorUri, 'actorUri');

        const page = Math.max(1, parseInt(ctx.params?.page ?? ctx.meta.queryString?.page ?? '1', 10) || 1);
        const pageSize = Math.min(
          MAX_PAGE_SIZE,
          Math.max(1, parseInt(ctx.params?.pageSize ?? ctx.meta.queryString?.pageSize ?? String(DEFAULT_PAGE_SIZE), 10) || DEFAULT_PAGE_SIZE),
        );

        const actor = await ctx.call('activitypub.actor.get', { actorUri, webId: 'system' });
        if (!actor) {
          ctx.meta.$statusCode = 404;
          return { error: 'not_found', message: `Actor not found: ${actorUri}` };
        }

        const inboxUri = actor.inbox || actor['as:inbox'];
        if (!inboxUri) {
          ctx.meta.$statusCode = 200;
          return { items: [], totalItems: 0, nextPage: null };
        }

        const collection = await ctx.call('activitypub.collection.get', {
          resourceUri: inboxUri,
          page,
          itemsPerPage: pageSize,
          webId: actorUri,
        });

        ctx.meta.$statusCode = 200;
        return this.serializeCollection(collection, page, pageSize);
      },
    },

    // =========================================================================
    // GET /outbox?actorUri={uri}&page={n}&pageSize={n}
    // =========================================================================
    getOutbox: {
      async handler(ctx) {
        const actorUri = this.requireString(
          ctx.params?.actorUri ?? ctx.meta.queryString?.actorUri,
          'actorUri',
        );
        ctx.meta.dataset = this.getDataset(actorUri);

        if (!ctx.meta.isMasterCall && ctx.meta.authenticatedActorUri !== actorUri) {
          throw new MoleculerError('Forbidden: Token does not match requested actor', 403, 'FORBIDDEN');
        }

        this.validateHttpUrl(actorUri, 'actorUri');

        const page = Math.max(1, parseInt(ctx.params?.page ?? ctx.meta.queryString?.page ?? '1', 10) || 1);
        const pageSize = Math.min(
          MAX_PAGE_SIZE,
          Math.max(1, parseInt(ctx.params?.pageSize ?? ctx.meta.queryString?.pageSize ?? String(DEFAULT_PAGE_SIZE), 10) || DEFAULT_PAGE_SIZE),
        );

        const actor = await ctx.call('activitypub.actor.get', { actorUri, webId: 'system' });
        if (!actor) {
          ctx.meta.$statusCode = 404;
          return { error: 'not_found', message: `Actor not found: ${actorUri}` };
        }

        const outboxUri = actor.outbox || actor['as:outbox'];
        if (!outboxUri) {
          ctx.meta.$statusCode = 200;
          return { items: [], totalItems: 0, nextPage: null };
        }

        // Outbox is publicly readable — use system to bypass WAC
        const collection = await ctx.call('activitypub.collection.get', {
          resourceUri: outboxUri,
          page,
          itemsPerPage: pageSize,
          webId: 'system',
        });

        ctx.meta.$statusCode = 200;
        return this.serializeCollection(collection, page, pageSize);
      },
    },

    // =========================================================================
    // PUT /profile
    // Body: { actorUri: string; displayName?: string; summary?: string }
    // =========================================================================
    updateProfile: {
      async handler(ctx) {
        const actorUri = this.requireString(ctx.params?.actorUri, 'actorUri');
        ctx.meta.dataset = this.getDataset(actorUri);

        if (!ctx.meta.isMasterCall && ctx.meta.authenticatedActorUri !== actorUri) {
          throw new MoleculerError('Forbidden: Token does not match requested actor', 403, 'FORBIDDEN');
        }

        this.validateHttpUrl(actorUri, 'actorUri');

        const displayName = typeof ctx.params?.displayName === 'string'
          ? ctx.params.displayName.trim().slice(0, 128)
          : undefined;
        const summary = typeof ctx.params?.summary === 'string'
          ? ctx.params.summary.trim().slice(0, 512)
          : undefined;

        if (displayName === undefined && summary === undefined) {
          throw new MoleculerError(
            'At least one of displayName or summary is required',
            400,
            'INVALID_INPUT',
          );
        }

        // Fetch current actor document
        const actor = await ctx.call('activitypub.actor.get', { actorUri, webId: 'system' });
        if (!actor) {
          ctx.meta.$statusCode = 404;
          return { error: 'not_found', message: `Actor not found: ${actorUri}` };
        }

        // Build the updated actor document
        const updated = { ...actor };
        if (displayName !== undefined) {
          updated.name = displayName;
        }
        if (summary !== undefined) {
          updated.summary = summary;
        }

        // Persist via ldp.resource.put as the actor (enforces WAC ownership)
        await ctx.call('ldp.resource.put', {
          resourceUri: actorUri,
          resource: updated,
          contentType: 'application/ld+json',
          webId: actorUri,
        });

        ctx.meta.$statusCode = 200;
        return { ok: true };
      },
    },

    // =========================================================================
    // POST /auth/verify
    // Body: { canonicalAccountId: string; password: string }
    // =========================================================================
    verifyPassword: {
      async handler(ctx) {
        if (!ctx.meta.isMasterCall) {
          throw new MoleculerError('Forbidden: Requires master token authentication', 403, 'FORBIDDEN');
        }

        const canonicalAccountId = this.requireString(
          ctx.params?.canonicalAccountId,
          'canonicalAccountId',
        );
        const password = typeof ctx.params?.password === 'string' ? ctx.params.password : '';

        if (!password) {
          throw new MoleculerError('password is required', 400, 'INVALID_INPUT');
        }

        // Look up the account by webId to get the username
        let account;
        try {
          account = await ctx.call('auth.account.findByWebId', { webId: canonicalAccountId });
        } catch {
          account = null;
        }

        if (!account) {
          this.dummyPasswordCheck(password);
          ctx.meta.$statusCode = 401;
          return { ok: false, reason: 'invalid_credentials' };
        }

        // auth.account.verify returns the account on success, throws on failure
        try {
          await ctx.call('auth.account.verify', {
            username: account.username,
            password,
          });
        } catch {
          ctx.meta.$statusCode = 401;
          return { ok: false, reason: 'invalid_credentials' };
        }

        const token = this.generateUserToken(canonicalAccountId, this.settings.auth.bearerToken);

        ctx.meta.$statusCode = 200;
        return { ok: true, scope: 'full', token };
      },
    },
  },

  methods: {

    // -------------------------------------------------------------------------
    // Auth helpers
    // -------------------------------------------------------------------------

    generateUserToken(actorUri, secretKey) {
      const exp = Date.now() + 24 * 60 * 60 * 1000;
      const payload = JSON.stringify({ actorUri, exp });
      const base64Payload = Buffer.from(payload).toString('base64url');
      const signature = crypto
        .createHmac('sha256', secretKey)
        .update(base64Payload)
        .digest('base64url');
      return `${base64Payload}.${signature}`;
    },

    verifyUserToken(token, secretKey) {
      try {
        const parts = token.split('.');
        if (parts.length !== 2) return null;
        const [base64Payload, signature] = parts;
        
        const expectedSignature = crypto
          .createHmac('sha256', secretKey)
          .update(base64Payload)
          .digest('base64url');
        
        const sigBuffer = Buffer.from(signature, 'utf8');
        const expBuffer = Buffer.from(expectedSignature, 'utf8');
        
        if (sigBuffer.length !== expBuffer.length || !crypto.timingSafeEqual(sigBuffer, expBuffer)) {
          return null;
        }
        
        const payloadStr = Buffer.from(base64Payload, 'base64url').toString('utf8');
        const payload = JSON.parse(payloadStr);
        
        if (!payload.actorUri || typeof payload.exp !== 'number') {
          return null;
        }
        
        if (payload.exp < Date.now()) {
          return null;
        }
        
        return payload.actorUri;
      } catch (err) {
        return null;
      }
    },

    parseBearerToken(authHeader) {
      if (!authHeader || typeof authHeader !== 'string') return null;
      const match = /^Bearer\s+(\S+)$/i.exec(authHeader.trim());
      return match ? match[1] : null;
    },

    safeTokenEquals(expected, provided) {
      if (!expected || !provided) return false;
      const exp = Buffer.from(String(expected), 'utf8');
      const got = Buffer.from(String(provided), 'utf8');
      const maxLen = Math.max(exp.length, got.length);
      const expPadded = Buffer.alloc(maxLen, 0);
      const gotPadded = Buffer.alloc(maxLen, 0);
      exp.copy(expPadded);
      got.copy(gotPadded);
      return exp.length === got.length && crypto.timingSafeEqual(expPadded, gotPadded);
    },

    // Constant-time dummy check to prevent timing-based account enumeration
    dummyPasswordCheck(password) {
      const dummy = Buffer.from('dummy-password-for-timing-safety', 'utf8');
      const actual = Buffer.from(String(password), 'utf8');
      const maxLen = Math.max(dummy.length, actual.length);
      const a = Buffer.alloc(maxLen, 0);
      const b = Buffer.alloc(maxLen, 0);
      dummy.copy(a);
      actual.copy(b);
      crypto.timingSafeEqual(a, b);
    },

    // -------------------------------------------------------------------------
    // Input validation helpers
    // -------------------------------------------------------------------------

    requireString(value, fieldName) {
      const str = typeof value === 'string' ? value.trim() : '';
      if (!str) {
        throw new MoleculerError(`${fieldName} is required`, 400, 'INVALID_INPUT');
      }
      return str;
    },

    validateHttpUrl(value, fieldName) {
      try {
        const parsed = new URL(value);
        if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
          throw new Error('invalid protocol');
        }
        if (parsed.username || parsed.password) {
          throw new Error('credentials not allowed');
        }
      } catch {
        throw new MoleculerError(
          `${fieldName} must be a valid http(s) URL`,
          400,
          'INVALID_INPUT',
        );
      }
    },

    // -------------------------------------------------------------------------
    // ActivityPods helpers
    // -------------------------------------------------------------------------

    // Derive the Moleculer dataset name from an actor URI.
    // In pod-provider mode, the dataset is the username segment of the URI.
    // e.g. https://pod.example/alice → "alice"
    getDataset(actorUri) {
      try {
        const pathname = new URL(actorUri).pathname;
        // Strip leading slash and take the first path segment
        return pathname.replace(/^\//, '').split('/')[0] || undefined;
      } catch {
        return undefined;
      }
    },

    // Normalize a collection response into a consistent shape
    serializeCollection(collection, page, pageSize) {
      if (!collection || typeof collection !== 'object') {
        return { items: [], totalItems: 0, nextPage: null };
      }

      const rawItems =
        collection.orderedItems ||
        collection['as:orderedItems'] ||
        collection.items ||
        collection['as:items'] ||
        [];

      const items = Array.isArray(rawItems) ? rawItems : [];
      const totalItems =
        typeof collection.totalItems === 'number'
          ? collection.totalItems
          : typeof collection['as:totalItems'] === 'number'
            ? collection['as:totalItems']
            : items.length;

      const hasMore = items.length >= pageSize && page * pageSize < totalItems;

      return {
        items,
        totalItems,
        nextPage: hasMore ? page + 1 : null,
        page,
        pageSize,
      };
    },
  },
};
