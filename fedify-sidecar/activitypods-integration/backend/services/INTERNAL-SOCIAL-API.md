# Internal Social API

## Purpose

Provides HTTP endpoints that your app backend calls to perform social actions
on behalf of users — posting, reading, profile updates, and login — without
requiring users to go through an OIDC consent screen or interact with
ActivityPods directly.

Users sign up and log in through your app's own UI. ActivityPods is completely
invisible to them.

## How it bypasses OIDC

ActivityPods normally requires an OIDC token to verify who is making an HTTP
request. This service bypasses that by operating inside Moleculer's internal
service bus rather than through the HTTP authentication layer.

The key is `ctx.meta.webId`. When ActivityPods processes an outbox post, it
checks:

```javascript
if (ctx.meta.webId && ctx.meta.webId !== 'system' && actorUri !== ctx.meta.webId) {
  throw new UnAuthorizedError(...)
}
```

Our service sets `ctx.meta.webId = actorUri` on every internal call, so the
check passes without an OIDC token. The trust boundary is `ACTIVITYPODS_TOKEN`
— a shared secret between your app backend and ActivityPods.

## Trust chain

```
User → Your App (verifies your app's JWT)
           ↓
       POST /api/internal/social/outbox
       Authorization: Bearer ACTIVITYPODS_TOKEN
           ↓
       internal-social-api (Moleculer service)
           ↓
       ctx.call('activitypub.outbox.post', ..., { meta: { webId: actorUri } })
           ↓
       ActivityPods: webId === actorUri → trusted → processes activity
           ↓
       Fuseki write + AP federation
```

## Endpoints

All endpoints require `Authorization: Bearer <ACTIVITYPODS_TOKEN>`.

### POST /api/internal/social/outbox

Post any ActivityPub activity on behalf of a user.

**Request:**
```json
{
  "actorUri": "https://pod.example/alice",
  "activity": {
    "@context": "https://www.w3.org/ns/activitystreams",
    "type": "Create",
    "object": {
      "type": "Note",
      "content": "Hello world!",
      "to": ["https://www.w3.org/ns/activitystreams#Public"]
    }
  }
}
```

**Response (201):**
```json
{
  "activityUri": "https://pod.example/alice/data/abc123",
  "activity": { ... }
}
```

Works for any AP activity type: `Create`, `Follow`, `Like`, `Announce`,
`Undo`, `Delete`, etc.

---

### GET /api/internal/social/actor

Fetch a user's ActivityPub actor document (profile).

**Query params:** `actorUri` (required)

**Response (200):** Full AP actor document including inbox, outbox, followers,
following, public keys.

---

### GET /api/internal/social/inbox

Read a user's inbox with pagination.

**Query params:** `actorUri` (required), `page` (default: 1), `pageSize`
(default: 20, max: 100)

**Response (200):**
```json
{
  "items": [...],
  "totalItems": 42,
  "nextPage": 2,
  "page": 1,
  "pageSize": 20
}
```

---

### GET /api/internal/social/outbox

Read a user's outbox with pagination. Same shape as inbox response.

---

### PUT /api/internal/social/profile

Update a user's display name and/or bio.

**Request:**
```json
{
  "actorUri": "https://pod.example/alice",
  "displayName": "Alice Smith",
  "summary": "Building cool things"
}
```

**Response (200):** `{ "ok": true }`

---

### POST /api/internal/social/auth/verify

Verify a user's password. Use this as the login check in your app before
issuing your own JWT.

**Request:**
```json
{
  "canonicalAccountId": "https://pod.example/alice",
  "password": "userpassword"
}
```

**Response (200):** `{ "ok": true, "scope": "full" }`

**Response (401):** `{ "ok": false, "reason": "invalid_password" }`

**Response (404):** `{ "ok": false, "reason": "account_not_found" }`

---

## Configuration

Set in ActivityPods backend `.env`:

```bash
ACTIVITYPODS_TOKEN=<long-random-secret>   # must match sidecar's ACTIVITYPODS_TOKEN
```

## Security in production

The `/api/internal/*` routes must **never be publicly reachable**. Enforce
this at the reverse proxy (Nginx/Caddy/Traefik):

- Expose only public AP endpoints to the internet (`/inbox`, `/outbox`,
  `/.well-known/*`, `/users/*`, etc.)
- Block `/api/internal/*` at the proxy — only your app server (same private
  network) should reach it
- Use a strong random `ACTIVITYPODS_TOKEN` (32+ characters)

`ACTIVITYPODS_TOKEN` is your last line of defense. Network-level isolation is
the primary protection.

## Installation

Copy this file to your ActivityPods backend services directory:

```bash
cp internal-social-api.service.js /path/to/activitypods/pod-provider/backend/services/
```

Moleculer picks it up automatically on next start (no config changes needed).
The service registers itself once `api`, `activitypub.outbox`, `activitypub.actor`,
`activitypub.collection`, and `auth.account` are all available.
