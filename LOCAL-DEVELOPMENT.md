# Local Development Guide

This guide starts the complete local development environment for the Mastopod
Federation Architecture workspace: ActivityPods, the Fedify sidecar, the media
pipeline, Redis, RedPanda, and OpenSearch.

For the architecture and authority boundaries, read
[ARCHITECTURE-BASELINE.md](./ARCHITECTURE-BASELINE.md) first.

## What Runs Locally

```text
ActivityPods frontend       http://localhost:5000
ActivityPods backend        http://localhost:3000
Fedify sidecar              http://localhost:8080
Media pipeline sidecar      http://localhost:8090
OpenSearch                  http://localhost:9200
RedPanda Kafka API          localhost:19092
RedPanda Console            http://localhost:8081
Redis                       localhost:6379
```

The development stack has these responsibilities:

```text
ActivityPods
  owns pod data, accounts, permissions, WebIDs, and private signing keys
       |
       | authenticated internal APIs (ACTIVITYPODS_TOKEN)
       v
Fedify sidecar
  owns remote ActivityPub HTTP, delivery/retry, ATProto runtime, and projection
       |                         |
       v                         v
Redis work queues          RedPanda durable event logs
                                  |
                                  v
                           OpenSearch and other consumers
```

Redis is only for transient, acknowledged work queues. RedPanda is only for
durable, replayable event logs. Do not use them interchangeably.

## Prerequisites

- Node.js 20 or newer and npm
- Docker Desktop or OrbStack with approximately 4 CPUs and 8 GB RAM available
- `lsof` (used by the local bootstrap and status scripts)
- macOS/Linux shell environment

The scripts can also start Colima when Docker is unavailable and `colima` is
installed.

## Install Dependencies

From the workspace root, install the sidecar and media dependencies:

```sh
cd fedify-sidecar
npm ci

cd ../media-pipeline-sidecar
npm ci
```

The current checkout normally includes installed dependencies for the
ActivityPods backend and frontend. If they are absent, install them separately:

```sh
cd activitpod-aptoroto/activitypods/pod-provider/backend
npm install

cd ../frontend
npm install
```

## Configure Local Environment

The sidecar local profile is `fedify-sidecar/.env.local`. Create it from the
provided template if it does not already exist:

```sh
cd fedify-sidecar
cp -n .env.local.example .env.local
```

For the standard local stack, retain these endpoint values:

```dotenv
NODE_ENV=development
PORT=8080
ACTIVITYPODS_URL=http://localhost:3000
REDIS_URL=redis://localhost:6379
REDPANDA_BROKERS=localhost:19092
OPENSEARCH_NODE=http://localhost:9200
```

### Required shared secrets

`ACTIVITYPODS_TOKEN` is the authentication boundary for sidecar-to-provider
internal APIs. It must have the same value in:

- `fedify-sidecar/.env.local`
- `activitpod-aptoroto/activitypods/pod-provider/backend/.env`

`SIDECAR_TOKEN` authenticates ActivityPods outbox webhooks to the sidecar. Set
the same value in the provider configuration and the sidecar configuration.

Local placeholder values are acceptable only for a machine-local environment.
Use distinct strong secrets for any shared, staging, or public deployment.

## Start the Full Stack

Run the supported bootstrap from the sidecar directory:

```sh
cd fedify-sidecar
./activitypods-integration/scripts/bootstrap-local-dev.sh
```

The script:

1. Starts the ActivityPods Compose dependencies.
2. Starts RedPanda, OpenSearch, dashboards, Prometheus, and Grafana.
3. Creates `fedify-sidecar/.env.local` from the template when missing.
4. Bootstraps required RedPanda topics.
5. Starts the ActivityPods backend and frontend as host processes.
6. Starts the Fedify sidecar and media pipeline as host processes.

Logs and PID files for the host processes are stored in:

```text
activitpod-aptoroto/activitypods/.logs/
activitpod-aptoroto/activitypods/.pids/
```

## Verify the Stack

Use the companion status script:

```sh
cd fedify-sidecar
./activitypods-integration/scripts/status-local-dev.sh
```

Useful direct checks:

```sh
curl http://localhost:8080/health
curl http://localhost:8080/metrics
curl http://localhost:8090/health
curl http://localhost:9200
```

If the sidecar starts but cannot process public events, verify the broker and
bootstrap topics again:

```sh
cd fedify-sidecar
npm run topics:bootstrap
npm run topics:verify
```

## Everyday Component Workflows

### Fedify sidecar

```sh
cd fedify-sidecar
npm run dev
npm run typecheck
npm test
```

The main runtime composition is in `fedify-sidecar/src/index.ts`. It registers
the ActivityPub inbox/webhook routes, queue workers, Fedify integration,
ActivityPods clients, protocol bridge, and optional ATProto XRPC/OAuth routes.

### Media pipeline

```sh
cd media-pipeline-sidecar
npm run server:dev
```

The pipeline receives internally authenticated media-ingest events from
ActivityPods, processes them through workers, and can publish asset events for
downstream consumers.

### ActivityPods integration contract

Provider-side integration services live in:

```text
activitpod-aptoroto/activitypods/pod-provider/backend/services/
```

Important routes include:

- `POST /api/internal/signatures/batch`: ActivityPub signing
- `POST /api/internal/inbox/receive`: verified inbound ActivityPub delivery
- `/api/internal/identity/*`: identity projections for the sidecar
- `/api/internal/activitypub-bridge/*`: bridge recipient and media resolution
- `/api/internal/media-pipeline/*`: canonical media source and asset operations

These routes are private: do not publish `/api/internal/*` through a public
proxy.

## Local Feature Flags

The bootstrap intentionally uses a safe local profile:

```dotenv
AT_LOCAL_FIXTURE=true
ENABLE_XRPC_SERVER=false
ENABLE_AT_JETSTREAM=false
ENABLE_FEDIFY_RUNTIME_INTEGRATION=false
```

This is appropriate for local implementation and automated proof work. It is
not a live-federation setup.

For external federation testing, you need a publicly reachable HTTPS hostname,
set `DOMAIN` accordingly, and explicitly enable the relevant runtime feature
flags. Ensure the external proxy exposes only the intended public federation
routes; leave internal provider APIs private.

## Working With Memory

`memory/` is a separate app repository. Its `make start` command starts its
own PodProvider environment, so do not run it alongside this full-stack
bootstrap without reconciling its ports, Docker network, and provider URL.

For integration work, prefer one provider stack and configure Memory's API and
frontend to point at that provider. See `memory/README.md` for its standalone
development commands.

## Stop the Stack

Use the paired shutdown script:

```sh
cd fedify-sidecar
./activitypods-integration/scripts/shutdown-local-dev.sh
```

This stops the host processes tracked by the bootstrap and brings down both the
federation and ActivityPods Compose stacks. It does not remove Docker volumes.

## Troubleshooting

### Stale PID file or port already in use

Run the status script first. If it reports stale PID files after a crashed
process, run the shutdown script and then bootstrap again.

### Docker daemon unavailable

Start Docker Desktop or OrbStack. If using Colima, ensure `colima` is installed;
the bootstrap will attempt to start it with the required resources.

### Authentication failures between the sidecar and provider

Confirm `ACTIVITYPODS_TOKEN` is identical in both local environment files.
Confirm `SIDECAR_TOKEN` is identical at the webhook sender and receiver.

### Sidecar starts but topic validation fails

Confirm RedPanda listens on `localhost:19092`, then run `npm run
topics:bootstrap` followed by `npm run topics:verify` from `fedify-sidecar`.
