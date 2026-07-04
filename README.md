# better-edge-flow https://better-edge-flow-website.vercel.app/
<p align="center">
  <img src="https://img.shields.io/npm/v/better-edge-flow?style=for-the-badge&color=6366f1&labelColor=1e1b4b" alt="npm version" />
  <img src="https://img.shields.io/npm/dm/better-edge-flow?style=for-the-badge&color=8b5cf6&labelColor=1e1b4b" alt="npm downloads" />
  <img src="https://img.shields.io/badge/TypeScript-Ready-3178c6?style=for-the-badge&logo=typescript&labelColor=1e1b4b" alt="TypeScript" />
  <img src="https://img.shields.io/badge/Edge-Native-f59e0b?style=for-the-badge&labelColor=1e1b4b" alt="Edge Native" />
  <img src="https://img.shields.io/badge/License-MIT-10b981?style=for-the-badge&labelColor=1e1b4b" alt="MIT License" />
</p>

<p align="center">
  <strong>Durable workflow execution for serverless and edge runtimes.</strong><br />
  Zero sidecars. Zero binaries. Just TypeScript and your database.
</p>

---

## What is better-edge-flow?

`better-edge-flow` is a **lightweight, database-backed durable execution framework** built natively for serverless and edge environments — Cloudflare Workers, Vercel Edge Functions, AWS Lambda, and beyond.

It gives your TypeScript code **long-running, resumable, fault-tolerant workflow capabilities** using only your existing database (PostgreSQL or SQLite), without requiring any additional infrastructure, sidecars, or proprietary runtimes.

Think of it as **Temporal for the edge** — but as a simple npm package.

```
npm install better-edge-flow
```

---

## Why better-edge-flow?

| | better-edge-flow | Temporal | Dapr | DBOS |
|---|:---:|:---:|:---:|:---:|
| Runs in Cloudflare Workers | ✅ | ❌ | ❌ | ❌ |
| Runs in Vercel Edge Functions | ✅ | ❌ | ❌ | ❌ |
| Zero sidecars or binaries | ✅ | ❌ | ❌ | ❌ |
| Use your own database | ✅ | ❌ | Partial | ❌ |
| TypeScript-native | ✅ | Partial | Partial | ✅ |
| Simple npm install | ✅ | ❌ | ❌ | ❌ |
| No platform lock-in | ✅ | ❌ | ❌ | ❌ |

---

## Features

- 🔁 **Durable Execution** — Workflows survive crashes, cold starts, and redeploys
- 😴 **Durable Sleeps** — `ctx.sleep("3d")` suspends and resumes across serverless invocations
- 📨 **Event Signals** — Wait for external webhooks durably (`ctx.waitForEvent`)
- 🔄 **Automatic Retries** — Configurable exponential backoff per activity step
- ↩️ **Saga Compensations** — Register rollback handlers that auto-execute on failure
- 🔀 **Dynamic Code Versioning** — Deploy new code safely while old executions are still running
- 🚦 **Activity Rate Limiting** — Prevent API throttling with built-in queue concurrency control
- 🏢 **Multi-Tenant Namespaces** — Partition executions by `tenantId` and `namespace`
- 🔐 **Payload Encryption at Rest** — Pluggable encryption provider for sensitive workflow data
- ⚡ **Local Activities** — In-memory fast steps with deferred DB writes (up to 90% fewer queries)
- 📡 **OpenTelemetry Tracing** — Native OTel spans for end-to-end distributed tracing
- 🧅 **Onion Interceptors** — Chainable inbound/outbound middleware for workflows and activities
- 📦 **Signal Batching** — Debounced event buffering to collapse burst webhook signals
- ⏱️ **Execution Timeouts** — Configurable deadlines with automatic Saga rollbacks
- 💾 **State Checkpoints** — Mid-loop state snapshots for safe long-running iteration recovery
- 🔑 **Idempotent Events** — Unique event key deduplication prevents duplicate webhook processing
- 🧹 **History Pruning** — Opt-in deletion of completed step logs to minimize database storage
- 🌐 **Standard Web API Handler** — Single `flow.handler(req)` for edge/serverless router mounting
- 🛠️ **Type-Safe Client SDK** — End-to-end TypeScript autocomplete for starts, events, and cancellations

---

## Quick Start

### 1. Install

```bash
npm install better-edge-flow drizzle-orm
```

### 2. Set up your Database Adapter

```typescript
// db.ts
import { createClient } from "@libsql/client";
import { drizzle } from "drizzle-orm/libsql";

const client = createClient({ url: "file:flow.db" });
export const db = drizzle(client);
```

### 3. Define and Run a Workflow

```typescript
import { createBetterFlow } from "better-edge-flow";
import { drizzleAdapter } from "better-edge-flow/adapters/drizzle";
import { db } from "./db.js";

const flow = createBetterFlow({
  adapter: drizzleAdapter({ db, dialect: "sqlite" }),
});

// Define a durable workflow
const onboardingFlow = flow.define("userOnboarding", async (ctx, input: { userId: string; email: string }) => {
  // Step 1: Fetch user — cached on replay, never re-executed
  const user = await ctx.run("fetchUser", async () => {
    return await db.query.users.findFirst({ where: eq(users.id, input.userId) });
  });

  // Step 2: Sleep for 3 days — survives Lambda cold starts and redeploys!
  await ctx.sleep("3d");

  // Step 3: Wait for an external payment webhook — durably suspends
  const payment = await ctx.waitForEvent("stripe-payment-confirmed", { timeout: "7d" });

  // Step 4: Branch on result
  if (payment.success) {
    return await ctx.run("sendProWelcome", () => sendEmail(user.email, "pro"));
  } else {
    return await ctx.run("sendTrialEmail", () => sendEmail(user.email, "trial"));
  }
});

// Start a workflow execution
const executionId = await onboardingFlow.start({ userId: "user_123", email: "jane@example.com" });
```

### 4. Mount the HTTP Handler

```typescript
import { Hono } from "hono";

const app = new Hono();

// Single-line router mount — exposes /start, /event, /cancel, /cron, /executions
app.all("/api/flow/*", (c) => flow.handler(c.req.raw));
```

### 5. Publish an Event (Webhook)

```bash
curl -X POST https://your-app.com/api/flow/event \
  -H "Content-Type: application/json" \
  -d '{"executionId": "exec_123", "eventName": "stripe-payment-confirmed", "payload": {"success": true}}'
```

---

## Advanced Usage

### Saga Compensations (Automatic Rollbacks)

```typescript
flow.define("createOrder", async (ctx, input) => {
  ctx.registerCompensation("cancelCharge", () => stripe.refund(input.chargeId));
  ctx.registerCompensation("releaseInventory", () => inventory.release(input.itemId));

  await ctx.run("chargeCustomer", () => stripe.charge(input.amount));
  await ctx.run("reserveInventory", () => inventory.reserve(input.itemId));

  // If anything throws, compensations run in reverse order automatically
  await ctx.run("shipOrder", () => shipping.ship(input.address));
});
```

### Activity Retries with Exponential Backoff

```typescript
const result = await ctx.run("callExternalAPI", fetchData, {
  retries: 5,
  initialDelay: "2s",
  backoffFactor: 2, // Retries at 2s, 4s, 8s, 16s, 32s
});
```

### Activity Rate Limiting

```typescript
await ctx.run("sendEmail", () => sendgrid.send(email), {
  rateLimit: { queue: "email-sender", limit: 100, window: "1m" }
});
```

### Payload Encryption at Rest

```typescript
const flow = createBetterFlow({
  adapter: drizzleAdapter({ db, dialect: "postgresql" }),
  encryption: {
    encrypt: (plaintext) => aes256.encrypt(plaintext, process.env.SECRET_KEY),
    decrypt: (ciphertext) => aes256.decrypt(ciphertext, process.env.SECRET_KEY),
  },
});
```

### Local Activities (Zero DB Writes)

```typescript
// Runs in-memory with no database writes — up to 90% fewer queries
const parsed = await ctx.runLocal("parsePayload", () => JSON.parse(rawData));
```

### Dynamic Code Versioning

```typescript
flow.define("processOrder", async (ctx, input) => {
  const isNewVersion = await ctx.getVersion("add-fraud-check", { min: 2 });

  if (isNewVersion) {
    await ctx.run("fraudCheck", () => runFraudAnalysis(input)); // New code path
  }

  await ctx.run("fulfillOrder", () => fulfillment.process(input));
});
```

### Workflow Interceptors (Middleware)

```typescript
const flow = createBetterFlow({
  adapter: drizzleAdapter({ db, dialect: "postgresql" }),
  interceptors: [
    {
      inbound: async (ctx, input, next) => {
        console.log(`[Workflow Started] Input:`, input);
        return next();
      },
      outbound: async (ctx, stepName, next) => {
        const start = Date.now();
        const result = await next();
        console.log(`[Step: ${stepName}] completed in ${Date.now() - start}ms`);
        return result;
      },
    }
  ],
});
```

---

## Database Migrations

We ship pre-generated SQL migration files for both PostgreSQL and SQLite inside the npm package.

### PostgreSQL

```bash
# migrations/pg/0000_smart_morbius.sql is included in the package
npx drizzle-kit migrate --config=node_modules/better-edge-flow/drizzle.pg.config.ts
```

Or apply the SQL file directly to your database using your preferred migration runner.

### SQLite

```bash
# migrations/sqlite/0000_overjoyed_stick.sql is included in the package
npx drizzle-kit migrate --config=node_modules/better-edge-flow/drizzle.sqlite.config.ts
```

---

## Production Deployment

### Vercel / Next.js

```typescript
// app/api/flow/[...path]/route.ts
import { flow } from "@/lib/flow";

export async function POST(req: Request) {
  return flow.handler(req);
}

export async function GET(req: Request) {
  return flow.handler(req);
}
```

Add a cron to `vercel.json` to sweep expired timers:
```json
{
  "crons": [{ "path": "/api/flow/cron", "schedule": "* * * * *" }]
}
```

### Cloudflare Workers

```typescript
export default {
  async fetch(request: Request, env: Env) {
    return flow.handler(request);
  },
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(flow.checkTimers()); // Sweep expired timers from cron trigger
  },
};
```

### OpenTelemetry Tracing

```typescript
import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";

const sdk = new NodeSDK({
  traceExporter: new OTLPTraceExporter({ url: process.env.OTEL_ENDPOINT }),
});
sdk.start();
```

---

## API Reference

| Method | Description |
|---|---|
| `flow.define(name, handler)` | Register a durable workflow |
| `flow.start(name, input, options?)` | Start a new workflow execution |
| `flow.resume(executionId)` | Resume a suspended execution |
| `flow.cancel(executionId)` | Cancel a running execution |
| `flow.publishEvent(id, event, payload)` | Send a signal event to a workflow |
| `flow.checkTimers()` | Evaluate expired sleep timers and deadlines |
| `flow.handler(req)` | Standard Web API HTTP request handler |
| `ctx.run(name, fn, options?)` | Execute a durable activity step |
| `ctx.runLocal(name, fn)` | Execute an in-memory local activity (no DB write) |
| `ctx.sleep(duration)` | Suspend execution for a duration (`"10s"`, `"3d"`) |
| `ctx.waitForEvent(name, options?)` | Suspend and wait for an external signal event |
| `ctx.registerCompensation(name, fn)` | Register a Saga rollback handler |
| `ctx.getVersion(changeId, options)` | Branch on code version for safe deploys |
| `ctx.checkpoint(id, state)` | Save a mid-loop state snapshot |
| `ctx.log(message)` | Replay-safe logging (muted during replays) |

---

## Configuration

```typescript
createBetterFlow({
  adapter,                        // Required: database adapter
  defaultWorkflowTimeout: "24h",  // Global execution deadline
  eventDebounceMs: 100,           // Batch rapid signal events (ms)
  pruneHistoryOnComplete: true,   // Delete step logs when execution completes
  encryption: {                   // Encrypt payloads at rest
    encrypt: (text) => ...,
    decrypt: (text) => ...,
  },
  interceptors: [...],            // Chainable inbound/outbound middlewares
});
```

---

## License

MIT © [Deepak Nagendran](https://github.com/deepuonthemove)
