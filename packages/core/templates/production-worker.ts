/**
 * Better-Flow Production Serverless/Edge Deployment Template
 * Demonstrates:
 * 1. OpenTelemetry SDK initialization and Trace Span exporter integration (OTel)
 * 2. Neon Serverless WebSocket PostgreSQL Connection Pooling
 * 3. Hono router mounting with POST /cron sweeper schedules
 */

import { Hono } from "hono";
import { cors } from "hono/cors";
import { createBetterFlow } from "better-flow";
import { drizzleAdapter } from "better-flow/adapters/drizzle";
import { drizzle } from "drizzle-orm/neon-serverless";
import { Pool, neonConfig } from "@neondatabase/serverless";
import ws from "ws";

// 1. Configure Neon Serverless for WebSockets-based connection pooling
neonConfig.webSocketConstructor = ws;

// 2. Initialize OpenTelemetry SDK (Configured for Honeycomb/Datadog OTLP exporter)
import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { Resource } from "@opentelemetry/resources";
import { SemanticResourceAttributes } from "@opentelemetry/semantic-conventions";

const sdk = new NodeSDK({
  resource: new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: "better-flow-production",
  }),
  traceExporter: new OTLPTraceExporter({
    url: process.env.OTEL_EXPORTER_OTLP_ENDPOINT || "http://localhost:4317",
    headers: {
      "x-honeycomb-team": process.env.HONEYCOMB_API_KEY || "",
    }
  }),
});

// Start the OTel SDK tracing collector (can be disabled in development)
if (process.env.NODE_ENV === "production") {
  sdk.start();
  process.on("SIGTERM", () => {
    sdk.shutdown()
      .then(() => console.log("Tracing terminated"))
      .catch((error) => console.log("Error terminating tracing", error))
      .finally(() => process.exit(0));
  });
}

// 3. Setup PostgreSQL Connection Pool
const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error("DATABASE_URL environment variable is missing!");
}

// Use a single pooled connection manager matching serverless container scales
const pool = new Pool({ connectionString });
const db = drizzle(pool);

// 4. Initialize Better-Flow Engine with Postgres Drizzle Adapter
export const flow = createBetterFlow({
  adapter: drizzleAdapter({ db, dialect: "postgresql" }),
  // Clean up completed steps dynamically in DB (opt-in storage optimization)
  pruneHistoryOnComplete: true,
  // 100ms debounce signal windows to batch high-concurrency event webhooks
  eventDebounceMs: 100,
  // Default workflow execution deadlines
  defaultWorkflowTimeout: "24h"
});

// Define your business workflows here
flow.define("processPayment", async (ctx, input: { orderId: string; amount: number }) => {
  await ctx.run("verifyInvoice", () => {
    ctx.log("Processing order invoice for order: " + input.orderId);
    return { status: "VERIFIED" };
  });

  await ctx.sleep("10s");

  return { success: true };
});

// 5. Build Hono Routing App
const app = new Hono();

app.use("/*", cors({
  origin: process.env.ALLOWED_ORIGINS?.split(",") || "*",
  allowMethods: ["GET", "POST", "OPTIONS"],
  allowHeaders: ["Content-Type"]
}));

// Expose standard API endpoints for dashboard & webhooks
app.all("/api/better-flow/*", async (c) => {
  return await flow.handler(c.req.raw);
});

// Proactive Serverless wakeup router for cron schedulers (triggered once/min)
app.post("/cron-sweeper", async (c) => {
  const authHeader = c.req.header("Authorization");
  if (process.env.CRON_SECRET && authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return c.text("Unauthorized", 401);
  }
  
  await flow.checkTimers();
  return c.text("Sweeper completed successfully.");
});

app.get("/health", (c) => c.text("Healthy"));

export default app;
