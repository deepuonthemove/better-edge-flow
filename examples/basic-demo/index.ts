import { serve } from "@hono/node-server";
import { Hono } from "hono";
import { cors } from "hono/cors";
import { createBetterFlow } from "better-edge-flow";
import { drizzleAdapter } from "better-edge-flow/adapters/drizzle";
import { db } from "./db.js";

const app = new Hono();

// Enable CORS for dashboard visual monitoring integration
app.use("/*", cors({
  origin: "*",
  allowMethods: ["GET", "POST", "OPTIONS"],
  allowHeaders: ["Content-Type"]
}));

// Initialize Better-Flow using Drizzle SQLite adapter
const flow = createBetterFlow({
  adapter: drizzleAdapter({ db, dialect: "sqlite" }),
  events: {
    "stripe-payment-webhook": {} as { success: boolean; amount: number }
  }
});

// Define the onboarding durable workflow
export const signupOnboardingFlow = flow.define("userOnboarding", async (ctx, input: { userId: string; email: string; name: string }) => {
  // Step 1: Query database (Activity execution wrapped durably)
  const user = await ctx.run("fetchUser", async () => {
    console.log(`[Workflow] Fetching user: ${input.userId}`);
    return { id: input.userId, name: input.name, email: input.email };
  });

  // Step 2: Call LLM / Generate content
  const welcomeCopy = await ctx.run("generateWelcomeLetter", async () => {
    console.log(`[Workflow] Generating welcome letter for ${user.name}`);
    // Simulate an API call to an LLM
    return `Hey ${user.name}! Welcome to Better-Flow. This letter was generated durably.`;
  });

  // Step 3: Sleep for 10 seconds (Shortened from 3 days for testing purposes!)
  console.log(`[Workflow] Suspending execution to sleep for 10 seconds...`);
  await ctx.sleep("10s");
  console.log(`[Workflow] Resumed from 10s sleep!`);

  // Step 4: Suspend execution awaiting external stripe billing webhook callback
  console.log(`[Workflow] Waiting for stripe-payment-webhook...`);
  const paymentEvent = await ctx.waitForEvent("stripe-payment-webhook", {
    timeout: "5m"
  });
  console.log(`[Workflow] Received paymentEvent:`, paymentEvent);

  // Step 5: Conditional Branching based on dynamic events
  if (paymentEvent.success) {
    return await ctx.run("sendProEmail", async () => {
      console.log(`[Workflow] Sending PRO onboarding email to ${user.email}`);
      return { sent: true, type: "PRO", welcomeCopy };
    });
  } else {
    return await ctx.run("sendTrialEndingEmail", async () => {
      console.log(`[Workflow] Sending TRIAL ending email to ${user.email}`);
      return { sent: true, type: "TRIAL_ENDING" };
    });
  }
});

// Mount Better-Flow standard HTTP handler
app.all("/api/better-flow/*", async (c) => {
  // Forward to standard Web API handler
  return await flow.handler(c.req.raw);
});

// Direct test route to trigger a workflow run manually via browser
app.get("/trigger", async (c) => {
  const userId = `user_${Math.floor(Math.random() * 1000)}`;
  const executionId = await flow.start("userOnboarding", {
    userId,
    name: "John Doe",
    email: "john.doe@example.com"
  });
  return c.json({ triggered: true, executionId });
});

// Health check status endpoint
app.get("/", (c) => {
  return c.text("Better-Flow Sandbox Server is running on port 3000!");
});

console.log("Better-Flow API Server starting on http://localhost:3001");
serve({
  fetch: app.fetch,
  port: 3001
});
