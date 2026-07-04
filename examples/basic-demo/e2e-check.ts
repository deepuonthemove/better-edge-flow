import { setTimeout } from "timers/promises";

console.log("=== BETTER-FLOW HTTP E2E VERIFICATION ===");

// 1. Start a new workflow execution
const resStart = await fetch("http://localhost:3001/api/better-flow/start", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    workflowName: "userOnboarding",
    executionId: "e2e_exec_test_123",
    input: {
      userId: "usr_alice",
      name: "Alice Cooper",
      email: "alice@example.com"
    }
  })
});
console.log("1. Workflow Trigger response:", await resStart.json());

// 2. Fetch history immediately (should show activities completed and sleep pending)
const resHistory1 = await fetch("http://localhost:3001/api/better-flow/executions/e2e_exec_test_123/history");
const history1 = await resHistory1.json() as any[];
console.log("\n2. Initial timeline steps:");
history1.forEach(s => {
  console.log(` - Step [${s.stepIndex}]: "${s.stepName}" (${s.stepType}) -> Status: ${s.status}`);
});

// 3. Wait 11 seconds for the 10-second sleep to expire
console.log("\n3. Waiting 11 seconds for sleep timer to expire...");
await setTimeout(11000);

// 4. Tick cron to resume expired sleep states
const resCron = await fetch("http://localhost:3001/api/better-flow/cron", { method: "POST" });
console.log("4. Cron Tick response:", await resCron.json());

// 5. Fetch history again (sleep should be completed, webhook event should be pending)
const resHistory2 = await fetch("http://localhost:3001/api/better-flow/executions/e2e_exec_test_123/history");
const history2 = await resHistory2.json() as any[];
console.log("\n5. Timeline steps after Cron Tick:");
history2.forEach(s => {
  console.log(` - Step [${s.stepIndex}]: "${s.stepName}" (${s.stepType}) -> Status: ${s.status}`);
});

// 6. Publish Stripe webhook event to resume the workflow
console.log("\n6. Sending mock stripe-payment-webhook event...");
const resWebhook = await fetch("http://localhost:3001/api/better-flow/event", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    executionId: "e2e_exec_test_123",
    eventName: "stripe-payment-webhook",
    payload: { success: true, amount: 4900 }
  })
});
console.log("6. Webhook publish response:", await resWebhook.json());

// 7. Verify final workflow completion and PRO email step output
const resExecutions = await fetch("http://localhost:3001/api/better-flow/executions");
const executions = await resExecutions.json() as any[];
const myExec = executions.find(e => e.id === "e2e_exec_test_123");

console.log("\n7. Final Workflow Execution State:");
console.log(` - Status: ${myExec?.status}`);
console.log(` - Output:`, myExec?.output);
console.log(` - Error:`, myExec?.error);
console.log("=========================================");
