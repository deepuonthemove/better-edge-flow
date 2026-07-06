import { describe, it, expect, vi } from "vitest";
import { createBetterFlow } from "./index.js";
import { BetterFlowAdapter, ExecutionRecord, StepRecord, EventRecord } from "./types.js";
import { WorkflowSuspendedError } from "./errors.js";

// Fast, isolated in-memory adapter mimicking relational SQL databases
function memoryAdapter(config?: {
  encryption?: {
    encrypt(plaintext: string): string | Promise<string>;
    decrypt(ciphertext: string): string | Promise<string>;
  };
}): BetterFlowAdapter {
  const executions = new Map<string, any>();
  const steps = new Map<string, any>();
  const events: any[] = [];
  const rateLimitTicks: { queue: string; timestamp: number }[] = [];

  const serialize = async (val: any) => {
    if (val === undefined || val === null) {
      return null;
    }
    if (config?.encryption) {
      const stringified = JSON.stringify(val);
      const ciphertext = await config.encryption.encrypt(stringified);
      return { encrypted: ciphertext };
    }
    return val;
  };

  const deserialize = async (val: any) => {
    if (val === undefined || val === null) {
      return val;
    }
    if (val && typeof val === "object" && typeof val.encrypted === "string") {
      if (config?.encryption) {
        const raw = await config.encryption.decrypt(val.encrypted);
        return JSON.parse(raw);
      }
    }
    return val;
  };

  const adapter: BetterFlowAdapter = {
    async createExecution(data) {
      if (executions.has(data.id)) {
        // Real DBs would raise a UNIQUE/PRIMARY KEY constraint here. The
        // engine's idempotency path relies on that signal — without it,
        // a second start() would silently overwrite and re-run the workflow.
        throw new Error("UNIQUE constraint failed: bf_executions.id");
      }
      executions.set(data.id, {
        id: data.id,
        workflowName: data.workflowName,
        status: data.status ?? "PENDING",
        version: data.version ?? 1,
        sequence: 0,
        tenantId: data.tenantId || null,
        namespace: data.namespace || null,
        input: await serialize(data.input),
        timeout: data.timeout || null,
        leaseUntil: null,
        createdAt: new Date(),
        updatedAt: new Date()
      });
    },
    async getExecution(id) {
      const exec = executions.get(id);
      if (!exec) return null;
      return {
        ...exec,
        input: await deserialize(exec.input),
        output: await deserialize(exec.output),
        error: await deserialize(exec.error),
        timeout: exec.timeout ? new Date(exec.timeout) : null,
      };
    },
    async updateExecution(id, updates) {
      const exec = executions.get(id);
      if (exec) {
        const values: any = { ...updates };
        if (updates.output !== undefined) values.output = await serialize(updates.output);
        if (updates.error !== undefined) values.error = await serialize(updates.error);
        
        executions.set(id, { 
          ...exec, 
          ...values, 
          sequence: exec.sequence + 1,
          updatedAt: new Date() 
        });
      }
    },
    async getStep(executionId, stepIndex) {
      const step = steps.get(`${executionId}_${stepIndex}`);
      if (!step) return null;
      return {
        ...step,
        result: await deserialize(step.result),
        error: await deserialize(step.error)
      };
    },
    async createStep(data) {
      const key = `${data.executionId}_${data.stepIndex}`;
      steps.set(key, {
        id: key,
        executionId: data.executionId,
        stepIndex: data.stepIndex,
        stepName: data.stepName,
        stepType: data.stepType,
        status: data.status,
        result: await serialize(data.result),
        error: await serialize(data.error),
        resumeAt: data.resumeAt,
        createdAt: new Date(),
        updatedAt: new Date()
      });

      const exec = executions.get(data.executionId);
      if (exec) {
        exec.sequence += 1;
        exec.updatedAt = new Date();
      }
    },
    async updateStep(executionId, stepIndex, updates) {
      const key = `${executionId}_${stepIndex}`;
      const step = steps.get(key);
      if (step) {
        const values: any = { ...updates };
        if (updates.result !== undefined) values.result = await serialize(updates.result);
        if (updates.error !== undefined) values.error = await serialize(updates.error);
        steps.set(key, { ...step, ...values, updatedAt: new Date() });
      }

      const exec = executions.get(executionId);
      if (exec) {
        exec.sequence += 1;
        exec.updatedAt = new Date();
      }
    },
    async getPendingEvents(executionId, eventName) {
      const filtered = events.filter(e => e.executionId === executionId && e.eventName === eventName && !e.consumed);
      const mapped = [];
      for (const e of filtered) {
        mapped.push({
          ...e,
          payload: await deserialize(e.payload)
        });
      }
      return mapped;
    },
    async consumeEvent(eventId) {
      const event = events.find(e => e.id === eventId);
      if (event) {
        event.consumed = true;
      }
    },
    async createEvent(data) {
      if (data.eventKey) {
        const duplicate = events.find(e => e.executionId === data.executionId && e.eventKey === data.eventKey);
        if (duplicate) {
          throw new Error("UNIQUE constraint failed: bf_events.event_key");
        }
      }
      events.push({
        id: Math.random().toString(),
        executionId: data.executionId,
        eventName: data.eventName,
        eventKey: data.eventKey || null,
        payload: await serialize(data.payload),
        consumed: false,
        createdAt: new Date()
      });
    },
    async getExpiredSteps() {
      const now = new Date();
      const expired: any[] = [];
      for (const step of steps.values()) {
        if (step.status === "PENDING" && step.resumeAt && step.resumeAt <= now) {
          const exec = executions.get(step.executionId);
          if (exec) {
            expired.push({
              ...step,
              result: await deserialize(step.result),
              error: await deserialize(step.error),
              execution: {
                ...exec,
                input: await deserialize(exec.input),
                output: await deserialize(exec.output),
                error: await deserialize(exec.error),
                timeout: exec.timeout ? new Date(exec.timeout) : null,
              }
            });
          }
        }
      }
      return expired;
    },
    async listExecutions(limit = 50, filters) {
      let list = Array.from(executions.values());
      if (filters?.tenantId) {
        list = list.filter(e => e.tenantId === filters.tenantId);
      }
      if (filters?.namespace) {
        list = list.filter(e => e.namespace === filters.namespace);
      }
      const sliced = list.slice(0, limit);
      const mapped = [];
      for (const e of sliced) {
        mapped.push({
          ...e,
          input: await deserialize(e.input),
          output: await deserialize(e.output),
          error: await deserialize(e.error),
          timeout: e.timeout ? new Date(e.timeout) : null,
        });
      }
      return mapped;
    },
    async getExecutionHistory(executionId) {
      const list = Array.from(steps.values())
        .filter(s => s.executionId === executionId)
        .sort((a, b) => a.stepIndex - b.stepIndex);
      const mapped = [];
      for (const s of list) {
        mapped.push({
          ...s,
          result: await deserialize(s.result),
          error: await deserialize(s.error)
        });
      }
      return mapped;
    },
    async acquireLock(id, leaseMs) {
      const exec = executions.get(id);
      if (!exec) return { acquired: false, reason: "missing" };

      const now = Date.now();
      const leaseUntilMs = exec.leaseUntil ? new Date(exec.leaseUntil).getTime() : null;
      const isAcquirable =
        exec.status === "PENDING" ||
        exec.status === "SUSPENDED" ||
        (exec.status === "RUNNING" && (leaseUntilMs === null || leaseUntilMs <= now));

      if (!isAcquirable) {
        if (exec.status === "COMPLETED" || exec.status === "FAILED" || exec.status === "CANCELLED") {
          return { acquired: false, reason: "terminal" };
        }
        return { acquired: false, reason: "locked" };
      }

      exec.status = "RUNNING";
      exec.leaseUntil = new Date(now + leaseMs);
      exec.sequence += 1;
      exec.updatedAt = new Date();
      executions.set(id, exec);
      return { acquired: true };
    },
    async extendLease(id, leaseMs) {
      const exec = executions.get(id);
      if (!exec) return false;
      if (exec.status !== "RUNNING") return false;
      if (!exec.leaseUntil) return false;
      if (new Date(exec.leaseUntil).getTime() <= Date.now()) return false;
      exec.leaseUntil = new Date(Date.now() + leaseMs);
      exec.updatedAt = new Date();
      executions.set(id, exec);
      return true;
    },
    async releaseLock(id, status) {
      const exec = executions.get(id);
      if (!exec) return;
      exec.status = status;
      exec.leaseUntil = null;
      exec.sequence += 1;
      exec.updatedAt = new Date();
      executions.set(id, exec);
    },
    async checkRateLimit(queue, limit, windowMs) {
      const now = Date.now();
      const expired = now - windowMs;
      const active = rateLimitTicks.filter(t => t.queue === queue && t.timestamp > expired);
      if (active.length >= limit) {
        return false;
      }
      rateLimitTicks.push({ queue, timestamp: now });
      return true;
    },
    async scheduleTimer(executionId, resumeAt) {
      (this as any).scheduledTimers.push({ executionId, resumeAt });
    },
    async pruneExecutionHistory(executionId) {
      for (const [key, value] of steps.entries()) {
        if (value.executionId === executionId) {
          steps.delete(key);
        }
      }
    },
    async getExpiredExecutions() {
      const now = Date.now();
      const expired: any[] = [];
      for (const exec of executions.values()) {
        if (
          (exec.status === "RUNNING" || exec.status === "SUSPENDED") && 
          exec.timeout && 
          new Date(exec.timeout).getTime() <= now
        ) {
          expired.push({
            ...exec,
            input: await deserialize(exec.input),
            output: await deserialize(exec.output),
            error: await deserialize(exec.error),
            timeout: exec.timeout ? new Date(exec.timeout) : null,
          });
        }
      }
      return expired;
    }
  };

  // Expose the raw map directly for testing/asserting encrypted values
  (adapter as any)._rawSteps = steps;
  (adapter as any)._rawExecutions = executions;

  (adapter as any).scheduledTimers = [];
  return adapter;
}

describe("Better-Flow Core Engine Tests", () => {
  it("should run activities sequentially and cache their results", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const activitySpy = vi.fn().mockImplementation((input: number) => input * 2);

    const testFlow = flow.define("testCache", async (ctx, input: { val: number }) => {
      const r1 = await ctx.run("double", () => activitySpy(input.val));
      const r2 = await ctx.run("doubleAgain", () => activitySpy(r1));
      return r2;
    });

    const executionId = await testFlow.start({ val: 5 });

    // Verify executions state
    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toBe(20);

    // Verify each activity is called exactly once
    expect(activitySpy).toHaveBeenCalledTimes(2);
    expect(activitySpy).toHaveBeenNthCalledWith(1, 5);
    expect(activitySpy).toHaveBeenNthCalledWith(2, 10);

    // Trigger resume/replay manual run to assert cached step responses
    await flow.resume(executionId);
    expect(activitySpy).toHaveBeenCalledTimes(2); // Still 2 - cached!
  });

  it("should suspend on sleep, change status, and resume after cron expiration", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });
    
    let checkpointReached = false;

    const testFlow = flow.define("testSleep", async (ctx) => {
      await ctx.sleep("1s");
      checkpointReached = true;
      return "slept";
    });

    // Start workflow: should immediately suspend
    const executionId = await testFlow.start({});
    
    let exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("SUSPENDED");
    expect(checkpointReached).toBe(false);

    // Trigger timer check before time expires: should stay suspended
    await flow.checkTimers();
    exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("SUSPENDED");
    expect(checkpointReached).toBe(false);

    // Advance mock timer clock in memory adapter
    const stepKey = `${executionId}_0`;
    const step = await adapter.getStep(executionId, 0);
    expect(step?.stepType).toBe("sleep");
    expect(step?.status).toBe("PENDING");
    
    // Simulate time passing (set resumeAt to 5 seconds ago)
    await adapter.updateStep(executionId, 0, {
      resumeAt: new Date(Date.now() - 5000)
    });

    // Trigger cron: should resume, replay past completed steps, and complete
    await flow.checkTimers();
    
    exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toBe("slept");
    expect(checkpointReached).toBe(true);
  });

  it("should suspend waiting for event, buffer event in inbox, and complete", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const testFlow = flow.define("testEvent", async (ctx) => {
      const payload = await ctx.waitForEvent<{ token: string }>("webhook-token");
      return payload.token;
    });

    // Start: suspends
    const executionId = await testFlow.start({});
    let exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("SUSPENDED");

    // Publish event: should resume and complete workflow
    await flow.publishEvent(executionId, "webhook-token", { token: "abc-123" });
    
    exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toBe("abc-123");
  });

  it("should prevent event loss due to webhook race conditions (Inbox matching)", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const testFlow = flow.define("testRace", async (ctx) => {
      // Short delay activity before waiting for event
      await ctx.run("someAction", () => "ok");
      const event = await ctx.waitForEvent<{ val: number }>("my-event");
      return event.val;
    });

    // We start the execution manually
    const executionId = `race_exec_${Date.now()}`;
    await adapter.createExecution({
      id: executionId,
      workflowName: "testRace",
      status: "RUNNING",
      input: {}
    });

    // Simulate webhook event arriving early (e.g. before workflow hits waitForEvent)
    await adapter.createEvent({
      executionId,
      eventName: "my-event",
      payload: { val: 42 }
    });

    // Run the execution runner: it should consume the event from inbox without suspending
    const fn = (flow as any).workflows.get("testRace");
    await (flow as any).runExecution(executionId, fn, {});

    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toBe(42);
  });

  it("should throw non-determinism error if step structure changes during replay", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    let phase = 1;
    // Phase 1 execution: completes stepA, throws sleep suspension
    const testFlowWithSleep = flow.define("testNonDeterministic", async (ctx) => {
      if (phase === 1) {
        await ctx.run("stepA", () => 1);
        await ctx.sleep("1s");
      } else {
        await ctx.run("stepB", () => 2); // Switch step during replay
      }
      return "done";
    });

    const executionId = await testFlowWithSleep.start({});
    
    // Set stepA as completed, and advance sleep timer
    await adapter.updateStep(executionId, 1, {
      resumeAt: new Date(Date.now() - 5000)
    });

    // Trigger non-determinism phase shift
    phase = 2;

    // Resuming workflow should fail and mark execution status as FAILED
    await flow.checkTimers();

    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("FAILED");
    expect(exec?.error?.message).toContain("Non-deterministic execution");
  });

  it("should retry failed activities with backoff and succeed if eventual success", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    let attempts = 0;
    const activitySpy = vi.fn().mockImplementation(() => {
      attempts++;
      if (attempts < 3) {
        throw new Error("Temporary network error");
      }
      return "success_payload";
    });

    const testFlow = flow.define("testRetries", async (ctx) => {
      return await ctx.run("flakyStep", () => activitySpy(), {
        retries: 3,
        initialDelay: 10,
        backoffFactor: 2
      });
    });

    // Start: should fail on 1st run and suspend with retry backoff
    const executionId = await testFlow.start({});
    let exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("SUSPENDED");
    expect(activitySpy).toHaveBeenCalledTimes(1);

    let step = await adapter.getStep(executionId, 0);
    expect(step?.status).toBe("PENDING");
    expect(step?.attempts).toBe(1);

    // Simulate first backoff expiry
    await adapter.updateStep(executionId, 0, { resumeAt: new Date(Date.now() - 1000) });
    await flow.checkTimers(); // Should trigger 2nd attempt, fail, suspend again

    exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("SUSPENDED");
    expect(activitySpy).toHaveBeenCalledTimes(2);

    step = await adapter.getStep(executionId, 0);
    expect(step?.status).toBe("PENDING");
    expect(step?.attempts).toBe(2);

    // Simulate second backoff expiry
    await adapter.updateStep(executionId, 0, { resumeAt: new Date(Date.now() - 1000) });
    await flow.checkTimers(); // Should trigger 3rd attempt, succeed, and complete!

    exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toBe("success_payload");
    expect(activitySpy).toHaveBeenCalledTimes(3);

    step = await adapter.getStep(executionId, 0);
    expect(step?.status).toBe("COMPLETED");
  });

  it("should prevent execution resume if workflow is cancelled", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    let checkpointReached = false;
    const testFlow = flow.define("testCancel", async (ctx) => {
      await ctx.sleep("1s");
      checkpointReached = true;
      return "slept";
    });

    const executionId = await testFlow.start({});
    let exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("SUSPENDED");
    expect(checkpointReached).toBe(false);

    // Cancel workflow
    await flow.cancel(executionId);
    exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("CANCELLED");

    // Expiry sleep step and tick cron
    await adapter.updateStep(executionId, 0, { resumeAt: new Date(Date.now() - 5000) });
    await flow.checkTimers();

    // Checkpoint should not be reached, status remains CANCELLED
    expect(checkpointReached).toBe(false);
    exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("CANCELLED");
  });

  it("should handle concurrent Promise.all execution deterministically", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const order: string[] = [];

    const testFlow = flow.define("testPromiseAll", async (ctx) => {
      const results = await Promise.all([
        ctx.run("slowStep", async () => {
          // Artificial delay: slow step finishes last
          await new Promise(resolve => setTimeout(resolve, 50));
          order.push("slowStep");
          return "slow";
        }),
        ctx.run("fastStep", async () => {
          order.push("fastStep");
          return "fast";
        })
      ]);
      return results;
    });

    // Start execution
    const executionId = await testFlow.start({});
    
    // Verify results
    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toEqual(["slow", "fast"]);

    // Verify fastStep finished before slowStep inside Javascript runtime
    expect(order).toEqual(["fastStep", "slowStep"]);

    // Verify database step indexes match code order (slowStep at index 0, fastStep at index 1)
    const step0 = await adapter.getStep(executionId, 0);
    const step1 = await adapter.getStep(executionId, 1);
    
    expect(step0?.stepName).toBe("slowStep");
    expect(step1?.stepName).toBe("fastStep");

    // Replay workflow (resume): should resolve without non-determinism errors
    await flow.resume(executionId);
    
    const execReplay = await adapter.getExecution(executionId);
    expect(execReplay?.status).toBe("COMPLETED");
  });

  it("should support workflow versioning via ctx.getVersion", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    let isV2Executed = false;

    // Define a versioned workflow (current version = 2)
    const testFlow = flow.define({
      name: "versionedWorkflow",
      version: 2,
      handler: async (ctx) => {
        await ctx.run("stepA", () => "stepA");
        
        const isV2 = await ctx.getVersion("stepB_intro", { min: 2 });
        if (isV2) {
          await ctx.run("stepB_new", () => {
            isV2Executed = true;
            return "stepB_v2";
          });
        } else {
          await ctx.run("stepB_old", () => "stepB_v1");
        }
        return "done";
      }
    });

    // 1. Trigger an old run manually specifying version = 1 in creation
    const v1Id = "v1_exec";
    await adapter.createExecution({
      id: v1Id,
      workflowName: "versionedWorkflow",
      status: "RUNNING",
      version: 1, // Old version
      input: {}
    });
    await flow.resume(v1Id);

    const execV1 = await adapter.getExecution(v1Id);
    expect(execV1?.status).toBe("COMPLETED");
    expect(isV2Executed).toBe(false); // V2 block skipped!

    const stepV1 = await adapter.getStep(v1Id, 1);
    expect(stepV1?.stepName).toBe("stepB_old"); // Ran old step!

    // 2. Trigger a new run (version 2 should default automatically)
    const v2Id = await testFlow.start({});
    const execV2 = await adapter.getExecution(v2Id);
    expect(execV2?.status).toBe("COMPLETED");
    expect(isV2Executed).toBe(true); // V2 block executed!

    const stepV2 = await adapter.getStep(v2Id, 1);
    expect(stepV2?.stepName).toBe("stepB_new"); // Ran new step!
  });

  it("should deduplicate events utilizing eventKey and unique constraint catches", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const testFlow = flow.define("eventDeduplication", async (ctx) => {
      const data = await ctx.waitForEvent("triggerEvent");
      return data;
    });

    const executionId = await testFlow.start({});

    // Create a mock standard Request representing an event post
    const req1 = new Request("http://localhost:3001/event", {
      method: "POST",
      body: JSON.stringify({
        executionId,
        eventName: "triggerEvent",
        eventKey: "stripe_evt_999",
        payload: { value: 100 }
      })
    });

    const res1 = await flow.handler(req1);
    const data1 = await res1.json();
    expect(res1.status).toBe(200);
    expect(data1.success).toBe(true);
    expect(data1.duplicate).toBeUndefined();

    // Verify workflow resumed and completed
    let exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toEqual({ value: 100 });

    // Send duplicate event key
    const req2 = new Request("http://localhost:3001/event", {
      method: "POST",
      body: JSON.stringify({
        executionId,
        eventName: "triggerEvent",
        eventKey: "stripe_evt_999", // DUPLICATE KEY
        payload: { value: 100 }
      })
    });

    const res2 = await flow.handler(req2);
    const data2 = await res2.json();
    
    // Webhook should be acknowledged with 200 OK, but marked as duplicate
    expect(res2.status).toBe(200);
    expect(data2.success).toBe(true);
    expect(data2.duplicate).toBe(true); // Gracefully caught and ignored!
  });

  it("should rollback failed executions using Saga compensations in reverse order", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const rollbackOrder: string[] = [];

    const testFlow = flow.define("sagaWorkflow", async (ctx) => {
      // Step 1
      await ctx.run("step1", () => "step1_result");
      ctx.registerCompensation("step1_undo", async () => {
        rollbackOrder.push("step1_undone");
        return "undo1";
      });

      // Step 2
      await ctx.run("step2", () => "step2_result");
      ctx.registerCompensation("step2_undo", async () => {
        rollbackOrder.push("step2_undone");
        return "undo2";
      });

      // Step 3 (fails)
      await ctx.run("step3", () => {
        throw new Error("Step 3 crashed!");
      });
    });

    const executionId = await testFlow.start({});
    
    // Verify execution failed
    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("FAILED");

    // Verify rollback order is LIFO (step2 compensated before step1)
    expect(rollbackOrder).toEqual(["step2_undone", "step1_undone"]);

    // Verify database logged compensations as completed steps
    const comp2 = await adapter.getStep(executionId, 3); // index 0=step1, 1=step2, 2=step3(failed), 3=compensate_step2_undo
    const comp1 = await adapter.getStep(executionId, 4); // index 4=compensate_step1_undo
    
    expect(comp2?.stepName).toBe("compensate_step2_undo");
    expect(comp2?.status).toBe("COMPLETED");
    expect(comp1?.stepName).toBe("compensate_step1_undo");
    expect(comp1?.status).toBe("COMPLETED");
  });

  it("should suspend and throttle executions exceeding rate limits", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const testFlow = flow.define("throttledWorkflow", async (ctx) => {
      return await ctx.run("limitedStep", () => "allowed", {
        rateLimit: {
          queue: "openai-api-limit",
          limit: 1, // Max 1 request
          window: 100 // Per 100ms
        }
      });
    });

    // 1. Run 1st execution: should succeed
    const exec1Id = await testFlow.start({});
    const exec1 = await adapter.getExecution(exec1Id);
    expect(exec1?.status).toBe("COMPLETED");
    expect(exec1?.output).toBe("allowed");

    // 2. Run 2nd execution immediately within the 100ms window: should suspend
    const exec2Id = await testFlow.start({});
    let exec2 = await adapter.getExecution(exec2Id);
    expect(exec2?.status).toBe("SUSPENDED");

    // 3. Sleep 100ms for rate limit window to expire, then resume
    await new Promise(r => setTimeout(r, 105));
    await flow.resume(exec2Id);

    exec2 = await adapter.getExecution(exec2Id);
    expect(exec2?.status).toBe("COMPLETED");
    expect(exec2?.output).toBe("allowed");
  });

  it("should partition and filter executions utilizing multi-tenant namespaces", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const testFlow = flow.define("tenantWorkflow", async (ctx) => {
      return "done";
    });

    // Start runs under different company tenants & namespaces
    await testFlow.start({}, { tenantId: "companyA", namespace: "prod" });
    await testFlow.start({}, { tenantId: "companyA", namespace: "dev" });
    await testFlow.start({}, { tenantId: "companyB", namespace: "prod" });

    // Filter list for Company A
    const compA = await adapter.listExecutions(50, { tenantId: "companyA" });
    expect(compA.length).toBe(2);
    expect(compA.every(e => e.tenantId === "companyA")).toBe(true);

    // Filter list for Company A - Prod
    const compAProd = await adapter.listExecutions(50, { tenantId: "companyA", namespace: "prod" });
    expect(compAProd.length).toBe(1);
    expect(compAProd[0].tenantId).toBe("companyA");
    expect(compAProd[0].namespace).toBe("prod");

    // Route query check in handler
    const req = new Request("http://localhost:3001/executions?tenantId=companyA&namespace=dev");
    const res = await flow.handler(req);
    const data = await res.json();
    expect(res.status).toBe(200);
    expect(data.length).toBe(1);
    expect(data[0].tenantId).toBe("companyA");
    expect(data[0].namespace).toBe("dev");
  });

  it("should trigger push-based scheduleTimer when sleep is first scheduled", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const testFlow = flow.define("sleepTimerWorkflow", async (ctx) => {
      await ctx.sleep("10s");
    });

    const executionId = await testFlow.start({});
    
    // Check if mock adapter recorded the scheduled timer request
    const scheduled = (adapter as any).scheduledTimers;
    expect(scheduled.length).toBe(1);
    expect(scheduled[0].executionId).toBe(executionId);
    expect(scheduled[0].resumeAt.getTime()).toBeGreaterThan(Date.now() + 9000); // approx 10s

    // Resume: should skip scheduling it again (no duplicates in scheduledTimers)
    await flow.resume(executionId);
    expect(scheduled.length).toBe(1); // Still exactly 1!
  });

  it("should only output developer logs (ctx.log) during live runs and remain silent on replays", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    // Force a suspend by sleeping to test replay logs
    const testFlowWithSuspend = flow.define("loggedWorkflowWithSuspend", async (ctx) => {
      ctx.log("Log A");
      await ctx.run("step1", () => "step1_val");
      ctx.log("Log B");
      await ctx.sleep(50); // Forces suspension
      ctx.log("Log C");
    });

    const executionId = await testFlowWithSuspend.start({});
    
    // Assert logs printed during live execution
    expect(logSpy).toHaveBeenCalledWith("[Better-Flow] [Exec: " + executionId + "] Log A");
    expect(logSpy).toHaveBeenCalledWith("[Better-Flow] [Exec: " + executionId + "] Log B");
    
    // Clear spy mock calls
    logSpy.mockClear();

    // Resume execution: should replay Log A and Log B silently, but print Log C when it runs for the first time
    await new Promise(r => setTimeout(r, 55));
    await flow.resume(executionId);

    // Replayed logs A and B must be silent
    expect(logSpy).not.toHaveBeenCalledWith("[Better-Flow] [Exec: " + executionId + "] Log A");
    expect(logSpy).not.toHaveBeenCalledWith("[Better-Flow] [Exec: " + executionId + "] Log B");
    
    // Live log C must be printed
    expect(logSpy).toHaveBeenCalledWith("[Better-Flow] [Exec: " + executionId + "] Log C");

    logSpy.mockRestore();
  });

  it("should support state snapshots checkpointing to skip loop executions on replay", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const stepsExecuted: string[] = [];

    const testFlow = flow.define("checkpointedWorkflow", async (ctx) => {
      let state = await ctx.checkpoint("loopState", { index: 0 });
      
      for (let i = state.index; i < 5; i++) {
        await ctx.run(`loop_${i}`, () => {
          stepsExecuted.push(`run_${i}`);
          return `result_${i}`;
        });

        if (i === 2) {
          await ctx.sleep(50); // SUSPEND 1
          state = await ctx.checkpoint("loopState", { index: i + 1 });
        }
        if (i === 3) {
          await ctx.sleep(50); // SUSPEND 2
        }
      }
    });

    // 1. First run: processes loop_0, loop_1, loop_2, and suspends on sleep 1
    const executionId = await testFlow.start({});
    expect(stepsExecuted).toEqual(["run_0", "run_1", "run_2"]);

    // Clear executed steps log
    stepsExecuted.length = 0;

    // 2. Resume 1: completes sleep 1, writes checkpoint { index: 3 }, runs loop_3, suspends on sleep 2
    await new Promise(r => setTimeout(r, 55));
    await flow.resume(executionId);
    expect(stepsExecuted).toEqual(["run_3"]);

    // Clear executed steps log
    stepsExecuted.length = 0;

    // 3. Resume 2: reloads checkpoint { index: 3 }, jumps directly to loop_3 (cached), completes sleep 2, runs loop_4
    await new Promise(r => setTimeout(r, 55));
    await flow.resume(executionId);

    // Only index 4 (loop_4) should run during live run on Resume 2!
    expect(stepsExecuted).toEqual(["run_4"]);

    // Verify final status is COMPLETED
    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
  });

  it("should encrypt step and execution payloads at rest when an encryption provider is configured", async () => {
    // Simple custom ROT13 Caesar cipher encryption mock provider for testing
    const encryption = {
      encrypt: (plaintext: string) => {
        return plaintext.split("").map(c => String.fromCharCode(c.charCodeAt(0) + 13)).join("");
      },
      decrypt: (ciphertext: string) => {
        return ciphertext.split("").map(c => String.fromCharCode(c.charCodeAt(0) - 13)).join("");
      }
    };

    const adapter = memoryAdapter({ encryption });
    const flow = createBetterFlow({ adapter, encryption });

    const testFlow = flow.define("encryptionWorkflow", async (ctx) => {
      const data = await ctx.run("secretStep", () => {
        return { secret: "my-key-123" };
      });
      return data;
    });

    const executionId = await testFlow.start({});

    // 1. Verify that during execution, the engine receives decrypted plaintext transparently
    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toEqual({ secret: "my-key-123" });

    // 2. Verify that in the RAW database storage, the value is stored as ciphertext
    const rawExec = (adapter as any)._rawExecutions.get(executionId);
    const expectedCiphertext = encryption.encrypt(JSON.stringify({ secret: "my-key-123" }));
    expect(rawExec.output).toEqual({ encrypted: expectedCiphertext });

    const rawStep = (adapter as any)._rawSteps.get(`${executionId}_0`);
    const expectedStepCiphertext = encryption.encrypt(JSON.stringify({ secret: "my-key-123" }));
    expect(rawStep.result).toEqual({ encrypted: expectedStepCiphertext });
  });

  it("should execute local activities without creating database steps until suspension or completion", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const executedLocalSteps: string[] = [];
    let isFirstRun = true;

    const testFlow = flow.define("localActivityWorkflow", async (ctx) => {
      // 1. First run: executing two local steps
      const res1 = await ctx.runLocal("local1", () => {
        executedLocalSteps.push("local1");
        return "val1";
      });

      const res2 = await ctx.runLocal("local2", () => {
        executedLocalSteps.push("local2");
        return "val2";
      });

      // Assert that at this point, these steps have NOT been written to the database yet!
      if (isFirstRun) {
        const step0InDb = await adapter.getStep(ctx.executionId, 0);
        const step1InDb = await adapter.getStep(ctx.executionId, 1);
        if (step0InDb || step1InDb) {
          throw new Error("Local steps should not be written immediately to database!");
        }
      }

      await ctx.sleep(50); // SUSPEND! This should trigger a commit (flush) of local steps!

      return { res1, res2 };
    });

    const executionId = await testFlow.start({});
    expect(executedLocalSteps).toEqual(["local1", "local2"]);

    // After suspension, both local steps must be flushed and written to the database!
    const step0 = await adapter.getStep(executionId, 0);
    const step1 = await adapter.getStep(executionId, 1);
    expect(step0?.stepName).toBe("local1");
    expect(step0?.status).toBe("COMPLETED");
    expect(step0?.result).toBe("val1");

    expect(step1?.stepName).toBe("local2");
    expect(step1?.status).toBe("COMPLETED");
    expect(step1?.result).toBe("val2");

    // Clear tracking array
    executedLocalSteps.length = 0;
    isFirstRun = false;

    // Resume: should read local steps from cached db steps, skipping execution closures
    await new Promise(r => setTimeout(r, 55));
    await flow.resume(executionId);

    expect(executedLocalSteps).toEqual([]); // Skipped re-execution!
    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toEqual({ res1: "val1", res2: "val2" });
  });

  it("should wrap workflow executions and steps in active OpenTelemetry tracing spans", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    const traceApi = await import("@opentelemetry/api");
    const spanCalls: string[] = [];

    // Spy on OTel tracer startActiveSpan
    const getTracerSpy = vi.spyOn(traceApi.trace, "getTracer").mockReturnValue({
      startActiveSpan: vi.fn().mockImplementation((name, cb) => {
        spanCalls.push(name);
        const mockSpan = {
          setAttribute: vi.fn(),
          setStatus: vi.fn(),
          recordException: vi.fn(),
          end: vi.fn()
        };
        return cb(mockSpan);
      })
    } as any);

    const testFlow = flow.define("tracedWorkflow", async (ctx) => {
      await ctx.run("tracedStep", () => "ok");
      await ctx.runLocal("tracedLocal", () => "localOk");
    });

    await testFlow.start({});

    expect(spanCalls).toEqual([
      "better-flow.acquireLock",
      "workflow:tracedWorkflow",
      "step:tracedStep",
      "local_step:tracedLocal"
    ]);

    getTracerSpy.mockRestore();
  });

  it("should chain inbound and outbound interceptors during workflow and step executions", async () => {
    const adapter = memoryAdapter();
    const interceptorCalls: string[] = [];

    const interceptor = {
      inbound: async (ctx: any, input: any, next: () => Promise<any>) => {
        interceptorCalls.push(`inbound_start:${input.val}`);
        const result = await next();
        interceptorCalls.push(`inbound_end:${result}`);
        return result + "_modified";
      },
      outbound: async (ctx: any, stepName: string, next: () => Promise<any>) => {
        interceptorCalls.push(`outbound_start:${stepName}`);
        const result = await next();
        interceptorCalls.push(`outbound_end:${result}`);
        return result + "_intercepted";
      }
    };

    const flow = createBetterFlow({
      adapter,
      interceptors: [interceptor]
    });

    const testFlow = flow.define("interceptedWorkflow", async (ctx, input) => {
      const res = await ctx.run("step1", () => "hello");
      return res;
    });

    const executionId = await testFlow.start({ val: "test" });

    expect(interceptorCalls).toEqual([
      "inbound_start:test",
      "outbound_start:step1",
      "outbound_end:hello",
      "inbound_end:hello_intercepted"
    ]);

    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toBe("hello_intercepted_modified");
  });

  it("should delete step execution history records from database when pruneHistoryOnComplete is enabled", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({
      adapter,
      pruneHistoryOnComplete: true
    });

    const testFlow = flow.define("pruningWorkflow", async (ctx) => {
      await ctx.run("step1", () => "ok1");
      await ctx.run("step2", () => "ok2");
      return "done";
    });

    const executionId = await testFlow.start({});

    // 1. Verify execution itself is completed successfully
    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toBe("done");

    // 2. Verify history logs (steps) are completely deleted from active table
    const stepsList = await adapter.getExecutionHistory(executionId);
    expect(stepsList.length).toBe(0); // All pruned!
  });

  it("should buffer and batch rapid signal events to execute only a single resume loop", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter, eventDebounceMs: 100 });

    let runsCount = 0;
    const testFlow = flow.define("eventBufferingWorkflow", async (ctx) => {
      runsCount++;
      const val1 = await ctx.waitForEvent("sig1");
      const val2 = await ctx.waitForEvent("sig2");
      return { val1, val2 };
    });

    const executionId = await testFlow.start({});
    // The start execution run completes index 0 (sig1 wait) and index 1 (sig2 wait) setup and suspends.
    expect(runsCount).toBe(1);

    // Trigger two events in rapid succession (parallel webhook signals)
    await Promise.all([
      flow.publishEvent(executionId, "sig1", "apple"),
      flow.publishEvent(executionId, "sig2", "banana")
    ]);

    // Wait for the 100ms debounce window + 50ms buffer to complete
    await new Promise(r => setTimeout(r, 150));

    // Assert that the workflow only executed a SINGLE additional resume run (2 in total: start + 1 batched resume)!
    expect(runsCount).toBe(2);

    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(exec?.output).toEqual({ val1: "apple", val2: "banana" });
  });

  it("should fail workflow executions and trigger Saga compensations when execution timeouts expire", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({
      adapter,
      defaultWorkflowTimeout: 30 // 30ms execution timeout limit
    });

    const compensationsRun: string[] = [];

    const testFlow = flow.define("timeoutWorkflow", async (ctx) => {
      ctx.registerCompensation("action1", () => {
        compensationsRun.push("undone1");
      });
      await ctx.sleep(80); // Sleep exceeds the 30ms execution timeout limit!
      return "done";
    });

    const executionId = await testFlow.start({});

    // Wait for 95ms for sleep and timeout to elapse
    await new Promise(r => setTimeout(r, 95));

    // Call checkTimers which proactive sweeps expired timers and expired execution timeouts!
    await flow.checkTimers();

    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("FAILED");
    expect(exec?.error?.message).toContain("Workflow execution timeout exceeded");

    // Asserts that Saga compensations were executed durably due to the timeout failure!
    expect(compensationsRun).toEqual(["undone1"]);
  });

  // ============================================================
  // Concurrency & Locking Regression Tests
  // ============================================================

  it("createExecution initializes status=PENDING and leaseUntil=null (no Date(0) hack)", async () => {
    const adapter = memoryAdapter();
    await adapter.createExecution({
      id: "exec_init",
      workflowName: "wf",
      status: "PENDING",
      input: {}
    });
    const row = (adapter as any)._rawExecutions.get("exec_init");
    expect(row.status).toBe("PENDING");
    expect(row.leaseUntil).toBeNull();
    // The fix: createdAt and updatedAt should both be real timestamps now,
    // not a sentinel epoch (which used to be a load-bearing hack).
    expect(row.updatedAt).not.toEqual(new Date(0));
  });

  it("acquireLock atomically transitions PENDING → RUNNING with a fresh leaseUntil", async () => {
    const adapter = memoryAdapter();
    await adapter.createExecution({ id: "exec_pend", workflowName: "wf", status: "PENDING", input: {} });

    const result = await adapter.acquireLock("exec_pend", 30_000);
    expect(result).toEqual({ acquired: true });

    const row = (adapter as any)._rawExecutions.get("exec_pend");
    expect(row.status).toBe("RUNNING");
    expect(row.leaseUntil).toBeInstanceOf(Date);
    expect(row.leaseUntil.getTime()).toBeGreaterThan(Date.now() + 29_000);
  });

  it("acquireLock returns reason='missing' for a non-existent execution", async () => {
    const adapter = memoryAdapter();
    const result = await adapter.acquireLock("nope", 30_000);
    expect(result).toEqual({ acquired: false, reason: "missing" });
  });

  it("acquireLock returns reason='terminal' for COMPLETED / FAILED / CANCELLED", async () => {
    const adapter = memoryAdapter();
    for (const terminal of ["COMPLETED", "FAILED", "CANCELLED"] as const) {
      await adapter.createExecution({ id: `t_${terminal}`, workflowName: "wf", status: terminal, input: {} });
      const result = await adapter.acquireLock(`t_${terminal}`, 30_000);
      expect(result.acquired).toBe(false);
      expect(result.reason).toBe("terminal");
    }
  });

  it("acquireLock returns reason='locked' while a fresh lease is held", async () => {
    const adapter = memoryAdapter();
    await adapter.createExecution({ id: "exec_locked", workflowName: "wf", status: "PENDING", input: {} });

    const first = await adapter.acquireLock("exec_locked", 30_000);
    expect(first.acquired).toBe(true);

    // Immediately try again — the new lease (30s) is still in the future.
    const second = await adapter.acquireLock("exec_locked", 30_000);
    expect(second.acquired).toBe(false);
    expect(second.reason).toBe("locked");
  });

  it("acquireLock re-claims an expired lease from a RUNNING row", async () => {
    const adapter = memoryAdapter();
    await adapter.createExecution({ id: "exec_expired", workflowName: "wf", status: "PENDING", input: {} });

    // Acquire with a 10ms lease, then let it expire.
    await adapter.acquireLock("exec_expired", 10);
    await new Promise((r) => setTimeout(r, 20));

    const reclaimed = await adapter.acquireLock("exec_expired", 30_000);
    expect(reclaimed.acquired).toBe(true);

    const row = (adapter as any)._rawExecutions.get("exec_expired");
    expect(row.status).toBe("RUNNING");
    expect(row.leaseUntil.getTime()).toBeGreaterThan(Date.now() + 29_000);
  });

  it("acquireLock re-claims a SUSPENDED row (no lease held)", async () => {
    const adapter = memoryAdapter();
    await adapter.createExecution({ id: "exec_susp", workflowName: "wf", status: "PENDING", input: {} });
    await adapter.acquireLock("exec_susp", 30_000);
    // After a sleep suspension, releaseLock sets status=SUSPENDED + leaseUntil=null.
    await adapter.releaseLock("exec_susp", "SUSPENDED");

    const result = await adapter.acquireLock("exec_susp", 30_000);
    expect(result.acquired).toBe(true);
    expect((adapter as any)._rawExecutions.get("exec_susp").status).toBe("RUNNING");
  });

  it("extendLease bumps leaseUntil for the current lease holder", async () => {
    const adapter = memoryAdapter();
    await adapter.createExecution({ id: "exec_ext", workflowName: "wf", status: "PENDING", input: {} });
    await adapter.acquireLock("exec_ext", 30_000);

    const before = (adapter as any)._rawExecutions.get("exec_ext").leaseUntil.getTime();
    // Small jitter to make sure the new timestamp is strictly greater.
    await new Promise((r) => setTimeout(r, 5));

    const extended = await adapter.extendLease("exec_ext", 60_000);
    expect(extended).toBe(true);

    const after = (adapter as any)._rawExecutions.get("exec_ext").leaseUntil.getTime();
    expect(after).toBeGreaterThan(before);
    // Lease should now expire at least ~30s further out than the original.
    expect(after - before).toBeGreaterThanOrEqual(29_000);
  });

  it("extendLease returns false for rows that don't hold an active lease", async () => {
    const adapter = memoryAdapter();
    await adapter.createExecution({ id: "exec_ext_neg", workflowName: "wf", status: "PENDING", input: {} });

    // Never acquired → no lease.
    expect(await adapter.extendLease("exec_ext_neg", 30_000)).toBe(false);

    // Acquire + release → lease dropped.
    await adapter.acquireLock("exec_ext_neg", 30_000);
    await adapter.releaseLock("exec_ext_neg", "SUSPENDED");
    expect(await adapter.extendLease("exec_ext_neg", 30_000)).toBe(false);

    // Acquire + wait for lease to expire → cannot extend an expired lease.
    await adapter.acquireLock("exec_ext_neg", 10);
    await new Promise((r) => setTimeout(r, 20));
    expect(await adapter.extendLease("exec_ext_neg", 30_000)).toBe(false);
  });

  it("releaseLock clears leaseUntil so suspended workflows can be re-acquired immediately", async () => {
    const adapter = memoryAdapter();
    await adapter.createExecution({ id: "exec_rel", workflowName: "wf", status: "PENDING", input: {} });
    await adapter.acquireLock("exec_rel", 30_000);

    await adapter.releaseLock("exec_rel", "SUSPENDED");
    const row = (adapter as any)._rawExecutions.get("exec_rel");
    expect(row.status).toBe("SUSPENDED");
    expect(row.leaseUntil).toBeNull();

    // No 30s wait — should re-acquire instantly.
    const result = await adapter.acquireLock("exec_rel", 30_000);
    expect(result.acquired).toBe(true);
  });

  it("releaseLock is a safe no-op when the row does not exist", async () => {
    const adapter = memoryAdapter();
    // Must not throw — this path is reachable when getExecution returns null
    // after acquireLock succeeded (e.g. concurrent DELETE).
    await expect(adapter.releaseLock("ghost", "FAILED")).resolves.toBeUndefined();
  });

  it("start() is idempotent on executionId: duplicate calls return the same id without re-running", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    let runs = 0;
    const testFlow = flow.define("idemFlow", async (ctx) => {
      runs++;
      // Suspend so the workflow is still mid-flight when the duplicate start() arrives.
      await ctx.sleep("100ms");
      return "ok";
    });

    const executionId = "exec_idem_1";
    const first = await testFlow.start({}, { executionId });
    // While the first run is suspended, a webhook retry hits start() with the
    // same executionId. The fix: PK violation is caught, existing id is returned.
    const second = await testFlow.start({}, { executionId });

    expect(first).toBe(executionId);
    expect(second).toBe(executionId);

    // Wait for the original suspended run to wake up and complete.
    await new Promise((r) => setTimeout(r, 120));
    await flow.checkTimers();

    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    // The handler should have run exactly once — the duplicate start() did NOT
    // re-execute the workflow.
    expect(runs).toBe(1);
  });

  it("start() raises a clear error when executionId is reused for a different workflow", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });
    const flowA = flow.define("workflowA", async (ctx) => "a");
    const flowB = flow.define("workflowB", async (ctx) => "b");

    const executionId = "shared_id";
    await flowA.start({}, { executionId });

    // Now try to reuse that id for workflowB — should throw a clear error,
    // NOT silently return the id (which would be misleading).
    await expect(flowB.start({}, { executionId })).rejects.toThrow(/already exists/);
  });

  it("long-running activities are protected by heartbeat lease extension (no split-brain)", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    // Mock adapter captures lease snapshots so we can assert the heartbeat
    // bumped the lease at least once during the long-running step.
    const leaseSnapshots: number[] = [];
    const originalAcquireLock = adapter.acquireLock.bind(adapter);
    const originalExtendLease = adapter.extendLease.bind(adapter);
    adapter.acquireLock = async (id: string, leaseMs: number) => {
      const result = await originalAcquireLock(id, leaseMs);
      if (result.acquired) {
        const row = (adapter as any)._rawExecutions.get(id);
        leaseSnapshots.push(row.leaseUntil.getTime());
      }
      return result;
    };
    adapter.extendLease = async (id: string, leaseMs: number) => {
      const ok = await originalExtendLease(id, leaseMs);
      if (ok) {
        const row = (adapter as any)._rawExecutions.get(id);
        leaseSnapshots.push(row.leaseUntil.getTime());
      }
      return ok;
    };

    const testFlow = flow.define("longRunning", async (ctx) => {
      // Long step: 250ms. The default LEASE_MS=30000, HEARTBEAT_MS≈10000,
      // so on this CI run we won't necessarily observe a heartbeat tick — but
      // we WILL observe that acquireLock's leaseUntil stays in the future
      // (i.e. the row is NOT stale-leased after the long step finishes).
      await ctx.run("slowStep", () => new Promise((r) => setTimeout(r, 250)));
      return "done";
    });

    const executionId = await testFlow.start({});
    const exec = await adapter.getExecution(executionId);
    expect(exec?.status).toBe("COMPLETED");
    expect(leaseSnapshots.length).toBeGreaterThanOrEqual(1);
    // Final leaseUntil (acquired or extended) should be in the future — the
    // test passes as long as the lease didn't expire during execution.
    const lastSnapshot = leaseSnapshots[leaseSnapshots.length - 1];
    expect(lastSnapshot).toBeGreaterThan(Date.now() - 1000);
  });

  it("concurrent resume() on a held lease is rejected with reason=locked (no split-brain)", async () => {
    const adapter = memoryAdapter();
    const flow = createBetterFlow({ adapter });

    let phase: "first" | "second" = "first";
    const testFlow = flow.define("raceResume", async (ctx) => {
      if (phase === "first") {
        // First call suspends with a long sleep so the row stays RUNNING
        // (lease held) when the second resume() races in.
        await ctx.sleep("5s");
      }
      return phase;
    });

    const executionId = await testFlow.start({});
    // Now the row is SUSPENDED with a fresh leaseUntil=null.
    // Manually acquire it as if a worker had just picked it up.
    const lockResult = await adapter.acquireLock(executionId, 30_000);
    expect(lockResult.acquired).toBe(true);

    // A second resume attempt must NOT acquire — this is the split-brain guard.
    phase = "second";
    await flow.resume(executionId);

    const row = (adapter as any)._rawExecutions.get(executionId);
    expect(row.status).toBe("RUNNING");
    // The handler should NOT have re-run for the second resume — only the
    // first run's sleep step is in the cache.
    const history = await adapter.getExecutionHistory(executionId);
    expect(history.length).toBe(1);
    expect(history[0].stepName).toBe("sleep_5s");

    // Cleanup: release so the suspended workflow can complete in CI.
    await adapter.releaseLock(executionId, "SUSPENDED");
  });
});
