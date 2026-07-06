import { trace, SpanStatusCode } from "@opentelemetry/api";
import { BetterFlowConfig, BetterFlowAdapter, WorkflowFn, WorkflowContext } from "./types.js";
import { WorkflowContextImpl } from "./context.js";
import { WorkflowSuspendedError } from "./errors.js";
import { parseDuration } from "./parser.js";

export interface RegisteredWorkflow<TInput = any, TOutput = any> {
  name: string;
  start: (
    input: TInput, 
    options?: string | { executionId?: string; tenantId?: string; namespace?: string; timeout?: string | number }
  ) => Promise<string>;
}

export class BetterFlow<TEvents extends Record<string, any> = Record<string, any>> {
  private workflows = new Map<string, WorkflowFn<any, any, TEvents>>();
  private workflowVersions = new Map<string, number>();
  private resumeDebouncers = new Map<string, NodeJS.Timeout>();
  public readonly adapter: BetterFlowAdapter;

  constructor(private config: BetterFlowConfig<TEvents>) {
    this.adapter = config.adapter;
  }

  /**
   * Define a type-safe workflow with a unique name and a handler execution function.
   */
  define<TInput = any, TOutput = any>(
    configOrName: string | { name: string; version?: number; handler: WorkflowFn<TInput, TOutput, TEvents> },
    handlerFn?: WorkflowFn<TInput, TOutput, TEvents>
  ): RegisteredWorkflow<TInput, TOutput> {
    let name: string;
    let version = 1;
    let handler: WorkflowFn<TInput, TOutput, TEvents>;

    if (typeof configOrName === "string") {
      name = configOrName;
      if (!handlerFn) {
        throw new Error(`Handler function is required when passing name as string.`);
      }
      handler = handlerFn;
    } else {
      name = configOrName.name;
      version = configOrName.version ?? 1;
      handler = configOrName.handler;
    }

    if (this.workflows.has(name)) {
      throw new Error(`Workflow with name "${name}" is already defined.`);
    }
    this.workflows.set(name, handler);
    this.workflowVersions.set(name, version);
    
    return {
      name,
      start: (
        input: TInput, 
        options?: string | { executionId?: string; tenantId?: string; namespace?: string; timeout?: string | number }
      ) => this.start(name, input, options)
    };
  }

  /**
   * Starts a new workflow execution.
   *
   * Idempotent on `executionId`: if a row already exists for the supplied id we
   * return that id instead of throwing on the PK violation. This makes client
   * retries of the same webhook safe — they no longer surface as 500s.
   */
  async start(
    workflowName: string,
    input: any,
    optionsOrId?: string | { executionId?: string; tenantId?: string; namespace?: string; timeout?: string | number }
  ): Promise<string> {
    let executionId: string = crypto.randomUUID();
    let tenantId: string | null = null;
    let namespace: string | null = null;
    let timeoutVal: string | number | undefined = this.config.defaultWorkflowTimeout;

    if (typeof optionsOrId === "string") {
      executionId = optionsOrId;
    } else if (optionsOrId && typeof optionsOrId === "object") {
      executionId = optionsOrId.executionId || executionId;
      tenantId = optionsOrId.tenantId || null;
      namespace = optionsOrId.namespace || null;
      timeoutVal = optionsOrId.timeout ?? timeoutVal;
    }

    const fn = this.workflows.get(workflowName);
    if (!fn) {
      throw new Error(`Workflow "${workflowName}" not found in registry.`);
    }

    const version = this.workflowVersions.get(workflowName) ?? 1;
    let timeout: Date | null = null;
    if (timeoutVal) {
      timeout = new Date(Date.now() + parseDuration(timeoutVal));
    }

    try {
      await this.adapter.createExecution({
        id: executionId,
        workflowName,
        status: "PENDING",
        version,
        tenantId,
        namespace,
        input,
        timeout
      });
    } catch (err: any) {
      // PK / unique violation = executionId already exists. Treat as idempotent:
      // return the existing id without re-running. Caller (HTTP handler, webhook
      // retry, etc.) gets the same executionId back instead of a 500.
      const isUniqueViolation =
        err?.message?.includes("UNIQUE") ||
        err?.message?.includes("unique") ||
        err?.message?.includes("constraint") ||
        err?.message?.includes("PRIMARY KEY") ||
        err?.code === "23505" ||
        err?.code === "SQLITE_CONSTRAINT_PRIMARYKEY" ||
        err?.code === "SQLITE_CONSTRAINT_UNIQUE";

      if (isUniqueViolation) {
        const existing = await this.adapter.getExecution(executionId);
        if (existing && existing.workflowName === workflowName) {
          return executionId;
        }
        // Mismatch: caller reused an id that belongs to a different workflow. Bubble up.
        throw new Error(
          `Execution "${executionId}" already exists with workflowName "${existing?.workflowName}", cannot reuse for "${workflowName}".`
        );
      }
      throw err;
    }

    await this.runExecution(executionId, fn, input);
    return executionId;
  }

  /**
   * Resumes a suspended execution from where it last paused.
   */
  async resume(executionId: string): Promise<void> {
    const execution = await this.adapter.getExecution(executionId);
    if (!execution) {
      throw new Error(`Execution "${executionId}" not found.`);
    }
    if (execution.status === "COMPLETED" || execution.status === "FAILED" || execution.status === "CANCELLED") {
      return;
    }

    const fn = this.workflows.get(execution.workflowName);
    if (!fn) {
      throw new Error(`Workflow "${execution.workflowName}" not found in registry.`);
    }

    await this.runExecution(executionId, fn, execution.input);
  }

  /**
   * Cancels a running workflow execution.
   */
  async cancel(executionId: string): Promise<void> {
    await this.adapter.updateExecution(executionId, {
      status: "CANCELLED",
      updatedAt: new Date()
    });
  }

  /**
   * Internal wrapper to run the execution replay loop and catch suspension vs failures.
   *
   * Locking model (post-fix):
   *   - acquireLock is a single atomic conditional UPDATE — dialect-portable.
   *   - A heartbeat setInterval extends the lease while execution is in flight, so
   *     long-running activities (longer than LEASE_MS) don't get re-claimed by a
   *     concurrent worker.
   *   - releaseLock unconditionally drops the lease (leaseUntil = null), so a
   *     suspended workflow is immediately re-acquirable without waiting for the
   *     lease to expire.
   *   - Lock failure (already running / terminal / missing row) is logged with a
   *     structured reason and emitted as a span event so operators can debug.
   */
  private async runExecution(executionId: string, fn: WorkflowFn<any, any, TEvents>, input: any): Promise<void> {
    const LEASE_MS = 30000; // 30 seconds lock lease
    const HEARTBEAT_MS = Math.max(1000, Math.floor(LEASE_MS / 3)); // ~10s, min 1s
    const lockResult = await this.adapter.acquireLock(executionId, LEASE_MS);

    // Observability for lock acquisition outcomes. We emit a dedicated OTel span
    // so the timing and correlation of lock attempts is visible in any
    // registered exporter. The structured `console.*` calls below carry the
    // reason for failure (missing / locked / terminal).
    const tracer = trace.getTracer("better-flow");
    tracer.startActiveSpan("better-flow.acquireLock", (span) => {
      span.setAttribute("better-flow.execution_id", executionId);
      span.setAttribute("better-flow.acquired", lockResult.acquired);
      span.setAttribute("better-flow.lock_reason", lockResult.reason ?? "acquired");
      span.end();
    });

    if (!lockResult.acquired) {
      // Structured logging on lock failure so operators can debug:
      //   - missing  → row was deleted out from under us
      //   - terminal → resume() on a completed/failed/cancelled execution (normal cron sweep)
      //   - locked   → another worker is currently running this execution
      const reason = lockResult.reason ?? "unknown";
      if (reason === "missing") {
        console.warn(`[Better-Flow] Execution "${executionId}" does not exist; skipping run.`);
      } else if (reason === "terminal") {
        console.info(`[Better-Flow] Execution "${executionId}" already terminal; skipping run.`);
      } else {
        console.warn(
          `[Better-Flow] Execution "${executionId}" is currently locked by another worker; skipping run.`
        );
      }
      return;
    }

    const execution = await this.adapter.getExecution(executionId);
    if (!execution) {
      // Edge case: row vanished between acquireLock and getExecution (e.g. manual
      // DELETE). releaseLock now no-ops on missing rows safely.
      await this.adapter.releaseLock(executionId, "FAILED");
      return;
    }

    // Start the heartbeat BEFORE we hand off to the handler. It extends the
    // lease every HEARTBEAT_MS while execution runs. The interval is cleared
    // in the `finally` block below, no matter how the execution ends.
    const heartbeat = setInterval(() => {
      // Fire-and-forget; extendLease failure is fine — it just means somebody
      // else owns the lease now and our next attempt will detect that.
      this.adapter.extendLease(executionId, LEASE_MS).catch((err) => {
        console.warn(`[Better-Flow] Heartbeat extendLease failed for "${executionId}":`, err);
      });
    }, HEARTBEAT_MS);
    // Don't keep the Node event loop alive solely for heartbeats in serverless —
    // they should be cheap and die with the function.
    if (typeof (heartbeat as any).unref === "function") {
      (heartbeat as any).unref();
    }

    const ctx = new WorkflowContextImpl<TEvents>(executionId, this.adapter, this.config);

    await tracer.startActiveSpan(`workflow:${execution.workflowName}`, async (span) => {
      span.setAttribute("better-flow.execution_id", executionId);
      span.setAttribute("better-flow.version", execution.version);

      try {
        await ctx.initialize(); // Pre-load all step records in memory to protect against replication lag

        // Onion-model inbound interceptor chain wrapping
        let next = async () => {
          return await fn(ctx, input);
        };

        if (this.config.interceptors) {
          for (let i = this.config.interceptors.length - 1; i >= 0; i--) {
            const interceptor = this.config.interceptors[i];
            if (interceptor.inbound) {
              const currentNext = next;
              const currentInterceptor = interceptor;
              next = async () => {
                return await currentInterceptor.inbound!(ctx, input, currentNext);
              };
            }
          }
        }

        const output = await next();

        // Flush any pending deferred local activity steps before completion
        await ctx.flushPendingLocalSteps();

        await this.adapter.updateExecution(executionId, { output });

        // Prune step history records from database if opt-in configured
        if (this.config.pruneHistoryOnComplete && this.adapter.pruneExecutionHistory) {
          await this.adapter.pruneExecutionHistory(executionId);
        }

        await this.adapter.releaseLock(executionId, "COMPLETED");
        span.setStatus({ code: SpanStatusCode.OK });
      } catch (err: any) {
        // Flush pending local steps on failure or suspension too to capture log history
        await ctx.flushPendingLocalSteps();

        if (err instanceof WorkflowSuspendedError) {
          await this.adapter.releaseLock(executionId, "SUSPENDED");
          span.setStatus({ code: SpanStatusCode.OK });
        } else {
          span.recordException(err);
          span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });

          // Unhandled failure: trigger Saga compensations rollback!
          console.error(`[Better-Flow DEBUG] Non-suspended error in runExecution for "${executionId}":`, err);
          try {
            console.warn(`[Better-Flow] Execution "${executionId}" failed. Triggering Saga rollbacks.`);
            await ctx.runCompensations();

            await this.adapter.updateExecution(executionId, {
              error: { message: err.message, stack: err.stack }
            });

            // Prune step history records from database on permanent failures
            if (this.config.pruneHistoryOnComplete && this.adapter.pruneExecutionHistory) {
              await this.adapter.pruneExecutionHistory(executionId);
            }

            await this.adapter.releaseLock(executionId, "FAILED");
          } catch (rollbackErr: any) {
            if (rollbackErr instanceof WorkflowSuspendedError) {
              // Compensations suspended or throttled, save as SUSPENDED so we resume later to continue compensating!
              await this.adapter.releaseLock(executionId, "SUSPENDED");
            } else {
              // Saga rollback itself crashed permanently
              await this.adapter.updateExecution(executionId, {
                error: {
                  message: `Workflow failed: ${err.message}. Rollback failed: ${rollbackErr.message}`,
                  stack: rollbackErr.stack
                }
              });

              if (this.config.pruneHistoryOnComplete && this.adapter.pruneExecutionHistory) {
                await this.adapter.pruneExecutionHistory(executionId);
              }

              await this.adapter.releaseLock(executionId, "FAILED");
            }
          }
        }
      } finally {
        clearInterval(heartbeat);
        span.end();
      }
    });
  }

  /**
   * Publishes an event to a specific workflow execution. This is usually triggered by a webhook.
   */
  async publishEvent(executionId: string, eventName: string, payload: any, eventKey?: string): Promise<void> {
    await this.adapter.createEvent({ executionId, eventName, eventKey, payload });
    
    const debounceMs = this.config.eventDebounceMs ?? 0;
    if (debounceMs > 0) {
      // Debounced / buffered resume call to batch signals arriving in rapid succession
      let debouncer = this.resumeDebouncers.get(executionId);
      if (debouncer) {
        clearTimeout(debouncer);
      }

      debouncer = setTimeout(async () => {
        this.resumeDebouncers.delete(executionId);
        try {
          await this.resume(executionId);
        } catch (err) {
          console.error(`[Better-Flow] Failed to resume execution "${executionId}" from event trigger:`, err);
        }
      }, debounceMs);

      this.resumeDebouncers.set(executionId, debouncer);
    } else {
      // Direct synchronous execution loop resume (no delay)
      await this.resume(executionId);
    }
  }

  /**
   * Evaluates expired sleep timers and resumes them.
   */
  async checkTimers(): Promise<void> {
    // 1. Evaluate expired sleep timer steps
    const expiredSteps = await this.adapter.getExpiredSteps();
    for (const step of expiredSteps) {
      try {
        await this.resume(step.executionId);
      } catch (err) {}
    }

    // 2. Proactively evaluate and fail any execution deadlines that have expired
    if (this.adapter.getExpiredExecutions) {
      const expiredExecutions = await this.adapter.getExpiredExecutions();
      for (const exec of expiredExecutions) {
        try {
          await this.resume(exec.id);
        } catch (err) {}
      }
    }
  }

  /**
   * Web standard HTTP request/response handler.
   * Acts as a single entry point router for edge/serverless endpoints.
   */
  async handler(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;
    const method = req.method;

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    if (method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      // POST: Start a new workflow execution
      if (method === "POST" && path.endsWith("/start")) {
        const body = (await req.json()) as any;
        if (!body.workflowName) {
          return new Response(JSON.stringify({ error: "Missing workflowName" }), {
            status: 400,
            headers: { "Content-Type": "application/json", ...corsHeaders }
          });
        }
        const executionId = await this.start(body.workflowName, body.input, {
          executionId: body.executionId,
          tenantId: body.tenantId,
          namespace: body.namespace,
          timeout: body.timeout
        });
        return new Response(JSON.stringify({ success: true, executionId }), {
          status: 200,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }

      // POST: Publish an event (e.g. webhook notification)
      if (method === "POST" && path.endsWith("/event")) {
        const body = (await req.json()) as any;
        if (!body.executionId || !body.eventName) {
          return new Response(JSON.stringify({ error: "Missing executionId or eventName" }), {
            status: 400,
            headers: { "Content-Type": "application/json", ...corsHeaders }
          });
        }
        try {
          await this.publishEvent(body.executionId, body.eventName, body.payload, body.eventKey);
        } catch (err: any) {
          const isUniqueViolation = 
            err.message?.includes("UNIQUE") || 
            err.message?.includes("constraint") || 
            err.code === "23505" || 
            err.message?.includes("Constraint");

          if (isUniqueViolation) {
            console.warn(`[Better-Flow] Duplicate event detected for key: "${body.eventKey}". Skipping.`);
            return new Response(JSON.stringify({ success: true, duplicate: true }), {
              status: 200,
              headers: { "Content-Type": "application/json", ...corsHeaders }
            });
          }
          throw err;
        }
        return new Response(JSON.stringify({ success: true }), {
          status: 200,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }

      // POST: Cancel a running execution
      if (method === "POST" && path.endsWith("/cancel")) {
        const body = (await req.json()) as any;
        if (!body.executionId) {
          return new Response(JSON.stringify({ error: "Missing executionId" }), {
            status: 400,
            headers: { "Content-Type": "application/json", ...corsHeaders }
          });
        }
        await this.cancel(body.executionId);
        return new Response(JSON.stringify({ success: true }), {
          status: 200,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }

      // POST: Trigger cron checker (for expired sleeps, timeouts, and event timeouts)
      if (method === "POST" && path.endsWith("/cron")) {
        await this.checkTimers();
        return new Response(JSON.stringify({ success: true }), {
          status: 200,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }

      // GET: List all executions (with optional tenantId/namespace filtering)
      if (method === "GET" && path.endsWith("/executions")) {
        const tenantId = url.searchParams.get("tenantId") || undefined;
        const namespace = url.searchParams.get("namespace") || undefined;
        const executions = await this.adapter.listExecutions(50, { tenantId, namespace });
        return new Response(JSON.stringify(executions), {
          status: 200,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }

      // GET: Get execution history timeline
      const historyMatch = path.match(/\/executions\/([^\/]+)\/history$/);
      if (method === "GET" && historyMatch) {
        const executionId = historyMatch[1];
        const steps = await this.adapter.getExecutionHistory(executionId);
        return new Response(JSON.stringify(steps), {
          status: 200,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }

      return new Response(JSON.stringify({ error: `Route not found: ${method} ${path}` }), {
        status: 404,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (err: any) {
      return new Response(JSON.stringify({ error: err.message || "Internal Server Error" }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
}

export function createBetterFlow<TEvents extends Record<string, any> = Record<string, any>>(
  config: BetterFlowConfig<TEvents>
): BetterFlow<TEvents> {
  return new BetterFlow<TEvents>(config);
}

export * from "./types.js";
export * from "./errors.js";
export * from "./parser.js";
export { WorkflowContextImpl } from "./context.js";
export { createFlowClient, FlowClientConfig, WorkflowRegistrySchema } from "./client.js";
