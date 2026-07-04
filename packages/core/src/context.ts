import { trace, SpanStatusCode } from "@opentelemetry/api";
import { BetterFlowAdapter, WorkflowContext, RetryOptions, StepRecord, BetterFlowConfig } from "./types.js";
import { WorkflowSuspendedError } from "./errors.js";
import { parseDuration } from "./parser.js";

export class WorkflowContextImpl<TEvents extends Record<string, any> = Record<string, any>> implements WorkflowContext<TEvents> {
  private stepCounter = 0;
  private dbQueue: Promise<any> = Promise.resolve();
  private cachedSteps: Map<number, StepRecord> = new Map();
  private executionVersion: number | null = null;
  private compensations: { name: string; fn: () => Promise<any> | any }[] = [];
  private isReplayMode = true;
  private initializedCheckpoints = new Set<string>();
  private pendingLocalSteps: { stepIndex: number; stepName: string; result: any }[] = [];
  private timeout: Date | null = null;
  private isCompensating = false;

  constructor(
    public readonly executionId: string,
    private readonly adapter: BetterFlowAdapter,
    private readonly config?: BetterFlowConfig<TEvents>
  ) {}

  async initialize() {
    const history = await this.adapter.getExecutionHistory(this.executionId);
    for (const step of history) {
      this.cachedSteps.set(step.stepIndex, step);
    }
    if (history.length === 0) {
      this.isReplayMode = false;
    }

    const exec = await this.adapter.getExecution(this.executionId);
    if (exec?.timeout) {
      this.timeout = new Date(exec.timeout);
    }
  }

  private checkTimeout() {
    if (this.isCompensating) {
      return;
    }
    if (this.timeout && Date.now() >= this.timeout.getTime()) {
      throw new Error(`Workflow execution timeout exceeded. Deadline was ${this.timeout.toISOString()}`);
    }
  }

  async getVersion(changeId: string, options: { min: number }): Promise<boolean> {
    if (this.executionVersion === null) {
      const exec = await this.adapter.getExecution(this.executionId);
      this.executionVersion = exec?.version ?? 1;
    }
    return this.executionVersion >= options.min;
  }

  registerCompensation(name: string, fn: () => Promise<any> | any): void {
    this.compensations.push({ name, fn });
  }

  async runCompensations(): Promise<void> {
    this.isCompensating = true;
    try {
      while (this.compensations.length > 0) {
        const comp = this.compensations.pop()!;
        const stepName = `compensate_${comp.name}`;
        await this.run(stepName, comp.fn);
      }
    } finally {
      this.isCompensating = false;
    }
  }

  log(message: string, ...args: any[]): void {
    const isReplaying = this.stepCounter < this.cachedSteps.size;
    if (!isReplaying) {
      console.log(`[Better-Flow] [Exec: ${this.executionId}] ${message}`, ...args);
    }
  }

  async checkpoint<T>(checkpointId: string, state: T): Promise<T> {
    const stepName = `checkpoint_${checkpointId}`;
    
    if (!this.initializedCheckpoints.has(checkpointId)) {
      this.initializedCheckpoints.add(checkpointId);

      // Find the latest completed checkpoint in our history cache
      let latestResult: T | undefined;
      let lastCheckpointIndex = -1;
      for (const step of this.cachedSteps.values()) {
        if (step.stepName === stepName && step.status === "COMPLETED") {
          latestResult = step.result;
          lastCheckpointIndex = step.stepIndex;
        }
      }

      if (latestResult !== undefined && lastCheckpointIndex !== -1) {
        // Align the step counter to the index immediately following the restored checkpoint
        this.stepCounter = lastCheckpointIndex + 1;
        return latestResult;
      }
    }

    // Otherwise, register or load as a normal step
    return await this.run(stepName, () => state);
  }

  async flushPendingLocalSteps(): Promise<void> {
    while (this.pendingLocalSteps.length > 0) {
      const localStep = this.pendingLocalSteps.shift()!;
      const existing = await this.adapter.getStep(this.executionId, localStep.stepIndex);
      if (!existing) {
        await this.adapter.createStep({
          executionId: this.executionId,
          stepIndex: localStep.stepIndex,
          stepName: localStep.stepName,
          stepType: "run",
          status: "COMPLETED",
          result: localStep.result
        });
      }
    }
  }

  async runLocal<T>(stepName: string, fn: () => Promise<T> | T): Promise<T> {
    const stepIndex = this.stepCounter++;

    // Check cache first (if replaying)
    const step = this.cachedSteps.get(stepIndex);
    if (step) {
      if (step.stepName !== stepName) {
        throw new Error(
          `Non-deterministic execution: expected local step "${step.stepName}" but encountered "${stepName}" at step index ${stepIndex}.`
        );
      }
      if (step.status === "COMPLETED") {
        return step.result as T;
      }
      if (step.status === "FAILED") {
        const errObj = step.error || {};
        const err = new Error(errObj.message || `Step "${stepName}" failed in a previous execution`);
        err.stack = errObj.stack;
        throw err;
      }
    }

    this.checkTimeout();

    const tracer = trace.getTracer("better-flow");
    return await tracer.startActiveSpan(`local_step:${stepName}`, async (span) => {
      span.setAttribute("better-flow.step_index", stepIndex);
      span.setAttribute("better-flow.step_type", "local");
      
      try {
        let next = async () => {
          return await fn();
        };

        if (this.config?.interceptors) {
          for (let i = this.config.interceptors.length - 1; i >= 0; i--) {
            const interceptor = this.config.interceptors[i];
            if (interceptor.outbound) {
              const currentNext = next;
              const currentInterceptor = interceptor;
              next = async () => {
                return await currentInterceptor.outbound!(this, stepName, currentNext);
              };
            }
          }
        }

        const result = await next();
        
        // Queue deferred step write
        this.pendingLocalSteps.push({ stepIndex, stepName, result });
        
        // Cache in memory to align any subsequent loops/lookups
        const localRecord: StepRecord = {
          id: `${this.executionId}_${stepIndex}`,
          executionId: this.executionId,
          stepIndex,
          stepName,
          stepType: "run",
          status: "COMPLETED",
          result,
          createdAt: new Date(),
          updatedAt: new Date()
        };
        this.cachedSteps.set(stepIndex, localRecord);

        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (err: any) {
        span.recordException(err);
        span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
        throw err;
      } finally {
        span.end();
      }
    });
  }

  async run<T>(stepName: string, fn: () => Promise<T> | T, options?: RetryOptions): Promise<T> {
    const stepIndex = this.stepCounter++;

    // Synchronize initial lookup and creation in the DB queue to guarantee sequential indexing
    const step = await (this.dbQueue = this.dbQueue.then(async () => {
      let s = this.cachedSteps.get(stepIndex);
      if (!s) {
        this.isReplayMode = false;
        s = {
          id: `${this.executionId}_${stepIndex}`,
          executionId: this.executionId,
          stepIndex,
          stepName,
          stepType: "run",
          status: "PENDING",
          attempts: 0,
          createdAt: new Date(),
          updatedAt: new Date()
        };
        await this.adapter.createStep(s);
        this.cachedSteps.set(stepIndex, s);
      }
      return s;
    }));

    if (step) {
      if (step.stepName !== stepName) {
        throw new Error(
          `Non-deterministic execution: expected step "${step.stepName}" but encountered "${stepName}" at step index ${stepIndex}.`
        );
      }
      if (step.status === "COMPLETED") {
        return step.result as T;
      }
      if (step.status === "FAILED") {
        const errObj = step.error || {};
        const err = new Error(errObj.message || `Step "${stepName}" failed in a previous execution`);
        err.stack = errObj.stack;
        throw err;
      }
      
      // If PENDING and has a resumeAt, check if wait or throttle is over
      if (step.status === "PENDING" && step.stepType === "run" && step.resumeAt) {
        const now = new Date();
        const resumeAt = new Date(step.resumeAt);
        if (now >= resumeAt) {
          // Timer expired, fall through to re-execute the closure
        } else {
          // Still waiting for retry backoff or rate-limiting delay
          throw new WorkflowSuspendedError(`Retrying step "${stepName}" at ${resumeAt.toISOString()}`, resumeAt);
        }
      }
    }

    this.checkTimeout();

    // Check rate limit if specified on first run or retry execution
    if (options?.rateLimit && this.adapter.checkRateLimit) {
      const { queue, limit, window } = options.rateLimit;
      const windowMs = parseDuration(window);
      const allowed = await this.adapter.checkRateLimit(queue, limit, windowMs);
      if (!allowed) {
        // Throttled: schedule a retry in the next cycle (half window)
        const retryDelayMs = Math.ceil(windowMs / 2);
        const resumeAt = new Date(Date.now() + retryDelayMs);
        
        await this.adapter.updateStep(this.executionId, stepIndex, {
          status: "PENDING",
          resumeAt
        });
        step.status = "PENDING";
        step.resumeAt = resumeAt;

        throw new WorkflowSuspendedError(
          `Step "${stepName}" throttled by rate limit queue "${queue}". Retrying in ${retryDelayMs}ms.`,
          resumeAt
        );
      }
    }

    const tracer = trace.getTracer("better-flow");
    return await tracer.startActiveSpan(`step:${stepName}`, async (span) => {
      span.setAttribute("better-flow.step_index", stepIndex);
      span.setAttribute("better-flow.step_type", "run");

      try {
        let next = async () => {
          return await fn();
        };

        if (this.config?.interceptors) {
          for (let i = this.config.interceptors.length - 1; i >= 0; i--) {
            const interceptor = this.config.interceptors[i];
            if (interceptor.outbound) {
              const currentNext = next;
              const currentInterceptor = interceptor;
              next = async () => {
                return await currentInterceptor.outbound!(this, stepName, currentNext);
              };
            }
          }
        }

        const result = await next();

        await this.adapter.updateStep(this.executionId, stepIndex, {
          status: "COMPLETED",
          result
        });
        step.status = "COMPLETED";
        step.result = result;
        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (err: any) {
        // Don't intercept WorkflowSuspendedError - it's a control flow exception!
        if (err instanceof WorkflowSuspendedError) {
          throw err;
        }

        const maxRetries = options?.retries ?? 0;
        const currentAttempt = step?.attempts ?? 0;

        if (currentAttempt < maxRetries) {
          const factor = options?.backoffFactor ?? 2;
          const initialDelayVal = options?.initialDelay ?? "2s";
          const initialDelayMs = parseDuration(initialDelayVal);
          const backoffMs = initialDelayMs * Math.pow(factor, currentAttempt);
          const resumeAt = new Date(Date.now() + backoffMs);

          const errorObj = { message: err.message, stack: err.stack };
          await this.adapter.updateStep(this.executionId, stepIndex, {
            status: "PENDING",
            attempts: currentAttempt + 1,
            resumeAt,
            error: errorObj
          });

          step.status = "PENDING";
          step.attempts = currentAttempt + 1;
          step.resumeAt = resumeAt;
          step.error = errorObj;

          throw new WorkflowSuspendedError(
            `Step "${stepName}" failed. Scheduling retry attempt ${currentAttempt + 1}/${maxRetries} in ${backoffMs}ms (at ${resumeAt.toISOString()})`,
            resumeAt
          );
        }

        // No retries remaining: fail the step
        const errorObj = { message: err.message, stack: err.stack };
        await this.adapter.updateStep(this.executionId, stepIndex, {
          status: "FAILED",
          attempts: currentAttempt + 1,
          error: errorObj
        });

        step.status = "FAILED";
        step.attempts = currentAttempt + 1;
        step.error = errorObj;

        span.recordException(err);
        span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
        throw err;
      } finally {
        span.end();
      }
    });
  }

  async sleep(duration: string | number): Promise<void> {
    const stepIndex = this.stepCounter++;
    const ms = parseDuration(duration);

    // Queue setup of sleep timer row
    const step = await (this.dbQueue = this.dbQueue.then(async () => {
      let s = this.cachedSteps.get(stepIndex);
      if (!s) {
        this.isReplayMode = false;
        const resumeAt = new Date(Date.now() + ms);
        s = {
          id: `${this.executionId}_${stepIndex}`,
          executionId: this.executionId,
          stepIndex,
          stepName: `sleep_${duration}`,
          stepType: "sleep",
          status: "PENDING",
          resumeAt,
          attempts: 0,
          createdAt: new Date(),
          updatedAt: new Date()
        };
        await this.adapter.createStep(s);
        if (this.adapter.scheduleTimer) {
          await this.adapter.scheduleTimer(this.executionId, resumeAt);
        }
        this.cachedSteps.set(stepIndex, s);
      }
      return s;
    }));

    if (step) {
      if (step.stepType !== "sleep") {
        throw new Error(
          `Non-deterministic execution: expected step type "sleep" but encountered "${step.stepType}" at step index ${stepIndex}.`
        );
      }
      if (step.status === "COMPLETED") {
        return;
      }
    }

    this.checkTimeout();

    if (step && step.status === "PENDING") {
      const now = new Date();
      const resumeAt = step.resumeAt ? new Date(step.resumeAt) : new Date(0);
      if (now >= resumeAt) {
        await this.adapter.updateStep(this.executionId, stepIndex, {
          status: "COMPLETED"
        });
        step.status = "COMPLETED";
        return;
      } else {
        throw new WorkflowSuspendedError(`Sleeping until ${resumeAt.toISOString()}`, resumeAt);
      }
    }

    const resumeAt = new Date(Date.now() + ms);
    throw new WorkflowSuspendedError(`Sleeping until ${resumeAt.toISOString()}`, resumeAt);
  }

  async waitForEvent<T = any>(eventName: string, options?: { timeout?: string | number }): Promise<T> {
    const stepIndex = this.stepCounter++;

    // Queue setup and inbox checking of event wait state
    const step = await (this.dbQueue = this.dbQueue.then(async () => {
      let s = this.cachedSteps.get(stepIndex);
      if (!s) {
        this.isReplayMode = false;
        const pendingEvents = await this.adapter.getPendingEvents(this.executionId, eventName);
        if (pendingEvents.length > 0) {
          const event = pendingEvents[0];
          await this.adapter.consumeEvent(event.id);
          s = {
            id: `${this.executionId}_${stepIndex}`,
            executionId: this.executionId,
            stepIndex,
            stepName: eventName,
            stepType: "event",
            status: "COMPLETED",
            result: event.payload,
            attempts: 0,
            createdAt: new Date(),
            updatedAt: new Date()
          };
          await this.adapter.createStep(s);
          this.cachedSteps.set(stepIndex, s);
        } else {
          const timeoutMs = options?.timeout ? parseDuration(options.timeout) : 24 * 60 * 60 * 1000;
          const resumeAt = new Date(Date.now() + timeoutMs);
          s = {
            id: `${this.executionId}_${stepIndex}`,
            executionId: this.executionId,
            stepIndex,
            stepName: eventName,
            stepType: "event",
            status: "PENDING",
            resumeAt,
            attempts: 0,
            createdAt: new Date(),
            updatedAt: new Date()
          };
          await this.adapter.createStep(s);
          this.cachedSteps.set(stepIndex, s);
        }
      }
      return s;
    }));

    if (step) {
      if (step.stepType !== "event") {
        throw new Error(
          `Non-deterministic execution: expected step type "event" but encountered "${step.stepType}" at step index ${stepIndex}.`
        );
      }
      if (step.status === "COMPLETED") {
        return step.result as T;
      }
    }

    this.checkTimeout();

    if (step && step.status === "PENDING") {
      const pendingEvents = await this.adapter.getPendingEvents(this.executionId, eventName);
      if (pendingEvents.length > 0) {
        const event = pendingEvents[0];
        await this.adapter.consumeEvent(event.id);
        await this.adapter.updateStep(this.executionId, stepIndex, {
          status: "COMPLETED",
          result: event.payload
        });
        step.status = "COMPLETED";
        step.result = event.payload;
        return event.payload as T;
      }

      const now = new Date();
      const resumeAt = step.resumeAt ? new Date(step.resumeAt) : new Date(0);
      if (now >= resumeAt) {
        const errorObj = { message: `Timeout waiting for event "${eventName}"` };
        await this.adapter.updateStep(this.executionId, stepIndex, {
          status: "FAILED",
          error: errorObj
        });
        step.status = "FAILED";
        step.error = errorObj;
        throw new Error(`Timeout waiting for event "${eventName}"`);
      }

      throw new WorkflowSuspendedError(`Waiting for event "${eventName}"`, resumeAt);
    }

    const recheckStep = this.cachedSteps.get(stepIndex);
    if (recheckStep && recheckStep.status === "COMPLETED") {
      return recheckStep.result as T;
    }

    const timeoutMs = options?.timeout ? parseDuration(options.timeout) : 24 * 60 * 60 * 1000;
    const resumeAt = new Date(Date.now() + timeoutMs);
    throw new WorkflowSuspendedError(`Waiting for event "${eventName}"`, resumeAt);
  }
}
