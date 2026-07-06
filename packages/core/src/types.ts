export interface ExecutionRecord {
  id: string;
  workflowName: string;
  status: 'PENDING' | 'RUNNING' | 'SUSPENDED' | 'COMPLETED' | 'FAILED' | 'CANCELLED';
  version: number;
  sequence: number;
  tenantId?: string | null;
  namespace?: string | null;
  input: any;
  output?: any;
  error?: any;
  timeout?: Date | null;
  leaseUntil?: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface StepRecord {
  id: string;
  executionId: string;
  stepIndex: number;
  stepName: string;
  stepType: 'run' | 'sleep' | 'event';
  status: 'PENDING' | 'COMPLETED' | 'FAILED';
  result?: any;
  error?: any;
  resumeAt?: Date | null;
  attempts?: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface EventRecord {
  id: string;
  executionId: string;
  eventName: string;
  eventKey?: string | null;
  payload: any;
  consumed: boolean;
  createdAt: Date;
}

export interface BetterFlowAdapter {
  createExecution(data: { 
    id: string; 
    workflowName: string; 
    status: ExecutionRecord['status']; 
    version?: number; 
    tenantId?: string | null;
    namespace?: string | null;
    input: any; 
    timeout?: Date | null;
  }): Promise<void>;
  getExecution(id: string): Promise<ExecutionRecord | null>;
  updateExecution(id: string, updates: Partial<ExecutionRecord>): Promise<void>;

  getStep(executionId: string, stepIndex: number): Promise<StepRecord | null>;
  createStep(data: {
    executionId: string;
    stepIndex: number;
    stepName: string;
    stepType: StepRecord['stepType'];
    status: StepRecord['status'];
    resumeAt?: Date | null;
    result?: any;
    error?: any;
    attempts?: number;
  }): Promise<void>;
  updateStep(executionId: string, stepIndex: number, updates: Partial<StepRecord>): Promise<void>;

  getPendingEvents(executionId: string, eventName: string): Promise<EventRecord[]>;
  consumeEvent(eventId: string): Promise<void>;
  createEvent(data: { executionId: string; eventName: string; eventKey?: string; payload: any }): Promise<void>;
  
  getExpiredSteps(): Promise<(StepRecord & { execution: ExecutionRecord })[]>;
  
  // Dynamic table queries for dashboard
  listExecutions(limit?: number, filters?: { tenantId?: string; namespace?: string }): Promise<ExecutionRecord[]>;
  getExecutionHistory(executionId: string): Promise<StepRecord[]>;

  // Distributed Concurrency Lease Locking
  // Returns { acquired: boolean, reason?: 'missing' | 'locked' | 'terminal' }
  // acquireLock is atomic at the SQL layer — dialect-portable.
  acquireLock(id: string, leaseMs: number): Promise<{ acquired: boolean; reason?: 'missing' | 'locked' | 'terminal' }>;
  // Heartbeat / lease extension. Only succeeds if THIS caller currently holds the lease
  // (matched by leaseUntil). Returns true if extended.
  extendLease(id: string, leaseMs: number): Promise<boolean>;
  releaseLock(id: string, status: ExecutionRecord['status']): Promise<void>;

  // Rate Limiting (Optional)
  checkRateLimit?(queue: string, limit: number, windowMs: number): Promise<boolean>;

  // Push-Based Schedulers (Optional)
  scheduleTimer?(executionId: string, resumeAt: Date): Promise<void>;

  // Pruning and Archival (Optional)
  pruneExecutionHistory?(executionId: string): Promise<void>;
  archiveExecution?(executionId: string, history: StepRecord[]): Promise<void>;

  // Proactive Timeouts (Optional)
  getExpiredExecutions?(): Promise<ExecutionRecord[]>;
}

export interface EncryptionProvider {
  encrypt(plaintext: string): string | Promise<string>;
  decrypt(ciphertext: string): string | Promise<string>;
}

export interface WorkflowInterceptor {
  inbound?: (ctx: WorkflowContext, input: any, next: () => Promise<any>) => Promise<any>;
  outbound?: (ctx: WorkflowContext, stepName: string, next: () => Promise<any>) => Promise<any>;
}

export interface BetterFlowConfig<TEvents extends Record<string, any> = Record<string, any>> {
  adapter: BetterFlowAdapter;
  maxRetries?: number;
  events?: TEvents;
  encryption?: EncryptionProvider;
  interceptors?: WorkflowInterceptor[];
  pruneHistoryOnComplete?: boolean;
  defaultWorkflowTimeout?: string | number;
  eventDebounceMs?: number;
}

export interface RetryOptions {
  retries?: number;
  backoffFactor?: number;
  initialDelay?: string | number;
  rateLimit?: { queue: string; limit: number; window: string | number };
}

export interface WorkflowContext<TEvents extends Record<string, any> = Record<string, any>> {
  executionId: string;
  run<T>(stepName: string, fn: () => Promise<T> | T, options?: RetryOptions): Promise<T>;
  sleep(duration: string | number): Promise<void>;
  waitForEvent<K extends keyof TEvents>(eventName: K, options?: { timeout?: string | number }): Promise<TEvents[K]>;
  waitForEvent<T = any>(eventName: string, options?: { timeout?: string | number }): Promise<T>;
  getVersion(changeId: string, options: { min: number }): Promise<boolean>;
  registerCompensation(name: string, fn: () => Promise<any> | any): void;
  
  // Replay-Safe Developer Logging
  log(message: string, ...args: any[]): void;

  // Memory State Snapshots
  checkpoint<T>(checkpointId: string, state: T): Promise<T>;

  // Local Activity (Zero-DB low latency step)
  runLocal<T>(stepName: string, fn: () => Promise<T> | T): Promise<T>;
}

export type WorkflowFn<TInput = any, TOutput = any, TEvents extends Record<string, any> = Record<string, any>> = (
  ctx: WorkflowContext<TEvents>,
  input: TInput
) => Promise<TOutput>;
