import { and, eq, lte, desc, sql, or, isNull, gt } from "drizzle-orm";
import { BetterFlowAdapter, ExecutionRecord, StepRecord } from "../../types.js";
import { getDrizzleSchema } from "./schema.js";

export interface DrizzleAdapterConfig {
  db: any;
  dialect: "postgresql" | "sqlite";
  tablePrefix?: string;
  encryption?: {
    encrypt(plaintext: string): string | Promise<string>;
    decrypt(ciphertext: string): string | Promise<string>;
  };
}

export function drizzleAdapter(config: DrizzleAdapterConfig): BetterFlowAdapter {
  const { db, dialect } = config;
  const tables = getDrizzleSchema({ dialect, tablePrefix: config.tablePrefix });
  const { executions, steps, events, rateLimits } = tables;

  // Handles payload encryption-at-rest with JSON wrappers for Postgres/SQLite column types
  const serialize = async (val: any) => {
    if (val === undefined || val === null) {
      return null;
    }
    const stringified = JSON.stringify(val);
    if (config.encryption) {
      const ciphertext = await config.encryption.encrypt(stringified);
      return { encrypted: ciphertext };
    }
    return dialect === "sqlite" ? stringified : val;
  };

  const deserialize = async (val: any) => {
    if (val === undefined || val === null) {
      return val;
    }
    let raw = val;
    if (val && typeof val === "object" && typeof val.encrypted === "string") {
      if (config.encryption) {
        raw = await config.encryption.decrypt(val.encrypted);
      } else {
        throw new Error("Encrypted payload encountered but no encryption provider is configured.");
      }
    } else if (dialect === "sqlite" && typeof val === "string") {
      try {
        const parsed = JSON.parse(val);
        if (parsed && typeof parsed === "object" && typeof parsed.encrypted === "string") {
          if (config.encryption) {
            raw = await config.encryption.decrypt(parsed.encrypted);
          } else {
            throw new Error("Encrypted payload encountered but no encryption provider is configured.");
          }
        } else {
          raw = parsed;
        }
      } catch {
        raw = val;
      }
    }

    if (typeof raw === "string") {
      try {
        return JSON.parse(raw);
      } catch {
        return raw;
      }
    }
    return raw;
  };

  return {
    async createExecution(data) {
      // Always write status='PENDING' and leaseUntil=null. acquireLock is the only writer
      // of status='RUNNING' + a real leaseUntil. This removes the old "new Date(0) hack"
      // where the engine relied on a sentinel timestamp to pass the lease check.
      await db.insert(executions).values({
        id: data.id,
        workflowName: data.workflowName,
        status: "PENDING",
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
      const result = await db.select().from(executions).where(eq(executions.id, id));
      const row = result[0];
      if (!row) {
        return null;
      }
      return {
        id: row.id,
        workflowName: row.workflowName,
        status: row.status as any,
        version: row.version ?? 1,
        sequence: row.sequence ?? 0,
        tenantId: row.tenantId,
        namespace: row.namespace,
        input: await deserialize(row.input),
        output: await deserialize(row.output),
        error: await deserialize(row.error),
        timeout: row.timeout ? new Date(row.timeout) : null,
        createdAt: new Date(row.createdAt),
        updatedAt: new Date(row.updatedAt)
      };
    },

    async updateExecution(id, updates) {
      const values: any = { 
        updatedAt: new Date(),
        sequence: sql`${executions.sequence} + 1`
      };
      if (updates.status !== undefined) {
        values.status = updates.status;
      }
      if (updates.output !== undefined) {
        values.output = await serialize(updates.output);
      }
      if (updates.error !== undefined) {
        values.error = await serialize(updates.error);
      }

      await db.update(executions).set(values).where(eq(executions.id, id));
    },

    async getStep(executionId, stepIndex) {
      const result = await db
        .select()
        .from(steps)
        .where(and(eq(steps.executionId, executionId), eq(steps.stepIndex, stepIndex)));
      const row = result[0];
      if (!row) {
        return null;
      }
      return {
        id: row.id,
        executionId: row.executionId,
        stepIndex: row.stepIndex,
        stepName: row.stepName,
        stepType: row.stepType as any,
        status: row.status as any,
        result: await deserialize(row.result),
        error: await deserialize(row.error),
        resumeAt: row.resumeAt ? new Date(row.resumeAt) : null,
        attempts: row.attempts ?? 0,
        createdAt: new Date(row.createdAt),
        updatedAt: new Date(row.updatedAt)
      };
    },

    async createStep(data) {
      const stepId = `${data.executionId}_${data.stepIndex}`;
      await db.insert(steps).values({
        id: stepId,
        executionId: data.executionId,
        stepIndex: data.stepIndex,
        stepName: data.stepName,
        stepType: data.stepType,
        status: data.status,
        resumeAt: data.resumeAt || null,
        attempts: data.attempts || 0,
        createdAt: new Date(),
        updatedAt: new Date()
      });

      await db.update(executions)
        .set({ sequence: sql`${executions.sequence} + 1`, updatedAt: new Date() })
        .where(eq(executions.id, data.executionId));
    },

    async updateStep(executionId, stepIndex, updates) {
      const values: any = { updatedAt: new Date() };
      if (updates.status !== undefined) {
        values.status = updates.status;
      }
      if (updates.result !== undefined) {
        values.result = await serialize(updates.result);
      }
      if (updates.error !== undefined) {
        values.error = await serialize(updates.error);
      }
      if (updates.attempts !== undefined) {
        values.attempts = updates.attempts;
      }
      if (updates.resumeAt !== undefined) {
        values.resumeAt = updates.resumeAt;
      }

      await db
        .update(steps)
        .set(values)
        .where(and(eq(steps.executionId, executionId), eq(steps.stepIndex, stepIndex)));

      await db.update(executions)
        .set({ sequence: sql`${executions.sequence} + 1`, updatedAt: new Date() })
        .where(eq(executions.id, executionId));
    },

    async getPendingEvents(executionId, eventName) {
      const result = await db
        .select()
        .from(events)
        .where(and(eq(events.executionId, executionId), eq(events.eventName, eventName), eq(events.consumed, 0)));
      
      const mapped = [];
      for (const row of result) {
        mapped.push({
          id: row.id,
          executionId: row.executionId,
          eventName: row.eventName,
          eventKey: row.eventKey,
          payload: await deserialize(row.payload),
          consumed: row.consumed === 1,
          createdAt: new Date(row.createdAt)
        });
      }
      return mapped;
    },

    async consumeEvent(eventId) {
      await db.update(events).set({ consumed: 1 }).where(eq(events.id, eventId));
    },

    async createEvent(data) {
      const eventId = crypto.randomUUID();
      await db.insert(events).values({
        id: eventId,
        executionId: data.executionId,
        eventName: data.eventName,
        eventKey: data.eventKey || null,
        payload: await serialize(data.payload),
        consumed: 0,
        createdAt: new Date()
      });
    },

    async getExpiredSteps() {
      const now = new Date();
      const result = await db
        .select()
        .from(steps)
        .where(and(eq(steps.status, "PENDING"), lte(steps.resumeAt, now)));

      const expiredSteps = [];
      for (const row of result) {
        const execRow = await db.select().from(executions).where(eq(executions.id, row.executionId));
        if (execRow[0]) {
          expiredSteps.push({
            id: row.id,
            executionId: row.executionId,
            stepIndex: row.stepIndex,
            stepName: row.stepName,
            stepType: row.stepType as any,
            status: row.status as any,
            result: await deserialize(row.result),
            error: await deserialize(row.error),
            resumeAt: row.resumeAt ? new Date(row.resumeAt) : null,
            createdAt: new Date(row.createdAt),
            updatedAt: new Date(row.updatedAt),
            execution: {
              id: execRow[0].id,
              workflowName: execRow[0].workflowName,
              status: execRow[0].status as any,
              version: execRow[0].version ?? 1,
              sequence: execRow[0].sequence ?? 0,
              tenantId: execRow[0].tenantId,
              namespace: execRow[0].namespace,
              input: await deserialize(execRow[0].input),
              output: await deserialize(execRow[0].output),
              error: await deserialize(execRow[0].error),
              timeout: execRow[0].timeout ? new Date(execRow[0].timeout) : null,
              createdAt: new Date(execRow[0].createdAt),
              updatedAt: new Date(execRow[0].updatedAt)
            }
          });
        }
      }
      return expiredSteps;
    },

    async listExecutions(limit = 50, filters) {
      const conditions = [];
      if (filters?.tenantId) {
        conditions.push(eq(executions.tenantId, filters.tenantId));
      }
      if (filters?.namespace) {
        conditions.push(eq(executions.namespace, filters.namespace));
      }

      let query = db.select().from(executions).orderBy(desc(executions.createdAt)).limit(limit);
      if (conditions.length > 0) {
        query = db.select().from(executions).where(and(...conditions)).orderBy(desc(executions.createdAt)).limit(limit);
      }

      const result = await query;
      const mapped = [];
      for (const row of result) {
        mapped.push({
          id: row.id,
          workflowName: row.workflowName,
          status: row.status as any,
          version: row.version ?? 1,
          sequence: row.sequence ?? 0,
          tenantId: row.tenantId,
          namespace: row.namespace,
          input: await deserialize(row.input),
          output: await deserialize(row.output),
          error: await deserialize(row.error),
          timeout: row.timeout ? new Date(row.timeout) : null,
          createdAt: new Date(row.createdAt),
          updatedAt: new Date(row.updatedAt)
        });
      }
      return mapped;
    },

    async getExecutionHistory(executionId) {
      const result = await db
        .select()
        .from(steps)
        .where(eq(steps.executionId, executionId))
        .orderBy(steps.stepIndex);
      
      const mapped = [];
      for (const row of result) {
        mapped.push({
          id: row.id,
          executionId: row.executionId,
          stepIndex: row.stepIndex,
          stepName: row.stepName,
          stepType: row.stepType as any,
          status: row.status as any,
          result: await deserialize(row.result),
          error: await deserialize(row.error),
          resumeAt: row.resumeAt ? new Date(row.resumeAt) : null,
          attempts: row.attempts ?? 0,
          createdAt: new Date(row.createdAt),
          updatedAt: new Date(row.updatedAt)
        });
      }
      return mapped;
    },

    async acquireLock(id, leaseMs) {
      // Atomic conditional update: claim the lease iff the row is in an acquirable state.
      // We translate "acquirable" into a single SQL predicate so both PG and SQLite get
      // the same guarantee without relying on dialect-specific row locks:
      //   - status IN ('PENDING', 'SUSPENDED')          — never started, or paused
      //   - status = 'RUNNING' AND lease_until <= now  — running but lease expired
      //   - status = 'RUNNING' AND lease_until IS NULL — running without a lease (legacy rows)
      // Terminal statuses ('COMPLETED', 'FAILED', 'CANCELLED') are never re-acquirable.
      const now = new Date();
      const leaseUntil = new Date(now.getTime() + leaseMs);

      const result = await db
        .update(executions)
        .set({
          status: "RUNNING",
          leaseUntil,
          updatedAt: now,
          sequence: sql`${executions.sequence} + 1`
        })
        .where(and(
          eq(executions.id, id),
          or(
            eq(executions.status, "PENDING"),
            eq(executions.status, "SUSPENDED"),
            and(
              eq(executions.status, "RUNNING"),
              or(
                isNull(executions.leaseUntil),
                lte(executions.leaseUntil, now)
              )
            )
          )
        ))
        .returning({ id: executions.id });

      if (result.length > 0) {
        return { acquired: true };
      }

      // UPDATE matched zero rows — figure out *why* so the caller can log it.
      const row = await db
        .select({
          status: executions.status,
          leaseUntil: executions.leaseUntil
        })
        .from(executions)
        .where(eq(executions.id, id))
        .limit(1);

      const current = row[0];
      if (!current) {
        return { acquired: false, reason: "missing" };
      }
      if (current.status === "COMPLETED" || current.status === "FAILED" || current.status === "CANCELLED") {
        return { acquired: false, reason: "terminal" };
      }
      return { acquired: false, reason: "locked" };
    },

    async extendLease(id, leaseMs) {
      // Conditional extend: only the current lease holder can extend.
      // Matches iff lease_until is in the future AND belongs to us (still RUNNING).
      const newLeaseUntil = new Date(Date.now() + leaseMs);
      const result = await db
        .update(executions)
        .set({ leaseUntil: newLeaseUntil, updatedAt: new Date() })
        .where(and(
          eq(executions.id, id),
          eq(executions.status, "RUNNING"),
          gt(executions.leaseUntil, new Date())
        ))
        .returning({ id: executions.id });
      return result.length > 0;
    },

    async releaseLock(id, status) {
      // Guard: silently no-op if the row doesn't exist (engine path sometimes hits this).
      const existing = await db
        .select({ id: executions.id })
        .from(executions)
        .where(eq(executions.id, id))
        .limit(1);
      if (existing.length === 0) return;

      await db.update(executions)
        .set({
          status,
          leaseUntil: null,
          sequence: sql`${executions.sequence} + 1`,
          updatedAt: new Date()
        })
        .where(eq(executions.id, id));
    },

    async checkRateLimit(queue, limit, windowMs) {
      const now = Date.now();
      const windowStart = new Date(now - windowMs);
      
      // Clean sweep expired ticks
      await db.delete(rateLimits).where(lte(rateLimits.createdAt, windowStart));

      // Check current window ticks
      const activeTicks = await db.select().from(rateLimits).where(eq(rateLimits.queue, queue));
      if (activeTicks.length >= limit) {
        return false;
      }

      // Log tick
      await db.insert(rateLimits).values({
        id: crypto.randomUUID(),
        queue,
        createdAt: new Date()
      });
      return true;
    },

    async pruneExecutionHistory(executionId) {
      await db.delete(steps).where(eq(steps.executionId, executionId));
    },

    async getExpiredExecutions() {
      const now = new Date();
      const result = await db
        .select()
        .from(executions)
        .where(and(
          or(eq(executions.status, "RUNNING"), eq(executions.status, "SUSPENDED")),
          lte(executions.timeout, now)
        ));

      const expired = [];
      for (const row of result) {
        expired.push({
          id: row.id,
          workflowName: row.workflowName,
          status: row.status as any,
          version: row.version ?? 1,
          sequence: row.sequence ?? 0,
          tenantId: row.tenantId,
          namespace: row.namespace,
          input: await deserialize(row.input),
          output: await deserialize(row.output),
          error: await deserialize(row.error),
          timeout: row.timeout ? new Date(row.timeout) : null,
          createdAt: new Date(row.createdAt),
          updatedAt: new Date(row.updatedAt)
        });
      }
      return expired;
    }
  };
}
