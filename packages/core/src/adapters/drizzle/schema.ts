import { pgTable, varchar, timestamp, integer, jsonb } from "drizzle-orm/pg-core";
import { sqliteTable, text as sqliteText, integer as sqliteInteger } from "drizzle-orm/sqlite-core";

export function getDrizzleSchema(config: { dialect: "postgresql" | "sqlite"; tablePrefix?: string }) {
  const prefix = config.tablePrefix || "bf_";
  const dialect = config.dialect;

  if (dialect === "postgresql") {
    const executions = pgTable(`${prefix}executions`, {
      id: varchar("id", { length: 255 }).primaryKey(),
      workflowName: varchar("workflow_name", { length: 255 }).notNull(),
      status: varchar("status", { length: 50 }).notNull(),
      version: integer("version").default(1).notNull(),
      sequence: integer("sequence").default(0).notNull(),
      tenantId: varchar("tenant_id", { length: 255 }),
      namespace: varchar("namespace", { length: 255 }),
      input: jsonb("input"),
      output: jsonb("output"),
      error: jsonb("error"),
      timeout: timestamp("timeout"),
      createdAt: timestamp("created_at").defaultNow().notNull(),
      updatedAt: timestamp("updated_at").defaultNow().notNull(),
    });

    const steps = pgTable(`${prefix}steps`, {
      id: varchar("id", { length: 255 }).primaryKey(),
      executionId: varchar("execution_id", { length: 255 }).notNull(),
      stepIndex: integer("step_index").notNull(),
      stepName: varchar("step_name", { length: 255 }).notNull(),
      stepType: varchar("step_type", { length: 50 }).notNull(),
      status: varchar("status", { length: 50 }).notNull(),
      result: jsonb("result"),
      error: jsonb("error"),
      resumeAt: timestamp("resume_at"),
      attempts: integer("attempts").default(0),
      createdAt: timestamp("created_at").defaultNow().notNull(),
      updatedAt: timestamp("updated_at").defaultNow().notNull(),
    });

    const events = pgTable(`${prefix}events`, {
      id: varchar("id", { length: 255 }).primaryKey(),
      executionId: varchar("execution_id", { length: 255 }).notNull(),
      eventName: varchar("event_name", { length: 255 }).notNull(),
      eventKey: varchar("event_key", { length: 255 }).unique(),
      payload: jsonb("payload"),
      consumed: integer("consumed").default(0).notNull(), // 0 = false, 1 = true
      createdAt: timestamp("created_at").defaultNow().notNull(),
    });

    const rateLimits = pgTable(`${prefix}rate_limits`, {
      id: varchar("id", { length: 255 }).primaryKey(),
      queue: varchar("queue", { length: 255 }).notNull(),
      createdAt: timestamp("created_at").defaultNow().notNull(),
    });

    return { executions, steps, events, rateLimits };
  } else if (dialect === "sqlite") {
    const executions = sqliteTable(`${prefix}executions`, {
      id: sqliteText("id").primaryKey(),
      workflowName: sqliteText("workflow_name").notNull(),
      status: sqliteText("status").notNull(),
      version: sqliteInteger("version").default(1).notNull(),
      sequence: sqliteInteger("sequence").default(0).notNull(),
      tenantId: sqliteText("tenant_id"),
      namespace: sqliteText("namespace"),
      input: sqliteText("input"),
      output: sqliteText("output"),
      error: sqliteText("error"),
      timeout: sqliteInteger("timeout", { mode: "timestamp" }),
      createdAt: sqliteInteger("created_at", { mode: "timestamp" }).notNull(),
      updatedAt: sqliteInteger("updated_at", { mode: "timestamp" }).notNull(),
    });

    const steps = sqliteTable(`${prefix}steps`, {
      id: sqliteText("id").primaryKey(),
      executionId: sqliteText("execution_id").notNull(),
      stepIndex: sqliteInteger("step_index").notNull(),
      stepName: sqliteText("step_name").notNull(),
      stepType: sqliteText("step_type").notNull(),
      status: sqliteText("status").notNull(),
      result: sqliteText("result"),
      error: sqliteText("error"),
      resumeAt: sqliteInteger("resume_at", { mode: "timestamp" }),
      attempts: sqliteInteger("attempts").default(0),
      createdAt: sqliteInteger("created_at", { mode: "timestamp" }).notNull(),
      updatedAt: sqliteInteger("updated_at", { mode: "timestamp" }).notNull(),
    });

    const events = sqliteTable(`${prefix}events`, {
      id: sqliteText("id").primaryKey(),
      executionId: sqliteText("execution_id").notNull(),
      eventName: sqliteText("event_name").notNull(),
      eventKey: sqliteText("event_key").unique(),
      payload: sqliteText("payload"),
      consumed: sqliteInteger("consumed").default(0).notNull(), // 0 = false, 1 = true
      createdAt: sqliteInteger("created_at", { mode: "timestamp" }).notNull(),
    });

    const rateLimits = sqliteTable(`${prefix}rate_limits`, {
      id: sqliteText("id").primaryKey(),
      queue: sqliteText("queue").notNull(),
      createdAt: sqliteInteger("created_at", { mode: "timestamp" }).notNull(),
    });

    return { executions, steps, events, rateLimits };
  } else {
    throw new Error(`Unsupported dialect: ${dialect}`);
  }
}
