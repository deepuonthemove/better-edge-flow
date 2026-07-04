import { createClient } from "@libsql/client";
import { drizzle } from "drizzle-orm/libsql";
import { getDrizzleSchema } from "better-edge-flow/adapters/drizzle";

// Initialize local LibSQL client (SQLite file-backed)
const client = createClient({
  url: "file:flow.db"
});

// Execute async table creation DDLs using top-level await
await client.execute(`
  CREATE TABLE IF NOT EXISTS bf_executions (
    id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    status TEXT NOT NULL,
    input TEXT,
    output TEXT,
    error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
`);

await client.execute(`
  CREATE TABLE IF NOT EXISTS bf_steps (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    step_index INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    step_type TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT,
    error TEXT,
    resume_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
`);

await client.execute(`
  CREATE TABLE IF NOT EXISTS bf_events (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    payload TEXT,
    consumed INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
  );
`);

export const db = drizzle(client);

export const flowSchema = getDrizzleSchema({
  dialect: "sqlite",
  tablePrefix: "bf_"
});
