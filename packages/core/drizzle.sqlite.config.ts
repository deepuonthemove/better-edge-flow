import { defineConfig } from "drizzle-kit";

export default defineConfig({
  schema: "./src/adapters/drizzle/sqlite-static-schema.ts",
  out: "./migrations/sqlite",
  dialect: "sqlite",
  dbCredentials: {
    url: process.env.DATABASE_URL || "file:flow.db",
  },
});
