import { defineConfig } from "drizzle-kit";

export default defineConfig({
  schema: "./src/adapters/drizzle/pg-static-schema.ts",
  out: "./migrations/pg",
  dialect: "postgresql",
  dbCredentials: {
    url: process.env.DATABASE_URL || "postgresql://postgres:postgres@localhost:5432/postgres",
  },
});
