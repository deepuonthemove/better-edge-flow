import { getDrizzleSchema } from "./schema.js";

// Export schema tables at top-level to support static analysis by Drizzle Kit CLI
export const { executions, steps, events, rateLimits } = getDrizzleSchema({
  dialect: "sqlite",
  tablePrefix: "bf_"
});
