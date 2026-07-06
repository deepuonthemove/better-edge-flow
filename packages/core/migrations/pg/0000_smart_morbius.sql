CREATE TABLE "bf_events" (
	"id" varchar(255) PRIMARY KEY NOT NULL,
	"execution_id" varchar(255) NOT NULL,
	"event_name" varchar(255) NOT NULL,
	"event_key" varchar(255),
	"payload" jsonb,
	"consumed" integer DEFAULT 0 NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "bf_events_event_key_unique" UNIQUE("event_key")
);
--> statement-breakpoint
CREATE TABLE "bf_executions" (
	"id" varchar(255) PRIMARY KEY NOT NULL,
	"workflow_name" varchar(255) NOT NULL,
	"status" varchar(50) NOT NULL,
	"version" integer DEFAULT 1 NOT NULL,
	"sequence" integer DEFAULT 0 NOT NULL,
	"tenant_id" varchar(255),
	"namespace" varchar(255),
	"input" jsonb,
	"output" jsonb,
	"error" jsonb,
	"timeout" timestamp,
	"lease_until" timestamp,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "bf_rate_limits" (
	"id" varchar(255) PRIMARY KEY NOT NULL,
	"queue" varchar(255) NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "bf_steps" (
	"id" varchar(255) PRIMARY KEY NOT NULL,
	"execution_id" varchar(255) NOT NULL,
	"step_index" integer NOT NULL,
	"step_name" varchar(255) NOT NULL,
	"step_type" varchar(50) NOT NULL,
	"status" varchar(50) NOT NULL,
	"result" jsonb,
	"error" jsonb,
	"resume_at" timestamp,
	"attempts" integer DEFAULT 0,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
