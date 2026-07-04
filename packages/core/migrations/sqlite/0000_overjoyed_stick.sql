CREATE TABLE `bf_events` (
	`id` text PRIMARY KEY NOT NULL,
	`execution_id` text NOT NULL,
	`event_name` text NOT NULL,
	`event_key` text,
	`payload` text,
	`consumed` integer DEFAULT 0 NOT NULL,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `bf_events_event_key_unique` ON `bf_events` (`event_key`);--> statement-breakpoint
CREATE TABLE `bf_executions` (
	`id` text PRIMARY KEY NOT NULL,
	`workflow_name` text NOT NULL,
	`status` text NOT NULL,
	`version` integer DEFAULT 1 NOT NULL,
	`sequence` integer DEFAULT 0 NOT NULL,
	`tenant_id` text,
	`namespace` text,
	`input` text,
	`output` text,
	`error` text,
	`timeout` integer,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `bf_rate_limits` (
	`id` text PRIMARY KEY NOT NULL,
	`queue` text NOT NULL,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `bf_steps` (
	`id` text PRIMARY KEY NOT NULL,
	`execution_id` text NOT NULL,
	`step_index` integer NOT NULL,
	`step_name` text NOT NULL,
	`step_type` text NOT NULL,
	`status` text NOT NULL,
	`result` text,
	`error` text,
	`resume_at` integer,
	`attempts` integer DEFAULT 0,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL
);
