export class WorkflowSuspendedError extends Error {
  constructor(
    public reason: string,
    public resumeAt?: Date | null
  ) {
    super(`Workflow suspended: ${reason}`);
    this.name = "WorkflowSuspendedError";
    Object.setPrototypeOf(this, WorkflowSuspendedError.prototype);
  }
}
