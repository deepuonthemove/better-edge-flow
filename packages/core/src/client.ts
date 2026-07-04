export interface WorkflowRegistrySchema {
  workflows: Record<string, { input: any; output: any }>;
  events: Record<string, any>;
}

export interface FlowClientConfig {
  url: string;
  headers?: Record<string, string> | (() => Record<string, string> | Promise<Record<string, string>>);
}

export function createFlowClient<TSchema extends WorkflowRegistrySchema>(config: FlowClientConfig) {
  const getHeaders = async () => {
    if (typeof config.headers === "function") {
      return await config.headers();
    }
    return config.headers || {};
  };

  return {
    async start<K extends keyof TSchema["workflows"] & string>(
      workflowName: K,
      input: TSchema["workflows"][K]["input"],
      options?: { executionId?: string; tenantId?: string; namespace?: string }
    ): Promise<{ success: boolean; executionId: string }> {
      const headers = await getHeaders();
      const res = await fetch(`${config.url}/start`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...headers
        },
        body: JSON.stringify({
          workflowName,
          input,
          executionId: options?.executionId,
          tenantId: options?.tenantId,
          namespace: options?.namespace
        })
      });
      if (!res.ok) {
        throw new Error(`Failed to start workflow: ${res.statusText}`);
      }
      return await res.json();
    },

    async publishEvent<E extends keyof TSchema["events"] & string>(
      executionId: string,
      eventName: E,
      payload: TSchema["events"][E],
      eventKey?: string
    ): Promise<{ success: boolean; duplicate?: boolean }> {
      const headers = await getHeaders();
      const res = await fetch(`${config.url}/event`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...headers
        },
        body: JSON.stringify({
          executionId,
          eventName,
          payload,
          eventKey
        })
      });
      if (!res.ok) {
        throw new Error(`Failed to publish event: ${res.statusText}`);
      }
      return await res.json();
    },

    async cancel(executionId: string): Promise<{ success: boolean }> {
      const headers = await getHeaders();
      const res = await fetch(`${config.url}/cancel`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...headers
        },
        body: JSON.stringify({
          executionId
        })
      });
      if (!res.ok) {
        throw new Error(`Failed to cancel workflow: ${res.statusText}`);
      }
      return await res.json();
    }
  };
}
