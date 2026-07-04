import React, { useState, useEffect } from "react";
import { 
  Play, 
  Send, 
  Activity, 
  Clock, 
  Inbox, 
  AlertCircle, 
  CheckCircle2, 
  RefreshCw,
  Database,
  User,
  Sparkles,
  Search
} from "lucide-react";

interface Execution {
  id: string;
  workflowName: string;
  status: "RUNNING" | "SUSPENDED" | "COMPLETED" | "FAILED";
  input: any;
  output?: any;
  error?: any;
  createdAt: string;
  updatedAt: string;
}

interface Step {
  id: string;
  executionId: string;
  stepIndex: number;
  stepName: string;
  stepType: "run" | "sleep" | "event";
  status: "PENDING" | "COMPLETED" | "FAILED";
  result?: any;
  error?: any;
  resumeAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export default function App() {
  const [apiUrl, setApiUrl] = useState("http://localhost:3001/api/better-flow");
  const [executions, setExecutions] = useState<Execution[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [history, setHistory] = useState<Step[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  
  // New workflow trigger form
  const [triggerName, setTriggerName] = useState("John Doe");
  const [triggerEmail, setTriggerEmail] = useState("john.doe@example.com");
  const [isTriggering, setIsTriggering] = useState(false);

  // Webhook event modal/state
  const [webhookPayload, setWebhookPayload] = useState('{\n  "success": true,\n  "amount": 4900\n}');

  // Fetch executions list
  const fetchExecutions = async (silent = false) => {
    if (!silent) setIsRefreshing(true);
    try {
      const res = await fetch(`${apiUrl}/executions`);
      if (res.ok) {
        const data = await res.json();
        setExecutions(data);
        setIsConnected(true);
      } else {
        setIsConnected(false);
      }
    } catch (e) {
      setIsConnected(false);
    } finally {
      if (!silent) setIsRefreshing(false);
    }
  };

  // Fetch steps history for chosen execution
  const fetchHistory = async (id: string) => {
    try {
      const res = await fetch(`${apiUrl}/executions/${id}/history`);
      if (res.ok) {
        const data = await res.json();
        setHistory(data);
      }
    } catch (e) {
      console.error("Failed to fetch history", e);
    }
  };

  // Poll for updates if selected execution is running or suspended
  useEffect(() => {
    fetchExecutions();
    const interval = setInterval(() => {
      fetchExecutions(true);
    }, 3000);
    return () => clearInterval(interval);
  }, [apiUrl]);

  // Load history when selection changes
  useEffect(() => {
    if (selectedId) {
      fetchHistory(selectedId);
      const interval = setInterval(() => {
        fetchHistory(selectedId);
      }, 2000);
      return () => clearInterval(interval);
    } else {
      setHistory([]);
    }
  }, [selectedId]);

  // Auto-select first execution if none selected
  useEffect(() => {
    if (executions.length > 0 && !selectedId) {
      setSelectedId(executions[0].id);
    }
  }, [executions]);

  // Trigger new execution
  const handleTrigger = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsTriggering(true);
    try {
      const randomId = `exec_${Math.floor(Math.random() * 900000 + 100000)}`;
      const res = await fetch(`${apiUrl}/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workflowName: "userOnboarding",
          executionId: randomId,
          input: {
            userId: `usr_${Math.floor(Math.random() * 1000)}`,
            name: triggerName,
            email: triggerEmail
          }
        })
      });
      if (res.ok) {
        const data = await res.json();
        setSelectedId(data.executionId);
        await fetchExecutions();
      }
    } catch (e) {
      alert("Failed to start workflow. Check API status.");
    } finally {
      setIsTriggering(false);
    }
  };

  // Trigger Cron Tickers
  const handleCronTick = async () => {
    try {
      const res = await fetch(`${apiUrl}/cron`, { method: "POST" });
      if (res.ok) {
        await fetchExecutions();
        if (selectedId) await fetchHistory(selectedId);
      }
    } catch (e) {
      alert("Failed to trigger cron.");
    }
  };

  // Publish Stripe mock webhook event
  const handleSendWebhook = async (id: string, name: string) => {
    try {
      let parsed = {};
      try {
        parsed = JSON.parse(webhookPayload);
      } catch {
        alert("Invalid JSON webhook payload.");
        return;
      }
      const res = await fetch(`${apiUrl}/event`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          executionId: id,
          eventName: name,
          payload: parsed
        })
      });
      if (res.ok) {
        await fetchExecutions();
        if (selectedId) await fetchHistory(selectedId);
      }
    } catch (e) {
      alert("Failed to send webhook event.");
    }
  };

  const selectedExecution = executions.find(e => e.id === selectedId);

  const filteredExecutions = executions.filter(e => 
    e.id.toLowerCase().includes(searchQuery.toLowerCase()) ||
    e.workflowName.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div style={{ padding: "24px", maxWidth: "1600px", margin: "0 auto" }}>
      
      {/* Header Panel */}
      <header className="glass-panel" style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "16px 24px", marginBottom: "24px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
          <div style={{ width: "36px", height: "36px", background: "linear-gradient(135deg, #6366f1 0%, #a855f7 100%)", borderRadius: "10px", display: "flex", alignItems: "center", justifyItems: "center", justifyContent: "center", boxShadow: "0 0 16px rgba(99, 102, 241, 0.4)" }}>
            <Sparkles size={20} color="white" />
          </div>
          <div>
            <h1 style={{ margin: 0, fontSize: "22px", fontWeight: 700, letterSpacing: "-0.5px", background: "linear-gradient(135deg, #fff 40%, #a5b4fc 100%)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" }}>Better-Flow</h1>
            <span style={{ fontSize: "12px", color: "#64748b", fontWeight: 500 }}>Durable execution dashboard</span>
          </div>
        </div>

        {/* API connection and cron controls */}
        <div style={{ display: "flex", alignItems: "center", gap: "16px" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "8px", background: "rgba(0,0,0,0.2)", padding: "6px 12px", borderRadius: "8px", border: "1px solid rgba(255,255,255,0.05)" }}>
            <span style={{ width: "8px", height: "8px", borderRadius: "50%", backgroundColor: isConnected ? "#10b981" : "#ef4444", boxShadow: isConnected ? "0 0 8px #10b981" : "0 0 8px #ef4444" }}></span>
            <input 
              type="text" 
              value={apiUrl} 
              onChange={(e) => setApiUrl(e.target.value)}
              style={{ background: "none", border: "none", color: "#94a3b8", fontSize: "13px", width: "260px", outline: "none", fontFamily: "monospace" }} 
            />
          </div>

          <button onClick={() => fetchExecutions()} className="btn-secondary" style={{ padding: "8px 12px", display: "flex", alignItems: "center", gap: "6px", fontSize: "13px" }}>
            <RefreshCw size={14} className={isRefreshing ? "animate-spin" : ""} />
            Sync
          </button>

          <button onClick={handleCronTick} className="btn-primary" style={{ padding: "8px 14px", display: "flex", alignItems: "center", gap: "6px", fontSize: "13px" }}>
            <Clock size={14} />
            Cron Tick (Advance Timers)
          </button>
        </div>
      </header>

      {!isConnected && (
        <div className="glass-panel" style={{ padding: "24px", borderLeft: "4px solid #ef4444", marginBottom: "24px", display: "flex", gap: "16px", alignItems: "center" }}>
          <AlertCircle size={32} color="#ef4444" />
          <div>
            <h3 style={{ margin: "0 0 4px 0", color: "#f87171" }}>API Connection Offline</h3>
            <p style={{ margin: 0, color: "#94a3b8", fontSize: "14px" }}>
              Unable to reach the Better-Flow API endpoint. Please make sure the sandbox server is running by executing:
              <code style={{ background: "rgba(239, 68, 68, 0.15)", padding: "2px 6px", borderRadius: "4px", marginLeft: "6px", color: "#fca5a5", fontFamily: "monospace" }}>npm run dev:demo</code> in your terminal.
            </p>
          </div>
        </div>
      )}

      {/* Main Dashboard Layout */}
      <div style={{ display: "grid", gridTemplateColumns: "360px 1fr", gap: "24px", alignItems: "start" }}>
        
        {/* Left Side: Start Workflow Form and Executions List */}
        <div style={{ display: "flex", flexDirection: "column", gap: "24px", width: "100%" }}>
          
          {/* Start Workflow Panel */}
          <section className="glass-panel" style={{ padding: "20px" }}>
            <h2 style={{ margin: "0 0 16px 0", fontSize: "16px", fontWeight: 600, display: "flex", alignItems: "center", gap: "8px" }}>
              <Play size={16} color="#6366f1" />
              Trigger New Onboarding Flow
            </h2>
            <form onSubmit={handleTrigger} style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
              <div>
                <label style={{ display: "block", fontSize: "12px", color: "#94a3b8", marginBottom: "4px", fontWeight: 500 }}>User Name</label>
                <input 
                  type="text" 
                  value={triggerName} 
                  onChange={(e) => setTriggerName(e.target.value)}
                  required
                  style={{ width: "93%", background: "rgba(0,0,0,0.2)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: "6px", padding: "8px 12px", color: "white", outline: "none" }}
                />
              </div>
              <div>
                <label style={{ display: "block", fontSize: "12px", color: "#94a3b8", marginBottom: "4px", fontWeight: 500 }}>User Email</label>
                <input 
                  type="email" 
                  value={triggerEmail} 
                  onChange={(e) => setTriggerEmail(e.target.value)}
                  required
                  style={{ width: "93%", background: "rgba(0,0,0,0.2)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: "6px", padding: "8px 12px", color: "white", outline: "none" }}
                />
              </div>
              <button type="submit" disabled={isTriggering} className="btn-primary" style={{ display: "flex", alignItems: "center", justifyItems: "center", justifyContent: "center", gap: "8px", width: "100%", marginTop: "4px" }}>
                <Activity size={16} />
                {isTriggering ? "Spawning..." : "Spawn Onboarding Flow"}
              </button>
            </form>
          </section>

          {/* Executions List Panel */}
          <section className="glass-panel" style={{ padding: "20px", display: "flex", flexDirection: "column", minHeight: "450px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
              <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 600 }}>Active Executions</h2>
              <span style={{ fontSize: "12px", color: "#64748b", background: "rgba(255,255,255,0.05)", padding: "2px 8px", borderRadius: "10px" }}>{executions.length} total</span>
            </div>

            <div style={{ display: "flex", alignItems: "center", gap: "8px", background: "rgba(0,0,0,0.2)", padding: "6px 12px", borderRadius: "8px", border: "1px solid rgba(255,255,255,0.05)", marginBottom: "12px" }}>
              <Search size={14} color="#64748b" />
              <input 
                type="text" 
                placeholder="Search by id or name..." 
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                style={{ background: "none", border: "none", color: "white", fontSize: "13px", outline: "none", width: "100%" }}
              />
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "8px", overflowY: "auto", maxHeight: "400px", paddingRight: "4px" }}>
              {filteredExecutions.length === 0 ? (
                <div style={{ textAlign: "center", padding: "40px 0", color: "#64748b" }}>
                  <Database size={24} style={{ marginBottom: "8px", opacity: 0.5 }} />
                  <p style={{ margin: 0, fontSize: "13px" }}>No executions found</p>
                </div>
              ) : (
                filteredExecutions.map(exec => {
                  const isSelected = exec.id === selectedId;
                  const statusColors = {
                    RUNNING: { bg: "rgba(99, 102, 241, 0.15)", border: "rgba(99, 102, 241, 0.4)", text: "#818cf8" },
                    SUSPENDED: { bg: "rgba(245, 158, 11, 0.15)", border: "rgba(245, 158, 11, 0.4)", text: "#fbbf24" },
                    COMPLETED: { bg: "rgba(16, 185, 129, 0.15)", border: "rgba(16, 185, 129, 0.4)", text: "#34d399" },
                    FAILED: { bg: "rgba(239, 68, 68, 0.15)", border: "rgba(239, 68, 68, 0.4)", text: "#f87171" }
                  };
                  const color = statusColors[exec.status] || statusColors.RUNNING;

                  return (
                    <div 
                      key={exec.id} 
                      onClick={() => setSelectedId(exec.id)}
                      style={{ 
                        padding: "12px", 
                        borderRadius: "10px", 
                        background: isSelected ? "rgba(99, 102, 241, 0.08)" : "rgba(255,255,255,0.02)", 
                        border: `1px solid ${isSelected ? "rgba(99, 102, 241, 0.4)" : "rgba(255,255,255,0.04)"}`,
                        cursor: "pointer",
                        transition: "all 0.2s"
                      }}
                    >
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
                        <span style={{ fontWeight: 600, fontSize: "14px", color: isSelected ? "#a5b4fc" : "#e2e8f0" }}>
                          {exec.workflowName}
                        </span>
                        <span style={{ 
                          fontSize: "10px", 
                          fontWeight: 700, 
                          padding: "2px 8px", 
                          borderRadius: "20px", 
                          backgroundColor: color.bg, 
                          color: color.text, 
                          border: `1px solid ${color.border}` 
                        }}>
                          {exec.status}
                        </span>
                      </div>
                      <div style={{ display: "flex", justifyContent: "space-between", fontSize: "11px", color: "#64748b" }}>
                        <span style={{ fontFamily: "monospace" }}>{exec.id.substring(0, 12)}...</span>
                        <span>{new Date(exec.createdAt).toLocaleTimeString()}</span>
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </section>
        </div>

        {/* Right Side: Detailed Workflow Visual Trace and Payloads */}
        <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
          
          {/* Main Visualizer Panel */}
          {selectedExecution ? (
            <>
              <section className="glass-panel" style={{ padding: "24px" }}>
                {/* Meta details */}
                <div style={{ display: "flex", justifyContent: "space-between", borderBottom: "1px solid rgba(255,255,255,0.05)", paddingBottom: "16px", marginBottom: "20px" }}>
                  <div>
                    <span style={{ fontSize: "12px", color: "#6366f1", fontWeight: 600, textTransform: "uppercase", letterSpacing: "1px" }}>Execution Details</span>
                    <h2 style={{ margin: "4px 0 0 0", fontSize: "20px", fontWeight: 700, fontFamily: "monospace" }}>{selectedExecution.id}</h2>
                  </div>
                  <div style={{ textAlign: "right" }}>
                    <span style={{ fontSize: "12px", color: "#64748b" }}>Triggered</span>
                    <div style={{ fontSize: "14px", color: "#cbd5e1", marginTop: "2px" }}>
                      {new Date(selectedExecution.createdAt).toLocaleString()}
                    </div>
                  </div>
                </div>

                {/* SVG/HTML visual step timeline */}
                <h3 style={{ margin: "0 0 16px 0", fontSize: "14px", color: "#94a3b8", textTransform: "uppercase", letterSpacing: "0.5px" }}>Visual Workflow Replay Log</h3>
                
                <div className="timeline-track" style={{ display: "flex", flexDirection: "column", gap: "16px", position: "relative" }}>
                  
                  {/* Step list mapping */}
                  {history.length === 0 ? (
                    <div style={{ padding: "24px", color: "#64748b", fontStyle: "italic", fontSize: "13px" }}>
                      No replay steps recorded in history yet. Replay starting...
                    </div>
                  ) : (
                    history.map((step) => {
                      const typeConfig = {
                        run: { label: "Activity (run)", bg: "rgba(16, 185, 129, 0.1)", border: "rgba(16, 185, 129, 0.3)", text: "#34d399", icon: <Database size={13} /> },
                        sleep: { label: "Timer (sleep)", bg: "rgba(99, 102, 241, 0.1)", border: "rgba(99, 102, 241, 0.3)", text: "#818cf8", icon: <Clock size={13} /> },
                        event: { label: "Webhook (wait)", bg: "rgba(245, 158, 11, 0.1)", border: "rgba(245, 158, 11, 0.3)", text: "#fbbf24", icon: <Inbox size={13} /> }
                      }[step.stepType];

                      const statusConfig = {
                        PENDING: { label: "Awaiting", bg: "rgba(245, 158, 11, 0.15)", text: "#fbbf24" },
                        COMPLETED: { label: "Cached / Success", bg: "rgba(16, 185, 129, 0.15)", text: "#34d399" },
                        FAILED: { label: "Failed / Timeout", bg: "rgba(239, 68, 68, 0.15)", text: "#f87171" }
                      }[step.status];

                      return (
                        <div 
                          key={step.id} 
                          style={{ 
                            marginLeft: "36px", 
                            padding: "16px", 
                            background: "rgba(255,255,255,0.015)", 
                            border: "1px solid rgba(255,255,255,0.04)", 
                            borderRadius: "10px", 
                            display: "grid", 
                            gridTemplateColumns: "1fr auto", 
                            gap: "16px", 
                            alignItems: "center",
                            position: "relative"
                          }}
                        >
                          {/* Left node dot marker */}
                          <div style={{ 
                            position: "absolute", 
                            left: "-38px", 
                            top: "22px", 
                            width: "12px", 
                            height: "12px", 
                            borderRadius: "50%", 
                            backgroundColor: step.status === "COMPLETED" ? "#10b981" : step.status === "PENDING" ? "#fb923c" : "#f87171",
                            border: "3px solid #0b0f19",
                            zIndex: 2,
                            boxShadow: step.status === "PENDING" ? "0 0 10px #fb923c" : "none"
                          }}></div>

                          {/* Step Main Info */}
                          <div>
                            <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "4px" }}>
                              <span style={{ fontSize: "11px", color: "#64748b", fontFamily: "monospace", background: "rgba(0,0,0,0.3)", padding: "1px 6px", borderRadius: "4px" }}>
                                INDEX {step.stepIndex}
                              </span>
                              <span style={{ 
                                display: "flex", 
                                alignItems: "center", 
                                gap: "4px", 
                                fontSize: "10px", 
                                padding: "2px 6px", 
                                borderRadius: "4px", 
                                backgroundColor: typeConfig.bg, 
                                color: typeConfig.text, 
                                border: `1px solid ${typeConfig.border}`,
                                fontWeight: 600
                              }}>
                                {typeConfig.icon}
                                {typeConfig.label}
                              </span>
                              <span style={{ fontSize: "11px", color: statusConfig.text, fontWeight: 500 }}>
                                • {statusConfig.label}
                              </span>
                            </div>
                            
                            <h4 style={{ margin: "4px 0", fontSize: "15px", fontWeight: 600, color: "#f1f5f9" }}>
                              {step.stepName}
                            </h4>

                            {/* Render step details (output or sleep timer details) */}
                            {step.status === "COMPLETED" && step.result && (
                              <div style={{ marginTop: "8px", fontSize: "12px", color: "#94a3b8", background: "rgba(0,0,0,0.15)", padding: "8px 12px", borderRadius: "6px", border: "1px solid rgba(255,255,255,0.03)", fontFamily: "monospace", overflowX: "auto" }}>
                                <div style={{ color: "#64748b", fontSize: "10px", marginBottom: "2px", fontWeight: 600, textTransform: "uppercase" }}>CACHED VALUE</div>
                                {typeof step.result === "object" ? JSON.stringify(step.result, null, 2) : String(step.result)}
                              </div>
                            )}

                            {step.status === "PENDING" && step.stepType === "sleep" && step.resumeAt && (
                              <div style={{ marginTop: "6px", fontSize: "12px", color: "#f59e0b", display: "flex", alignItems: "center", gap: "6px" }}>
                                <Clock size={12} />
                                Sleeping until: {new Date(step.resumeAt).toLocaleTimeString()} (Tick cron to wake up)
                              </div>
                            )}

                            {step.status === "PENDING" && step.stepType === "event" && (
                              <div style={{ marginTop: "12px" }}>
                                <div style={{ marginBottom: "6px" }}>
                                  <label style={{ fontSize: "11px", color: "#64748b", display: "block", marginBottom: "4px" }}>WEBHOOK INPUT (JSON)</label>
                                  <textarea 
                                    value={webhookPayload}
                                    onChange={(e) => setWebhookPayload(e.target.value)}
                                    rows={3}
                                    style={{ width: "90%", background: "rgba(0,0,0,0.3)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: "6px", color: "#cbd5e1", padding: "6px 10px", fontFamily: "monospace", fontSize: "12px", resize: "none" }}
                                  />
                                </div>
                                <button 
                                  onClick={() => handleSendWebhook(selectedExecution.id, step.stepName)}
                                  className="btn-primary" 
                                  style={{ padding: "6px 12px", fontSize: "12px", display: "inline-flex", alignItems: "center", gap: "6px" }}
                                >
                                  <Send size={12} />
                                  Trigger Stripe Webhook
                                </button>
                              </div>
                            )}
                          </div>

                          {/* Right side: step timestamp */}
                          <div style={{ fontSize: "11px", color: "#475569" }}>
                            {new Date(step.createdAt).toLocaleTimeString()}
                          </div>

                        </div>
                      );
                    })
                  )}

                </div>
              </section>

              {/* Input/Output Data Panel */}
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "24px" }}>
                
                {/* Inputs */}
                <section className="glass-panel" style={{ padding: "20px" }}>
                  <h3 style={{ margin: "0 0 12px 0", fontSize: "14px", color: "#94a3b8", textTransform: "uppercase", display: "flex", alignItems: "center", gap: "6px" }}>
                    <User size={14} color="#6366f1" />
                    Workflow Input Payload
                  </h3>
                  <pre style={{ margin: 0, padding: "12px", background: "rgba(0,0,0,0.2)", borderRadius: "8px", border: "1px solid rgba(255,255,255,0.05)", fontSize: "12px", fontFamily: "monospace", overflowX: "auto" }}>
                    {JSON.stringify(selectedExecution.input, null, 2)}
                  </pre>
                </section>

                {/* Outputs/Errors */}
                <section className="glass-panel" style={{ padding: "20px" }}>
                  <h3 style={{ margin: "0 0 12px 0", fontSize: "14px", color: "#94a3b8", textTransform: "uppercase", display: "flex", alignItems: "center", gap: "6px" }}>
                    {selectedExecution.status === "FAILED" ? (
                      <>
                        <AlertCircle size={14} color="#ef4444" />
                        Execution Error
                      </>
                    ) : (
                      <>
                        <CheckCircle2 size={14} color="#10b981" />
                        Workflow Output Result
                      </>
                    )}
                  </h3>
                  <pre style={{ 
                    margin: 0, 
                    padding: "12px", 
                    background: selectedExecution.status === "FAILED" ? "rgba(239, 68, 68, 0.05)" : "rgba(0,0,0,0.2)", 
                    borderRadius: "8px", 
                    border: `1px solid ${selectedExecution.status === "FAILED" ? "rgba(239, 68, 68, 0.2)" : "rgba(255,255,255,0.05)"}`, 
                    fontSize: "12px", 
                    fontFamily: "monospace", 
                    color: selectedExecution.status === "FAILED" ? "#f87171" : "#e2e8f0",
                    overflowX: "auto" 
                  }}>
                    {selectedExecution.status === "COMPLETED" && selectedExecution.output && JSON.stringify(selectedExecution.output, null, 2)}
                    {selectedExecution.status === "FAILED" && selectedExecution.error && JSON.stringify(selectedExecution.error, null, 2)}
                    {selectedExecution.status === "RUNNING" && '"Workflow is currently replaying activities..."'}
                    {selectedExecution.status === "SUSPENDED" && '"Workflow is currently suspended (waiting for timer/event)"'}
                    {!selectedExecution.output && !selectedExecution.error && selectedExecution.status === "COMPLETED" && '"null"'}
                  </pre>
                </section>

              </div>
            </>
          ) : (
            <div className="glass-panel" style={{ padding: "48px", textAlign: "center", color: "#64748b" }}>
              <Activity size={36} style={{ marginBottom: "12px", opacity: 0.5 }} className="animate-spin-slow" />
              <h3>Select a workflow execution</h3>
              <p style={{ margin: 0, fontSize: "14px" }}>Click an execution card from the left panel to inspect step history and trigger timers or events.</p>
            </div>
          )}

        </div>
      </div>
      
    </div>
  );
}
