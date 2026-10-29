import { Type } from "@earendil-works/pi-ai";

// Thin native adapter. The shared Memento API owns scheduling, evidence and writes.
const TOOL = "memento_work";
const options = JSON.parse(process.env.MEMENTO_WORKER_OPTIONS || "{}");
const initialPool = JSON.parse(process.env.MEMENTO_WORKER_POOL || "{}");
const url = process.env.MEMENTO_WORKER_URL;
if (!url || !options.agent || !options.timezone) throw new Error("Missing worker configuration");
// RPC reserves stdout; host consumes only these framed metadata records on stderr.
const emit = (value: object) => console.error(JSON.stringify(value));

export default function worker(pi: any) {
  pi.registerTool({
    name: TOOL, label: "Memento work", description:
      "Inspect the full eligible memory pool, claim bounded work, and submit exact-schema output through Memento. " +
      "Actions pool, claim, submit, complete, status, release, renew. Pool command:{kind,after?,limit?}. " +
      "Claim command:{kinds:[extract|consolidate],batch_size?:1..128}. " +
      "Submit command:{complete:{id,output},next?:{kinds:[KIND],batch_size:N}}. " +
      "Complete command:{id,output}. Status/release/renew command:{id}. " +
      "Identity, schedule, model metadata and private lease tokens are supplied by the host.",
    parameters: Type.Object({ action: Type.Union(["pool","claim","submit","complete","status","release","renew"].map(value => Type.Literal(value))),
      command: Type.Any() }),
    async execute(_id: string, params: any, signal: AbortSignal) {
      const response = await fetch(url + "/work", { method: "POST", signal,
        headers: { "Content-Type": "application/json" }, body: JSON.stringify(params) });
      const text = await response.text();
      if (text.length > 8 * 1024 * 1024) throw new Error("Processing response exceeds limit");
      if (!response.ok) {
        let reason = "processing_request_failed";
        try { const value = JSON.parse(text); if (typeof value.detail === "string") reason = value.detail; } catch {}
        throw new Error("MEMENTO_HTTP_" + response.status + ": " + reason);
      }
      return { content: [{ type: "text", text }], details: undefined };
    },
  });
  let current: any[] = [], last: any = {}, model: string | undefined, usage: any;
  let completed = 0;
  let contextStart = 0, resetContext = true;
  const cooling = new Map<string, number>();
  const claims = new Map<string, any>();
  // Host-computed due kinds (window and pool) seed availability; later receipts update it.
  const dueKinds: string[] = JSON.parse(process.env.MEMENTO_WORKER_KINDS || '["extract","consolidate"]');
  const emptyKinds = new Set<string>(["extract", "consolidate"].filter(kind => !dueKinds.includes(kind)));
  function claim(command: any) {
    if (current.length) throw new Error("Complete or release current work first");
    if (!Array.isArray(command.kinds) || command.kinds.length !== 1 || !["extract", "consolidate"].includes(command.kinds[0]))
      throw new Error("Choose one work kind");
    if (emptyKinds.has(command.kinds[0])) throw new Error("That work kind is empty; try the other kind or finish");
    const now = Date.now();
    for (const [id, until] of cooling) if (until <= now) cooling.delete(id);
    return { ...options, kinds: command.kinds, batch_size: command.batch_size ?? 64,
      continuous: true, exclude: [...cooling.keys()] };
  }
  function completion(command: any) {
    const leased = claims.get(command.id);
    if (!leased) throw new Error("Work identifier is not held by this worker");
    return { id: command.id, owner: leased.owner, agent: options.agent,
      output: command.output, model, usage };
  }
  pi.on("session_start", async () => {
    // Rotation keeps the last service availability; only a receipt reopens a kind.
    current = []; last = initialPool; completed = 0; claims.clear();
    contextStart = 0; resetContext = true;
    const until = Date.now() + 15000;
    while (!pi.getActiveTools().includes(TOOL) && Date.now() < until)
      await new Promise(resolve => setTimeout(resolve, 25));
    emit({ type: "memento_worker_catalog", active: pi.getActiveTools(),
      all: pi.getAllTools().map((tool: any) => ({ name: tool.name, exposure: tool.exposure,
        reachesTools: !!tool.reachesTools })) });
  });
  pi.on("message_end", (event: any) => {
    if (event.message?.role === "assistant") {
      model = event.message.model;
      usage = event.message.usage;
      const numeric: any = {};
      for (const key of ["input","output","cacheRead","cacheWrite","totalTokens"])
        if (typeof usage?.[key] === "number") numeric[key] = usage[key];
      emit({ type: "memento_worker_model", model, usage: numeric, stop_reason: event.message.stopReason });
    }
  });
  pi.on("tool_call", (event: any) => {
    if (event.toolName !== TOOL) return { block: true, reason: "Only Memento work is permitted" };
    const input = event.input;
    try {
      if (input.action === "claim") input.command = claim(input.command);
      else if (input.action === "submit") {
        const next = input.command.next;
        input.command = { complete: completion(input.command.complete) };
        // One safe session boundary after eight durable commits. No next lease.
        if (next && completed < 7 && !emptyKinds.has(next.kinds?.[0])) {
          const saved = current; current = [];
          try { input.command.next = claim(next); } finally { current = saved; }
        }
      } else if (input.action === "complete") input.command = completion(input.command);
      else if (["release", "renew"].includes(input.action)) {
        const leased = claims.get(input.command.id);
        if (!leased) throw new Error("Work identifier is not held by this worker");
        input.command = { id: leased.id, owner: leased.owner,
          ...(input.action === "release" ? { error: "service_rejected" } : {}) };
      } else if (!["pool", "status"].includes(input.action)) throw new Error("Unknown processing action");
    } catch (error: any) {
      const causes: Record<string,string> = {
        "Complete or release current work first":"current_work_held",
        "Choose one work kind":"invalid_kind",
        "That work kind is empty; try the other kind or finish":"empty_kind",
        "Work identifier is not held by this worker":"foreign_job",
        "Unknown processing action":"unknown_action",
      };
      const cause = causes[error?.message] || "invalid_command";
      last = { error: "invalid_or_foreign_work", cause, action: ["pool","claim","submit","complete","status","release","renew"].includes(input.action) ? input.action : "unknown" };
      emit({ type: "memento_worker_state", error: true, code: "invalid_or_foreign_work", cause,
        action: ["pool","claim","submit","complete","status","release","renew"].includes(input.action) ? input.action : "unknown" });
      return { block: true, reason: "Invalid or foreign processing work; use the current work and supplied schema" };
    }
  });
  pi.on("tool_result", (event: any) => {
    if (event.toolName !== TOOL) return;
    let value: any;
    try { value = JSON.parse(event.content.filter((part: any) => part.type === "text").map((part: any) => part.text).join("\n")); }
    catch {
      last = { error: "processing_request_failed", detail: event.content };
      const text = event.content.filter((part: any) => part.type === "text").map((part: any) => part.text).join("\n");
      // Bounded service detail only; the host logs it solely when it is an allowlisted fixed code.
      const failure = text.match(/MEMENTO_HTTP_(\d{3}): ([^\n]{1,160})/);
      emit({ type: "memento_worker_state", error: true, code: failure?.[2],
        action: ["pool","claim","submit","complete","status","release","renew"].includes(event.input?.action) ? event.input.action : "unknown",
        http_status: Number(failure?.[1] ?? text.match(/MEMENTO_HTTP_(\d{3})/)?.[1]) || undefined });
      return;
    }
    const action = event.input.action;
    const completedId = value.completed || (action === "complete" && !event.isError ? event.input.command.id : null)
      || (action === "status" && value.state === "done" && claims.has(value.id) ? value.id : null);
    if (completedId) {
      claims.delete(completedId); current = current.filter(item => item.id !== completedId);
      cooling.set(completedId, Date.now() + 900000); completed++; resetContext = true;
    }
    if (action === "release" && value.released) {
      claims.delete(event.input.command.id); current = [];
      cooling.set(event.input.command.id, Date.now() + 900000); resetContext = true;
    }
    const offered = action === "claim" ? value : value.next;
    const held: any[] = [];
    if (offered && Array.isArray(offered.work)) {
      for (const item of offered.work) {
        claims.set(item.id, { id: item.id, owner: item.owner, kind: item.kind });
        held.push({ id: item.id, owner: item.owner, kind: item.kind });
        delete item.owner;
        // Fixed policies are already in the system prefix; do not repeat them per unit.
        delete item.instruction; delete item.schema;
      }
      current = offered.work;
      const requested = action === "claim" ? event.input.command.kinds?.[0] : event.input.command.next?.kinds?.[0];
      // The requested kind is known only by its own claim; the other kind follows the
      // service's post-scheduling claimable count (window, cooldown and exclusions).
      for (const kind of ["extract", "consolidate"]) {
        const count = offered.backlog?.claimable?.[kind];
        if (kind === requested) current.length ? emptyKinds.delete(kind) : emptyKinds.add(kind);
        else if (typeof count === "number") count > 0 ? emptyKinds.delete(kind) : emptyKinds.add(kind);
      }
    }
    if (offered) delete offered.work;
    last = value;
    emit({ type: "memento_worker_state", claimed: held, lease_seconds: offered?.lease_seconds,
      kind: action === "claim" ? event.input.command.kinds?.[0] : event.input.command.next?.kinds?.[0],
      empty_kinds: [...emptyKinds],
      completed: completedId, released: action === "release" && value.released ? event.input.command.id : null,
      boundary: completed >= 8 && !current.length, empty: !!offered && !current.length });
    return { content: [{ type: "text", text: JSON.stringify({ current_work: current.map(item => ({ id: item.id, kind: item.kind })), receipt: last,
      empty_kinds: [...emptyKinds],
      session_boundary: completed >= 8 && !current.length }) }], structuredContent: undefined };
  });
  pi.on("context", (event: any = { messages: [] }) => {
    // Keep native reasoning/tool history within a unit. Dropping it after every
    // tool creates amnesia loops; clear completed work only at a verified boundary.
    if (resetContext) { contextStart = event.messages.length; resetContext = false; }
    return { messages: [{ role: "user", timestamp: Date.now(),
      content: [{ type: "text", text: JSON.stringify({ current, receipt: last, empty_kinds: [...emptyKinds] }) }] },
      ...event.messages.slice(contextStart).map((message: any) => {
        if (message.role !== "assistant" || !Array.isArray(message.content)) return message;
        return { ...message, content: message.content.map((block: any) => {
          if (block.type !== "toolCall" || block.name !== TOOL || !block.arguments?.command) return block;
          const command = { ...block.arguments.command }; delete command.owner;
          if (command.complete) { command.complete = { ...command.complete }; delete command.complete.owner; }
          return { ...block, arguments: { ...block.arguments, command } };
        }) };
      })] };
  });
}
