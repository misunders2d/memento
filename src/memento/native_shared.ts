import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { Type as SchemaType, TSchema } from "typebox";

// Only the common, public extension surface. Harness-specific imports and schema
// builders stay in the two native entrypoints; state belongs to each factory.
type RecordValue = Record<string, unknown>;
type Message = RecordValue & { role?: string; timestamp?: number; toolCallId?: string; content?: unknown };
type Entry = RecordValue & { id: string; message?: Message };
type NativeEvent = { message?: Message; prompt?: string; messages: Message[];
  toolName?: string; input?: unknown; isError?: boolean };
type Context = { cwd: string; sessionManager: {
  getSessionId(): string;
  getSessionFile(): string | undefined;
  getEntries(): Entry[];
} };
export type NativeAPI = {
  on(event: string, handler: (event: NativeEvent, ctx: Context) => unknown): unknown;
  registerFlag(name: string, options: { type: "string"; description: string }): void;
  getFlag(name: string): string | boolean | undefined;
  registerTool(tool: {
    name: string; label: string; description: string; parameters: TSchema;
    loadMode?: "essential"; approval?: "read" | "write";
    execute(id: string, params: RecordValue, signal: AbortSignal | undefined, update: unknown, ctx: Context):
      Promise<{ content: { type: "text"; text: string }[]; details: RecordValue }>;
  }): void;
};
type Loader = { config: string; bridge: string; python?: string };
type Live = { entry_id: string; message: Message; committed: boolean };

function textOf(message: Message): string {
  if (typeof message.content === "string") return message.content;
  if (!Array.isArray(message.content)) return "";
  return message.content.filter((block: unknown): block is { type: "text"; text: string } =>
    typeof block === "object" && block !== null && "type" in block && block.type === "text"
      && "text" in block && typeof block.text === "string")
    .map((block) => block.text).join("\n");
}

function messageKey(message: Message): string | undefined {
  // Timestamps alone are not entry IDs. This key is only used when exactly one
  // committed message matches an observed live snapshot.
  if (message.timestamp === undefined || typeof message.role !== "string") return;
  return JSON.stringify([message.role, message.timestamp, message.toolCallId ?? null]);
}

function parseObject(text: string): RecordValue {
  const value: unknown = JSON.parse(text);
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Memento expected a JSON object");
  }
  return value as RecordValue;
}

function load(api: NativeAPI): Loader {
  let defaults: Partial<Loader> = {};
  try {
    const value = parseObject(readFileSync(new URL("./memento-config.json", import.meta.url), "utf8"));
    defaults = {
      ...(typeof value.config === "string" ? { config: value.config } : {}),
      ...(typeof value.bridge === "string" ? { bridge: value.bridge } : {}),
      ...(typeof value.python === "string" ? { python: value.python } : {}),
    };
  } catch (error: unknown) {
    if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
  }
  const flag = api.getFlag("memento-config");
  const config = typeof flag === "string" ? flag : process.env.MEMENTO_CONFIG ?? defaults.config;
  if (!config) throw new Error("Memento requires --memento-config or its installed memento-config.json");
  return {
    config,
    bridge: process.env.MEMENTO_BRIDGE ?? defaults.bridge ?? fileURLToPath(new URL("./bridge.py", import.meta.url)),
    python: process.env.MEMENTO_PYTHON ?? defaults.python ?? "python3",
  };
}

const TOOL_FAILURES: Record<string, string> = {
  timeout: "Memento tool failed (timeout). Check service responsiveness, then retry the same tool call.",
  network: "Memento tool failed (connection). Check the configured service connection, then retry the same tool call.",
  lock: "Memento tool failed (local lock). Wait for local capture to finish, then retry the same tool call.",
  validation: "Memento tool failed (invalid data). Check tool arguments and bridge configuration, then retry the same tool call.",
  invalid_response: "Memento tool failed (invalid response). Check the configured service and bridge installation, then retry the same tool call.",
  response_too_large: "Memento tool failed (response too large). Narrow the request, then retry the same tool call.",
};

function toolFailure(output: string): Error {
  try {
    const value = parseObject(output);
    const detail = value.diagnostic;
    if (value.error === "bridge_failed" && detail !== null && typeof detail === "object" && !Array.isArray(detail)) {
      const { category, http_status } = detail as RecordValue;
      if (category === "http" && typeof http_status === "number" && Number.isInteger(http_status) && http_status >= 100 && http_status <= 599) {
        return new Error(`Memento tool failed (HTTP ${http_status}). Check the configured service and tool arguments, then retry the same tool call.`);
      }
      if (typeof category === "string" && Object.hasOwn(TOOL_FAILURES, category)) return new Error(TOOL_FAILURES[category]);
    }
  } catch { /* Legacy or malformed diagnostics retain the safe fallback. */ }
  return new Error("Memento bridge failed");
}

function bridge(loader: Loader, input: RecordValue, timeout: number, signal?: AbortSignal): Promise<RecordValue> {
  const { promise, resolve, reject } = Promise.withResolvers<RecordValue>();
  if (signal?.aborted) {
    reject(new Error("Memento request aborted"));
    return promise;
  }
  let child: ChildProcessWithoutNullStreams;
  try {
    child = spawn(loader.python!, [loader.bridge, "native", "--config", loader.config], {
      stdio: ["pipe", "pipe", "pipe"], windowsHide: true,
    });
  } catch {
    reject(new Error("Memento bridge could not start. Check the configured Python and bridge installation."));
    return promise;
  }
    let output = "";
    let settled = false;
    const finish = (error?: Error, result?: RecordValue) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      if (error) reject(error); else resolve(result!);
    };
    const abort = () => { child.kill(); finish(new Error("Memento request aborted")); };
    const timer = setTimeout(() => { child.kill(); finish(new Error(input.action === "tool" ? TOOL_FAILURES.timeout : "Memento bridge deadline exceeded")); }, timeout);
    signal?.addEventListener("abort", abort, { once: true });
    child.on("error", () => finish(new Error("Memento bridge could not start. Check the configured Python and bridge installation.")));
    // Never echo captured prompts, native records or subprocess diagnostics.
    child.stderr.resume();
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (part: string) => {
      output += part;
      if (output.length > 2_000_000) { child.kill(); finish(new Error(input.action === "tool" ? TOOL_FAILURES.response_too_large : "Memento bridge response too large")); }
    });
    child.stdin.on("error", () => finish(new Error("Memento bridge input failed. Check the bridge installation, then retry the same tool call.")));
    child.on("close", (code) => {
      if (settled) return;
      if (code !== 0) return finish(input.action === "tool" ? toolFailure(output) : new Error("Memento bridge failed"));
      try {
        const value = input.action === "tool" ? { result: JSON.parse(output || "null") as unknown }
          : parseObject(output || "{}");
        finish(undefined, value);
      }
      catch { finish(new Error(input.action === "tool" ? TOOL_FAILURES.invalid_response : "Memento bridge returned invalid JSON")); }
    });
    child.stdin.end(JSON.stringify(input));
  return promise;
}

export function installNative(api: NativeAPI, Type: typeof SchemaType, harness: "pi" | "omp"): void {
  if (process.env.MEMENTO_PROCESSING === "1") return;
  api.registerFlag("memento-config", { type: "string", description: "Private Memento runner configuration path" });
  let loader: Loader;
  let current: Context | undefined;
  let session = "";
  let sent = new Set<string>();
  let live: Live[] = [];
  let sentLive = new Set<string>();
  let cache = "";
  let shown: string[] = [];
  let query = "";
  let injected = false;
  let generation = 0;
  let recallGeneration = 0;
  let pendingRecall: { turn: string; token: string; deadline: number } | undefined;
  let activeTurn = "";
  let stepEpoch = 0;
  let pendingStep: { token: string; deadline: number } | undefined;
  let startingStep: Promise<void> | undefined;
  let stepCache = "";
  let stepExpires = 0;
  let stepStarted = false;
  let capturePending = false;
  let captureAgain = false;
  let warned = false;
  let recallWarned = false;

  function warnRecall(): void {
    if (!recallWarned) {
      recallWarned = true;
      console.error("Memento passive recall is unavailable; continuing without fresh memory context. Check the private bridge configuration.");
    }
  }

  function warn(): void {
    if (!warned) {
      warned = true;
      console.error("Memento capture is pending; committed journal reconciliation will retry. Check the private bridge configuration.");
    }
  }
  function bind(ctx: Context): void {
    // Native contexts expire at shutdown/reload. Retain the public manager, not
    // its guarded context getter, for detached journal reconciliation.
    const manager = ctx.sessionManager;
    const id = manager.getSessionId();
    current = { cwd: ctx.cwd, sessionManager: manager };
    if (id === session) return;
    session = id;
    generation++;
    sent = new Set();
    sentLive = new Set();
    live = [];
    cache = "";
    shown = [];
    query = "";
    injected = false;
    pendingRecall = undefined;
    clearStep();
  }
  function clearStep(): void {
    activeTurn = "";
    stepEpoch++;
    pendingStep = undefined;
    startingStep = undefined;
    stepCache = "";
    stepExpires = 0;
    stepStarted = false;
  }
  function envelope(action: string, ctx: Context): RecordValue {
    return { harness, session_id: ctx.sessionManager.getSessionId(),
      session_file: ctx.sessionManager.getSessionFile(), cwd: ctx.cwd, action };
  }
  function capture(ctx: Context): void {
    bind(ctx);
    if (!loader) return;
    if (capturePending) { captureAgain = true; return; }
    capturePending = true;
    const epoch = generation;
    const manager = current!.sessionManager;
    const base = envelope("capture", current!);
    const passive = injected ? { query, shown: [...shown] } : undefined;
    // Defer serialization/spooling until after the native notification returns.
    setTimeout(() => {
      if (epoch !== generation) { capturePending = false; captureAgain = false; if (current) capture(current); return; }
      const entries = manager.getEntries();
      const pending = entries.filter((entry) => !sent.has(entry.id));
      const byKey = new Map<string, Entry[]>();
      for (const entry of live.length ? entries : []) {
        if (!entry.message) continue;
        const key = messageKey(entry.message);
        if (!key) continue;
        const matches = byKey.get(key);
        if (matches) matches.push(entry); else byKey.set(key, [entry]);
      }
      const observations: Live[] = [];
      for (const item of live) {
        const key = messageKey(item.message);
        const matches = key ? byKey.get(key) : undefined;
        if (!item.committed && matches?.length === 1) {
          item.entry_id = matches[0].id;
          item.committed = true;
        }
        const identity = `${item.committed}:${item.entry_id}`;
        if (!sentLive.has(identity)) observations.push(item);
      }
      const observationIds = observations.map((item) => `${item.committed}:${item.entry_id}`);
      if (!pending.length && !observations.length) {
        capturePending = false;
        return;
      }
      const request = { ...base, entries: pending,
        live: observations,
        ...(passive ? { passive } : {}), };
      void bridge(loader, request, 5000).then(() => {
        if (epoch !== generation) return;
        for (const entry of pending) sent.add(entry.id);
        for (const identity of observationIds) sentLive.add(identity);
        live = live.filter((item) => !item.committed);
        warned = false;
      }).catch(warn).finally(() => {
        capturePending = false;
        if (captureAgain) { captureAgain = false; if (current) capture(current); }
      });
    }, 0);
  }

  api.on("session_start", (_event, ctx) => {
    loader = load(api);
    bind(ctx);
    capture(ctx);
  });
  function applyRecall(result: RecordValue): void {
    if (typeof result.update_notice === "string" && result.update_notice) console.error(result.update_notice);
    if (result.error) warnRecall(); else if (!result.pending) recallWarned = false;
    if (typeof result.query === "string") query = result.query;
    // Keep complete conditional suggestions together under the bridge budget.
    const source = [typeof result.update_notice === "string" ? result.update_notice : "",
                    typeof result.context === "string" ? result.context : ""].filter(Boolean).join("\n");
    cache = Buffer.byteLength(source, "utf8") <= 1500 ? source : "";
    shown = Array.isArray(result.shown) ? result.shown.filter((id: unknown): id is string =>
      typeof id === "string" && cache.includes(id)) : [];
  }
  async function refresh(ctx: Context, prompt: string): Promise<void> {
    // Peer envelopes are captured, but the most recent human request remains
    // the automatic-recall anchor until another actual human prompt arrives.
    if (prompt.trimStart().startsWith("[HERDR AGENT MESSAGE]")) return;
    const epoch = generation;
    const recallEpoch = ++recallGeneration;
    const turn = randomUUID();
    clearStep();
    activeTurn = turn;
    const deadline = Date.now() + 16000; // Fifteen-second worker lifetime plus startup slack.
    pendingRecall = undefined;
    query = prompt;
    cache = "";
    shown = [];
    injected = false;
    if (!query || !loader) return;
    try {
      const result = await bridge(loader, { ...envelope("recall", ctx), query, turn }, 4000);
      if (epoch !== generation || recallEpoch !== recallGeneration) return;
      if (typeof result.recall_token === "string") pendingRecall = { turn, token: result.recall_token, deadline };
      applyRecall(result);
    } catch { warnRecall(); /* Passive recall failure must not block a native turn. */ }
  }
  api.on("before_agent_start", async (event, ctx) => {
    bind(ctx);
    capture(ctx);
    await refresh(ctx, typeof event.prompt === "string" ? event.prompt : textOf(event.message ?? {}));
  });
  function startStep(event: NativeEvent, ctx: Context, failed = false): void {
    bind(ctx);
    if (!activeTurn || !loader || !event.toolName || /memento/i.test(event.toolName)) return;
    if (startingStep) {
      const epoch = stepEpoch;
      if (failed) void startingStep.then(() => { if (epoch === stepEpoch) startStep(event, ctx, true); });
      return;
    }
    const turn = activeTurn, epoch = stepEpoch;
    stepStarted = true;
    // Start only local scheduling here. HTTP runs in the existing detached
    // worker, while the native tool proceeds with its original arguments.
    const request = bridge(loader, { ...envelope("step-recall", ctx), turn,
      tool_name: event.toolName, arguments: event.input ?? {}, failed }, 1000).then(result => {
      if (epoch !== stepEpoch || turn !== activeTurn) return;
      if (typeof result.step_token === "string" && result.pending) {
        pendingStep = { token: result.step_token, deadline: Date.now() + 16000 };
        if (result.scheduled) { stepCache = ""; stepExpires = 0; }
      }
    }).catch(() => { /* Local contention skips this action; native work continues. */ });
    startingStep = request;
    void request.finally(() => { if (startingStep === request) startingStep = undefined; });
  }
  api.on("tool_call", (event, ctx) => startStep(event, ctx));
  api.on("tool_result", (event, ctx) => { if (event.isError) startStep(event, ctx, true); });
  api.on("context", async (event, ctx) => {
    bind(ctx);
    if (pendingRecall && Date.now() >= pendingRecall.deadline) pendingRecall = undefined;
    const epoch = generation, recallEpoch = recallGeneration, pending = pendingRecall;
    if (pending && loader) {
      try {
        // Local claim only: the human lookup keeps its delivery priority.
        // Roll back claims after 200ms before commit; the 1s kill is a backstop
        // with return margin, rather than the normal claim deadline.
        const result = await bridge(loader, { ...envelope("recall-ready", ctx),
          turn: pending.turn, recall_token: pending.token, claim_before: Date.now() / 1000 + .2 }, 1000);
        if (epoch !== generation || recallEpoch !== recallGeneration) return;
        if (pendingRecall === pending) {
          if (!result.pending) pendingRecall = undefined;
          if ("context" in result) applyRecall(result);
        }
      } catch { /* Keep the existing offer and next-human fallback. */ }
      if (epoch !== generation || recallEpoch !== recallGeneration) return;
    }
    if (startingStep) await startingStep;
    if (epoch !== generation || recallEpoch !== recallGeneration) return;
    const step = pendingStep, turn = activeTurn, stepGeneration = stepEpoch;
    if (step && turn && loader) {
      try {
        const result = await bridge(loader, { ...envelope("step-ready", ctx), turn,
          step_token: step.token, claim_before: Date.now() / 1000 + .2 }, 1000);
        if (epoch !== generation || recallEpoch !== recallGeneration || stepGeneration !== stepEpoch) return;
        if (pendingStep === step) {
          if (!result.pending || Date.now() >= step.deadline) pendingStep = undefined;
          if (result.error) warnRecall(); else if (!result.pending) recallWarned = false;
          if (typeof result.context === "string") {
            stepCache = Buffer.byteLength(result.context, "utf8") <= 1500 ? result.context : "";
            stepExpires = Date.now() + 300000;
          }
        }
      } catch { if (Date.now() >= step.deadline) pendingStep = undefined; }
    }
    if (epoch !== generation || recallEpoch !== recallGeneration || stepGeneration !== stepEpoch) return;
    if (Date.now() >= stepExpires) stepCache = "";
    const context = [cache, stepCache].filter(Boolean).join("\n");
    if (!context) return;
    injected = true;
    // Request-local only: do not append to the journal or rewrite structured
    // native/provider messages. Reapply unchanged for tool-successor requests.
    return { messages: [...event.messages, { role: "user", content: [{ type: "text", text: context }], timestamp: 0 }] };
  });
  if (harness === "omp") {
    api.on("message_end", (event, ctx) => {
      bind(ctx);
      // OMP documents this event as a detached snapshot, not a durable commit.
      // Retain the full structure before its independent disk truncation path.
      if (event.message) live.push({ entry_id: `live:${randomUUID()}`, message: structuredClone(event.message), committed: false });
      capture(ctx);
    });
  }
  function endStep(ctx: Context): void {
    const turn = activeTurn;
    const started = stepStarted;
    clearStep();
    if (turn && started && loader) void bridge(loader, { ...envelope("step-end", ctx), turn }, 1000).catch(() => {});
  }
  api.on("turn_end", (_event, ctx) => capture(ctx));
  api.on("agent_end", (_event, ctx) => { capture(ctx); endStep(ctx); });
  api.on("session_shutdown", (_event, ctx) => {
    clearInterval(timer);
    capture(ctx);
    endStep(ctx);
  });
  api.on("session_compact", (_event, ctx) => {
    bind(ctx);
    injected = false;
    capture(ctx);
    if (loader) void bridge(loader, envelope("compaction", ctx), 5000).catch(warn);
    // Keep this turn's request-local offer and pending recall across compaction.
  });
  if (harness === "omp") {
    for (const event of ["session_switch", "session_branch", "session_tree"]) {
      api.on(event, (_event, ctx) => {
        generation++;
        pendingRecall = undefined;
        clearStep();
        cache = "";
        shown = [];
        injected = false;
        capture(ctx);
      });
    }
  }
  // OMP notifications race its independently queued persistence. Reconciliation
  // retries obtain native committed IDs without replacing its private append tap.
  const timer = setInterval(() => { if (current) capture(current); }, 2000);
  timer.unref();
  registerTools(api, Type, harness, (ctx) => {
    bind(ctx);
    if (!loader) loader = load(api);
    return { loader, base: envelope("tool", ctx) };
  });
}

function omitEmptyFields(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(omitEmptyFields);
  if (typeof value !== "object" || value === null) return value;
  const result: RecordValue = {};
  for (const [key, item] of Object.entries(value)) {
    if (item !== "") result[key] = omitEmptyFields(item);
  }
  return result;
}

function registerTools(api: NativeAPI, T: typeof SchemaType, harness: string,
  state: (ctx: Context) => { loader: Loader; base: RecordValue }): void {
  const string = () => T.String({ minLength: 1 });
  // Native providers may fill an omitted optional string with an empty value.
  const optionalString = (schema: TSchema) => T.Optional(T.Union([schema, T.Literal("")]));
  const evidence = T.Array(T.Object({ episode_id: optionalString(T.String({ minLength: 1, maxLength: 64 })),
    quote: string(), occurrence: T.Optional(T.Integer({ minimum: 0 })) }, { additionalProperties: false }),
  { minItems: 1, maxItems: 100 });
  const command = (fields: Record<string, TSchema>) => T.Object({ why: string(), evidence, ...fields }, { additionalProperties: false });
  const date = optionalString(T.String({ format: "date-time" }));
  const scopeLabel = T.Union([T.Literal("personal"), T.Literal("professional")]);
  const scope = T.Array(scopeLabel, { minItems: 1, maxItems: 2, uniqueItems: true });
  const definitions: [string, string, TSchema][] = [
    ["recall", "Recall evidence-backed memory or raw activity excerpts with source/since filters or an empty query. memory_since (inclusive) and memory_until (exclusive) instead filter stored memories, with or without a query, by the latest original source time of their current evidence; unknown dates are excluded. Resolve relative periods such as the past two weeks to aware timestamps. When choosing how to carry out a task and automatic context is insufficient, consult relevant prior experiences. Use deep when passive context is insufficient. Cite used IDs as [memory:ID].", T.Object({
      query: T.Optional(T.String({ default: "", maxLength: 4096 })), effort: optionalString(T.Union([T.Literal("cheap"), T.Literal("normal"), T.Literal("deep")])),
      limit: T.Optional(T.Integer({ minimum: 1, maximum: 100 })),
      scope: optionalString(scopeLabel),
      since: date, source: optionalString(T.String({ minLength: 1, maxLength: 256 })),
      memory_since: date, memory_until: date,
    }, { additionalProperties: false })],
    ["history", "Read bidirectional supersession lineage, immutable versions, word diffs, reasons and evidence quotes.", T.Object({ id: string() }, { additionalProperties: false })],
    ["remember", "Explicitly save an assertion or an already-uploaded artifact reference with exact evidence. Use memento media save to upload a local file; never send base64 in tool arguments. Native provenance, command identity and actor are supplied automatically.", T.Object({ command: T.Union([command({
      type: T.String({ pattern: "^[a-z][a-z0-9_]{0,63}$" }), text: string(), scope, valid_from: date, valid_to: date,
    }), command({ sha256: T.String({ pattern: "^[0-9a-f]{64}$" }),
      kind: T.Union(["image", "audio", "video", "document"].map((value) => T.Literal(value))),
      filename: T.String({ minLength: 1, maxLength: 256 }), scope,
    })]) }, { additionalProperties: false })],
    ["link", "Add an evidence-backed directional relation with an exact current-conversation quote; episode IDs are optional. Native provenance, command identity and actor are supplied automatically.", T.Object({ command: command({
      a: T.String({ minLength: 1, maxLength: 64 }), b: T.String({ minLength: 1, maxLength: 64 }),
      rel: T.Union(["belongs_to", "follows_up_on", "works_in", "decided_by", "mentions", "part_of", "caused_by", "owned_by", "related_to"].map((value) => T.Literal(value))),
    }) }, { additionalProperties: false })],
    ["supersede", "Atomically correct, change, or refine a belief using an exact current-conversation quote; episode IDs are optional. Refinement keeps the old assertion true but less complete. Native provenance, command identity and actor are supplied automatically.", T.Object({ command: command({
      old: T.String({ minLength: 1, maxLength: 64 }),
      new: T.Object({ type: T.String({ minLength: 1, maxLength: 64 }), text: string(), scope }, { additionalProperties: false }),
      kind: optionalString(T.Union([T.Literal("correction"), T.Literal("change"), T.Literal("refinement")])), valid_at: date,
    }) }, { additionalProperties: false })],
    ["reconcile", "On explicit user request, combine proven duplicates into survivor. Read history, supply current versions and exact original evidence for every member; explain sameness in why. Names alone do not prove identity. Preserves history, relations and loops. Pins, sensitive types and different scopes return a human review proposal, never approval. Native command identity and actor are supplied automatically.", T.Object({ command: command({
      survivor: T.String({ minLength: 1, maxLength: 64 }),
      absorbed: T.Array(T.String({ minLength: 1, maxLength: 64 }), { minItems: 1, maxItems: 8, uniqueItems: true }),
      expected_versions: T.Record(T.String({ minLength: 1, maxLength: 64 }), T.Integer({ minimum: 1 }), { minProperties: 2, maxProperties: 9 }),
    }) }, { additionalProperties: false })],
    ["loop", "When asked, list or adjust a follow-up marker on an existing memory. Later hides only the marker from loop lists, not the fact from recall. Only an explicit remind action sets a reminder time. Native mutation identity and actor are supplied automatically.", T.Object({
      action: T.Union(["list", "open", "close", "later", "remind"].map((value) => T.Literal(value))),
      limit: T.Optional(T.Integer({ minimum: 1, maximum: 100, default: 20 })),
      id: optionalString(T.String({ minLength: 1, maxLength: 64 })),
      days: T.Optional(T.Number({ exclusiveMinimum: 0, default: 7 })),
      due_at: date, due_before: date, scope: optionalString(scopeLabel), why: optionalString(string()),
    }, { additionalProperties: false })],
  ];
  for (const [verb, description, parameters] of definitions) {
    api.registerTool({
      name: `memento_${verb}`, label: `Memento ${verb}`, description, parameters,
      ...(harness === "omp" ? { loadMode: "essential", approval: ["recall", "history"].includes(verb) ? "read" : "write" } : {}),
      async execute(toolCallId: string, params: RecordValue, signal: AbortSignal | undefined, _onUpdate: unknown, ctx: Context) {
        const { loader, base } = state(ctx);
        const args = omitEmptyFields(params) as RecordValue;
        // OMP supplies an intent argument outside the declared tool contract.
        // Keep it in the native journal, not in the service's flat wire fields.
        if (harness === "omp") delete args.i;
        if (verb === "loop" && args.action === "list") {
          delete args.id;
          delete args.days;
        }
        const commandValue = args.command;
        const hasCommand = typeof commandValue === "object" && commandValue !== null && !Array.isArray(commandValue);
        if (hasCommand || (verb === "loop" && args.action !== "list")) {
          const config = parseObject(readFileSync(loader.config, "utf8"));
          if (typeof config.uid !== "number" || !Number.isInteger(config.uid)) throw new Error("Memento requires a configured uid");
          const identity = { command_id: `${harness}:${base.session_id}:${toolCallId}:${verb}`, actor: `${harness}:${config.uid}` };
          if (verb === "loop") Object.assign(args, identity);
          else if (hasCommand) args.command = { ...commandValue, ...identity };
        }
        const result = await bridge(loader, { ...base, tool: verb, arguments: args,
          ...(["remember", "link", "supersede", "reconcile"].includes(verb) ? { entries: ctx.sessionManager.getEntries() } : {}),
        }, verb === "recall" ? 45_000 : 15_000, signal);
        return { content: [{ type: "text", text: JSON.stringify(result) }], details: result };
      },
    });
  }
}
