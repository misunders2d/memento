// Synthetic callback qualification, loaded by the installed native CLI. No model/network calls.
import worker from "../src/memento/native_worker.ts";

export default async function probe() {
  const handlers: Record<string, any> = {};
  let definition: any;
  const fake = {
    registerTool(value: any) { definition = value; },
    on(name: string, handler: any) { handlers[name] = handler; },
    getActiveTools() { return ["memento_work"]; },
    getAllTools() { return [{ name: "memento_work", exposure: "direct" }]; },
  };
  process.env.MEMENTO_WORKER_KINDS = '["extract"]';
  worker(fake);
  const assert = (value: any, label: string) => { if (!value) throw new Error(label); };
  await handlers.session_start();
  handlers.context({ messages: [] });
  assert(definition.name === "memento_work", "one work definition");
  const kinds = () => JSON.parse(handlers.context().messages[0].content[0].text).empty_kinds.sort().join();
  // The probe host reports only extraction due (closed consolidation window).
  assert(kinds() === "consolidate", "host-ineligible kind starts unavailable");
  assert(handlers.tool_call({ toolName: "memento_work", input: { action: "claim", command: { kinds: ["consolidate"] } } })?.block,
    "closed window is not claimed");
  const result = (input: any, value: any) => handlers.tool_result({ toolName: "memento_work", input,
    content: [{ type: "text", text: JSON.stringify(value) }], isError: false });
  // Demand extraction is ready even though the ordinary conversation cursor is caught up.
  result({ action: "claim", command: { kinds: ["consolidate"] } }, { work: [], lease_seconds: 180,
    backlog: { pending_extract_conversations: 0, pending_extract_jobs: 1, leased_extract_jobs: 0 } });
  const demand: any = { toolName: "memento_work", input: { action: "claim", command: {
    kinds: ["extract"], agent: "forged", timezone: "forged", consolidate_now: true } } };
  assert(!handlers.tool_call(demand)?.block, "demand extraction must remain claimable");
  assert(demand.input.command.agent === "synthetic" && demand.input.command.timezone === "UTC", "fixed identity and timezone");
  assert(!demand.input.command.consolidate_now, "no invented night bypass");
  result(demand.input, { work: [{ id: "held", owner: "private-capability", kind: "extract",
    instruction: "fixed policy", schema: {}, input: { windows: [{ id: "w0", text: "old synthetic evidence" }] } }],
    lease_seconds: 180, backlog: { pending_extract_conversations: 0, pending_extract_jobs: 0, leased_extract_jobs: 1,
      claimable: { extract: 0, consolidate: 1 } } });
  const foreign = handlers.tool_call({ toolName: "memento_work", input: { action: "complete", command: { id: "foreign", output: {} } } });
  assert(foreign?.block, "foreign lease rejected before service call");
  const attempts = [{ role: "assistant", content: [{ type: "toolCall", name: "memento_work", id: "synthetic-call",
    arguments: { action: "renew", command: { id: "held", owner: "private-capability" } } }] }];
  assert(handlers.context({ messages: attempts }).messages.length === 2, "native attempt history retained within current unit");
  assert(!JSON.stringify(handlers.context({ messages: attempts })).includes("private-capability"), "native history cannot expose bound lease capabilities");
  let context = handlers.context().messages[0].content[0].text;
  assert(!context.includes("private-capability") && context.includes("old synthetic evidence"), "evidence retained, lease capability private");
  const submit: any = { toolName: "memento_work", input: { action: "complete", command: { id: "held", output: {} } } };
  assert(!handlers.tool_call(submit)?.block && submit.input.command.owner === "private-capability", "held lease bound by adapter");
  result(submit.input, { candidate_count: 0 });
  const next: any = { toolName: "memento_work", input: { action: "claim", command: { kinds: ["consolidate"] } } };
  assert(!handlers.tool_call(next)?.block, "service-reported claimable work reopens a kind");
  result(next.input, { work: [], lease_seconds: 180,
    backlog: { pending_extract_conversations: 0, pending_extract_jobs: 0, leased_extract_jobs: 0, claimable: { extract: 0, consolidate: 0 } } });
  context = handlers.context().messages[0].content[0].text;
  const state = JSON.parse(context);
  assert(!context.includes("old synthetic evidence"), "completed source removed from model context");
  assert(state.empty_kinds.includes("extract") && state.empty_kinds.includes("consolidate"), "both exhausted kinds survive context projection");
  assert(handlers.tool_call({ toolName: "memento_work", input: { action: "claim", command: { kinds: ["extract"] } } })?.block,
    "repeated empty request blocked");
  result({ action: "claim", command: { kinds: ["consolidate"] } }, {
    work: [{ id: "valid-consolidation", owner: "private-next", kind: "consolidate", input: {} }],
    lease_seconds: 180, backlog: { pending_extract_conversations: 0, pending_extract_jobs: 0, leased_extract_jobs: 0 } });
  const completionWithEmptyNext: any = { toolName: "memento_work", input: { action: "submit", command: {
    complete: { id: "valid-consolidation", output: {} }, next: { kinds: ["extract"] } } } };
  assert(!handlers.tool_call(completionWithEmptyNext)?.block, "empty next kind must never block a valid completion");
  assert(!completionWithEmptyNext.input.command.next && completionWithEmptyNext.input.command.complete.owner === "private-next",
    "commit current work while omitting the known empty next claim");
  const records: any[] = [], print = console.error;
  console.error = (text: string) => { records.push(JSON.parse(text)); };
  try {
    handlers.tool_result({ toolName: "memento_work", input: { action: "submit", command: {} }, isError: true,
      content: [{ type: "text", text: "MEMENTO_HTTP_422: Loop responsibility requires exact source evidence" }] });
  } finally { console.error = print; }
  assert(records.length === 1 && records[0].code === "Loop responsibility requires exact source evidence"
    && records[0].action === "submit" && records[0].http_status === 422, "fixed rejection detail and action reach the host allowlist");
  // Window opens or extraction adds candidates: only consolidation reopens.
  result({ action: "claim", command: { kinds: ["extract"] } }, { work: [], lease_seconds: 180,
    backlog: { claimable: { extract: 0, consolidate: 2 } } });
  assert(kinds() === "extract", "claimable consolidation reopens without reopening exhausted extraction");
  await handlers.session_start();
  assert(kinds() === "extract", "session rotation keeps service availability");
  result({ action: "claim", command: { kinds: ["consolidate"] } }, { work: [{ id: "c2", owner: "o2", kind: "consolidate", input: {} }],
    lease_seconds: 180, backlog: { claimable: { extract: 1, consolidate: 0 } } });
  assert(kinds() === "", "newly eligible extraction reopens");
  result({ action: "submit", command: { complete: { id: "c2" }, next: { kinds: ["extract"] } } }, { completed: "c2",
    next: { work: [{ id: "e3", owner: "o3", kind: "extract", input: {} }], lease_seconds: 180, backlog: { claimable: { extract: 0, consolidate: 0 } } } });
  assert(kinds() === "consolidate", "closing window marks consolidation unavailable during extraction");
  console.error("WORKER_CALLBACK_PROBE " + JSON.stringify({ passed: true, demand_job: true,
    foreign_lease: true, private_capability: true, bounded_context: true, continuity: true, empty_next_commit: true, native_task_history: true,
    rejection_detail: true, kind_availability: true }));
}
