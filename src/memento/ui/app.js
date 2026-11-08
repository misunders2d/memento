'use strict';
// Read-only viewer. All memory text is untrusted: build DOM with text nodes only, never HTML strings.

const TYPES = ['experience', 'knowledge', 'decision', 'media', 'person', 'organization', 'project', 'concept', 'place', 'tool'];
const TYPE_COLORS = ['#9b59d0', '#2f6fde', '#d64545', '#7a8c99', '#2e9e6a', '#0f8b8d', '#d9822b', '#c2a400', '#8d6e63', '#e05297'];
const css = name => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const $ = id => document.getElementById(id);

const state = {view: 'memories', scope: '', responsibility: '', all: false, data: null, graph: null, renderer: null,
  selected: null, matches: new Set(), focus: new Set(), moved: new Set(), dragging: null, dragged: false,
  conv: 0, cand: 0, candQuery: '', sideCollapsed: false, returnFocus: null};
const worker = new Worker('static/layout-worker.js');
let layoutRun = 0;
let graphFetch = null;

function el(tag, props, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props || {})) {
    if (key === 'class') node.className = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value);
  }
  for (const child of children.flat(Infinity)) {
    if (child !== null && child !== undefined && child !== false) node.append(child instanceof Node ? child : String(child));
  }
  return node;
}

// List rows act like buttons: reachable by Tab and activated with Enter or Space as well as a tap or click.
function row(onActivate, ...children) {
  return el('li', {tabindex: '0', role: 'button', onclick: onActivate,
    onkeydown: event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); onActivate(); } }}, ...children);
}

const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)');
const motion = duration => reducedMotion.matches ? 0 : duration;

async function api(path, params, signal) {
  const url = new URL(path, location.href);
  for (const [key, value] of Object.entries(params || {})) {
    if (value !== '' && value !== null && value !== undefined && value !== false) url.searchParams.set(key, value);
  }
  const response = await fetch(url, {headers: {Accept: 'application/json'}, signal});
  if (!response.ok) throw new Error(`${response.status} ${(await response.json().catch(() => ({}))).detail || response.statusText}`);
  return response.json();
}

const typeColor = type => TYPE_COLORS[TYPES.indexOf(type)] || css('--muted');
const when = value => value ? new Date(value).toLocaleString() : '';
const loopColor = loop => loop.deferred ? css('--deferred') : loop.overdue ? css('--overdue') : loop.due_at ? css('--due') : css('--open');
const roleText = role => ({owner: 'For owner', other: 'Other person', unknown: 'Unresolved, no assignee'}[role] || 'Unresolved, no assignee');
const loopText = loop => `${roleText(loop.responsibility)} · ${loop.deferred ? 'deferred' : loop.overdue ? `overdue since ${when(loop.due_at)}` : loop.due_at ? `due ${when(loop.due_at)}` : 'open, no date'}`;
const harness = source => source.split(':').pop();

function nodeColor(node) {
  if (state.view === 'behaviors' && node.behavior) return css(node.behavior === 'explicit_user' ? '--explicit' : '--inferred');
  if (state.view === 'loops' && node.loop) return loopColor(node.loop);
  if ((state.view === 'behaviors' || state.view === 'loops') && !node.focus) return css('--dim');
  return typeColor(node.type);
}

function legend() {
  const keys = state.view === 'behaviors' ? [['You said (explicit)', css('--explicit')], ['Learned (inferred)', css('--inferred')], ['Related', css('--dim')]]
    : state.view === 'loops' ? [['Overdue', css('--overdue')], ['Due', css('--due')], ['No date', css('--open')], ['Deferred', css('--deferred')], ['Related', css('--dim')]]
    : TYPES.map((type, i) => [type, TYPE_COLORS[i]]);
  $('legend').replaceChildren(...keys.map(([label, color]) => {
    const dot = el('span', {class: 'dot'});
    dot.style.background = color;
    return el('span', {class: 'key'}, dot, label);
  }));
}

function addNode(graph, node, x, y) {
  graph.addNode(node.id, {x, y, label: node.label, size: 3 + 6 * Math.min(1, node.weight || 0.5),
    color: nodeColor(node), raw: node});
}

function buildGraph(data) {
  const graph = new graphology.Graph({multi: true, type: 'directed'});
  const radius = 10 * Math.sqrt(data.nodes.length + 1);
  for (const node of data.nodes) {
    const angle = Math.random() * 2 * Math.PI, distance = radius * Math.sqrt(Math.random());
    addNode(graph, node, distance * Math.cos(angle), distance * Math.sin(angle));
  }
  for (const edge of data.edges) addEdge(graph, edge);
  graph.forEachNode(node => graph.setNodeAttribute(node, 'size', graph.getNodeAttribute(node, 'size') + Math.min(6, Math.log2(1 + graph.degree(node)))));
  return graph;
}

function addEdge(graph, edge) {
  if (graph.hasEdge(edge.id) || !graph.hasNode(edge.src) || !graph.hasNode(edge.dst)) return;
  graph.addDirectedEdgeWithKey(edge.id, edge.src, edge.dst, {label: edge.rel, raw: edge,
    color: edge.loop ? css('--edge-loop') : css('--edge'), size: edge.loop ? 2.5 : 1,
    type: graph.order > 2000 ? 'line' : 'arrow'});
}

function layout(graph) {
  const nodes = graph.nodes(), index = new Map(nodes.map((node, i) => [node, i]));
  const edges = new Int32Array(graph.size * 2);
  let i = 0;
  graph.forEachEdge((edge, attributes, source, target) => { edges[i++] = index.get(source); edges[i++] = index.get(target); });
  const run = ++layoutRun;
  worker.onmessage = ({data}) => {
    if (data.run !== run || state.graph !== graph) return;
    nodes.forEach((node, n) => state.moved.has(node) ||
      graph.mergeNodeAttributes(node, {x: data.positions[2 * n], y: data.positions[2 * n + 1]}));
    if (data.done && state.renderer && !state.selected) state.matches.size ? fitMatches() : state.renderer.getCamera().animatedReset({duration: motion(300)});
  };
  worker.postMessage({x: nodes.map(node => graph.getNodeAttribute(node, 'x')),
    y: nodes.map(node => graph.getNodeAttribute(node, 'y')), edges});
}

// Sigma's default hover box is white; draw the hovered/selected label in the page theme instead.
function drawHover(context, data, settings) {
  const size = settings.labelSize, label = typeof data.label === 'string' ? data.label : '';
  context.font = `${settings.labelWeight} ${size}px ${settings.labelFont}`;
  const width = label ? context.measureText(label).width + 10 : 0, height = size + 8;
  context.fillStyle = css('--surface');
  context.shadowColor = 'rgba(0, 0, 0, .35)';
  context.shadowBlur = 8;
  context.beginPath();
  context.arc(data.x, data.y, data.size + 3, 0, 2 * Math.PI);
  if (width) context.rect(data.x + data.size, data.y - height / 2, width + 3, height);
  context.fill();
  context.shadowBlur = 0;
  if (label) {
    context.fillStyle = css('--text');
    context.fillText(label, data.x + data.size + 6, data.y + size / 3);
  }
}

function render(graph) {
  if (state.renderer) state.renderer.kill();
  state.dragging = null;
  const large = graph.order > 2000;
  const renderer = state.renderer = new Sigma(graph, $('canvas'), {
    renderEdgeLabels: true, zIndex: true, labelColor: {color: css('--text')},
    edgeLabelColor: {color: css('--muted')}, edgeLabelSize: 11, defaultDrawNodeHover: drawHover,
    labelRenderedSizeThreshold: large ? 12 : 6, hideEdgesOnMove: large,
    nodeReducer: (node, data) => reduceNode(graph, node, data),
    edgeReducer: (edge, data) => reduceEdge(graph, edge, data)});
  renderer.on('clickNode', ({node}) => { if (!state.dragged) select(node); });
  renderer.on('clickStage', () => { if (!state.dragged) select(null); });
  // Drag a node out of the way. Positions live only in this page; nothing is saved.
  renderer.on('downNode', ({node}) => {
    state.dragging = node; state.dragged = false;
    // Grabbing a node settles the layout, so the frame stays still while dragging.
    worker.postMessage({stop: true}); layoutRun++;
    if (!renderer.getCustomBBox()) renderer.setCustomBBox(renderer.getBBox());
  });
  renderer.getMouseCaptor().on('mousemovebody', event => {
    if (!state.dragging || !graph.hasNode(state.dragging)) return;
    const position = renderer.viewportToGraph(event);
    graph.mergeNodeAttributes(state.dragging, {x: position.x, y: position.y});
    state.moved.add(state.dragging);
    state.dragged = true;
    event.preventSigmaDefault();
    event.original.preventDefault();
    event.original.stopPropagation();
  });
  const drop = () => { state.dragging = null; setTimeout(() => { state.dragged = false; }, 0); };
  renderer.getMouseCaptor().on('mouseup', drop);
  renderer.getMouseCaptor().on('mouseleave', drop);
  // A touch tap also fires downNode but never a mouseup, so end the grab when the finger lifts.
  renderer.getTouchCaptor().on('touchup', drop);
}

function reduceNode(graph, node, data) {
  const result = {...data};
  const context = (state.view === 'behaviors' || state.view === 'loops') && !data.raw.focus;
  if (context && !(state.selected && graph.hasNode(state.selected) && graph.areNeighbors(node, state.selected))) result.label = '';
  if (state.focus.size && !state.focus.has(node)) Object.assign(result, {color: css('--dim'), label: '', zIndex: 0});
  // Matches outrank neighbours for label space; the label grid still prevents overlapping text.
  if (state.matches.has(node)) Object.assign(result, {zIndex: 2, size: data.size + 2});
  if (state.selected && graph.hasNode(state.selected)) {
    if (node === state.selected) Object.assign(result, {highlighted: true, zIndex: 3});
    else if (!graph.areNeighbors(node, state.selected)) Object.assign(result, {color: css('--dim'), label: '', zIndex: 0});
  }
  return result;
}

function reduceEdge(graph, edge, data) {
  if (state.selected && graph.hasNode(state.selected)) {
    if (!graph.hasExtremity(edge, state.selected)) return {...data, hidden: true};
  } else if (state.focus.size && !(state.focus.has(graph.source(edge)) && state.focus.has(graph.target(edge)))) {
    return {...data, hidden: true};
  }
  return data;
}

// Search keeps the graph for context: matches and their direct neighbours stay in colour, the rest is greyed.
function updateFocus() {
  const graph = state.graph;
  state.focus = new Set();
  if (!graph) return;
  for (const node of state.matches) {
    if (!graph.hasNode(node)) continue;
    state.focus.add(node);
    graph.forEachNeighbor(node, neighbor => state.focus.add(neighbor));
  }
}

function fitMatches() {
  const renderer = state.renderer, points = [...state.matches].map(node => renderer.getNodeDisplayData(node)).filter(Boolean);
  if (!points.length) return;
  const xs = points.map(point => point.x), ys = points.map(point => point.y);
  const [minX, maxX, minY, maxY] = [Math.min(...xs), Math.max(...xs), Math.min(...ys), Math.max(...ys)];
  renderer.getCamera().animate({x: (minX + maxX) / 2, y: (minY + maxY) / 2,
    ratio: Math.min(1, Math.max(0.05, 1.3 * Math.max(maxX - minX, maxY - minY)))}, {duration: motion(400)});
}

function status(text) {
  $('status').textContent = text;
  for (const id of ['zoom-in', 'zoom-out', 'zoom-fit']) $(id).disabled = !state.renderer;
}

function summary(data) {
  const shown = state.graph ? `${state.graph.order.toLocaleString()} of ${data.total_nodes.toLocaleString()} memories, ${state.graph.size.toLocaleString()} links` : '';
  const unresolved = state.view === 'loops' && state.responsibility === 'unknown' ? 'unresolved, no assignee' : 'open loops';
  return `${shown} · ${data.loops.open.toLocaleString()} ${unresolved}${data.loops.deferred ? `, ${data.loops.deferred.toLocaleString()} deferred` : ''}${state.responsibility === '' && data.loops.unresolved ? ` · ${data.loops.unresolved.toLocaleString()} unresolved` : ''}`;
}

// A newer view or filter replaces any graph request still in flight.
function abortGraph() {
  if (graphFetch) graphFetch.abort();
  graphFetch = null;
}

async function loadGraph() {
  abortGraph();
  const request = graphFetch = new AbortController();
  status('Loading…');
  select(null);
  try {
    const data = await api('api/graph', {view: state.view, scope: state.scope, all: state.all,
      responsibility: state.view === 'loops' ? state.responsibility : ''}, request.signal);
    if (request.signal.aborted) return;
    state.data = data;
    if (data.too_large) {
      if (state.renderer) state.renderer.kill();
      state.renderer = state.graph = null;
      worker.postMessage({stop: true}); layoutRun++;
      status(`${data.total_nodes.toLocaleString()} memories is more than this page can draw at once. Turn off "Show all" or narrow the scope.`);
    } else {
      state.graph = buildGraph(data);
      state.moved = new Set();
      updateFocus();
      render(state.graph);
      layout(state.graph);
      status(data.nodes.length ? summary(data) : 'Nothing to show here yet.');
    }
    legend();
    sidelist();
  } catch (error) {
    if (request.signal.aborted) return;
    status(`Could not load: ${error.message}`);
  }
}

function sidelist() {
  const box = $('sidelist');
  const parts = [];
  if (state.matches.size || state.searchResults) {
    parts.push(el('h3', {}, `Search results (${state.searchResults.length})`),
      el('ol', {class: 'rows'}, state.searchResults.map(item => row(() => reveal(item.id),
        el('span', {class: 'badge'}, item.type), item.label))));
  }
  if (state.view === 'loops' && state.data) {
    const {candidates} = state.data.loops;
    parts.push(el('h3', {}, `Open loops waiting for processing (${candidates.total})`),
      candidates.total ? el('ol', {class: 'rows'}, candidates.items.map(item => row(() => showCandidate(item.id),
        el('span', {class: item.loop.overdue ? 'badge overdue' : 'badge loop'}, loopText(item.loop)), item.label)))
        : el('p', {class: 'meta'}, 'None.'));
    if (candidates.total > candidates.items.length) parts.push(el('p', {class: 'meta'}, `Showing ${candidates.items.length}; see Unprocessed for all candidates.`));
  }
  const titles = parts.filter(part => part.tagName === 'H3').map(part => part.textContent);
  const toggle = el('button', {type: 'button', class: 'sheet-toggle', 'aria-expanded': String(!state.sideCollapsed),
    'aria-controls': 'sidelist-body', onclick: () => { state.sideCollapsed = !state.sideCollapsed; sidelist(); }},
  state.sideCollapsed ? titles.join(' · ') : 'Hide list');
  const body = el('div', {id: 'sidelist-body'}, parts);
  body.hidden = state.sideCollapsed;
  box.replaceChildren(toggle, body);
  box.hidden = !parts.length;
}

async function select(node) {
  state.selected = node;
  state.renderer && state.renderer.refresh();
  if (!node) return closePanel();
  if ($('panel').hidden) openPanel([el('p', {class: 'loading'}, 'Loading…')]);
  try {
    const [detail, history] = await Promise.all([api(`api/node/${encodeURIComponent(node)}`),
      api(`../history/${encodeURIComponent(node)}`).catch(() => ({versions: []}))]);
    if (state.selected !== node) return;
    openPanel(nodePanel(detail, history.versions));
  } catch (error) {
    openPanel([el('p', {}, `Could not load details: ${error.message}`)]);
  }
}

function nodePanel(detail, versions) {
  const parts = [el('div', {class: 'meta'}, el('span', {class: 'badge'}, detail.type), detail.scope.join(' + ')),
    el('div', {class: 'text'}, detail.text)];
  if (detail.loop) parts.push(el('p', {}, el('span', {class: detail.loop.overdue ? 'badge overdue' : 'badge loop'}, 'loop'), loopText(detail.loop)));
  if (detail.behavior) {
    const b = detail.behavior;
    parts.push(el('h3', {}, b.authority === 'explicit_user' ? 'Behavior — you said this' : 'Behavior — inferred lesson'),
      el('dl', {}, el('dt', {}, 'When'), el('dd', {}, b.applicability), el('dt', {}, 'Do'), el('dd', {}, b.conduct),
        el('dt', {}, 'Why'), el('dd', {}, b.rationale),
        b.exceptions.length ? [el('dt', {}, 'Except'), el('dd', {}, b.exceptions.join('\n'))] : null));
  }
  if (detail.links.length) {
    parts.push(el('h3', {}, `Connections (${detail.links.length})`), el('ol', {class: 'rows'}, detail.links.map(link =>
      row(() => reveal(link.id), el('span', {class: 'meta'}, link.outgoing ? `${link.rel} → ` : `← ${link.rel} `),
        el('span', {class: 'badge'}, link.type), link.label))));
  }
  parts.push(historyPanel(versions));
  return parts;
}

function historyPanel(versions) {
  if (!versions.length) return el('p', {class: 'meta'}, 'No history available.');
  return [el('h3', {}, 'History and evidence'), versions.map(version => el('div', {class: 'version'},
    el('div', {class: 'meta'}, `v${version.version} · ${version.state} · ${when(version.recorded_at)}${version.actor ? ` · ${version.actor}` : ''}`),
    version.why ? el('div', {}, version.why) : null,
    version.text && versions.length > 1 ? el('div', {class: 'text'}, version.text) : null,
    (version.evidence || []).map(item => el('blockquote', {}, item.quote)))) ];
}

async function reveal(id) {
  const graph = state.graph;
  if (!graph) return;
  if (!graph.hasNode(id)) {
    try {
      const data = await api('api/graph', {view: 'around', id, scope: state.scope});
      const anchor = data.nodes.map(node => node.id).find(node => graph.hasNode(node));
      const base = anchor ? graph.getNodeAttributes(anchor) : {x: 0, y: 0};
      for (const node of data.nodes) {
        if (!graph.hasNode(node.id)) addNode(graph, node, base.x + (Math.random() - .5) * 20, base.y + (Math.random() - .5) * 20);
      }
      for (const edge of data.edges) addEdge(graph, edge);
      updateFocus();
      status(summary(state.data));
    } catch (error) {
      return status(`Could not open: ${error.message}`);
    }
  }
  const position = state.renderer.getNodeDisplayData(id);
  if (position) state.renderer.getCamera().animate({x: position.x, y: position.y, ratio: 0.25}, {duration: motion(400)});
  select(id);
}

function openPanel(children) {
  const panel = $('panel'), opening = panel.hidden;
  $('panel-body').replaceChildren(...children.flat(Infinity).filter(child => child !== null && child !== undefined));
  if (!opening) return;
  // Remember where the reader was so closing the details returns them there.
  state.returnFocus = panel.contains(document.activeElement) ? state.returnFocus : document.activeElement;
  panel.hidden = false;
  panel.scrollTop = 0;
  panel.focus({preventScroll: true});
}
function closePanel() {
  const panel = $('panel'), wasOpen = !panel.hidden, hadFocus = panel.contains(document.activeElement);
  panel.hidden = true;
  $('panel-body').replaceChildren();
  const target = state.returnFocus;
  state.returnFocus = null;
  if (wasOpen && hadFocus && target && target.isConnected && target !== document.body) target.focus({preventScroll: true});
}

async function search(event) {
  event.preventDefault();
  const q = $('q').value.trim();
  state.sideCollapsed = false;
  if (!q) {
    state.matches = new Set(); state.searchResults = null;
  } else {
    if (state.view === 'unprocessed' || state.view === 'processing') setView('memories');
    try {
      const {results} = await api('api/search', {q, scope: state.scope});
      state.searchResults = results;
      state.matches = new Set(results.map(item => item.id));
    } catch (error) {
      return status(`Search failed: ${error.message}`);
    }
  }
  updateFocus();
  if (state.renderer) {
    state.renderer.refresh();
    if (state.matches.size) fitMatches();
    else state.renderer.getCamera().animatedReset({duration: motion(300)});
  }
  if (state.graph && state.matches.size && !state.focus.size) status('No matches in this graph; pick a result to reveal it.');
  else if (state.data && state.graph) status(summary(state.data));
  sidelist();
}

async function loadConversations() {
  const page = 50, {total, items} = await api('api/conversations', {offset: state.conv, limit: page});
  $('conv-total').textContent = `(${total.toLocaleString()})`;
  $('conversations').replaceChildren(...items.map(item => row(() => showConversation(item),
    el('span', {class: 'badge'}, harness(item.source)), item.conversation_id,
    el('div', {class: 'meta'}, `last activity ${when(item.latest_at)}` +
      (item.parked ? ` · historical backlog, nightly after fresh work (live cursor ${item.live_cursor})` : '')))));
  $('conv-prev').disabled = state.conv === 0;
  $('conv-next').disabled = state.conv + page >= total;
}

async function showConversation(item, after = item.cursor, previous = []) {
  if (!previous.length && $('panel').hidden) openPanel([el('p', {class: 'loading'}, 'Loading…')]);
  try {
    const {items} = await api('api/conversation', {source: item.source, conversation_id: item.conversation_id, after, limit: 50});
    const episodes = previous.concat(items.map(episode => el('div', {class: 'episode'},
      el('div', {class: 'meta'}, `${episode.kind} · ${when(episode.at)}`),
      el('pre', {}, episode.text === null ? '[source text unavailable]' : episode.text + (episode.truncated ? '\n…' : '')))));
    const more = items.length === 50 ? el('button', {type: 'button', onclick: () => showConversation(item, items[items.length - 1].seq, episodes)}, 'Load more') : null;
    openPanel([el('div', {class: 'meta'}, `${harness(item.source)} · ${item.conversation_id}`),
      el('h3', {}, 'Not yet extracted'), episodes.length ? episodes : el('p', {class: 'meta'}, 'No messages.'), more]);
  } catch (error) {
    openPanel([el('p', {}, `Could not load conversation: ${error.message}`)]);
  }
}

async function loadCandidates() {
  const page = 50, {total, items} = await api('api/candidates', {q: state.candQuery, offset: state.cand, limit: page});
  $('cand-total').textContent = `(${total.toLocaleString()})`;
  $('candidates').replaceChildren(...items.map(item => row(() => showCandidate(item.id),
    el('span', {class: 'badge'}, item.type), item.loop ? el('span', {class: 'badge loop'}, 'open loop') : null,
    item.decision ? el('span', {class: 'badge'}, item.decision) : null, item.text,
    el('div', {class: 'meta'}, `confidence ${item.confidence.toFixed(2)} · ${item.scope.join(' + ')} · ${when(item.recorded_at)}`))));
  $('cand-prev').disabled = state.cand === 0;
  $('cand-next').disabled = state.cand + page >= total;
}

async function showCandidate(id) {
  if ($('panel').hidden) openPanel([el('p', {class: 'loading'}, 'Loading…')]);
  try {
    const {versions} = await api(`../history/${encodeURIComponent(id)}`);
    const first = versions[0] || {};
    openPanel([el('div', {class: 'meta'}, el('span', {class: 'badge'}, first.type || 'candidate'), 'unconfirmed candidate'),
      el('div', {class: 'text'}, first.text || ''), historyPanel(versions)]);
  } catch (error) {
    openPanel([el('p', {}, `Could not load candidate: ${error.message}`)]);
  }
}

// On narrow screens the filters sit behind a toggle; mark it while any filter narrows the view.
function markFilters() {
  const active = state.scope || state.all || (state.view === 'loops' && state.responsibility);
  $('filters-toggle').classList.toggle('active', Boolean(active));
  $('filters-toggle').setAttribute('aria-label', active ? 'Filters (active)' : 'Filters');
}

function centerActiveTab() {
  const active = document.querySelector('#tabs [aria-pressed="true"]'), tabs = $('tabs');
  if (active && tabs.scrollWidth > tabs.clientWidth) tabs.scrollLeft = Math.max(0, active.offsetLeft - (tabs.clientWidth - active.offsetWidth) / 2);
}

function setView(view) {
  state.view = view;
  for (const button of document.querySelectorAll('#tabs button')) button.setAttribute('aria-pressed', String(button.dataset.view === view));
  centerActiveTab();
  const unprocessed = view === 'unprocessed', processing = view === 'processing', graphless = unprocessed || processing;
  if (graphless) {
    abortGraph();
    worker.postMessage({stop: true}); layoutRun++;
    if (state.renderer) state.renderer.kill();
    state.renderer = state.graph = null;
  }
  $('graph-view').hidden = graphless;
  $('unprocessed-view').hidden = !unprocessed;
  $('processing-view').hidden = !processing;
  $('responsibility').hidden = view !== 'loops';
  $('graph-tools').hidden = graphless;
  markFilters();
  closePanel();
  if (!processing) stopProcessing();
  if (processing) {
    loadProcessing();
  } else if (unprocessed) {
    Promise.all([loadConversations(), loadCandidates()]).catch(error => openPanel([el('p', {}, `Could not load: ${error.message}`)]));
  } else {
    loadGraph();
  }
}

// Processing dashboard: one compact global summary, at most one request in flight,
// refreshed only while this view is visible. Missing numbers render as "—", never 0.
const PROCESSING_REFRESH_MS = 60000;
const PROCESSING_TIMEOUT_MS = 15000;
const proc = {data: null, rendered: null, at: null, fetch: null, timer: null, error: null};
const num = value => Number.isFinite(value) ? value.toLocaleString() : '—';
const clock = date => date.toLocaleTimeString();

function metric(label, value, help, sub) {
  return [el('dt', {class: sub ? 'sub' : ''}, label), el('dd', {}, value),
    help ? el('dd', {class: 'help'}, help) : null];
}

function card(title, lead, rows, notes) {
  return el('section', {class: 'column proc-card', 'aria-label': title}, el('h2', {}, title),
    lead ? el('p', {class: 'meta'}, lead) : null, el('dl', {class: 'proc-metrics'}, rows),
    notes && notes.length ? el('details', {}, el('summary', {}, 'Definitions'), notes.map(note => el('p', {class: 'meta'}, note))) : null);
}

function errorRows(list) {
  return (list || []).map(item => metric(`${item.state === 'failed' ? 'Failed' : 'Retrying'}: ${item.category}`, num(item.count),
    item.category === 'night_window_closed' ? 'Routine: the consolidation window closed before the job finished.' : null, true));
}

function processingCards(data) {
  const x = data.extraction || {}, c = data.consolidation || {}, h = data.historical || {}, i = data.indexing || {};
  const r = x.remaining || {}, b = x.batched || {}, conv = x.conversations || {};
  const pick = (key, field) => (b[key] || {})[field];
  const coverage = Number.isFinite(r.counted_conversations) && Number.isFinite(x.pending_conversations)
    ? `Counted in ${num(r.counted_conversations)} of ${num(x.pending_conversations)} conversations with live work.` : '';
  const messages = card('Messages awaiting extraction', r.partial
    ? `Partial: ${coverage} The per-request bound was reached, so totals are lower bounds.` : coverage, [
    metric('Ready to extract', num(r.admissible_messages), 'Compacted, or the conversation has been idle 30 minutes.'),
    metric('in a queued batch', num(pick('pending_live', 'messages')), `${num(pick('pending_live', 'jobs'))} jobs`, true),
    metric('being extracted now', num(pick('leased_live', 'messages')), `${num(pick('leased_live', 'jobs'))} leased jobs`, true),
    metric('Waiting for compaction or inactivity', num(r.waiting_messages), 'Captured after the last compaction in a conversation active in the last 30 minutes.'),
    metric('Tool outputs and other input ready / waiting', `${num(r.admissible_other)} / ${num(r.waiting_other)}`, 'Extraction input that is not a user or assistant message.'),
  ], ['Messages are captured user and assistant messages that extraction reads, after each live extraction cursor. Skipped raw records, forgotten sources and legacy instruction text are excluded, using the same predicate as extraction.',
    'Batched counts are distinct messages named by queued or leased extraction jobs. They can exceed a partial lower-bound count of ready messages. A long message remains awaiting extraction until its remaining text has been processed.',
    'Parked historical messages are not included here. Per-message totals for extracted history are not available: this summary counts extraction work as jobs and conversations instead.']);
  const extraction = card('Extraction work', null, [
    metric('Conversations with live work', num(x.pending_conversations), 'Captured records after the live extraction cursor, eligible now or waiting.'),
    metric('in a live job', num(conv.in_live_job), null, true),
    metric('Conversations up to date', num(conv.up_to_date), `${num(conv.parked_live_up_to_date)} more parked conversations have no new live records`),
    metric('Tracked conversations', num(conv.tracked)),
    metric('Jobs queued', num(x.pending_jobs), Number.isFinite(x.retrying_jobs) && x.retrying_jobs ? `${num(x.retrying_jobs)} retrying after an error` : null),
    metric('Jobs running', num(x.leased_jobs)),
    metric('Jobs completed', num(x.completed_jobs), 'Extraction batches finished; not messages, and not consolidation.'),
    metric('Jobs failed', num(x.failed_jobs)),
    errorRows(x.errors),
  ], ['A job is one extraction batch of up to 64 text windows from one conversation. Completed extraction produces candidates; it does not mean they were consolidated or that memory quality was verified.']);
  const consolidation = card('Consolidation', null, [
    metric('Candidates waiting', num(c.pending_candidates), 'Extracted, not yet promoted, rejected or merged.'),
    metric('ready now', num(c.ready_candidates), 'Not deferred to a later review time and not waiting on a human review.', true),
    metric('Jobs queued', num(c.pending_jobs), Number.isFinite(c.retrying_jobs) && c.retrying_jobs ? `${num(c.retrying_jobs)} retrying after an error` : null),
    metric('Jobs running', num(c.leased_jobs)),
    metric('Jobs completed', num(c.completed_jobs)),
    metric('Jobs failed', num(c.failed_jobs)),
    metric('Jobs rebatched', num(c.rebatched_jobs), 'Routine server rebatches, not failures.'),
    errorRows(c.errors),
    metric('Repair groups pending / inspected', `${num((c.repair_groups || {}).pending)} / ${num((c.repair_groups || {}).done)}`),
  ], ['The processor controls the consolidation schedule. Its current schedule and whether it is online are not reported by this service.',
    'Inspected repair groups mean the bounded work was reviewed, not proof of semantic completeness.']);
  const historical = card('Historical backlog', h.active ? `Parked on ${when(h.activated_at)}.` : 'No historical backlog has been parked.', [
    metric('Historical conversations remaining', num(h.pending_conversations), 'Extracted automatically in the nightly window after eligible fresh work drains.'),
    metric('Historical conversations extracted', num(h.extracted_conversations), `Of ${num(h.parked_conversations)} originally parked conversations; extraction is not consolidation.`),
    metric('Historical extraction queued / running', `${num(h.queued_jobs)} / ${num(h.running_jobs)} jobs`, 'A queued retry yields to fresh work and waits outside the nightly window.'),
    metric('with new live records', num(h.live_pending_conversations), null, true),
    metric('Parked extraction jobs', num(x.parked_jobs), 'Stopped at parking; not failures, not completed.'),
    metric('Parked consolidation jobs', num(c.parked_jobs)),
    metric('Parked candidates', num(h.parked_candidates), 'Stored and searchable, outside consolidation.'),
    metric('Released candidates', num(h.released_candidates), 'Admitted to consolidation through a citation or explicit owner release; not necessarily consolidated yet.'),
    metric('Excluded legacy candidates', num(c.excluded_legacy_candidates), 'Supported only by legacy instruction text; never admitted to consolidation.'),
    metric('On-demand extraction queued / running', `${num(pick('pending_demand', 'messages'))} / ${num(pick('leased_demand', 'messages'))} messages`,
      `${num(pick('pending_demand', 'jobs'))} / ${num(pick('leased_demand', 'jobs'))} jobs, from cited parked history`),
  ], ['Historical extraction resumes its saved cursor in bounded batches. New eligible work takes priority before the next historical batch. Citing a parked passage can also queue on-demand extraction for that span.']);
  const indexed = i.available === false ? card('Search indexing', 'Indexing is unavailable on this service.', [])
    : card('Search indexing', i.not_counted ? 'No index count is available yet. Other processing counts remain available.'
      : `Counted ${Number.isFinite(i.age_seconds) ? `${num(Math.round(i.age_seconds / 60))} min ago` : 'at an unknown time'}.`, [
      metric('Index units indexed', num(i.indexed)),
      metric('Index units pending', num(i.pending)),
      metric('Index units failed', num(i.failed)),
      metric('Messages not yet chunked', num(i.unprepared_episodes)),
      i.error ? metric('Indexing error', String(i.error)) : null,
    ], ['Index units are message chunks, memories, candidates and media segments, not messages. Search readiness is live; counts are the latest background count.']);
  return [messages, extraction, consolidation, historical, indexed];
}

function renderProcessing() {
  const alert = $('proc-alert');
  if (proc.error) {
    alert.hidden = false;
    alert.textContent = proc.data ? `Refresh failed (${proc.error}). Showing data from ${clock(proc.at)}, which may be stale.`
      : `Could not load processing status: ${proc.error}`;
  } else {
    alert.hidden = true;
    alert.textContent = '';
  }
  $('proc-updated').textContent = proc.fetch ? (proc.data ? `Updated ${clock(proc.at)} · refreshing…` : 'Loading…')
    : proc.data ? `Updated ${clock(proc.at)}` : '';
  $('proc-refresh').disabled = Boolean(proc.fetch);
  if (proc.data && proc.rendered !== proc.data) {
    const root = $('proc-cards');
    const open = new Set([...root.querySelectorAll('.proc-card')]
      .filter(section => section.querySelector('details[open]'))
      .map(section => section.getAttribute('aria-label')));
    const focused = root.contains(document.activeElement) && document.activeElement.tagName === 'SUMMARY'
      ? document.activeElement.closest('.proc-card').getAttribute('aria-label') : null;
    root.replaceChildren(...processingCards(proc.data));
    for (const section of root.querySelectorAll('.proc-card')) {
      const title = section.getAttribute('aria-label'), details = section.querySelector('details');
      if (details) details.open = open.has(title);
      if (title === focused) section.querySelector('summary')?.focus({preventScroll: true});
    }
    proc.rendered = proc.data;
  }
  else if (!proc.data && !proc.fetch) $('proc-cards').replaceChildren();
}

function scheduleProcessing() {
  clearTimeout(proc.timer);
  proc.timer = null;
  if (state.view === 'processing' && document.visibilityState === 'visible') proc.timer = setTimeout(loadProcessing, PROCESSING_REFRESH_MS);
}

async function loadProcessing() {
  if (proc.fetch || state.view !== 'processing') return;
  const request = proc.fetch = new AbortController();
  let timedOut = false;
  const deadline = setTimeout(() => { timedOut = true; request.abort(); }, PROCESSING_TIMEOUT_MS);
  clearTimeout(proc.timer);
  renderProcessing();
  try {
    const response = await fetch(new URL('api/processing', location.href), {headers: {Accept: 'application/json'}, signal: request.signal});
    if (response.status === 404) throw new Error('not available on this service');
    if (!response.ok) throw new Error(`${response.status} ${(await response.json().catch(() => ({}))).detail || response.statusText}`);
    proc.data = await response.json();
    proc.at = new Date();
    proc.error = null;
  } catch (error) {
    if (request.signal.aborted && !timedOut) return;
    proc.error = timedOut ? 'request timed out; use Refresh to retry' : error.message;
  } finally {
    clearTimeout(deadline);
    if (proc.fetch === request) proc.fetch = null;
  }
  renderProcessing();
  scheduleProcessing();
}

function stopProcessing() {
  clearTimeout(proc.timer);
  proc.timer = null;
  if (proc.fetch) proc.fetch.abort();
  proc.fetch = null;
}

$('proc-refresh').addEventListener('click', loadProcessing);
document.addEventListener('visibilitychange', () => {
  if (state.view !== 'processing') return;
  if (document.visibilityState !== 'visible') return clearTimeout(proc.timer);
  if (!proc.at || Date.now() - proc.at.getTime() >= PROCESSING_REFRESH_MS) loadProcessing();
  else scheduleProcessing();
});

document.querySelectorAll('#tabs button').forEach(button => button.addEventListener('click', () => setView(button.dataset.view)));
window.addEventListener('resize', centerActiveTab);
$('search').addEventListener('submit', search);
$('q').addEventListener('input', event => { if (!event.target.value && (state.matches.size || state.searchResults)) search(event); });
const graphView = () => state.view !== 'unprocessed' && state.view !== 'processing';
$('scope').addEventListener('change', event => { state.scope = event.target.value; markFilters(); graphView() && loadGraph(); });
$('responsibility').addEventListener('change', event => { state.responsibility = event.target.value; markFilters(); loadGraph(); });
$('all').addEventListener('change', event => { state.all = event.target.checked; markFilters(); graphView() && loadGraph(); });
$('panel-close').addEventListener('click', () => select(null));
document.addEventListener('keydown', event => { if (event.key === 'Escape' && !$('panel').hidden) select(null); });
$('filters-toggle').addEventListener('click', () => {
  const open = document.querySelector('header').classList.toggle('filters-open');
  $('filters-toggle').setAttribute('aria-expanded', String(open));
});
$('legend-toggle').addEventListener('click', () => {
  const open = $('legend').classList.toggle('open');
  $('legend-toggle').setAttribute('aria-expanded', String(open));
});
$('zoom-in').addEventListener('click', () => state.renderer && state.renderer.getCamera().animatedZoom({duration: motion(200)}));
$('zoom-out').addEventListener('click', () => state.renderer && state.renderer.getCamera().animatedUnzoom({duration: motion(200)}));
$('zoom-fit').addEventListener('click', () => state.renderer && state.renderer.getCamera().animatedReset({duration: motion(300)}));
$('conv-prev').addEventListener('click', () => { state.conv = Math.max(0, state.conv - 50); loadConversations(); });
$('conv-next').addEventListener('click', () => { state.conv += 50; loadConversations(); });
$('cand-prev').addEventListener('click', () => { state.cand = Math.max(0, state.cand - 50); loadCandidates(); });
$('cand-next').addEventListener('click', () => { state.cand += 50; loadCandidates(); });
$('cand-search').addEventListener('submit', event => { event.preventDefault(); state.candQuery = $('cand-q').value.trim(); state.cand = 0; loadCandidates(); });
setView('memories');
