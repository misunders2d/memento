'use strict';
// Read-only viewer. All memory text is untrusted: build DOM with text nodes only, never HTML strings.

const TYPES = ['experience', 'knowledge', 'decision', 'media', 'person', 'organization', 'project', 'concept', 'place', 'tool'];
const TYPE_COLORS = ['#9b59d0', '#2f6fde', '#d64545', '#7a8c99', '#2e9e6a', '#0f8b8d', '#d9822b', '#c2a400', '#8d6e63', '#e05297'];
const css = name => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const $ = id => document.getElementById(id);

const state = {view: 'memories', scope: '', all: false, data: null, graph: null, renderer: null,
  selected: null, matches: new Set(), focus: new Set(), moved: new Set(), dragging: null, dragged: false,
  conv: 0, cand: 0, candQuery: ''};
const worker = new Worker('static/layout-worker.js');
let layoutRun = 0;

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

async function api(path, params) {
  const url = new URL(path, location.href);
  for (const [key, value] of Object.entries(params || {})) {
    if (value !== '' && value !== null && value !== undefined && value !== false) url.searchParams.set(key, value);
  }
  const response = await fetch(url, {headers: {Accept: 'application/json'}});
  if (!response.ok) throw new Error(`${response.status} ${(await response.json().catch(() => ({}))).detail || response.statusText}`);
  return response.json();
}

const typeColor = type => TYPE_COLORS[TYPES.indexOf(type)] || css('--muted');
const when = value => value ? new Date(value).toLocaleString() : '';
const loopColor = loop => loop.deferred ? css('--deferred') : loop.overdue ? css('--overdue') : loop.due_at ? css('--due') : css('--open');
const loopText = loop => loop.deferred ? 'deferred' : loop.overdue ? `overdue since ${when(loop.due_at)}` : loop.due_at ? `due ${when(loop.due_at)}` : 'open, no date';
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
    if (data.done && state.renderer && !state.selected) state.matches.size ? fitMatches() : state.renderer.getCamera().animatedReset({duration: 300});
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
    if (!state.dragging) return;
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
    ratio: Math.min(1, Math.max(0.05, 1.3 * Math.max(maxX - minX, maxY - minY)))}, {duration: 400});
}

function status(text) { $('status').textContent = text; }

function summary(data) {
  const shown = state.graph ? `${state.graph.order.toLocaleString()} of ${data.total_nodes.toLocaleString()} memories, ${state.graph.size.toLocaleString()} links` : '';
  return `${shown} · ${data.loops.open.toLocaleString()} open loops${data.loops.deferred ? `, ${data.loops.deferred.toLocaleString()} deferred` : ''}`;
}

async function loadGraph() {
  status('Loading…');
  select(null);
  try {
    const data = await api('api/graph', {view: state.view, scope: state.scope, all: state.all});
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
    status(`Could not load: ${error.message}`);
  }
}

function sidelist() {
  const box = $('sidelist');
  const parts = [];
  if (state.matches.size || state.searchResults) {
    parts.push(el('h3', {}, `Search results (${state.searchResults.length})`),
      el('ol', {class: 'rows'}, state.searchResults.map(item => el('li', {onclick: () => reveal(item.id)},
        el('span', {class: 'badge'}, item.type), item.label))));
  }
  if (state.view === 'loops' && state.data) {
    const {candidates} = state.data.loops;
    parts.push(el('h3', {}, `Open loops waiting for processing (${candidates.total})`),
      candidates.total ? el('ol', {class: 'rows'}, candidates.items.map(item => el('li', {onclick: () => showCandidate(item.id)},
        el('span', {class: item.loop.overdue ? 'badge overdue' : 'badge loop'}, loopText(item.loop)), item.label)))
        : el('p', {class: 'meta'}, 'None.'));
    if (candidates.total > candidates.items.length) parts.push(el('p', {class: 'meta'}, `Showing ${candidates.items.length}; see Unprocessed for all candidates.`));
  }
  box.replaceChildren(...parts);
  box.hidden = !parts.length;
}

async function select(node) {
  state.selected = node;
  state.renderer && state.renderer.refresh();
  if (!node) return closePanel();
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
      el('li', {onclick: () => reveal(link.id)}, el('span', {class: 'meta'}, link.outgoing ? `${link.rel} → ` : `← ${link.rel} `),
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
  if (position) state.renderer.getCamera().animate({x: position.x, y: position.y, ratio: 0.25}, {duration: 400});
  select(id);
}

function openPanel(children) { $('panel-body').replaceChildren(...children.flat(Infinity).filter(child => child !== null && child !== undefined)); $('panel').hidden = false; }
function closePanel() { $('panel').hidden = true; $('panel-body').replaceChildren(); }

async function search(event) {
  event.preventDefault();
  const q = $('q').value.trim();
  if (!q) {
    state.matches = new Set(); state.searchResults = null;
  } else {
    if (state.view === 'unprocessed') setView('memories');
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
    else state.renderer.getCamera().animatedReset({duration: 300});
  }
  if (state.graph && state.matches.size && !state.focus.size) status('No matches in this graph; pick a result to reveal it.');
  else if (state.data && state.graph) status(summary(state.data));
  sidelist();
}

async function loadConversations() {
  const page = 50, {total, items} = await api('api/conversations', {offset: state.conv, limit: page});
  $('conv-total').textContent = `(${total.toLocaleString()})`;
  $('conversations').replaceChildren(...items.map(item => el('li', {onclick: () => showConversation(item)},
    el('span', {class: 'badge'}, harness(item.source)), item.conversation_id,
    el('div', {class: 'meta'}, `last activity ${when(item.latest_at)}` +
      (item.parked ? ` · parked historical, not queued (live cursor ${item.live_cursor})` : '')))));
  $('conv-prev').disabled = state.conv === 0;
  $('conv-next').disabled = state.conv + page >= total;
}

async function showConversation(item, after = item.cursor, previous = []) {
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
  $('candidates').replaceChildren(...items.map(item => el('li', {onclick: () => showCandidate(item.id)},
    el('span', {class: 'badge'}, item.type), item.loop ? el('span', {class: 'badge loop'}, 'open loop') : null,
    item.decision ? el('span', {class: 'badge'}, item.decision) : null, item.text,
    el('div', {class: 'meta'}, `confidence ${item.confidence.toFixed(2)} · ${item.scope.join(' + ')} · ${when(item.recorded_at)}`))));
  $('cand-prev').disabled = state.cand === 0;
  $('cand-next').disabled = state.cand + page >= total;
}

async function showCandidate(id) {
  try {
    const {versions} = await api(`../history/${encodeURIComponent(id)}`);
    const first = versions[0] || {};
    openPanel([el('div', {class: 'meta'}, el('span', {class: 'badge'}, first.type || 'candidate'), 'unconfirmed candidate'),
      el('div', {class: 'text'}, first.text || ''), historyPanel(versions)]);
  } catch (error) {
    openPanel([el('p', {}, `Could not load candidate: ${error.message}`)]);
  }
}

function setView(view) {
  state.view = view;
  for (const button of document.querySelectorAll('#tabs button')) button.setAttribute('aria-pressed', String(button.dataset.view === view));
  const unprocessed = view === 'unprocessed';
  $('graph-view').hidden = unprocessed;
  $('unprocessed-view').hidden = !unprocessed;
  closePanel();
  if (unprocessed) {
    Promise.all([loadConversations(), loadCandidates()]).catch(error => openPanel([el('p', {}, `Could not load: ${error.message}`)]));
  } else {
    loadGraph();
  }
}

document.querySelectorAll('#tabs button').forEach(button => button.addEventListener('click', () => setView(button.dataset.view)));
$('search').addEventListener('submit', search);
$('q').addEventListener('input', event => { if (!event.target.value && (state.matches.size || state.searchResults)) search(event); });
$('scope').addEventListener('change', event => { state.scope = event.target.value; state.view === 'unprocessed' || loadGraph(); });
$('all').addEventListener('change', event => { state.all = event.target.checked; state.view === 'unprocessed' || loadGraph(); });
$('panel-close').addEventListener('click', () => select(null));
$('conv-prev').addEventListener('click', () => { state.conv = Math.max(0, state.conv - 50); loadConversations(); });
$('conv-next').addEventListener('click', () => { state.conv += 50; loadConversations(); });
$('cand-prev').addEventListener('click', () => { state.cand = Math.max(0, state.cand - 50); loadCandidates(); });
$('cand-next').addEventListener('click', () => { state.cand += 50; loadCandidates(); });
$('cand-search').addEventListener('submit', event => { event.preventDefault(); state.candQuery = $('cand-q').value.trim(); state.cand = 0; loadCandidates(); });
setView('memories');
