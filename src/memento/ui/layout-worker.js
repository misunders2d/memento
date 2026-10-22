// ForceAtlas2 off the main thread; a same-origin file so the page CSP needs no blob: workers.
importScripts('vendor/graphology.umd.min.js', 'vendor/forceatlas2.min.js');

let current = 0;

onmessage = ({data}) => {
  const run = ++current;
  if (data.stop) return;
  const graph = new graphology.Graph({type: 'undirected'});
  data.x.forEach((x, i) => graph.addNode(i, {x, y: data.y[i], size: 1}));
  for (let i = 0; i < data.edges.length; i += 2) {
    const a = data.edges[i], b = data.edges[i + 1];
    if (a !== b && !graph.hasEdge(a, b)) graph.addEdge(a, b);
  }
  const settings = {...forceAtlas2.inferSettings(graph), barnesHutOptimize: graph.order > 2000};
  const batch = graph.order > 10000 ? 1 : graph.order > 2000 ? 3 : 20;
  const total = graph.order > 10000 ? 60 : graph.order > 2000 ? 150 : 300;
  let done = 0;
  const step = () => {
    if (run !== current) return;
    forceAtlas2.assign(graph, {iterations: batch, settings});
    done += batch;
    const positions = new Float32Array(graph.order * 2);
    graph.forEachNode((node, attributes) => {
      positions[2 * node] = attributes.x;
      positions[2 * node + 1] = attributes.y;
    });
    postMessage({run, positions, done: done >= total}, [positions.buffer]);
    if (done < total) setTimeout(step, 0);
  };
  step();
};
