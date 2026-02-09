const assert = require("assert");
const { detectStreamingPipeline } = require("../engine/executePipelineStreaming");

(function main(){
  const plan = {
    version: "ir-dag-3.0-alpha",
    nodes: [
      { id: "s", op: "scan", params: { dataset: "in" } },
      { id: "f", op: "filter", params: { where: { op: "lit", value: true } } },
      { id: "p", op: "project", params: { exprs: [] } },
      { id: "k", op: "sink", params: { collection: "out" } }
    ],
    edges: [
      { from: "s", to: "f", port: "in" },
      { from: "f", to: "p", port: "in" },
      { from: "p", to: "k", port: "in" }
    ],
    outputs: ["k"]
  };

  const order = detectStreamingPipeline(plan);
  assert.deepEqual(order, ["s","f","p","k"]);

  const bad = JSON.parse(JSON.stringify(plan));
  bad.nodes.push({ id: "j", op: "join", params: {} });
  assert.equal(detectStreamingPipeline(bad), null);

  console.log("OK: v4.6 streaming detector smoke test");
})();
