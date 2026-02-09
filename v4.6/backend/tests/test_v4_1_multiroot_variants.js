const assert = require("assert");
const { optimizeDagV3_8 } = require("../engine/dagOptimizeV3_8");

// Two independent join components, each with 3 base relations => ordering alternatives exist.
(function main(){
  const plan = {
    version: "ir-dag-3.0-alpha",
    nodes: [
      { id:"a", op:"scan", params:{ rel:"A", estimatedRows: 10000 } },
      { id:"b", op:"scan", params:{ rel:"B", estimatedRows: 1000 } },
      { id:"e", op:"scan", params:{ rel:"E", estimatedRows: 2000 } },

      { id:"c", op:"scan", params:{ rel:"C", estimatedRows: 5000 } },
      { id:"d", op:"scan", params:{ rel:"D", estimatedRows: 800 } },
      { id:"f", op:"scan", params:{ rel:"F", estimatedRows: 1200 } },

      // component 1
      { id:"j1", op:"join", params:{ onRef:[{ left:{rel:"A",path:"k"}, right:{rel:"B",path:"k"} }] } },
      { id:"j1b", op:"join", params:{ onRef:[{ left:{rel:"A",path:"k2"}, right:{rel:"E",path:"k2"} }] } },

      // component 2
      { id:"j2", op:"join", params:{ onRef:[{ left:{rel:"C",path:"k"}, right:{rel:"D",path:"k"} }] } },
      { id:"j2b", op:"join", params:{ onRef:[{ left:{rel:"C",path:"k2"}, right:{rel:"F",path:"k2"} }] } },

      { id:"sink", op:"sink", params:{} }
    ],
    edges: [
      // comp 1: (A ⋈ B) ⋈ E
      { from:"a", to:"j1", port:"left" },
      { from:"b", to:"j1", port:"right" },
      { from:"j1", to:"j1b", port:"left" },
      { from:"e", to:"j1b", port:"right" },

      // comp 2: (C ⋈ D) ⋈ F
      { from:"c", to:"j2", port:"left" },
      { from:"d", to:"j2", port:"right" },
      { from:"j2", to:"j2b", port:"left" },
      { from:"f", to:"j2b", port:"right" },

      // both roots feed sink
      { from:"j1b", to:"sink", port:"in1" },
      { from:"j2b", to:"sink", port:"in2" }
    ],
    outputs: ["sink"]
  };

  const opt = optimizeDagV3_8(plan, new Map(), new Map(), new Map(), new Map(), new Map(), { joinOrder: { topK: 3, maxTotalCandidates: 12, maxRelsForBushy: 7 } });
  assert.ok(opt.ok, "optimizer should succeed");
  assert.ok(opt.variants && opt.variants.candidates && opt.variants.candidates.length >= 1, "expected multi-root coordinate variants");
  const any = opt.variants.candidates[0];
  assert.ok(any.rootJoinId, "expected rootJoinId on coordinate variant");
  console.log("OK: v4.1 multi-root variants smoke test passed");
})();
