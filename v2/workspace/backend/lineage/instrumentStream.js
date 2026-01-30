function instrumentUnaryStream({ opId, collector, upstream, mapRow }) {
  let started = false;

  return {
    async next() {
      if (!started) {
        started = true;
        collector.start(opId);
      }

      while (true) {
        const inRow = await upstream.next();
        if (!inRow) {
          collector.end(opId);
          return null;
        }

        collector.incRowsIn(opId, 1);

        const outRow = mapRow ? mapRow(inRow) : inRow;
        if (outRow == null) continue;

        collector.incRowsOut(opId, 1);
        return outRow;
      }
    }
  };
}

module.exports = { instrumentUnaryStream };
