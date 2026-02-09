class OnlineCov {
  constructor() {
    this.n = 0;
    this.meanX = 0;
    this.meanY = 0;
    this.c = 0;
    this.m2x = 0;
    this.m2y = 0;
  }

  push(x, y) {
    this.n += 1;

    const dx = x - this.meanX;
    this.meanX += dx / this.n;
    const dy = y - this.meanY;
    this.meanY += dy / this.n;

    this.c += dx * (y - this.meanY);
    this.m2x += dx * (x - this.meanX);
    this.m2y += dy * (y - this.meanY);
  }

  corr() {
    if (this.n < 2) return 0;
    const vx = this.m2x / (this.n - 1);
    const vy = this.m2y / (this.n - 1);
    if (vx <= 0 || vy <= 0) return 0;
    const cov = this.c / (this.n - 1);
    return cov / Math.sqrt(vx * vy);
  }
}

function pairKey(a, b) {
  return a < b ? `${a}||${b}` : `${b}||${a}`;
}

class CorrelationEngine {
  constructor(cfg = {}) {
    this.cfg = cfg;
    this.pairs = new Map();
    this.seriesSeen = new Set();
  }

  _shouldIncludeSeries(seriesKey) {
    if (this.cfg.seriesAllowlist && this.cfg.seriesAllowlist.length > 0) {
      return this.cfg.seriesAllowlist.includes(seriesKey);
    }
    if (this.cfg.maxSeries && this.seriesSeen.size >= this.cfg.maxSeries && !this.seriesSeen.has(seriesKey)) {
      return false;
    }
    return true;
  }

  observeBucket(bucket) {
    const keys = Array.from(bucket.keys()).filter((k) => this._shouldIncludeSeries(k));
    for (const k of keys) this.seriesSeen.add(k);

    for (let i = 0; i < keys.length; i++) {
      for (let j = i + 1; j < keys.length; j++) {
        const a = keys[i], b = keys[j];
        const x = bucket.get(a), y = bucket.get(b);
        if (!Number.isFinite(x) || !Number.isFinite(y)) continue;

        const pk = pairKey(a, b);
        let oc = this.pairs.get(pk);
        if (!oc) { oc = new OnlineCov(); this.pairs.set(pk, oc); }
        oc.push(x, y);
      }
    }
  }

  finalize() {
    const series = Array.from(this.seriesSeen).sort();
    const matrix = {};
    for (const s of series) matrix[s] = {};

    for (const [pk, oc] of this.pairs.entries()) {
      const [a, b] = pk.split("||");
      const c = oc.corr();
      matrix[a][b] = c;
      matrix[b][a] = c;
    }
    for (const s of series) matrix[s][s] = 1;

    return { series, matrix };
  }
}

module.exports = { CorrelationEngine };
