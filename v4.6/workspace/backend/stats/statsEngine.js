const { getPath } = require("../dq/path");
const { OnlineMoments, OnlineDownsideMoments, EWMA } = require("./online");
const { FrequencyInferer } = require("./frequency");
const { CorrelationEngine } = require("./correlation");

function toKey(row, keyPath) {
  if (!keyPath) return "__all__";
  const v = getPath(row, keyPath);
  return v === undefined || v === null ? "__null__" : String(v);
}

function toDateKey(dateVal) {
  const d = new Date(dateVal);
  if (!Number.isFinite(d.getTime())) return null;
  return d.toISOString();
}

class SeriesState {
  constructor(cfg) {
    this.cfg = cfg;
    this.prevPrice = null;

    this.retMoments = new OnlineMoments();
    this.downside = new OnlineDownsideMoments(cfg.sortinoTarget ?? 0);
    this.ewma = new EWMA(cfg.ewmaLambda ?? 0.94);

    this.equity = 1.0;
    this.peak = 1.0;
    this.maxDrawdown = 0;

    this.firstDate = null;
    this.lastDate = null;
  }

  pushReturn(r) {
    this.retMoments.push(r);
    this.downside.push(r);
    this.ewma.push(r);

    this.equity *= (1 + r);
    if (this.equity > this.peak) this.peak = this.equity;
    const dd = (this.peak - this.equity) / this.peak;
    if (dd > this.maxDrawdown) this.maxDrawdown = dd;
  }

  deriveReturnFromPrice(price) {
    if (this.prevPrice !== null && this.prevPrice !== 0) {
      const simple = (price / this.prevPrice) - 1;
      this.prevPrice = price;
      return simple;
    }
    this.prevPrice = price;
    return null;
  }

  push(row) {
    const cfg = this.cfg;

    const dateVal = cfg.datePath ? getPath(row, cfg.datePath) : null;
    if (dateVal) {
      const d = new Date(dateVal);
      if (Number.isFinite(d.getTime())) {
        if (!this.firstDate) this.firstDate = d;
        this.lastDate = d;
      }
    }

    let r = null;

    if (cfg.returnPath) {
      const v = Number(getPath(row, cfg.returnPath));
      if (Number.isFinite(v)) r = v;
    } else if (cfg.pricePath) {
      const p = Number(getPath(row, cfg.pricePath));
      if (Number.isFinite(p)) r = this.deriveReturnFromPrice(p);
    }

    if (r === null) return;

    if (cfg.logReturns) {
      if (r <= -1) return;
      r = Math.log1p(r);
    }

    this.pushReturn(r);
  }

  finalize(annualization = 252) {
    const n = this.retMoments.n;
    const mean = this.retMoments.mean;
    const vol = this.retMoments.std();
    const ewmaVol = this.ewma.std();
    const downsideStd = this.downside.downsideStd();

    const sharpe = vol > 0 ? (mean / vol) * Math.sqrt(annualization) : 0;
    const sortino = downsideStd > 0 ? (mean / downsideStd) * Math.sqrt(annualization) : 0;

    let cagr = null;
    if (this.firstDate && this.lastDate && this.lastDate > this.firstDate) {
      const years = (this.lastDate - this.firstDate) / (365.25 * 24 * 3600 * 1000);
      if (years > 0) cagr = Math.pow(this.equity, 1 / years) - 1;
    }

    return {
      n,
      meanReturn: mean,
      vol,
      ewmaVol,
      equityFinal: this.equity,
      maxDrawdown: this.maxDrawdown,
      sharpe,
      sortino,
      cagr
    };
  }
}

class StatsEngine {
  constructor(cfg) {
    this.cfg = cfg || {};
    this.enabled = !!this.cfg.enabled;

    this.series = new Map();
    this.totalRows = 0;

    this.freq = new FrequencyInferer(this.cfg.freqMaxSamples ?? 2000);

    this.corrEnabled = !!(this.cfg.correlation && this.cfg.correlation.enabled);
    this.corr = this.corrEnabled ? new CorrelationEngine(this.cfg.correlation) : null;

    this.currentDateKey = null;
    this.currentBucket = new Map();

    this.prevPriceBySeries = new Map();
  }

  _deriveReturn(seriesKey, row) {
    const cfg = this.cfg;

    let r = null;
    if (cfg.returnPath) {
      const v = Number(getPath(row, cfg.returnPath));
      if (Number.isFinite(v)) r = v;
    } else if (cfg.pricePath) {
      const p = Number(getPath(row, cfg.pricePath));
      if (!Number.isFinite(p)) return null;

      const prev = this.prevPriceBySeries.get(seriesKey);
      if (prev !== undefined && prev !== null && prev !== 0) {
        r = (p / prev) - 1;
      }
      this.prevPriceBySeries.set(seriesKey, p);
    }

    if (r === null) return null;

    if (cfg.logReturns) {
      if (r <= -1) return null;
      r = Math.log1p(r);
    }

    return r;
  }

  _flushCorrelationBucket() {
    if (!this.corrEnabled) return;
    if (this.currentBucket.size === 0) return;
    this.corr.observeBucket(this.currentBucket);
    this.currentBucket = new Map();
  }

  observe(row) {
    if (!this.enabled) return;
    this.totalRows++;

    const seriesKey = toKey(row, this.cfg.seriesKeyPath);

    if (this.cfg.datePath) {
      const dv = getPath(row, this.cfg.datePath);
      this.freq.observe(seriesKey, dv);
    }

    let st = this.series.get(seriesKey);
    if (!st) {
      st = new SeriesState(this.cfg);
      this.series.set(seriesKey, st);
    }
    st.push(row);

    if (this.corrEnabled) {
      if (!this.cfg.datePath) return;
      const dv = getPath(row, this.cfg.datePath);
      const dk = toDateKey(dv);
      if (!dk) return;

      if (this.currentDateKey === null) this.currentDateKey = dk;
      if (dk !== this.currentDateKey) {
        this._flushCorrelationBucket();
        this.currentDateKey = dk;
      }

      const r = this._deriveReturn(seriesKey, row);
      if (r !== null) this.currentBucket.set(seriesKey, r);
    }
  }

  finalize() {
    if (!this.enabled) return { enabled: false };

    this._flushCorrelationBucket();

    const inferred = this.freq.finalize(252);
    const annualization = this.cfg.annualization ?? inferred.annualization;

    const perSeries = {};
    for (const [k, st] of this.series.entries()) {
      perSeries[k] = st.finalize(annualization);
    }

    const correlation = this.corrEnabled ? this.corr.finalize() : null;

    return {
      enabled: true,
      createdAt: new Date(),
      totalRows: this.totalRows,
      config: {
        seriesKeyPath: this.cfg.seriesKeyPath || null,
        pricePath: this.cfg.pricePath || null,
        returnPath: this.cfg.returnPath || null,
        datePath: this.cfg.datePath || null,
        annualization,
        annualizationInferred: this.cfg.annualization == null ? inferred : { inferred: false },
        logReturns: !!this.cfg.logReturns,
        ewmaLambda: this.cfg.ewmaLambda ?? 0.94,
        correlation: this.cfg.correlation || { enabled: false }
      },
      perSeries,
      correlation
    };
  }
}

module.exports = { StatsEngine };
