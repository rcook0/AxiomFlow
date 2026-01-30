function secondsBetween(a, b) {
  const da = new Date(a).getTime();
  const db = new Date(b).getTime();
  if (!Number.isFinite(da) || !Number.isFinite(db)) return null;
  return Math.abs(db - da) / 1000;
}

function median(nums) {
  const a = nums.slice().sort((x, y) => x - y);
  const mid = Math.floor(a.length / 2);
  return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
}

function inferAnnualizationFromSeconds(medSec) {
  if (!medSec || !Number.isFinite(medSec) || medSec <= 0) return 252;

  const day = 24 * 3600;

  if (medSec >= 25 * day) return 12;
  if (medSec >= 6 * day) return 52;
  if (medSec >= 0.8 * day && medSec <= 1.2 * day) return 252;
  if (medSec >= 0.03 * day && medSec < 0.8 * day) return 252 * 24;
  if (medSec < 0.03 * day) return 252 * 24 * 60;
  return 252;
}

class FrequencyInferer {
  constructor(maxSamples = 2000) {
    this.maxSamples = maxSamples;
    this.deltas = [];
    this.lastDateBySeries = new Map();
  }

  observe(seriesKey, dateVal) {
    if (!dateVal) return;
    const last = this.lastDateBySeries.get(seriesKey);
    if (last) {
      const sec = secondsBetween(last, dateVal);
      if (sec && this.deltas.length < this.maxSamples) this.deltas.push(sec);
    }
    this.lastDateBySeries.set(seriesKey, dateVal);
  }

  finalize(defaultAnnualization = 252) {
    if (this.deltas.length < 10) return { annualization: defaultAnnualization, medianSeconds: null, inferred: false };
    const med = median(this.deltas);
    return { annualization: inferAnnualizationFromSeconds(med), medianSeconds: med, inferred: true };
  }
}

module.exports = { FrequencyInferer };
