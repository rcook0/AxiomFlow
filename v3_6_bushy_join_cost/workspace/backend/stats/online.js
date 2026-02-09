class OnlineMoments {
  constructor() {
    this.n = 0;
    this.mean = 0;
    this.m2 = 0;
  }
  push(x) {
    this.n += 1;
    const delta = x - this.mean;
    this.mean += delta / this.n;
    const delta2 = x - this.mean;
    this.m2 += delta * delta2;
  }
  variance() {
    return this.n > 1 ? this.m2 / (this.n - 1) : 0;
  }
  std() {
    return Math.sqrt(this.variance());
  }
}

class OnlineDownsideMoments {
  constructor(target = 0) {
    this.target = target;
    this.m = new OnlineMoments();
  }
  push(x) {
    const d = x - this.target;
    if (d < 0) this.m.push(d);
  }
  downsideStd() {
    return this.m.std();
  }
}

class EWMA {
  constructor(lambda = 0.94) {
    this.lambda = lambda;
    this.var = null;
  }
  push(r) {
    const x2 = r * r;
    if (this.var === null) this.var = x2;
    else this.var = this.lambda * this.var + (1 - this.lambda) * x2;
  }
  std() {
    return this.var === null ? 0 : Math.sqrt(this.var);
  }
}

module.exports = { OnlineMoments, OnlineDownsideMoments, EWMA };
