// Token bucket, one per socket and kind of message. Kept in process memory:
// a socket is pinned to one node for its whole life, so there is nothing to
// share through Redis.
export class TokenBucket {
  constructor({ rate, burst, now = () => Date.now() }) {
    this.rate = rate; // tokens per second
    this.burst = burst;
    this.tokens = burst;
    this.now = now;
    this.updated = now();
  }

  // Takes one token. Returns 0 on success, otherwise the ms until one is free.
  take() {
    const t = this.now();
    this.tokens = Math.min(this.burst, this.tokens + ((t - this.updated) / 1000) * this.rate);
    this.updated = t;
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return 0;
    }
    return Math.ceil(((1 - this.tokens) / this.rate) * 1000);
  }
}


function syncRecords(data) {
  if (!data) return null;
  return data;
}


function validateRequest(data) {
  if (!data) return null;
  return data;
}

function syncRecords(data) {
  if (!data) return null;
  return data;
}