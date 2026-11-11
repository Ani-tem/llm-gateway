// k6 load test for the gateway. Run with: make loadtest
//
// Three phases, each at a constant arrival rate (requests are started on a
// fixed schedule whether or not earlier ones have finished, so a slow server
// cannot hide behind a slow client):
//
//   uncached  every prompt is new: auth, rate limit, cache miss, upstream
//             call (mock, 20 ms), cache write, usage event
//   cached    a small set of prompts repeated: almost all cache hits
//   chaos     a route whose first provider fails 30% of the time, exercising
//             retries, failover and the circuit breaker
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://host.docker.internal:18080';
const KEY = __ENV.API_KEY || 'sk-loadtest';
const RATE = Number(__ENV.RATE || 500);
const DURATION = __ENV.DURATION || '30s';

function phase(exec, startTime) {
  return {
    executor: 'constant-arrival-rate',
    exec,
    startTime,
    rate: RATE,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: 100,
    maxVUs: 2000,
  };
}

export const options = {
  scenarios: {
    uncached: phase('uncached', '0s'),
    cached: phase('cached', '35s'),
    chaos: phase('chaos', '70s'),
  },
  // Thresholds on a scenario tag also make k6 print that scenario's numbers.
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{scenario:uncached}': ['p(99)<250'],
    'http_req_duration{scenario:cached}': ['p(99)<100'],
    'http_req_duration{scenario:chaos}': ['p(99)<1500'],
    'http_req_failed{scenario:chaos}': ['rate<0.01'],
    // A few are expected: k6 in Docker stalls on its first connections to the
    // host. The gateway's own log shows no request slower than ~260 ms.
    dropped_iterations: ['rate<5'],
  },
  summaryTrendStats: ['med', 'p(95)', 'p(99)', 'max'],
};

const params = {
  headers: { Authorization: `Bearer ${KEY}`, 'Content-Type': 'application/json' },
};

function chat(model, prompt) {
  const res = http.post(
    `${BASE}/v1/chat/completions`,
    JSON.stringify({ model, messages: [{ role: 'user', content: prompt }] }),
    params,
  );
  check(res, { 'status is 200': (r) => r.status === 200 });
  return res;
}

export function uncached() {
  chat('mock/echo', `unique ${__VU}-${__ITER}-${Date.now()}`);
}

export function cached() {
  chat('mock/echo', `popular question ${__ITER % 20}`);
}

export function chaos() {
  chat('chaos', `chaos ${__VU}-${__ITER}-${Date.now()}`);
}
