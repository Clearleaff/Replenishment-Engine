import http from 'k6/http';
import { check } from 'k6';

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],

  scenarios: {
    catalog_reads: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 300,

      stages: [
        { duration: '30s', target: 50 },
        { duration: '30s', target: 100 },
        { duration: '1m', target: 250 },
        { duration: '1m', target: 500 },
        { duration: '15s', target: 0 },
      ],
    },
  },

  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
    checks: ['rate>0.99'],
    dropped_iterations: ['count==0'],
  },
};

export default function () {
  const response = http.get(
    'http://127.0.0.1:5222/api/catalog/items?api-version=2.0&pageIndex=0&pageSize=10'
  );

  check(response, {
    'status is 200': (response) => response.status === 200,
  });
}