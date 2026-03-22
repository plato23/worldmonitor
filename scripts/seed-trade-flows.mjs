#!/usr/bin/env node
// Seed UN Comtrade strategic commodity trade flows (issue #2045).
// Uses the public preview endpoint — no auth required.

import { loadEnvFile, CHROME_UA, runSeed, sleep, writeExtraKey } from './_seed-utils.mjs';

loadEnvFile(import.meta.url);

const CANONICAL_KEY = 'comtrade:flows:v1';
const CACHE_TTL = 86400; // 24h
const KEY_PREFIX = 'comtrade:flows';
const COMTRADE_BASE = 'https://comtradeapi.un.org/public/v1';
const INTER_REQUEST_DELAY_MS = 3_000;
const ANOMALY_THRESHOLD = 0.30; // 30% YoY change

// Strategic reporters: US, China, Russia, Iran, India, Taiwan
const REPORTERS = [
  { code: '842', name: 'USA' },
  { code: '156', name: 'China' },
  { code: '643', name: 'Russia' },
  { code: '364', name: 'Iran' },
  { code: '356', name: 'India' },
  { code: '158', name: 'Taiwan' },
];

// Strategic HS commodity codes
const COMMODITIES = [
  { code: '2709', desc: 'Crude oil' },
  { code: '2711', desc: 'LNG / natural gas' },
  { code: '7108', desc: 'Gold' },
  { code: '8542', desc: 'Semiconductors' },
  { code: '9301', desc: 'Arms / military equipment' },
];

async function fetchFlows(reporter, commodity) {
  const url = new URL(`${COMTRADE_BASE}/preview/C/A/HS`);
  url.searchParams.set('reporterCode', reporter.code);
  url.searchParams.set('cmdCode', commodity.code);
  url.searchParams.set('flowCode', 'X,M'); // exports + imports

  const resp = await fetch(url.toString(), {
    headers: { 'User-Agent': CHROME_UA, Accept: 'application/json' },
    signal: AbortSignal.timeout(15_000),
  });

  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  const data = await resp.json();

  // Comtrade preview returns { data: [...] } with annual records
  const records = data?.data ?? [];
  if (!Array.isArray(records)) return [];

  // Group by period to compute YoY
  const byYear = new Map();
  for (const r of records) {
    const year = Number(r.period ?? r.refYear ?? r.refMonth?.slice(0, 4) ?? 0);
    if (!year) continue;
    const val = Number(r.primaryValue ?? r.cifvalue ?? r.fobvalue ?? 0);
    const wt = Number(r.netWgt ?? 0);
    const partnerCode = String(r.partnerCode ?? r.partner2Code ?? '000');
    const partnerName = String(r.partnerDesc ?? r.partner2Desc ?? 'World');
    byYear.set(year, { year, val, wt, partnerCode, partnerName });
  }

  const years = Array.from(byYear.keys()).sort((a, b) => a - b);
  const flows = [];

  for (const year of years) {
    const cur = byYear.get(year);
    const prev = byYear.get(year - 1);
    const yoyChange = prev && prev.val > 0 ? (cur.val - prev.val) / prev.val : 0;
    const isAnomaly = Math.abs(yoyChange) > ANOMALY_THRESHOLD;

    flows.push({
      reporterCode: reporter.code,
      reporterName: reporter.name,
      partnerCode: cur.partnerCode,
      partnerName: cur.partnerName,
      cmdCode: commodity.code,
      cmdDesc: commodity.desc,
      year,
      tradeValueUsd: cur.val,
      netWeightKg: cur.wt,
      yoyChange,
      isAnomaly,
    });
  }

  return flows;
}

async function fetchAllFlows() {
  const allFlows = [];
  const perKeyFlows = {};

  for (let ri = 0; ri < REPORTERS.length; ri++) {
    for (let ci = 0; ci < COMMODITIES.length; ci++) {
      const reporter = REPORTERS[ri];
      const commodity = COMMODITIES[ci];
      const label = `${reporter.name}/${commodity.desc}`;

      if (ri > 0 || ci > 0) await sleep(INTER_REQUEST_DELAY_MS);
      console.log(`  Fetching ${label}...`);

      let flows = [];
      try {
        flows = await fetchFlows(reporter, commodity);
        console.log(`    ${flows.length} records`);
      } catch (err) {
        console.warn(`    ${label}: failed (${err.message})`);
      }

      allFlows.push(...flows);
      const key = `${KEY_PREFIX}:${reporter.code}:${commodity.code}`;
      perKeyFlows[key] = { flows, fetchedAt: new Date().toISOString() };
    }
  }

  return { flows: allFlows, perKeyFlows, fetchedAt: new Date().toISOString() };
}

function validate(data) {
  return Array.isArray(data?.flows) && data.flows.length > 0;
}

function publishTransform(data) {
  const { perKeyFlows: _pkf, ...rest } = data;
  return rest;
}

async function afterPublish(data, _meta) {
  for (const [key, value] of Object.entries(data.perKeyFlows ?? {})) {
    await writeExtraKey(key, value, CACHE_TTL);
  }
}

runSeed('trade', 'comtrade-flows', CANONICAL_KEY, fetchAllFlows, {
  validateFn: validate,
  ttlSeconds: CACHE_TTL,
  sourceVersion: 'comtrade-preview-v1',
  publishTransform,
  afterPublish,
}).catch((err) => {
  const _cause = err.cause ? ` (cause: ${err.cause.message || err.cause.code || err.cause})` : '';
  console.error('FATAL:', (err.message || err) + _cause);
  process.exit(1);
});
