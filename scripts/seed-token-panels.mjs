#!/usr/bin/env node

import { loadEnvFile, loadSharedConfig, CHROME_UA, writeExtraKey, sleep } from './_seed-utils.mjs';

const defiConfig = loadSharedConfig('defi-tokens.json');
const aiConfig = loadSharedConfig('ai-tokens.json');
const otherConfig = loadSharedConfig('other-tokens.json');

loadEnvFile(import.meta.url);

const DEFI_KEY = 'market:defi-tokens:v1';
const AI_KEY = 'market:ai-tokens:v1';
const OTHER_KEY = 'market:other-tokens:v1';
const CACHE_TTL = 3600;

async function fetchWithRateLimitRetry(url, maxAttempts = 5, headers = { Accept: 'application/json', 'User-Agent': CHROME_UA }) {
  for (let i = 0; i < maxAttempts; i++) {
    const resp = await fetch(url, { headers, signal: AbortSignal.timeout(15_000) });
    if (resp.status === 429) {
      const wait = Math.min(10_000 * (i + 1), 60_000);
      console.warn(`  CoinGecko 429 — waiting ${wait / 1000}s (attempt ${i + 1}/${maxAttempts})`);
      await sleep(wait);
      continue;
    }
    if (!resp.ok) throw new Error(`CoinGecko HTTP ${resp.status}`);
    return resp;
  }
  throw new Error('CoinGecko rate limit exceeded after retries');
}

function mapTokens(ids, meta, byId) {
  const tokens = [];
  for (const id of ids) {
    const coin = byId.get(id);
    if (!coin) continue;
    const m = meta[id];
    tokens.push({
      name: m?.name || coin.name || id,
      symbol: m?.symbol || (coin.symbol || id).toUpperCase(),
      price: coin.current_price ?? 0,
      change24h: coin.price_change_percentage_24h ?? 0,
      change7d: coin.price_change_percentage_7d_in_currency ?? 0,
    });
  }
  return tokens;
}

async function main() {
  const allIds = [...new Set([...defiConfig.ids, ...aiConfig.ids, ...otherConfig.ids])];

  const apiKey = process.env.COINGECKO_API_KEY;
  const baseUrl = apiKey ? 'https://pro-api.coingecko.com/api/v3' : 'https://api.coingecko.com/api/v3';
  const url = `${baseUrl}/coins/markets?vs_currency=usd&ids=${allIds.join(',')}&order=market_cap_desc&sparkline=false&price_change_percentage=24h,7d`;
  const headers = { Accept: 'application/json', 'User-Agent': CHROME_UA };
  if (apiKey) headers['x-cg-pro-api-key'] = apiKey;

  console.log('=== market:token-panels Seed ===');
  console.log(`  Keys:    ${DEFI_KEY}, ${AI_KEY}, ${OTHER_KEY}`);

  const resp = await fetchWithRateLimitRetry(url, 5, headers);
  const data = await resp.json();
  if (!Array.isArray(data) || data.length === 0) throw new Error('CoinGecko returned no data');

  const byId = new Map(data.map(c => [c.id, c]));

  const defi = { tokens: mapTokens(defiConfig.ids, defiConfig.meta, byId) };
  const ai = { tokens: mapTokens(aiConfig.ids, aiConfig.meta, byId) };
  const other = { tokens: mapTokens(otherConfig.ids, otherConfig.meta, byId) };

  if (defi.tokens.length === 0 && ai.tokens.length === 0 && other.tokens.length === 0) {
    throw new Error('All token panels returned empty — refusing to overwrite cache');
  }

  await writeExtraKey(DEFI_KEY, defi, CACHE_TTL);
  await writeExtraKey(AI_KEY, ai, CACHE_TTL);
  await writeExtraKey(OTHER_KEY, other, CACHE_TTL);

  const total = defi.tokens.length + ai.tokens.length + other.tokens.length;
  console.log(`  Seeded: ${defi.tokens.length} DeFi, ${ai.tokens.length} AI, ${other.tokens.length} Other (${total} total)`);
  console.log('\n=== Done ===');
}

main().catch(err => {
  const _cause = err.cause ? ` (cause: ${err.cause.message || err.cause.code || err.cause})` : '';
  console.error('FATAL:', (err.message || err) + _cause);
  process.exit(1);
});
