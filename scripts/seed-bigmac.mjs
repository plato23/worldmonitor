#!/usr/bin/env node

import { loadEnvFile, CHROME_UA, runSeed, sleep } from './_seed-utils.mjs';

loadEnvFile(import.meta.url);

const CANONICAL_KEY = 'economic:bigmac:v1';
const CACHE_TTL = 86400; // 24h — Big Mac prices change rarely
const EXA_DELAY_MS = 150;

const FX_FALLBACKS = {
  AED: 0.2723, SAR: 0.2666, QAR: 0.2747, KWD: 3.2520,
  BHD: 2.6525, OMR: 2.5974, JOD: 1.4104, EGP: 0.0204, LBP: 0.0000112,
};

const COUNTRIES = [
  { code: 'AE', name: 'UAE',          currency: 'AED', flag: '🇦🇪' },
  { code: 'SA', name: 'Saudi Arabia', currency: 'SAR', flag: '🇸🇦' },
  { code: 'QA', name: 'Qatar',        currency: 'QAR', flag: '🇶🇦' },
  { code: 'KW', name: 'Kuwait',       currency: 'KWD', flag: '🇰🇼' },
  { code: 'BH', name: 'Bahrain',      currency: 'BHD', flag: '🇧🇭' },
  { code: 'OM', name: 'Oman',         currency: 'OMR', flag: '🇴🇲' },
  { code: 'EG', name: 'Egypt',        currency: 'EGP', flag: '🇪🇬' },
  { code: 'JO', name: 'Jordan',       currency: 'JOD', flag: '🇯🇴' },
  { code: 'LB', name: 'Lebanon',      currency: 'LBP', flag: '🇱🇧' },
];

const FX_SYMBOLS = {
  AED: 'AEDUSD=X', SAR: 'SARUSD=X', QAR: 'QARUSD=X', KWD: 'KWDUSD=X',
  BHD: 'BHDUSD=X', OMR: 'OMRUSD=X', EGP: 'EGPUSD=X', JOD: 'JODUSD=X', LBP: 'LBPUSD=X',
};

// Handle both plain numbers and thousands-separated (480,000 LBP)
const NUM = '\\d{1,3}(?:[,\\s]\\d{3})*(?:\\.\\d{1,3})?';
const CCY = 'AED|SAR|QAR|KWD|BHD|OMR|EGP|JOD|LBP|USD';
const PRICE_PATTERNS = [
  new RegExp(`(${NUM})\\s*(${CCY})`, 'i'),
  new RegExp(`(${CCY})\\s*(${NUM})`, 'i'),
];

function parseNum(s) { return parseFloat(s.replace(/[,\s]/g, '')); }

function matchPrice(text, url) {
  for (const re of PRICE_PATTERNS) {
    const match = text.match(re);
    if (match) {
      const [price, currency] = /^\d/.test(match[1])
        ? [parseNum(match[1]), match[2].toUpperCase()]
        : [parseNum(match[2]), match[1].toUpperCase()];
      if (price > 0 && price < 10_000_000) return { price, currency, source: url || '' };
    }
  }
  return null;
}

async function fetchFxRates() {
  const rates = {};
  for (const [currency, symbol] of Object.entries(FX_SYMBOLS)) {
    try {
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}`;
      const resp = await fetch(url, {
        headers: { 'User-Agent': CHROME_UA },
        signal: AbortSignal.timeout(8_000),
      });
      if (!resp.ok) { rates[currency] = FX_FALLBACKS[currency] ?? null; continue; }
      const data = await resp.json();
      const price = data?.chart?.result?.[0]?.meta?.regularMarketPrice;
      rates[currency] = (price != null && price > 0) ? price : (FX_FALLBACKS[currency] ?? null);
    } catch {
      rates[currency] = FX_FALLBACKS[currency] ?? null;
    }
    await sleep(100);
  }
  console.log('  FX rates fetched:', JSON.stringify(rates));
  return rates;
}

async function searchExa(query, includeDomains = null) {
  const apiKey = (process.env.EXA_API_KEYS || process.env.EXA_API_KEY || '').split(/[\n,]+/)[0].trim();
  if (!apiKey) throw new Error('EXA_API_KEYS or EXA_API_KEY not set');

  const body = {
    query,
    numResults: 5,
    type: 'auto',
    contents: { summary: { query: 'What is the current Big Mac price in local currency and USD?' } },
  };
  if (includeDomains) body.includeDomains = includeDomains;

  const resp = await fetch('https://api.exa.ai/search', {
    method: 'POST',
    headers: { 'x-api-key': apiKey, 'Content-Type': 'application/json', 'User-Agent': CHROME_UA },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    console.warn(`  EXA ${resp.status}: ${text.slice(0, 100)}`);
    return null;
  }
  return resp.json();
}

async function fetchBigMacPrices() {
  const fxRates = await fetchFxRates();
  const results = [];

  for (const country of COUNTRIES) {
    await sleep(EXA_DELAY_MS);
    console.log(`\n  Processing ${country.flag} ${country.name} (${country.currency})...`);

    const fxRate = fxRates[country.currency] ?? FX_FALLBACKS[country.currency] ?? null;
    let localPrice = null;
    let usdPrice = null;
    let sourceSite = '';

    try {
      // Include currency code in query — helps EXA find per-country specialist pages
      const query = `Big Mac price ${country.name} ${country.currency}`;
      const SPECIALIST_SITES = ['theburgerindex.com', 'eatmyindex.com'];

      // Specialist Big Mac Index sites only — clean, verified per-country data
      const exaResult = await searchExa(query, SPECIALIST_SITES);
      await sleep(EXA_DELAY_MS);

      if (exaResult?.results?.length) {
        for (const result of exaResult.results) {
          const summary = result?.summary;
          if (!summary || typeof summary !== 'string') continue;
          const hit = matchPrice(summary, result.url || '');
          if (hit?.currency === country.currency) {
            localPrice = hit.price;
            sourceSite = hit.source;
            break;
          }
        }
      }
    } catch (err) {
      console.warn(`    [${country.code}] EXA error: ${err.message}`);
    }

    if (usdPrice === null) {
      usdPrice = localPrice !== null && fxRate ? +(localPrice * fxRate).toFixed(4) : null;
    }
    const status = localPrice !== null ? `${localPrice} ${country.currency} = $${usdPrice}` : 'N/A';
    console.log(`    Big Mac: ${status}`);

    results.push({
      code: country.code,
      name: country.name,
      currency: country.currency,
      flag: country.flag,
      localPrice: localPrice !== null ? +localPrice.toFixed(4) : null,
      usdPrice,
      fxRate: fxRate || 0,
      sourceSite,
      available: usdPrice !== null,
    });
  }

  const withData = results.filter(r => r.usdPrice != null);
  const cheapest = withData.length ? withData.reduce((a, b) => a.usdPrice < b.usdPrice ? a : b).code : '';
  const mostExpensive = withData.length ? withData.reduce((a, b) => a.usdPrice > b.usdPrice ? a : b).code : '';

  return {
    countries: results,
    fetchedAt: new Date().toISOString(),
    cheapestCountry: cheapest,
    mostExpensiveCountry: mostExpensive,
  };
}

await runSeed('economic', 'bigmac', CANONICAL_KEY, fetchBigMacPrices, {
  ttlSeconds: CACHE_TTL,
  validateFn: (data) => data?.countries?.length > 0,
  recordCount: (data) => data?.countries?.filter(c => c.available).length || 0,
});
