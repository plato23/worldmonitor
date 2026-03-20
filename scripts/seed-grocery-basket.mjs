#!/usr/bin/env node

import { loadEnvFile, loadSharedConfig, CHROME_UA, runSeed, sleep } from './_seed-utils.mjs';

loadEnvFile(import.meta.url);

const config = loadSharedConfig('grocery-basket.json');

const CANONICAL_KEY = 'economic:grocery-basket:v1';
const CACHE_TTL = 21600; // 6h
const EXA_DELAY_MS = 150;

// Hardcoded FX fallbacks — used when Yahoo Finance returns null/zero
const FX_FALLBACKS = {
  // Middle East (pegged)
  AED: 0.2723, SAR: 0.2666, QAR: 0.2747, KWD: 3.2520,
  BHD: 2.6525, OMR: 2.5974, JOD: 1.4104, EGP: 0.0192, LBP: 0.0000112,
  // Major currencies
  USD: 1.0000, GBP: 1.2700, EUR: 1.0850, JPY: 0.0067,
  CNY: 0.1380, INR: 0.0120, AUD: 0.6500, CAD: 0.7400,
  BRL: 0.1900, MXN: 0.0490, ZAR: 0.0540, TRY: 0.0290,
  KRW: 0.0007, SGD: 0.7400, PKR: 0.0036,
  // Emerging
  NGN: 0.00062, KES: 0.0077, ARS: 0.00084, IDR: 0.000063, PHP: 0.0173,
};

async function fetchFxRates() {
  const rates = {};
  for (const [currency, symbol] of Object.entries(config.fxSymbols)) {
    if (currency === 'USD') { rates['USD'] = 1.0; continue; } // USD is the base
    try {
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}`;
      const resp = await fetch(url, {
        headers: { 'User-Agent': CHROME_UA },
        signal: AbortSignal.timeout(8_000),
      });
      if (!resp.ok) {
        rates[currency] = FX_FALLBACKS[currency] || null;
        continue;
      }
      const data = await resp.json();
      const price = data?.chart?.result?.[0]?.meta?.regularMarketPrice;
      rates[currency] = (price != null && price > 0) ? price : (FX_FALLBACKS[currency] ?? null);
    } catch {
      rates[currency] = FX_FALLBACKS[currency] || null;
    }
    await sleep(100);
  }
  console.log('  FX rates fetched:', JSON.stringify(rates));
  return rates;
}

async function searchExa(query) {
  // Support both EXA_API_KEYS (comma-separated, shared with ais-relay) and EXA_API_KEY
  const apiKey = (process.env.EXA_API_KEYS || process.env.EXA_API_KEY || '').split(/[\n,]+/)[0].trim();
  if (!apiKey) throw new Error('EXA_API_KEYS or EXA_API_KEY not set');

  const resp = await fetch('https://api.exa.ai/search', {
    method: 'POST',
    headers: {
      'x-api-key': apiKey,
      'Content-Type': 'application/json',
      'User-Agent': CHROME_UA,
    },
    body: JSON.stringify({
      query,
      // No includeDomains — EXA neural search finds better sources (price comparison
      // sites, retailer pages, market reports) than hardcoded domain lists
      numResults: 5,
      type: 'auto',
      contents: {
        summary: {
          query: 'What is the retail price of this product? Include currency symbol and amount.',
        },
      },
    }),
    signal: AbortSignal.timeout(15_000),
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    console.warn(`  EXA ${resp.status}: ${text.slice(0, 100)}`);
    return null;
  }
  return resp.json();
}

// All supported currency codes — keep in sync with grocery-basket.json fxSymbols
const CCY = 'USD|GBP|EUR|JPY|CNY|INR|AUD|CAD|BRL|MXN|ZAR|TRY|NGN|KRW|SGD|PKR|AED|SAR|QAR|KWD|BHD|OMR|EGP|JOD|LBP|KES|ARS|IDR|PHP';
const PRICE_PATTERNS = [
  new RegExp(`(\\d+(?:\\.\\d{1,3})?)\\s*(${CCY})`, 'i'),
  new RegExp(`(${CCY})\\s*(\\d+(?:\\.\\d{1,3})?)`, 'i'),
];

function matchPrice(text, url) {
  for (const re of PRICE_PATTERNS) {
    const match = text.match(re);
    if (match) {
      // Pattern 1: price then currency (match[1]=price, match[2]=currency)
      // Pattern 2: currency then price (match[1]=currency, match[2]=price)
      const [price, currency] = /^\d/.test(match[1])
        ? [parseFloat(match[1]), match[2].toUpperCase()]
        : [parseFloat(match[2]), match[1].toUpperCase()];
      if (price > 0 && price < 100000) return { price, currency, source: url || '' };
    }
  }
  return null;
}

function extractPrice(result, expectedCurrency) {
  const url = result.url || '';
  const summary = result?.summary;
  if (summary && typeof summary === 'string') {
    const hit = matchPrice(summary, url);
    // Reject if matched currency doesn't match the country we're querying for
    if (hit && hit.currency !== expectedCurrency) {
      console.warn(`    [extractPrice] currency mismatch: got ${hit.currency}, expected ${expectedCurrency} — ${url}`);
      return null;
    }
    if (hit) return hit;
  }
  // Fallback: title (no bare-number — too many false positives from weights/codes)
  const fromTitle = matchPrice(result.title || '', url);
  if (fromTitle && fromTitle.currency !== expectedCurrency) return null;
  return fromTitle;
}

async function fetchGroceryBasketPrices() {
  const fxRates = await fetchFxRates();

  const countriesResult = [];

  for (const country of config.countries) {
    console.log(`\n  Processing ${country.flag} ${country.name} (${country.currency})...`);
    const itemPrices = [];
    let totalUsd = 0;
    const fxRate = fxRates[country.currency] || FX_FALLBACKS[country.currency] || null;

    for (const item of config.items) {
      await sleep(EXA_DELAY_MS);

      let localPrice = null;
      let sourceSite = '';

      try {
        const query = `${item.query} ${country.name} supermarket retail price`;
        const exaResult = await searchExa(query);

        if (exaResult?.results?.length) {
          for (const result of exaResult.results) {
            const extracted = extractPrice(result, country.currency);
            if (extracted) {
              localPrice = extracted.price;
              sourceSite = extracted.source;
              break;
            }
          }
        }
      } catch (err) {
        console.warn(`    [${country.code}/${item.id}] EXA error: ${err.message}`);
      }

      const usdPrice = localPrice !== null && fxRate ? +(localPrice * fxRate).toFixed(4) : null;
      if (usdPrice !== null) totalUsd += usdPrice;

      itemPrices.push({
        itemId: item.id,
        itemName: item.name,
        unit: item.unit,
        localPrice: localPrice !== null ? +localPrice.toFixed(4) : null,
        usdPrice: usdPrice,
        currency: country.currency,
        sourceSite,
        available: localPrice !== null,
      });

      const status = localPrice !== null ? `${localPrice} ${country.currency} = $${usdPrice}` : 'N/A';
      console.log(`    ${item.id}: ${status}`);
    }

    countriesResult.push({
      code: country.code,
      name: country.name,
      currency: country.currency,
      flag: country.flag,
      totalUsd: +totalUsd.toFixed(2),
      fxRate: fxRate || 0,
      items: itemPrices,
    });
  }

  // Determine cheapest/most expensive
  const withData = countriesResult.filter(c => c.totalUsd > 0);
  const cheapest = withData.length ? withData.reduce((a, b) => a.totalUsd < b.totalUsd ? a : b).code : '';
  const mostExpensive = withData.length ? withData.reduce((a, b) => a.totalUsd > b.totalUsd ? a : b).code : '';

  return {
    countries: countriesResult,
    fetchedAt: new Date().toISOString(),
    cheapestCountry: cheapest,
    mostExpensiveCountry: mostExpensive,
  };
}

await runSeed('economic', 'grocery-basket', CANONICAL_KEY, fetchGroceryBasketPrices, {
  ttlSeconds: CACHE_TTL,
  validateFn: (data) => data?.countries?.length > 0,
  recordCount: (data) => data?.countries?.length || 0,
});
