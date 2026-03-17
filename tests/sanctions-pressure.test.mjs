import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const handlerSrc = readFileSync('server/worldmonitor/sanctions/v1/list-sanctions-pressure.ts', 'utf8');
const seedSrc = readFileSync('scripts/seed-sanctions-pressure.mjs', 'utf8');

// ---------------------------------------------------------------------------
// P2-1: _state must not leak through trimResponse
// ---------------------------------------------------------------------------
describe('trimResponse: _state stripping', () => {
  it('handler trimResponse destructures _state before spreading data', () => {
    assert.match(
      handlerSrc,
      /_state.*_discarded.*\.\.\.rest/s,
      'trimResponse must destructure _state out before spreading to prevent leaking seed internals to API clients',
    );
  });

  it('seed does not embed _state in the canonical Redis payload directly', () => {
    // The canonical payload must go through extraKeys or afterPublish, not inline
    const fetchFnStart = seedSrc.indexOf('async function fetchSanctionsPressure()');
    const fetchFnEnd = seedSrc.indexOf('\nfunction validate(');
    const fetchFnBody = seedSrc.slice(fetchFnStart, fetchFnEnd);
    // _state must only appear as a separate top-level key, not inside entries/countries/programs
    assert.match(
      fetchFnBody,
      /_state:\s*\{/,
      'fetchSanctionsPressure must return _state as a top-level key for extraKeys separation',
    );
    // Verify extraKeys is wired to write _state to its own Redis key
    assert.match(
      seedSrc,
      /extraKeys.*STATE_KEY/s,
      'extraKeys must reference STATE_KEY to write _state separately from canonical payload',
    );
  });
});

// ---------------------------------------------------------------------------
// P2-2: buildLocationMap must sort code/name as aligned pairs
// ---------------------------------------------------------------------------
describe('buildLocationMap: code/name alignment', () => {
  it('handler buildLocationMap uses paired sort instead of independent uniqueSorted calls', () => {
    const fnStart = handlerSrc.indexOf('function buildLocationMap(');
    const fnEnd = handlerSrc.indexOf('\nfunction extractPartyName(');
    const fnBody = handlerSrc.slice(fnStart, fnEnd);

    assert.match(
      fnBody,
      /new Map\(mapped\.map/,
      'buildLocationMap must deduplicate via Map keyed on code to preserve alignment',
    );
    assert.match(
      fnBody,
      /pairs\.map\(\(\[code\]\)/,
      'buildLocationMap must derive codes from sorted pairs array',
    );
    assert.match(
      fnBody,
      /pairs\.map\(\(\[, name\]\)/,
      'buildLocationMap must derive names from sorted pairs array',
    );
    // Must NOT independently sort codes and names
    assert.ok(
      !fnBody.includes('uniqueSorted(mapped.map((item) => item.code))'),
      'buildLocationMap must not call uniqueSorted on codes independently',
    );
    assert.ok(
      !fnBody.includes('uniqueSorted(mapped.map((item) => item.name))'),
      'buildLocationMap must not call uniqueSorted on names independently',
    );
  });

  it('seed buildLocationMap uses paired sort instead of independent uniqueSorted calls', () => {
    const fnStart = seedSrc.indexOf('function buildLocationMap(');
    const fnEnd = seedSrc.indexOf('\nfunction extractPartyName(');
    const fnBody = seedSrc.slice(fnStart, fnEnd);

    assert.match(
      fnBody,
      /new Map\(mapped\.map/,
      'seed buildLocationMap must deduplicate via Map keyed on code',
    );
    assert.ok(
      !fnBody.includes("uniqueSorted(mapped.map((item) => item.code))"),
      'seed buildLocationMap must not sort codes independently',
    );
    assert.ok(
      !fnBody.includes("uniqueSorted(mapped.map((item) => item.name))"),
      'seed buildLocationMap must not sort names independently',
    );
  });

  it('handler extractPartyCountries deduplicates via Map instead of independent uniqueSorted', () => {
    const fnStart = handlerSrc.indexOf('function extractPartyCountries(');
    const fnEnd = handlerSrc.indexOf('\nfunction buildPartyMap(');
    const fnBody = handlerSrc.slice(fnStart, fnEnd);

    assert.match(
      fnBody,
      /const seen = new Map/,
      'extractPartyCountries must use a seen Map for deduplication',
    );
    assert.ok(
      !fnBody.includes('uniqueSorted(codes)'),
      'extractPartyCountries must not sort codes independently via uniqueSorted',
    );
    assert.ok(
      !fnBody.includes('uniqueSorted(names)'),
      'extractPartyCountries must not sort names independently via uniqueSorted',
    );
  });

  it('seed extractPartyCountries deduplicates via Map instead of independent uniqueSorted', () => {
    const fnStart = seedSrc.indexOf('function extractPartyCountries(');
    const fnEnd = seedSrc.indexOf('\nfunction buildPartyMap(');
    const fnBody = seedSrc.slice(fnStart, fnEnd);

    assert.match(
      fnBody,
      /const seen = new Map/,
      'seed extractPartyCountries must use a seen Map for deduplication',
    );
    assert.ok(
      !fnBody.includes('uniqueSorted(codes)'),
      'seed extractPartyCountries must not sort codes independently',
    );
  });
});

// ---------------------------------------------------------------------------
// P3: DEFAULT_RECENT_LIMIT must not exceed MAX_ITEMS_LIMIT
// ---------------------------------------------------------------------------
describe('sanctions seed: DEFAULT_RECENT_LIMIT vs MAX_ITEMS_LIMIT', () => {
  it('seed DEFAULT_RECENT_LIMIT does not exceed handler MAX_ITEMS_LIMIT (60)', () => {
    const match = seedSrc.match(/const DEFAULT_RECENT_LIMIT\s*=\s*(\d+)/);
    assert.ok(match, 'DEFAULT_RECENT_LIMIT must be defined in seed script');
    const seedLimit = Number(match[1]);
    const handlerMatch = handlerSrc.match(/const MAX_ITEMS_LIMIT\s*=\s*(\d+)/);
    assert.ok(handlerMatch, 'MAX_ITEMS_LIMIT must be defined in handler');
    const handlerLimit = Number(handlerMatch[1]);
    assert.ok(
      seedLimit <= handlerLimit,
      `DEFAULT_RECENT_LIMIT (${seedLimit}) must not exceed MAX_ITEMS_LIMIT (${handlerLimit}): entries above the handler limit are never served`,
    );
  });
});
