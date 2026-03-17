import { XMLParser } from 'fast-xml-parser';

import type {
  ListSanctionsPressureRequest,
  ListSanctionsPressureResponse,
  ProgramSanctionsPressure,
  SanctionsEntityType,
  SanctionsEntry,
  SanctionsServiceHandler,
  ServerContext,
  CountrySanctionsPressure,
} from '../../../../src/generated/server/worldmonitor/sanctions/v1/service_server';
import { CHROME_UA } from '../../../_shared/constants';
import { cachedFetchJson, getCachedJson } from '../../../_shared/redis';

const REDIS_CACHE_KEY = 'sanctions:pressure:v1';
const REDIS_CACHE_TTL = 30 * 60;
const SEED_FRESHNESS_MS = 18 * 60 * 60 * 1000;
const DEFAULT_MAX_ITEMS = 25;
const MAX_ITEMS_LIMIT = 60;
const OFAC_TIMEOUT_MS = 12_000;
const PROGRAM_CODE_RE = /^[A-Z0-9][A-Z0-9-]{1,24}$/;

const OFAC_SOURCES = [
  { label: 'SDN', url: 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/sdn_advanced.xml' },
  { label: 'CONSOLIDATED', url: 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/cons_advanced.xml' },
] as const;

const XML_PARSER = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  removeNSPrefix: true,
  parseTagValue: false,
  trimValues: true,
});

type InternalEntry = SanctionsEntry;

function toInt64String(value: number): string {
  return String(Math.max(0, Math.trunc(value)));
}

function listify<T>(value: T | T[] | null | undefined): T[] {
  if (Array.isArray(value)) return value;
  return value == null ? [] : [value];
}

function textValue(value: unknown): string {
  if (value == null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    if (typeof obj['#text'] === 'string') return obj['#text'].trim();
    if (typeof obj.NamePartValue === 'string') return obj.NamePartValue.trim();
  }
  return '';
}

function buildEpoch(parts: Record<string, unknown> | undefined): number {
  const year = Number(parts?.Year || 0);
  if (!year) return 0;
  const month = Math.max(1, Number(parts?.Month || 1));
  const day = Math.max(1, Number(parts?.Day || 1));
  return Date.UTC(year, month - 1, day);
}

function uniqueSorted(values: string[]): string[] {
  return [...new Set(values.filter(Boolean).map((value) => value.trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b));
}

function compactNote(value: string): string {
  const note = value.replace(/\s+/g, ' ').trim();
  if (!note) return '';
  return note.length > 240 ? `${note.slice(0, 237)}...` : note;
}

function extractDocumentedName(documentedName: Record<string, unknown> | undefined): string {
  const parts = listify(documentedName?.DocumentedNamePart as Record<string, unknown> | Record<string, unknown>[])
    .map((part) => textValue((part as Record<string, unknown>)?.NamePartValue))
    .filter(Boolean);
  if (parts.length > 0) return parts.join(' ');
  return textValue(documentedName);
}

function buildReferenceMaps(doc: Record<string, unknown>) {
  const refs = (doc.ReferenceValueSets ?? {}) as Record<string, unknown>;
  const areaCodes = new Map<string, { code: string; name: string }>();
  for (const area of listify((refs.AreaCodeValues as Record<string, unknown> | undefined)?.AreaCode as Record<string, unknown> | Record<string, unknown>[])) {
    areaCodes.set(String(area.ID || ''), {
      code: textValue(area),
      name: String(area.Description || '').trim(),
    });
  }

  const featureTypes = new Map<string, string>();
  for (const feature of listify((refs.FeatureTypeValues as Record<string, unknown> | undefined)?.FeatureType as Record<string, unknown> | Record<string, unknown>[])) {
    featureTypes.set(String(feature.ID || ''), textValue(feature));
  }

  const legalBasis = new Map<string, string>();
  for (const basis of listify((refs.LegalBasisValues as Record<string, unknown> | undefined)?.LegalBasis as Record<string, unknown> | Record<string, unknown>[])) {
    legalBasis.set(String(basis.ID || ''), String(basis.LegalBasisShortRef || textValue(basis) || '').trim());
  }

  return { areaCodes, featureTypes, legalBasis };
}

function buildLocationMap(doc: Record<string, unknown>, areaCodes: Map<string, { code: string; name: string }>) {
  const locations = new Map<string, { codes: string[]; names: string[] }>();
  for (const location of listify(((doc.Locations as Record<string, unknown> | undefined)?.Location) as Record<string, unknown> | Record<string, unknown>[])) {
    const ids = listify(location.LocationAreaCode as Record<string, unknown> | Record<string, unknown>[]).map((item) => String(item.AreaCodeID || ''));
    const mapped = ids.map((id) => areaCodes.get(id)).filter((item): item is { code: string; name: string } => Boolean(item));
    // Sort code/name as pairs so codes[i] always corresponds to names[i]
    const pairs = [...new Map(mapped.map((item) => [item.code, item.name] as [string, string])).entries()]
      .filter(([code]) => code.length > 0)
      .sort(([a], [b]) => a.localeCompare(b));
    locations.set(String(location.ID || ''), {
      codes: pairs.map(([code]) => code),
      names: pairs.map(([, name]) => name),
    });
  }
  return locations;
}

function extractPartyName(profile: Record<string, unknown>): string {
  const identities = listify(profile.Identity as Record<string, unknown> | Record<string, unknown>[]);
  const aliases = identities.flatMap((identity) => listify(identity.Alias as Record<string, unknown> | Record<string, unknown>[]));
  const primaryAlias = aliases.find((alias) => alias?.Primary === 'true')
    || aliases.find((alias) => alias?.AliasTypeID === '1403')
    || aliases[0];
  return extractDocumentedName(primaryAlias?.DocumentedName as Record<string, unknown> | undefined);
}

function resolveEntityType(profile: Record<string, unknown>, featureTypes: Map<string, string>): SanctionsEntityType {
  const subtype = String(profile.PartySubTypeID || '');
  if (subtype === '1') return 'SANCTIONS_ENTITY_TYPE_VESSEL';
  if (subtype === '2') return 'SANCTIONS_ENTITY_TYPE_AIRCRAFT';

  const featureNames = listify(profile.Feature as Record<string, unknown> | Record<string, unknown>[])
    .map((feature) => featureTypes.get(String(feature?.FeatureTypeID || '')) || '')
    .filter(Boolean);

  if (featureNames.some((name) => /birth|citizenship|nationality/i.test(name))) {
    return 'SANCTIONS_ENTITY_TYPE_INDIVIDUAL';
  }
  return 'SANCTIONS_ENTITY_TYPE_ENTITY';
}

function extractPartyCountries(
  profile: Record<string, unknown>,
  featureTypes: Map<string, string>,
  locations: Map<string, { codes: string[]; names: string[] }>,
): { countryCodes: string[]; countryNames: string[] } {
  // Use a Map to deduplicate by code while preserving code→name alignment
  const seen = new Map<string, string>();

  for (const feature of listify(profile.Feature as Record<string, unknown> | Record<string, unknown>[])) {
    const featureType = featureTypes.get(String(feature?.FeatureTypeID || '')) || '';
    if (!/location/i.test(featureType)) continue;

    for (const version of listify(feature.FeatureVersion as Record<string, unknown> | Record<string, unknown>[])) {
      const locationIds = listify(version.VersionLocation as Record<string, unknown> | Record<string, unknown>[]).map((item) => String(item?.LocationID || ''));
      for (const locationId of locationIds) {
        const location = locations.get(locationId);
        if (!location) continue;
        location.codes.forEach((code, i) => {
          if (code && !seen.has(code)) seen.set(code, location.names[i] ?? '');
        });
      }
    }
  }

  const sorted = [...seen.entries()].sort(([a], [b]) => a.localeCompare(b));
  return {
    countryCodes: sorted.map(([c]) => c),
    countryNames: sorted.map(([, n]) => n),
  };
}

function buildPartyMap(
  doc: Record<string, unknown>,
  featureTypes: Map<string, string>,
  locations: Map<string, { codes: string[]; names: string[] }>,
) {
  const parties = new Map<string, { name: string; entityType: SanctionsEntityType; countryCodes: string[]; countryNames: string[] }>();

  for (const distinctParty of listify(((doc.DistinctParties as Record<string, unknown> | undefined)?.DistinctParty) as Record<string, unknown> | Record<string, unknown>[])) {
    const profile = distinctParty.Profile as Record<string, unknown> | undefined;
    const profileId = String(profile?.ID || distinctParty.FixedRef || '');
    if (!profile || !profileId) continue;

    parties.set(profileId, {
      name: extractPartyName(profile),
      entityType: resolveEntityType(profile, featureTypes),
      ...extractPartyCountries(profile, featureTypes, locations),
    });
  }

  return parties;
}

function extractPrograms(entry: Record<string, unknown>): string[] {
  const directPrograms = listify(entry.SanctionsMeasure as Record<string, unknown> | Record<string, unknown>[])
    .map((measure) => textValue(measure?.Comment))
    .filter((value) => PROGRAM_CODE_RE.test(value));
  return uniqueSorted(directPrograms);
}

function extractEffectiveAt(entry: Record<string, unknown>): number {
  const dates: number[] = [];

  for (const event of listify(entry.EntryEvent as Record<string, unknown> | Record<string, unknown>[])) {
    const epoch = buildEpoch(event.Date as Record<string, unknown> | undefined);
    if (epoch > 0) dates.push(epoch);
  }

  for (const measure of listify(entry.SanctionsMeasure as Record<string, unknown> | Record<string, unknown>[])) {
    const datePeriod = measure.DatePeriod as Record<string, unknown> | undefined;
    const epoch = buildEpoch((datePeriod?.Start as Record<string, unknown> | undefined)?.From as Record<string, unknown> | undefined || datePeriod?.Start as Record<string, unknown> | undefined);
    if (epoch > 0) dates.push(epoch);
  }

  return dates.length > 0 ? Math.max(...dates) : 0;
}

function extractNote(entry: Record<string, unknown>, legalBasis: Map<string, string>): string {
  const comments = listify(entry.SanctionsMeasure as Record<string, unknown> | Record<string, unknown>[])
    .map((measure) => textValue(measure?.Comment))
    .filter((value) => value && !PROGRAM_CODE_RE.test(value));
  if (comments.length > 0) return compactNote(comments[0]!);

  const legal = listify(entry.EntryEvent as Record<string, unknown> | Record<string, unknown>[])
    .map((event) => legalBasis.get(String(event?.LegalBasisID || '')) || '')
    .filter(Boolean);
  return compactNote(legal[0] || '');
}

function buildEntriesForDocument(doc: Record<string, unknown>, sourceLabel: 'SDN' | 'CONSOLIDATED') {
  const { areaCodes, featureTypes, legalBasis } = buildReferenceMaps(doc);
  const locations = buildLocationMap(doc, areaCodes);
  const parties = buildPartyMap(doc, featureTypes, locations);
  const datasetDate = buildEpoch(doc.DateOfIssue as Record<string, unknown> | undefined);
  const entries: InternalEntry[] = [];

  for (const entry of listify(((doc.SanctionsEntries as Record<string, unknown> | undefined)?.SanctionsEntry) as Record<string, unknown> | Record<string, unknown>[])) {
    const profileId = String(entry.ProfileID || '');
    const party = parties.get(profileId);
    const name = party?.name || 'Unnamed designation';
    const programs = extractPrograms(entry);

    entries.push({
      id: `${sourceLabel}:${String(entry.ID || profileId || name)}`,
      name,
      entityType: party?.entityType || 'SANCTIONS_ENTITY_TYPE_ENTITY',
      countryCodes: party?.countryCodes ?? [],
      countryNames: party?.countryNames ?? [],
      programs: programs.length > 0 ? programs : [sourceLabel],
      sourceLists: [sourceLabel],
      effectiveAt: toInt64String(extractEffectiveAt(entry)),
      isNew: false,
      note: extractNote(entry, legalBasis),
    });
  }

  return { entries, datasetDate };
}

function sortEntries(a: InternalEntry, b: InternalEntry): number {
  return (Number(b.isNew) - Number(a.isNew))
    || (Number(b.effectiveAt) - Number(a.effectiveAt))
    || a.name.localeCompare(b.name);
}

function buildCountryPressure(entries: InternalEntry[]): CountrySanctionsPressure[] {
  const map = new Map<string, CountrySanctionsPressure>();

  for (const entry of entries) {
    const codes = entry.countryCodes.length > 0 ? entry.countryCodes : ['XX'];
    const names = entry.countryNames.length > 0 ? entry.countryNames : ['Unknown'];

    codes.forEach((code, index) => {
      const key = `${code}:${names[index] || names[0] || 'Unknown'}`;
      const current = map.get(key) || {
        countryCode: code,
        countryName: names[index] || names[0] || 'Unknown',
        entryCount: 0,
        newEntryCount: 0,
        vesselCount: 0,
        aircraftCount: 0,
      };
      current.entryCount += 1;
      if (entry.isNew) current.newEntryCount += 1;
      if (entry.entityType === 'SANCTIONS_ENTITY_TYPE_VESSEL') current.vesselCount += 1;
      if (entry.entityType === 'SANCTIONS_ENTITY_TYPE_AIRCRAFT') current.aircraftCount += 1;
      map.set(key, current);
    });
  }

  return [...map.values()]
    .sort((a, b) => b.newEntryCount - a.newEntryCount || b.entryCount - a.entryCount || a.countryName.localeCompare(b.countryName))
    .slice(0, 12);
}

function buildProgramPressure(entries: InternalEntry[]): ProgramSanctionsPressure[] {
  const map = new Map<string, ProgramSanctionsPressure>();

  for (const entry of entries) {
    const programs = entry.programs.length > 0 ? entry.programs : ['UNSPECIFIED'];
    for (const program of programs) {
      const current = map.get(program) || { program, entryCount: 0, newEntryCount: 0 };
      current.entryCount += 1;
      if (entry.isNew) current.newEntryCount += 1;
      map.set(program, current);
    }
  }

  return [...map.values()]
    .sort((a, b) => b.newEntryCount - a.newEntryCount || b.entryCount - a.entryCount || a.program.localeCompare(b.program))
    .slice(0, 12);
}

async function fetchSource(source: typeof OFAC_SOURCES[number]) {
  const response = await fetch(source.url, {
    headers: { 'User-Agent': CHROME_UA },
    signal: AbortSignal.timeout(OFAC_TIMEOUT_MS),
  });
  if (!response.ok) {
    throw new Error(`OFAC ${source.label} HTTP ${response.status}`);
  }
  const xml = await response.text();
  const parsed = XML_PARSER.parse(xml)?.Sanctions as Record<string, unknown> | undefined;
  if (!parsed) throw new Error(`OFAC ${source.label} parse returned no Sanctions root`);
  return buildEntriesForDocument(parsed, source.label);
}

function trimResponse(data: ListSanctionsPressureResponse, maxItems: number): ListSanctionsPressureResponse {
  // Destructure out _state which may be present in seeded Redis payloads during the window
  // between atomicPublish and afterPublish deletion
  const { _state: _discarded, ...rest } = data as ListSanctionsPressureResponse & { _state?: unknown };
  return {
    ...rest,
    fetchedAt: String(data.fetchedAt ?? '0'),
    datasetDate: String(data.datasetDate ?? '0'),
    entries: (data.entries ?? []).map((entry) => ({
      ...entry,
      effectiveAt: String(entry.effectiveAt ?? '0'),
    })).slice(0, maxItems),
  };
}

async function trySeededData(maxItems: number): Promise<ListSanctionsPressureResponse | null> {
  try {
    const [data, meta] = await Promise.all([
      getCachedJson(REDIS_CACHE_KEY, true) as Promise<ListSanctionsPressureResponse | null>,
      getCachedJson('seed-meta:sanctions:pressure', true) as Promise<{ fetchedAt?: number } | null>,
    ]);
    if (!data || !meta?.fetchedAt) return null;
    if (Date.now() - meta.fetchedAt > SEED_FRESHNESS_MS) return null;
    return trimResponse(data, maxItems);
  } catch {
    return null;
  }
}

async function collectPressure(maxItems: number): Promise<ListSanctionsPressureResponse> {
  const results = await Promise.all(OFAC_SOURCES.map((source) => fetchSource(source)));
  const entries = results.flatMap((result) => result.entries).sort(sortEntries);
  const totalCount = entries.length;

  return {
    fetchedAt: toInt64String(Date.now()),
    datasetDate: toInt64String(results.reduce((max, result) => Math.max(max, result.datasetDate || 0), 0)),
    totalCount,
    sdnCount: results[0]?.entries.length ?? 0,
    consolidatedCount: results[1]?.entries.length ?? 0,
    newEntryCount: 0,
    vesselCount: entries.filter((entry) => entry.entityType === 'SANCTIONS_ENTITY_TYPE_VESSEL').length,
    aircraftCount: entries.filter((entry) => entry.entityType === 'SANCTIONS_ENTITY_TYPE_AIRCRAFT').length,
    countries: buildCountryPressure(entries),
    programs: buildProgramPressure(entries),
    entries: entries.slice(0, maxItems),
  };
}

function clampMaxItems(value: number): number {
  if (!Number.isFinite(value) || value <= 0) return DEFAULT_MAX_ITEMS;
  return Math.min(Math.max(Math.trunc(value), 1), MAX_ITEMS_LIMIT);
}

function emptyResponse(): ListSanctionsPressureResponse {
  return {
    entries: [],
    countries: [],
    programs: [],
    fetchedAt: '0',
    datasetDate: '0',
    totalCount: 0,
    sdnCount: 0,
    consolidatedCount: 0,
    newEntryCount: 0,
    vesselCount: 0,
    aircraftCount: 0,
  };
}

export const listSanctionsPressure: SanctionsServiceHandler['listSanctionsPressure'] = async (
  _ctx: ServerContext,
  req: ListSanctionsPressureRequest,
): Promise<ListSanctionsPressureResponse> => {
  const maxItems = clampMaxItems(req.maxItems);
  try {
    const seeded = await trySeededData(maxItems);
    if (seeded) return seeded;

    return await cachedFetchJson<ListSanctionsPressureResponse>(
      `${REDIS_CACHE_KEY}:live:${maxItems}`,
      REDIS_CACHE_TTL,
      async () => collectPressure(maxItems),
    ) ?? emptyResponse();
  } catch {
    return emptyResponse();
  }
};
