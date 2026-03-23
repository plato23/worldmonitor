import { toApiUrl } from '@/services/runtime';
import type { MarketImplicationsData, MarketImplicationCard } from '@/components/MarketImplicationsPanel';
import { getHydratedData } from '@/services/bootstrap';

export type { MarketImplicationsData, MarketImplicationCard };

let cachedData: MarketImplicationsData | null = null;
let cachedAt = 0;
const CACHE_TTL = 10 * 60 * 1000;

export function getCachedMarketImplications(): MarketImplicationsData | null {
  return cachedData;
}

export async function fetchMarketImplications(): Promise<MarketImplicationsData | null> {
  const now = Date.now();
  if (cachedData && !cachedData.degraded && now - cachedAt < CACHE_TTL) return cachedData;

  const hydrated = getHydratedData('marketImplications') as MarketImplicationsData | undefined;
  if (hydrated?.cards && Array.isArray(hydrated.cards) && hydrated.cards.length > 0 && !hydrated.degraded) {
    cachedData = hydrated;
    cachedAt = now;
    return cachedData;
  }

  try {
    const resp = await fetch(toApiUrl('/api/intelligence/v1/list-market-implications'), {
      signal: AbortSignal.timeout(15_000),
    });
    if (!resp.ok) return cachedData;

    const raw = (await resp.json()) as MarketImplicationsData;
    if (!Array.isArray(raw.cards)) return cachedData;

    cachedData = raw;
    cachedAt = now;
    return cachedData;
  } catch {
    return cachedData;
  }
}
