/**
 * SearchAdapter — two-stage grocery price pipeline.
 *
 * Stage 1 (Exa): neural search on retailer domain → ranked product page URLs
 * Stage 2 (Firecrawl): structured LLM extraction from the confirmed URL → {price, currency, inStock}
 *
 * Replaces ExaSearchAdapter's fragile regex-on-AI-summary approach.
 * Firecrawl renders JS so dynamic prices (Noon, etc.) are visible.
 * Domain allowlist + title plausibility check prevent wrong-product and SSRF risks.
 */
import { z } from 'zod';
import { loadAllBasketConfigs } from '../config/loader.js';
import type { ExaProvider } from '../acquisition/exa.js';
import type { FirecrawlProvider } from '../acquisition/firecrawl.js';
import type { RetailerConfig } from '../config/types.js';
import type { AdapterContext, FetchResult, ParsedProduct, RetailerAdapter, Target } from './types.js';

const MARKET_NAMES: Record<string, string> = {
  ae: 'UAE',
  sa: 'Saudi Arabia',
  kw: 'Kuwait',
  qa: 'Qatar',
  bh: 'Bahrain',
  om: 'Oman',
  eg: 'Egypt',
};

/**
 * Token overlap: ≥40% of canonical name words (>2 chars) must appear in extracted productName.
 * Catches gross mismatches (seeds vs eggs, storage boxes vs milk) while tolerating
 * brand name differences ("Almarai Fresh Milk 1L" matches "Milk 1L").
 */
export function isTitlePlausible(canonicalName: string, productName: string | undefined): boolean {
  if (!productName) return false;
  const tokens = canonicalName.toLowerCase().split(/\W+/).filter((w) => w.length > 2);
  if (tokens.length === 0) return true;
  const extracted = productName.toLowerCase();
  const matches = tokens.filter((w) => extracted.includes(w));
  return matches.length >= Math.max(1, Math.ceil(tokens.length * 0.4));
}

/**
 * Safe host boundary check. Prevents evilluluhypermarket.com from passing
 * when allowedHost is luluhypermarket.com.
 */
export function isAllowedHost(url: string, allowedHost: string): boolean {
  try {
    const { hostname } = new URL(url);
    return hostname === allowedHost;
  } catch {
    return false;
  }
}

interface ExtractedProduct {
  productName?: string;
  price?: number;
  currency?: string;
  inStock?: boolean;
}

interface SearchPayload {
  extracted: ExtractedProduct;
  productUrl: string;
  canonicalName: string;
  basketSlug: string;
  itemCategory: string;
}

export class SearchAdapter implements RetailerAdapter {
  readonly key = 'search';

  constructor(
    private readonly exa: ExaProvider,
    private readonly firecrawl: FirecrawlProvider,
  ) {}

  async validateConfig(config: RetailerConfig): Promise<string[]> {
    const errors: string[] = [];
    if (!config.baseUrl) errors.push('baseUrl is required');
    return errors;
  }

  async discoverTargets(ctx: AdapterContext): Promise<Target[]> {
    const baskets = loadAllBasketConfigs().filter((b) => b.marketCode === ctx.config.marketCode);
    const domain = new URL(ctx.config.baseUrl).hostname;
    const targets: Target[] = [];

    for (const basket of baskets) {
      for (const item of basket.items) {
        targets.push({
          id: item.id,
          url: ctx.config.baseUrl,
          category: item.category,
          metadata: {
            canonicalName: item.canonicalName,
            domain,
            basketSlug: basket.slug,
            currency: ctx.config.currencyCode,
          },
        });
      }
    }

    return targets;
  }

  async fetchTarget(ctx: AdapterContext, target: Target): Promise<FetchResult> {
    const { canonicalName, domain, currency, basketSlug } = target.metadata as {
      canonicalName: string;
      domain: string;
      currency: string;
      basketSlug: string;
    };

    const marketName = MARKET_NAMES[ctx.config.marketCode] ?? '';
    const cfg = ctx.config.searchConfig;

    const query = cfg?.queryTemplate
      ? cfg.queryTemplate
          .replace('{canonicalName}', canonicalName)
          .replace('{currency}', currency)
          .replace('{market}', marketName)
          .trim()
      : `${canonicalName} ${marketName} ${currency}`.trim();

    // Stage 1: Exa URL discovery
    const exaResults = await this.exa.search(query, {
      numResults: cfg?.numResults ?? 3,
      includeDomains: [domain],
    });

    if (exaResults.length === 0) {
      throw new Error(`Exa: no pages found for "${canonicalName}" on ${domain}`);
    }

    const safeUrls = exaResults.map((r) => r.url).filter((url) => !!url && isAllowedHost(url, domain));

    ctx.logger.info(
      `  [search:discovery] ${canonicalName}: ${exaResults.length} URLs from Exa, ${safeUrls.length} passed domain check`,
    );

    if (safeUrls.length === 0) {
      throw new Error(`Exa: all ${exaResults.length} results failed domain check (expected hostname: ${domain})`);
    }

    // Stage 2: Firecrawl structured extraction — iterate safe URLs until one yields a valid price
    const extractSchema = {
      fields: {
        productName: { type: 'string' as const, description: 'Name or title of the product' },
        price: { type: 'number' as const, description: `Retail price in ${currency}` },
        currency: { type: 'string' as const, description: `Currency code, should be ${currency}` },
        inStock: { type: 'boolean' as const, description: 'Whether the product is currently in stock' },
      },
    };

    let extracted: ExtractedProduct | null = null;
    let usedUrl = safeUrls[0];
    const lastErrors: string[] = [];

    for (const url of safeUrls) {
      try {
        const result = await this.firecrawl.extract<ExtractedProduct>(url, extractSchema, { timeout: 30_000 });
        const data = result.data;
        const price = data?.price;

        if (typeof price !== 'number' || !Number.isFinite(price) || price <= 0) {
          ctx.logger.warn(`  [search:extract] ${canonicalName}: no price from ${url}, trying next`);
          continue;
        }

        if (!isTitlePlausible(canonicalName, data.productName)) {
          ctx.logger.warn(
            `  [search:extract] ${canonicalName}: title mismatch "${data.productName}" at ${url}, trying next`,
          );
          continue;
        }

        extracted = data;
        usedUrl = url;
        break;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        ctx.logger.warn(`  [search:extract] ${canonicalName}: Firecrawl error on ${url}: ${msg}`);
        lastErrors.push(msg);
      }
    }

    if (extracted === null) {
      throw new Error(
        `All ${safeUrls.length} URLs failed extraction for "${canonicalName}".${lastErrors.length ? ` Last: ${lastErrors.at(-1)}` : ''}`,
      );
    }

    ctx.logger.info(
      `  [search:extract] ${canonicalName}: price=${extracted.price} ${extracted.currency} from ${usedUrl}`,
    );

    return {
      url: usedUrl,
      html: JSON.stringify({
        extracted,
        productUrl: usedUrl,
        canonicalName,
        basketSlug,
        itemCategory: target.category,
      } satisfies SearchPayload),
      statusCode: 200,
      fetchedAt: new Date(),
    };
  }

  async parseListing(ctx: AdapterContext, result: FetchResult): Promise<ParsedProduct[]> {
    const { extracted, productUrl, canonicalName, basketSlug, itemCategory } =
      JSON.parse(result.html) as SearchPayload;

    const priceResult = z.number().positive().finite().safeParse(extracted?.price);
    if (!priceResult.success) {
      ctx.logger.warn(`  [search] ${canonicalName}: invalid price "${extracted?.price}" from ${productUrl}`);
      return [];
    }

    if (extracted.currency && extracted.currency.toUpperCase() !== ctx.config.currencyCode) {
      ctx.logger.warn(
        `  [search] ${canonicalName}: currency mismatch ${extracted.currency} ≠ ${ctx.config.currencyCode} at ${productUrl}`,
      );
      return [];
    }

    return [
      {
        sourceUrl: productUrl,
        rawTitle: extracted.productName ?? canonicalName,
        rawBrand: null,
        rawSizeText: null,
        imageUrl: null,
        categoryText: itemCategory,
        retailerSku: null,
        price: priceResult.data,
        listPrice: null,
        promoPrice: null,
        promoText: null,
        inStock: extracted.inStock ?? true,
        rawPayload: { extracted, basketSlug, itemCategory, canonicalName },
      },
    ];
  }

  async parseProduct(_ctx: AdapterContext, _result: FetchResult): Promise<ParsedProduct> {
    throw new Error('SearchAdapter does not support single-product parsing');
  }
}
