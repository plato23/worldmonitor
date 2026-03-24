import type {
  ForecastServiceHandler,
  ServerContext,
  GetSimulationOutcomeRequest,
  GetSimulationOutcomeResponse,
} from '../../../../src/generated/server/worldmonitor/forecast/v1/service_server';
import { getRawJson } from '../../../_shared/redis';
import { markNoCacheResponse } from '../../../_shared/response-headers';

const SIMULATION_OUTCOME_LATEST_KEY = 'forecast:simulation-outcome:latest';

const NOT_FOUND: GetSimulationOutcomeResponse = {
  found: false, runId: '', outcomeKey: '', schemaVersion: '', theaterCount: 0, generatedAt: 0, note: '', error: '',
};

export const getSimulationOutcome: ForecastServiceHandler['getSimulationOutcome'] = async (
  ctx: ServerContext,
  req: GetSimulationOutcomeRequest,
): Promise<GetSimulationOutcomeResponse> => {
  try {
    const pointer = await getRawJson(SIMULATION_OUTCOME_LATEST_KEY) as {
      runId: string; outcomeKey: string; schemaVersion: string; theaterCount: number; generatedAt: number;
    } | null;
    if (!pointer?.outcomeKey) {
      markNoCacheResponse(ctx.request); // don't cache not-found — outcome may appear soon after a simulation run
      return NOT_FOUND;
    }
    const note = req.runId && req.runId !== pointer.runId
      ? 'runId filter not yet active; returned outcome may differ from requested run'
      : '';
    return { found: true, runId: pointer.runId, outcomeKey: pointer.outcomeKey, schemaVersion: pointer.schemaVersion, theaterCount: pointer.theaterCount, generatedAt: pointer.generatedAt, note, error: '' };
  } catch (err) {
    console.warn('[getSimulationOutcome] Redis error:', err instanceof Error ? err.message : String(err));
    markNoCacheResponse(ctx.request); // don't cache error state
    return { ...NOT_FOUND, error: 'redis_unavailable' };
  }
};
