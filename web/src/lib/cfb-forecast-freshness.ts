/** Age is evaluated at kickoff for historical forecasts, never against today's clock. */
export function cfbForecastFreshness(generatedAt: string | null, kickoff: string | null, observedAt: string) {
  if (!generatedAt) return { stale: true, label: "Forecast missing" };
  const generated = Date.parse(generatedAt), observed = Date.parse(observedAt);
  const start = kickoff ? Date.parse(kickoff) : NaN;
  if (!Number.isFinite(generated) || !Number.isFinite(observed) || generated > observed || generated >= start)
    return { stale: true, label: "Forecast timestamp invalid" };
  const at = Number.isFinite(start) ? Math.min(observed, start) : observed;
  const hours = (at - generated) / 3600_000;
  const nearKickoff = Number.isFinite(start) && start - at <= 3 * 3600_000;
  const stale = hours > (nearKickoff ? 4 : 24);
  return { stale, label: `${stale ? "Stale forecast" : "Forecast age"}: ${hours.toFixed(1)} hours${observed >= start ? " at kickoff" : ""}` };
}
