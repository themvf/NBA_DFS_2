export type ProjectionExplanation = {
  method: string;
  lines: string[];
  notModeled: string[];
};

function numberValue(value: unknown): number | null {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function fixed(value: unknown, digits = 1): string | null {
  const parsed = numberValue(value);
  return parsed === null ? null : parsed.toFixed(digits);
}

export function buildProjectionExplanation(details: Record<string, unknown> | null): ProjectionExplanation {
  if (!details) return { method: "Projection detail unavailable", lines: [], notModeled: [] };
  const method = String(details.method || "legacy");
  const lines: string[] = [];
  const inputs = Array.isArray(details.season_inputs) ? details.season_inputs : [];

  for (const value of inputs) {
    if (!value || typeof value !== "object") continue;
    const row = value as Record<string, unknown>;
    const ppg = fixed(row.ppg);
    const weight = numberValue(row.weight);
    if (ppg && weight !== null) lines.push(`${row.season}: ${ppg} PPG × ${Math.round(weight * 100)}%`);
  }

  if (method === "history_regression") {
    const weighted = fixed(details.weighted_history_ppg);
    const prior = fixed(details.position_prior_ppg);
    const priorGames = fixed(details.regression_prior_games, 0);
    const regressed = fixed(details.regressed_ppg);
    if (weighted) lines.push(`Weighted history: ${weighted} PPG`);
    if (prior && priorGames && regressed) lines.push(`Position prior: ${prior} PPG over ${priorGames} equivalent games → ${regressed} PPG`);
  } else if (method === "rookie_prior" || method === "position_prior") {
    const prior = fixed(details.rookie_prior_points);
    if (prior) lines.push(`${method === "rookie_prior" ? "Rookie/draft" : "Position"} prior: ${prior} points`);
  } else if (method === "position_baseline") {
    const base = fixed(details.base_points_before_injury);
    if (base) lines.push(`Position baseline: ${base} points`);
  }

  const regressed = fixed(details.regressed_ppg, 2);
  const games = fixed(details.expected_games_before_injury, 2);
  const role = fixed(details.role_factor, 2);
  const base = fixed(details.base_points_before_injury);
  if (regressed && games && role && base) lines.push(`${regressed} PPG × ${games} games × ${role} role = ${base}`);
  const injury = fixed(details.injury_factor, 2);
  const final = fixed(details.final_points);
  if (injury && final && injury !== "1.00") lines.push(`${base ?? "Base"} × ${injury} injury factor = ${final}`);
  else if (final && !lines.some((line) => line.endsWith(`= ${final}`))) lines.push(`Final projection: ${final} points`);

  return {
    method: method === "history_regression" ? "Weighted history + regression" : method.replaceAll("_", " "),
    lines,
    notModeled: Array.isArray(details.not_modeled) ? details.not_modeled.map(String) : [],
  };
}
