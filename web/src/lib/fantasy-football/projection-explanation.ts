export type ProjectionExplanation = {
  method: string;
  lines: string[];
  notModeled: string[];
};

function numberValue(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
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
    const sampleGames = fixed(details.regression_sample_games, 0);
    const regressed = fixed(details.regressed_ppg);
    if (weighted) lines.push(`Weighted history: ${weighted} PPG`);
    if (prior && priorGames && sampleGames && regressed) lines.push(`Regression: ${sampleGames} historical games + ${priorGames} position-prior games at ${prior} PPG → ${regressed} PPG`);
    else if (prior && priorGames && regressed) lines.push(`Position prior: ${prior} PPG over ${priorGames} equivalent games → ${regressed} PPG`);
  } else if (method === "rookie_prior" || method === "position_prior" || method === "rookie_draft_curve") {
    const prior = fixed(details.rookie_prior_points);
    const draftNumber = numberValue(details.draft_number);
    const role = fixed(details.role_factor, 2);
    const roleAdjusted = fixed(details.base_points_before_injury);
    const usesDraftCapital = method === "rookie_prior" || method === "rookie_draft_curve";
    if (usesDraftCapital && draftNumber !== null) lines.push(`NFL draft selection: #${Math.round(draftNumber)}`);
    if (method === "rookie_draft_curve" && prior) {
      lines.push(`Draft-value curve prior (fit on 2023-2025 rookie outcomes): ${prior} points`);
    } else if (prior) {
      lines.push(`${method === "rookie_prior" ? "Position + draft-capital" : "Position"} prior: ${prior} points`);
    }
    const floorApplied = details.rookie_role_floor_applied === true;
    const floorPoints = fixed(details.rookie_role_floor_points);
    if (floorApplied && floorPoints && role && roleAdjusted) {
      lines.push(`Draft-capital value × ${role} role = below the confirmed-role floor`);
      lines.push(`Depth-chart role floor (position-average prior × ${role} role): ${floorPoints} points -- used instead`);
    } else if (prior && role && roleAdjusted) {
      lines.push(`${prior} prior × ${role} depth-chart role = ${roleAdjusted}`);
    }
  } else if (method === "position_baseline") {
    const base = fixed(details.base_points_before_injury);
    if (base) lines.push(`Position baseline: ${base} points`);
  }

  const regressed = fixed(details.regressed_ppg, 2);
  const games = fixed(details.baseline_games ?? details.expected_games_before_injury, 2);
  const role = fixed(details.role_factor, 2);
  const base = fixed(details.base_points_before_injury);
  if (regressed && games && role && base) lines.push(`${regressed} PPG × ${games} games × ${role} role = ${base}`);
  const availabilityGames = fixed(details.expected_games_after_injury, 1);
  if (details.availability_adjustment_applied_to_baseline === false && availabilityGames) {
    lines.push(`Availability estimate: ${availabilityGames} active games (modeled separately; not deducted from this baseline)`);
  }
  const injury = fixed(details.injury_factor, 2);
  const final = fixed(details.final_points);
  if (injury && final && injury !== "1.00") lines.push(`${base ?? "Base"} × ${injury} injury factor = ${final}`);
  else if (final && !lines.some((line) => line.endsWith(`= ${final}`))) lines.push(`Final projection: ${final} points`);

  return {
    method: method === "history_regression"
      ? "Weighted history + regression"
      : method === "rookie_draft_curve"
        ? "Draft-value curve"
        : method.replaceAll("_", " "),
    lines,
    notModeled: Array.isArray(details.not_modeled) ? details.not_modeled.map(String) : [],
  };
}
