/** The shared, full-game anytime-TD contract. No first-TD or regulation-only data. */
export const OUTCOME = "rush_or_receiving_td_including_overtime" as const;
export const MODELS = ["direct", "opportunity", "baseline"] as const;
export type Model = typeof MODELS[number];
export const LABELS: Record<Model, string> = { direct: "A · Direct probability", opportunity: "B · Opportunity model", baseline: "Historical baseline" };
export type Game = { gameId: string; season: number; week: number; kickoff: string; away: string; home: string; completed: boolean };
export type Evidence = { source: string; capturedAt: string; sha256: string };
export type Player = { identity: string; name: string; team: string; position: string; availability: string; probabilities: Record<Model, number> };
export type Forecast = {
  schemaVersion: 1; outcome: typeof OUTCOME; game: Game; generatedAt: string; inputCutoff: string;
  versions: Record<Model, string>; implementationSha256: string; evidence: Evidence[];
  availabilityNotes: string[]; players: Player[];
};
export type Actual = { identity: string; team: string; rushingTds: number; receivingTds: number };
export type Result = {
  schemaVersion: 1; outcome: typeof OUTCOME; gameId: string; verifiedAt: string;
  complete: true; includesOvertime: true; evidence: Evidence[];
  // Independently reconciled PBP and complete weekly box credits, including zeros
  // for every forecast player and all scorers outside the forecast roster.
  pbp: Actual[]; box: Actual[]; teamTotals: Record<string, number>;
};
export type RecordRow = { game: Game; forecast: Forecast | null; frozenAt: string | null; result: Result | null; error?: string };
function assert(value: unknown, message: string): asserts value { if (!value) throw new Error(message); }
export function timestamp(value: string): number { const n = Date.parse(value); if (!Number.isFinite(n)) throw new Error("Invalid timestamp"); return n; }
function evidence(rows: Evidence[], cutoff: number) {
  assert(Array.isArray(rows) && rows.length > 0, "Missing source evidence");
  for (const r of rows) assert(r.source?.trim() && /^[a-f0-9]{64}$/i.test(r.sha256) && timestamp(r.capturedAt) <= cutoff, "Invalid or future source evidence");
}
export function validateForecast(f: Forecast, game: Game, frozenAt: string) {
  assert(f.schemaVersion === 1 && f.outcome === OUTCOME, "Wrong forecast outcome/version");
  assert(f.game.gameId === game.gameId && f.game.season === game.season && f.game.week === game.week && f.game.home === game.home && f.game.away === game.away && timestamp(f.game.kickoff) === timestamp(game.kickoff), "Canonical game mismatch");
  const frozen = timestamp(frozenAt), generated = timestamp(f.generatedAt), cutoff = timestamp(f.inputCutoff);
  assert(cutoff <= generated && generated <= frozen && frozen <= timestamp(game.kickoff) - 20 * 60_000, "Forecast missed the pregame freeze deadline");
  assert(/^[a-f0-9]{64}$/i.test(f.implementationSha256), "Missing implementation digest");
  for (const m of MODELS) assert(f.versions[m]?.trim(), "Missing model version");
  evidence(f.evidence, cutoff);
  assert(Array.isArray(f.availabilityNotes), "Missing availability notes");
  assert(Array.isArray(f.players) && f.players.length > 0, "Empty forecast population");
  const ids = new Set<string>();
  for (const p of f.players) {
    assert(/^\d{2}-\d{7}$/.test(p.identity) && !ids.has(p.identity), "Duplicate or invalid GSIS identity"); ids.add(p.identity);
    assert(p.name?.trim() && p.availability?.trim() && [game.home, game.away].includes(p.team) && ["QB", "RB", "WR", "TE"].includes(p.position), "Invalid player metadata");
    for (const m of MODELS) assert(typeof p.probabilities[m] === "number" && Number.isFinite(p.probabilities[m]) && p.probabilities[m] >= 0 && p.probabilities[m] <= 1, "Invalid model probability");
  }
}
export function validateResult(r: Result, f: Forecast, game: Game, importedAt: string) {
  assert(r.schemaVersion === 1 && r.outcome === OUTCOME && r.gameId === game.gameId, "Wrong result game/outcome");
  assert(game.completed && r.complete === true && r.includesOvertime === true, "Results must cover a completed full game");
  assert(timestamp(r.verifiedAt) > timestamp(game.kickoff) && timestamp(r.verifiedAt) <= timestamp(importedAt), "Invalid result verification time");
  evidence(r.evidence, timestamp(r.verifiedAt));
  assert(r.evidence.length >= 2 && new Set(r.evidence.map(e => e.source)).size >= 2, "Results need PBP and box evidence");
  const index = (rows: Actual[]) => {
    assert(Array.isArray(rows), "Missing result rows");
    const map = new Map<string, Actual>();
    for (const a of rows) {
      assert(/^\d{2}-\d{7}$/.test(a.identity) && !map.has(a.identity) && [game.away, game.home].includes(a.team), "Invalid/duplicate result identity");
      assert(Number.isInteger(a.rushingTds) && a.rushingTds >= 0 && Number.isInteger(a.receivingTds) && a.receivingTds >= 0, "Invalid touchdown counts");
      map.set(a.identity, a);
    }
    return map;
  };
  const pbp = index(r.pbp), box = index(r.box);
  assert(pbp.size === box.size, "PBP/box identity mismatch");
  for (const [id, a] of pbp) { const b = box.get(id); assert(b && a.team === b.team && a.rushingTds === b.rushingTds && a.receivingTds === b.receivingTds, "PBP/box TD credits disagree"); }
  for (const p of f.players) assert(pbp.get(p.identity)?.team === p.team, "Missing forecast-player actual; missing is not zero");
  for (const team of [game.away, game.home]) {
    const total = [...pbp.values()].filter(p => p.team === team).reduce((n, p) => n + p.rushingTds + p.receivingTds, 0);
    assert(Number.isInteger(r.teamTotals[team]) && total === r.teamTotals[team], "Team TD reconciliation failed");
  }
}
export function actualCount(result: Result, identity: string): number | null {
  const row = result.pbp.find(r => r.identity === identity);
  return row ? row.rushingTds + row.receivingTds : null;
}
export function state(row: RecordRow, now: number): string {
  if (row.error) return "Data unavailable";
  if (!row.forecast) return now >= timestamp(row.game.kickoff) ? "No frozen forecast" : "Forecast pending";
  if (row.result) return "Verified results";
  if (row.game.completed) return "Awaiting verification";
  return now >= timestamp(row.game.kickoff) ? "Game started · results pending" : "Pregame · frozen";
}
export function scores(rows: RecordRow[]) {
  const eligible = rows.filter(r => !r.error && r.game.completed && r.forecast && r.result && r.forecast.players.every(p => actualCount(r.result!, p.identity) !== null));
  return MODELS.map(model => {
    const samples = eligible.flatMap(r => r.forecast!.players.map(p => ({ p: p.probabilities[model], y: Number(actualCount(r.result!, p.identity)! > 0) })));
    const count = samples.length;
    const bins = Array.from({ length: 5 }, (_, i) => {
      const members = samples.filter(s => Math.min(4, Math.floor(s.p * 5)) === i);
      return { low: i * .2, high: (i + 1) * .2, count: members.length,
        predicted: members.length ? members.reduce((s, r) => s + r.p, 0) / members.length : null,
        observed: members.length ? members.reduce((s, r) => s + r.y, 0) / members.length : null };
    });
    return { model, games: eligible.length, count, versions: [...new Set(eligible.map(r => r.forecast!.versions[model]))],
      brier: count ? samples.reduce((s, r) => s + (r.p - r.y) ** 2, 0) / count : null,
      logLoss: count ? samples.reduce((s, r) => { const p = Math.max(1e-12, Math.min(1 - 1e-12, r.p)); return s - r.y * Math.log(p) - (1 - r.y) * Math.log(1 - p); }, 0) / count : null, bins };
  });
}
