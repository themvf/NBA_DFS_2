/**
 * Boom trio alternatives (`nba-boom-trios-v1`), the web port of
 * research/nba_boom_trios.py. Registration: docs/nba-boom-trios.md. Pure: a
 * frozen ghost v2 snapshot (only the fields `slim` keeps) in, three trios out;
 * grade rows in, totals against the baseline out. The shared fixture
 * (tests/fixtures/nba_boom_trios_fixture.json) pins it to the Python.
 *
 *   baseline    the three highest market lines
 *   boom        keep #1 and #2; swap #3 for the strongest rising-shot-volume
 *               player within 3 projected points
 *   aggressive  keep #1; the best pair of rising-shot-volume players within 6
 *
 * The boom score, (FGA + 0.44 FTA) per game over the last 3 games minus the
 * last 20, only ranks replacements; it never touches projected points.
 */
export const BOOM_TRIOS_VERSION = "nba-boom-trios-v1";
export const BOOM_MIN = 2.0;
export const FT_WEIGHT = 0.44;
export const ONE_SWAP_MAX_DROP = 3.0;
export const TWO_SWAP_MAX_DROP = 6.0;
export const REGISTERED_AT = "2026-10-31T19:30:00+00:00";
export const REVIEW_AFTER_DATES = 30;

export type BoomCandidate = {
  event_id: string | number; player_key: string; player: string; history_status?: string | null;
  availability?: { status?: string | null } | null; predictions: { market_line?: number | null };
  features?: { shots_growth?: number | null; ft_growth?: number | null; fga3?: number | null; fta3?: number | null } | null;
};
export type BoomSnapshot = { game_date: string; sha256?: string | null; created_at?: string | null; mode?: string | null; candidates: BoomCandidate[] };
export type TrioMember = { event_id: string | number; player_key: string; player: string; role: "baseline" | "boom"; line: number;
  boom: number | null; shots_last3: number | null; shots_last20: number | null };
export type Trio = { members: TrioMember[]; projected_total: number; projected_drop?: number; replaced?: TrioMember | TrioMember[]; why?: string };
export type BoomSelection = { version: string; game_date: string; snapshot_sha256: string | null; snapshot_created_at: string | null;
  snapshot_mode: string | null; counts: boolean; pool: number; baseline: Trio | null; boom: Trio | null; aggressive: Trio | null; note?: string };

const finite = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);
const round2 = (v: number) => Math.round(v * 100) / 100;
const cmp = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
const line = (c: BoomCandidate) => c.predictions.market_line as number;

export function boomScore(c: BoomCandidate): number | null {
  const f = c.features;
  if (c.history_status !== "ready" || !f || !finite(f.shots_growth) || !finite(f.ft_growth)) return null;
  return f.shots_growth + FT_WEIGHT * f.ft_growth;
}

/** Mirrors nba-leader-evaluation-v1: one quote per player, a finite line, not reported Out; line descending, then key, then event. */
function pool(candidates: BoomCandidate[]): BoomCandidate[] {
  const counts = new Map<string, number>();
  for (const c of candidates) counts.set(c.player_key, (counts.get(c.player_key) ?? 0) + 1);
  return candidates
    .filter((c) => counts.get(c.player_key) === 1 && finite(c.predictions?.market_line) && String(c.availability?.status ?? "").toLowerCase() !== "out")
    .sort((a, b) => line(b) - line(a) || cmp(a.player_key, b.player_key) || cmp(String(a.event_id), String(b.event_id)));
}

function shots(c: BoomCandidate): [number, number] | null {
  const f = c.features;
  if (!f || ![f.fga3, f.fta3, f.shots_growth, f.ft_growth].every(finite)) return null;
  const recent = (f.fga3 as number) + FT_WEIGHT * (f.fta3 as number);
  const longer = ((f.fga3 as number) - (f.shots_growth as number)) + FT_WEIGHT * ((f.fta3 as number) - (f.ft_growth as number));
  return [recent, longer];
}

function member(c: BoomCandidate, role: "baseline" | "boom"): TrioMember {
  const s = shots(c);
  return { event_id: c.event_id, player_key: c.player_key, player: c.player, role, line: line(c), boom: boomScore(c),
    shots_last3: s ? round2(s[0]) : null, shots_last20: s ? round2(s[1]) : null };
}

function why(added: TrioMember[], drop: number): string {
  const parts = added.filter((m) => m.shots_last3 != null).map((m) =>
    `${m.player} has taken ${((m.shots_last3 as number) - (m.shots_last20 as number)).toFixed(1)} more shot attempts a game over his last 3 games than his last 20 (${(m.shots_last3 as number).toFixed(1)} vs ${(m.shots_last20 as number).toFixed(1)})`);
  return `Projected total drops by ${drop.toFixed(1)} points, but ${parts.join(" and ")}.`;
}

export function countsTowardRule(s: Pick<BoomSnapshot, "mode" | "created_at">): boolean {
  return s.mode === "prospective" && s.created_at != null && Date.parse(s.created_at) > Date.parse(REGISTERED_AT);
}

export function selectBoomTrios(snapshot: BoomSnapshot): BoomSelection {
  const ranked = pool(snapshot.candidates);
  const out: BoomSelection = { version: BOOM_TRIOS_VERSION, game_date: snapshot.game_date, snapshot_sha256: snapshot.sha256 ?? null,
    snapshot_created_at: snapshot.created_at ?? null, snapshot_mode: snapshot.mode ?? null, counts: countsTowardRule(snapshot),
    pool: ranked.length, baseline: null, boom: null, aggressive: null };
  if (ranked.length < 3) return { ...out, note: "fewer than three eligible players" };
  const base = ranked.slice(0, 3);
  const baseTotal = base.reduce((t, c) => t + line(c), 0);
  out.baseline = { members: base.map((c) => member(c, "baseline")), projected_total: round2(baseTotal) };
  const candidates = ranked.filter((c) => (boomScore(c) ?? -Infinity) >= BOOM_MIN);
  const byBoom = (a: BoomCandidate, b: BoomCandidate) => (boomScore(b) ?? 0) - (boomScore(a) ?? 0) || line(b) - line(a)
    || cmp(a.player_key, b.player_key) || cmp(String(a.event_id), String(b.event_id));

  const pick = candidates.filter((c) => !base.includes(c) && line(base[2]) - line(c) <= ONE_SWAP_MAX_DROP + 1e-9).sort(byBoom);
  if (pick.length) {
    const c = pick[0], drop = line(base[2]) - line(c), added = member(c, "boom");
    out.boom = { members: [member(base[0], "baseline"), member(base[1], "baseline"), added], replaced: member(base[2], "baseline"),
      projected_total: round2(baseTotal - drop), projected_drop: round2(drop), why: why([added], drop) };
  }

  const rest = candidates.filter((c) => c !== base[0]);
  let best: { key: [number, number, string, string]; a: BoomCandidate; b: BoomCandidate; total: number } | null = null;
  const less = (x: [number, number, string, string], y: [number, number, string, string]) =>
    x[0] !== y[0] ? x[0] < y[0] : x[1] !== y[1] ? x[1] < y[1] : x[2] !== y[2] ? x[2] < y[2] : x[3] < y[3];
  for (let i = 0; i < rest.length; i += 1) for (let j = i + 1; j < rest.length; j += 1) {
    const a = rest[i], b = rest[j], total = line(base[0]) + line(a) + line(b);
    if (baseTotal - total > TWO_SWAP_MAX_DROP + 1e-9) continue;
    const key: [number, number, string, string] = [-((boomScore(a) ?? 0) + (boomScore(b) ?? 0)), -total, a.player_key, b.player_key];
    if (!best || less(key, best.key)) best = { key, a, b, total };
  }
  if (best) {
    const pair = [best.a, best.b].sort((x, y) => line(y) - line(x));
    const added = pair.filter((c) => !base.includes(c)).map((c) => member(c, "boom"));
    const kept = pair.filter((c) => base.includes(c));
    const drop = baseTotal - best.total;
    out.aggressive = { members: [member(base[0], "baseline"), ...kept.map((c) => member(c, "baseline")), ...added],
      replaced: base.slice(1).filter((c) => !pair.includes(c)).map((c) => member(c, "baseline")),
      projected_total: round2(best.total), projected_drop: round2(drop),
      why: added.length ? why(added, drop) : "Both boom candidates were already in the baseline." };
  }
  return out;
}

export type BoomGradeRow = { event_id: string | number; player_key: string; status?: string | null; actual_points?: number | null; historical_p90?: number | null };
export type TrioResult = { status: "graded"; actual: number; delta: number; outcome: "helped" | "hurt" | "tied" } | { status: "no_alternative" | "incomplete" };
export type BoomGrade = { baseline: number | null; boom: TrioResult; aggressive: TrioResult };

/** Totals from the ghost grade rows. A trio with a DNP or unresolved member is incomplete, never scored as 0. */
export function gradeBoomTrios(selection: BoomSelection, rows: BoomGradeRow[]): BoomGrade {
  const byId = new Map(rows.map((r) => [`${r.event_id}|${r.player_key}`, r]));
  const actual = (m: TrioMember) => {
    const r = byId.get(`${m.event_id}|${m.player_key}`);
    return r && (r.status === "graded" || r.status === "push") && finite(r.actual_points) ? r.actual_points : null;
  };
  const total = (t: Trio | null) => {
    if (!t) return null;
    const v = t.members.map(actual);
    return v.some((x) => x == null) ? null : (v as number[]).reduce((a, b) => a + b, 0);
  };
  const baseline = total(selection.baseline);
  const result = (t: Trio | null): TrioResult => {
    if (!t) return { status: "no_alternative" };
    const v = total(t);
    if (v == null || baseline == null) return { status: "incomplete" };
    const delta = v - baseline;
    return { status: "graded", actual: v, delta, outcome: delta > 0 ? "helped" : delta < 0 ? "hurt" : "tied" };
  };
  return { baseline, boom: result(selection.boom), aggressive: result(selection.aggressive) };
}
