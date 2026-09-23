/**
 * Field-ownership audit, browser/server side.
 *
 * ## Why this exists twice
 *
 * This mirrors `model/nfl_dfs_field_audit.py` across the language boundary,
 * the same way `DETECTOR_REGISTRY` is mirrored in `model/line_alerts.py` and
 * `web/src/db/queries.ts`. Two implementations is a real cost and it is taken
 * deliberately:
 *
 *   - the DraftKings contest export is 64 MB, so it is parsed in the BROWSER
 *     and only the ~850-row ownership summary is sent to the server. A Python
 *     parser cannot run there.
 *   - the audit has to render in the app, because a weekly terminal command
 *     is a chore nobody runs.
 *
 * The Python CLI stays for backfill and for anything running in CI. Both write
 * the same two tables. THRESHOLDS AND VERDICT RULES MUST BE CHANGED IN BOTH;
 * `test:nfl-field-audit` pins the constants so a one-sided edit fails loudly.
 *
 * ## What the audit says
 *
 * Three facts per player, one interesting combination: a player we ranked
 * highly, the field ignored, and who then produced nothing is the market
 * knowing something we did not. The same player producing is a real edge.
 *
 * NOT an ownership fade. On the week-2 classic contest the winning entry
 * carried 106% cumulative ownership, the top 1,000 carried 119%, the field
 * 110% -- and our own lineups carried 96%. We were already more contrarian
 * than the people who beat us. Nothing here belongs in a lineup objective.
 */

export const FIELD_AUDIT_VERSION = "nfl-dfs-field-audit-v1";

/** Below this share of entries, the field ignored him. Nacua landed at 0.64%. */
export const IGNORED_BY_FIELD_PCT = 1.0;
/** How deep at each position we would plausibly have rostered someone. */
export const POSITION_DEPTH: Record<string, number> =
  { QB: 12, RB: 24, WR: 36, TE: 12, DST: 8, K: 8 };
/** ...and a depth cut alone is meaningless on a 53-player showdown. */
export const ROSTERABLE_PROJECTION_SHARE = 0.5;
/** At or below this, he did not produce. DK pays a listed absentee 0. */
export const NO_PRODUCTION_FPTS = 3.0;

export type FieldVerdict = "MARKET_KNEW" | "REAL_EDGE" | "UNINFORMATIVE";

export interface FieldPlayer {
  name: string;
  normalizedName: string;
  draftedPct: number;
  draftedBySlot: Record<string, number>;
  fpts: number;
}

export interface ParsedContest {
  players: FieldPlayer[];
  entryCount: number;
  winningScore: number | null;
  medianScore: number | null;
  minScore: number | null;
  format: "classic" | "showdown";
}

export const normalizeName = (name: string): string =>
  String(name ?? "").toLowerCase().replace(/[^a-z]/g, "");

/** Split one csv line, honouring quoted fields (lineup cells contain commas). */
function splitCsvLine(line: string): string[] {
  const out: string[] = [];
  let value = "", quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (quoted) {
      if (ch === '"' && line[i + 1] === '"') { value += '"'; i += 1; }
      else if (ch === '"') quoted = false;
      else value += ch;
    } else if (ch === '"') quoted = true;
    else if (ch === ",") { out.push(value); value = ""; }
    else value += ch;
  }
  out.push(value);
  return out;
}

/**
 * Parse a DraftKings contest standings export.
 *
 * The file holds TWO unrelated tables side by side: entries on the left
 * (Rank..Lineup) and per-player ownership on the right (Player, Roster
 * Position, %Drafted, FPTS). Rows past the end of the ownership table are
 * narrower, so WIDTH distinguishes them -- not emptiness, not row order.
 *
 * The multiplier trap: in a SHOWDOWN export the FPTS column is slot-specific.
 * A player appears once as FLEX with his base score and once as CPT with 1.5x
 * that. Reading whichever row comes last inflates every captain by 50%. Base
 * always comes from the FLEX row. In a classic export the slots agree, so the
 * rule is a no-op.
 */
export function parseContestExport(content: string): ParsedContest {
  const lines = content.split(/\r?\n/);
  if (lines.length < 2) throw new Error("The contest file is empty.");

  const fptsBySlot = new Map<string, Map<string, number>>();
  const draftedBySlot = new Map<string, Map<string, number>>();
  const display = new Map<string, string>();
  const scores: number[] = [];
  let entryCount = 0;

  for (let i = 1; i < lines.length; i += 1) {
    if (!lines[i].trim()) continue;
    const row = splitCsvLine(lines[i]);
    if (row.length >= 6 && row[0].trim()) {
      const points = Number(row[4]);
      if (Number.isFinite(points)) { scores.push(points); entryCount += 1; }
    }
    if (row.length >= 11 && row[7].trim()) {
      const key = normalizeName(row[7]);
      const slot = row[8].trim().toUpperCase();
      const fpts = Number(row[10]);
      const drafted = Number(String(row[9] ?? "0").replace("%", ""));
      if (!Number.isFinite(fpts) || !Number.isFinite(drafted)) continue;
      if (!display.has(key)) display.set(key, row[7].trim());
      if (!fptsBySlot.has(key)) { fptsBySlot.set(key, new Map()); draftedBySlot.set(key, new Map()); }
      fptsBySlot.get(key)!.set(slot, fpts);
      draftedBySlot.get(key)!.set(slot, drafted);
    }
  }
  if (!entryCount) throw new Error("No contest entries found. Is this a DraftKings contest-standings export?");
  if (!fptsBySlot.size) throw new Error("No player ownership rows found in this file.");

  const players: FieldPlayer[] = [];
  let showdown = false;
  for (const [key, bySlot] of fptsBySlot) {
    if (bySlot.has("CPT")) showdown = true;
    const base = bySlot.has("FLEX") ? bySlot.get("FLEX")!
      : bySlot.size === 1 && bySlot.has("CPT") ? bySlot.get("CPT")! / 1.5
      : [...bySlot.values()][0];
    const drafted = draftedBySlot.get(key)!;
    players.push({
      name: display.get(key) ?? key,
      normalizedName: key,
      draftedPct: Number([...drafted.values()].reduce((a, b) => a + b, 0).toFixed(4)),
      draftedBySlot: Object.fromEntries(drafted),
      fpts: Number(base.toFixed(4)),
    });
  }
  scores.sort((a, b) => b - a);
  return {
    players,
    entryCount,
    winningScore: scores[0] ?? null,
    medianScore: scores[Math.floor(scores.length / 2)] ?? null,
    minScore: scores[scores.length - 1] ?? null,
    format: showdown ? "showdown" : "classic",
  };
}

export interface AuditSlatePlayer {
  dkPlayerId: number;
  name: string;
  position: string;
  salary: number;
  ourProj: number | null;
  isOut: boolean;
}

export interface AuditRow {
  name: string; position: string; salary: number;
  ourProj: number; ourRank: number | null;
  fieldPct: number; actual: number | null; verdict: FieldVerdict;
}

export interface FieldAudit {
  version: string;
  rows: AuditRow[];
  flagged: AuditRow[];
  summary: {
    considered: number; flagged: number; marketKnew: number; realEdge: number;
    projectedPointsOnMarketKnew: number; unmatchedToFieldTable: number;
  };
}

export function auditSlate(
  slatePlayers: readonly AuditSlatePlayer[],
  field: ReadonlyMap<string, { draftedPct: number; fpts: number | null }>,
  options: { ignoredPct?: number; noProduction?: number; projectionShare?: number } = {},
): FieldAudit {
  const ignoredPct = options.ignoredPct ?? IGNORED_BY_FIELD_PCT;
  const noProduction = options.noProduction ?? NO_PRODUCTION_FPTS;
  const share = options.projectionShare ?? ROSTERABLE_PROJECTION_SHARE;

  const byPosition = new Map<string, AuditSlatePlayer[]>();
  for (const p of slatePlayers) {
    if (p.ourProj === null) continue;
    if (!byPosition.has(p.position)) byPosition.set(p.position, []);
    byPosition.get(p.position)!.push(p);
  }
  const ranks = new Map<number, number>();
  const leaders = new Map<string, number>();
  for (const [position, group] of byPosition) {
    const ordered = [...group].sort((a, b) => (b.ourProj! - a.ourProj!) || (b.salary - a.salary));
    leaders.set(position, ordered[0]?.ourProj ?? 0);
    ordered.forEach((p, i) => ranks.set(p.dkPlayerId, i + 1));
  }

  const rows: AuditRow[] = [];
  let unmatched = 0;
  for (const p of slatePlayers) {
    if (p.isOut || p.ourProj === null) continue;
    let observed = field.get(normalizeName(p.name));
    if (!observed) { observed = { draftedPct: 0, fpts: null }; unmatched += 1; }
    const rank = ranks.get(p.dkPlayerId) ?? null;
    const cut = POSITION_DEPTH[p.position] ?? 0;
    const floor = share * (leaders.get(p.position) ?? 0);
    const rosterable = rank !== null && rank <= cut && p.ourProj >= floor;
    const verdict: FieldVerdict =
      !rosterable || observed.draftedPct >= ignoredPct || observed.fpts === null ? "UNINFORMATIVE"
      : observed.fpts <= noProduction ? "MARKET_KNEW" : "REAL_EDGE";
    rows.push({
      name: p.name, position: p.position, salary: p.salary,
      ourProj: Number(p.ourProj.toFixed(2)), ourRank: rank,
      fieldPct: observed.draftedPct, actual: observed.fpts, verdict,
    });
  }

  const flagged = rows.filter((r) => r.verdict !== "UNINFORMATIVE").sort((a, b) => b.ourProj - a.ourProj);
  const marketKnew = flagged.filter((r) => r.verdict === "MARKET_KNEW");
  return {
    version: FIELD_AUDIT_VERSION,
    rows,
    flagged,
    summary: {
      considered: rows.length,
      flagged: flagged.length,
      marketKnew: marketKnew.length,
      realEdge: flagged.length - marketKnew.length,
      projectedPointsOnMarketKnew: Number(marketKnew.reduce((a, r) => a + r.ourProj, 0).toFixed(1)),
      unmatchedToFieldTable: unmatched,
    },
  };
}

/** Below this many flagged players the split cannot be told apart from luck. */
export const DESCRIPTIVE_ONLY_BELOW = 30;
