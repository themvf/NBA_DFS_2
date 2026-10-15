/**
 * Which projection should the ownership prior read: the model's own (form-chasing)
 * number, or the market-anchored one? (read-only, 2026-10-15)
 *
 * For every imported 2026 contest with realized ownership and a saved pregame
 * build, rebuild the market anchor for each player from that week's closing
 * lines (nfl_prop_implied_lines, DraftKings first), then run the ownership prior
 * twice from the frozen snapshot: (A) projection = the model's number, as the
 * prior read before the anchor shipped; (B) projection = the anchored number
 * where a full set of lines exists, else the model's, as the live read does
 * since 2026-10-12. Compare both with the field's drafted %. Nothing is fitted;
 * both priors are the shipped constants, so there is nothing to hold out.
 *
 *   npx tsx --conditions=react-server scripts/research-nfl-ownership-anchor-input.ts
 */
import { config } from "dotenv";
config({ path: ".env.local", quiet: true });

const pearson = (x: number[], y: number[]) => {
  const n = x.length; if (n < 3) return NaN;
  const mx = x.reduce((a, b) => a + b, 0) / n, my = y.reduce((a, b) => a + b, 0) / n;
  let sxy = 0, sxx = 0, syy = 0;
  for (let i = 0; i < n; i += 1) { sxy += (x[i] - mx) * (y[i] - my); sxx += (x[i] - mx) ** 2; syy += (y[i] - my) ** 2; }
  return sxy / Math.sqrt(sxx * syy);
};
const ranks = (v: number[]) => { const idx = v.map((x, i) => [x, i] as const).sort((a, b) => a[0] - b[0]); const r = new Array(v.length); idx.forEach(([, i], k) => { r[i] = k; }); return r as number[]; };
const spearman = (x: number[], y: number[]) => pearson(ranks(x), ranks(y));
const f2 = (v: number) => (Number.isFinite(v) ? v.toFixed(2) : "n/a");
const mean = (v: number[]) => v.reduce((a, b) => a + b, 0) / Math.max(1, v.length);

(async () => {
  const { projectOwnershipPrior } = await import("../src/lib/nfl-dfs/ownership-prior");
  const { anchorPlayer } = await import("../src/lib/nfl-dfs/market-anchor");
  type PropQuote = import("../src/lib/nfl-dfs/market-anchor").PropQuote;
  const { normalizeName } = await import("../src/lib/nfl-dfs/field-audit");
  const { db } = await import("../src/db"); const { sql } = await import("drizzle-orm");
  type Raw = Record<string, unknown>;
  const rowsOf = (r: unknown) => (r as { rows: Raw[] }).rows;
  const contests = rowsOf(await db.execute(sql`
    SELECT c.contest_id, c.format, c.season, c.week, c.slate_upload_id, c.entry_count,
      (SELECT o.run_id FROM nfl_dfs_optimizer_runs o WHERE o.upload_id = c.slate_upload_id AND o.status IN ('complete','partial')
         AND NOT (o.settings ? 'researchStudy') AND o.generated_lineups > 0 ORDER BY o.created_at DESC LIMIT 1) AS run_id
    FROM nfl_dfs_field_contests c
    WHERE c.season = 2026 AND c.slate_upload_id IS NOT NULL AND c.entry_count >= 10000
      AND EXISTS (SELECT 1 FROM nfl_dfs_field_ownership o WHERE o.contest_id = c.contest_id AND o.drafted_pct IS NOT NULL)
    ORDER BY c.week, c.format, c.entry_count DESC`));
  type Rec = { contest: string; week: number; format: string; name: string; position: string; salary: number; model: number; anchored: number | null; priorA: number; priorB: number; actual: number; history: number };
  const all: Rec[] = [];
  const perContest: string[] = [];
  for (const c of contests) {
    if (!c.run_id) { perContest.push(`wk${c.week} ${c.format} ${c.contest_id}: no saved pregame build, skipped`); continue; }
    const run = rowsOf(await db.execute(sql`SELECT input_snapshot, created_at FROM nfl_dfs_optimizer_runs WHERE run_id = ${String(c.run_id)}::uuid`))[0];
    const snap = (typeof run.input_snapshot === "string" ? JSON.parse(run.input_snapshot) : run.input_snapshot) as Raw[];
    if (snap.some((p) => p.marketAnchor)) { perContest.push(`wk${c.week} ${c.format} ${c.contest_id}: snapshot already anchored, skipped`); continue; }
    const lines = rowsOf(await db.execute(sql`
      SELECT DISTINCT ON (player_name, market) player_name, market, line, over_price, under_price, yes_price, book, captured_at
      FROM nfl_prop_implied_lines WHERE season = 2026 AND week = ${Number(c.week)} AND capture_role = 'close'
      ORDER BY player_name, market, (book = 'draftkings') DESC, captured_at DESC`));
    const byPlayer = new Map<string, Record<string, PropQuote>>();
    for (const l of lines) {
      const k = normalizeName(String(l.player_name));
      const q = byPlayer.get(k) ?? {};
      q[String(l.market)] = { line: l.line == null ? null : Number(l.line), over: l.over_price == null ? null : Number(l.over_price),
        under: l.under_price == null ? null : Number(l.under_price), yes: l.yes_price == null ? null : Number(l.yes_price) };
      byPlayer.set(k, q);
    }
    const anchored = new Map<number, number>();
    for (const p of snap) {
      const q = byPlayer.get(normalizeName(String(p.name)));
      if (!q) continue;
      const a = anchorPlayer({ position: String(p.position), ourProj: p.ourProj as number | null, floorFpts: (p.floor as number) ?? null,
        medianFpts: (p.median as number) ?? null, ceilingFpts: (p.ceiling as number) ?? null, lines: q, book: "closing", capturedAt: "" });
      if (a) anchored.set(p.dkPlayerId as number, a.after.mean);
    }
    const input = (useAnchor: boolean) => snap.map((p) => ({ dkPlayerId: p.dkPlayerId as number, position: String(p.position), salary: Number(p.salary),
      projection: useAnchor && anchored.has(p.dkPlayerId as number) ? anchored.get(p.dkPlayerId as number)! : (p.ourProj as number | null),
      dkAvg: (p.dkAvg as number) ?? null, isOut: p.isOut === true, dkStatus: (p.status as string) ?? null, captainSalary: (p.captainSalary as number) ?? null }));
    const format = String(c.format) as "classic" | "showdown";
    const priorA = new Map(projectOwnershipPrior(input(false), format).players.map((p) => [p.dkPlayerId, p.ownPct]));
    const priorB = new Map(projectOwnershipPrior(input(true), format).players.map((p) => [p.dkPlayerId, p.ownPct]));
    const actual = new Map(rowsOf(await db.execute(sql`SELECT normalized_name, drafted_pct FROM nfl_dfs_field_ownership WHERE contest_id = ${String(c.contest_id)} AND drafted_pct IS NOT NULL`))
      .map((r) => [String(r.normalized_name), Number(r.drafted_pct)]));
    const seen = new Set<string>();
    const recs: Rec[] = [];
    for (const p of snap) {
      const key = normalizeName(String(p.name));
      if (seen.has(key)) continue; seen.add(key);
      recs.push({ contest: String(c.contest_id), week: Number(c.week), format, name: String(p.name), position: String(p.position), salary: Number(p.salary),
        model: Number(p.ourProj ?? 0), anchored: anchored.get(p.dkPlayerId as number) ?? null, history: Number(p.historyGames ?? 0),
        priorA: priorA.get(p.dkPlayerId as number) ?? 0, priorB: priorB.get(p.dkPlayerId as number) ?? 0, actual: actual.get(key) ?? 0 });
    }
    all.push(...recs);
    const act = recs.map((r) => r.actual), a = recs.map((r) => r.priorA), b = recs.map((r) => r.priorB);
    const anchoredRecs = recs.filter((r) => r.anchored != null);
    perContest.push(`wk${c.week} ${format.padEnd(8)} ${String(c.contest_id).padEnd(10)} n=${recs.length} anchored=${anchoredRecs.length}` +
      ` | A(model)  rho ${f2(spearman(a, act))} mae ${f2(mean(a.map((v, i) => Math.abs(v - act[i]))))}` +
      ` | B(anchor) rho ${f2(spearman(b, act))} mae ${f2(mean(b.map((v, i) => Math.abs(v - act[i]))))}` +
      ` | anchored players only: mae A ${f2(mean(anchoredRecs.map((r) => Math.abs(r.priorA - r.actual))))} B ${f2(mean(anchoredRecs.map((r) => Math.abs(r.priorB - r.actual))))}`);
  }
  console.log(perContest.join("\n"));
  const pooled = (rs: Rec[], label: string) => {
    const act = rs.map((r) => r.actual);
    console.log(`${label}: n=${rs.length} | A(model) rho ${f2(spearman(rs.map((r) => r.priorA), act))} mae ${f2(mean(rs.map((r) => Math.abs(r.priorA - r.actual))))} bias ${f2(mean(rs.map((r) => r.priorA - r.actual)))}` +
      ` | B(anchor) rho ${f2(spearman(rs.map((r) => r.priorB), act))} mae ${f2(mean(rs.map((r) => Math.abs(r.priorB - r.actual))))} bias ${f2(mean(rs.map((r) => r.priorB - r.actual)))}`);
  };
  console.log("\nPooled over players");
  for (const fmt of ["classic", "showdown"]) { const rs = all.filter((r) => r.format === fmt); if (rs.length) pooled(rs, fmt); }
  const anchoredAll = all.filter((r) => r.anchored != null);
  pooled(anchoredAll, "anchored players only (where A and B differ)");
  // The Maye slice: the book sits well above the model (recent form bad), and the reverse.
  const gap = (r: Rec) => (r.anchored ?? r.model) - r.model;
  for (const [label, pick] of [["book >= model + 3 (form worse than the book thinks)", (r: Rec) => gap(r) >= 3], ["book <= model - 3 (form better than the book thinks)", (r: Rec) => gap(r) <= -3],
    ["established (6+ games), book >= model + 3: the slumping-veteran case", (r: Rec) => gap(r) >= 3 && r.history >= 6], ["established (6+ games), book <= model - 3: the hot-veteran case", (r: Rec) => gap(r) <= -3 && r.history >= 6]] as const) {
    const rs = anchoredAll.filter(pick);
    if (rs.length) {
      pooled(rs, label);
      console.log(`   mean actual own ${f2(mean(rs.map((r) => r.actual)))}%, prior A ${f2(mean(rs.map((r) => r.priorA)))}%, prior B ${f2(mean(rs.map((r) => r.priorB)))}%`);
    }
  }
  console.log("\nLargest book-vs-model gaps with ownership (anchored, model, actual own, prior A, prior B):");
  for (const r of [...anchoredAll].filter((r) => r.history >= 6).sort((x, y) => Math.abs(gap(y)) - Math.abs(gap(x))).slice(0, 12)) {
    console.log(`  wk${r.week} ${r.name.padEnd(22)} ${r.position} $${r.salary} book ${f2(r.anchored!)} model ${f2(r.model)} | own ${f2(r.actual)}% A ${f2(r.priorA)}% B ${f2(r.priorB)}%`);
  }
})();
