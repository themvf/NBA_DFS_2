/**
 * Grade the NFL ownership prior against imported contest ownership.
 *
 * For every contest in nfl_dfs_field_contests, rebuild the slate it was
 * imported against, compute the prior from that slate's own projections, and
 * compare with the field's actual drafted % (nfl_dfs_field_ownership) by
 * normalised name. Reports correlation, rank correlation, mean error and
 * bias per contest and per position, plus the largest misses.
 *
 *   npx tsx --conditions=react-server scripts/nfl-ownership-calibration.ts
 *
 * Descriptive only. The promotion gate for a fitted model is registered in
 * docs/nfl-ownership-model.md; this script reports against it but never
 * changes capability. Read-only: writes nothing.
 */
import { config } from "dotenv";
config({ path: ".env.local", quiet: true });

type Row = { name: string; position: string; team: string; salary: number; ours: number; actual: number };

const pearson = (x: number[], y: number[]) => {
  const n = x.length; if (n < 3) return NaN;
  const mx = x.reduce((a, b) => a + b, 0) / n, my = y.reduce((a, b) => a + b, 0) / n;
  let sxy = 0, sxx = 0, syy = 0;
  for (let i = 0; i < n; i += 1) { sxy += (x[i] - mx) * (y[i] - my); sxx += (x[i] - mx) ** 2; syy += (y[i] - my) ** 2; }
  return sxy / Math.sqrt(sxx * syy);
};
/** Averaged tied ranks. Before 2026-10-19 ties took arbitrary consecutive ranks, which with ~500 near-zero players inflated Spearman (0.80 reported vs 0.66 tie-correct on week 2). */
const ranks = (v: number[]) => {
  const idx = v.map((x, i) => [x, i] as const).sort((a, b) => a[0] - b[0]); const r = new Array<number>(v.length);
  for (let k = 0; k < idx.length;) { let j = k; while (j + 1 < idx.length && idx[j + 1][0] === idx[k][0]) j += 1; const avg = (k + j) / 2 + 1; for (let m = k; m <= j; m += 1) r[idx[m][1]] = avg; k = j + 1; }
  return r;
};
/** A missing label is a reconciled zero only when the contest record is complete enough to say so (docs/nfl-ownership-scorecard-v1.md section 3). */
const RECONCILED_ZERO_MIN_MASS = 0.995;
const spearman = (x: number[], y: number[]) => pearson(ranks(x), ranks(y));
const f1 = (v: number) => (Number.isFinite(v) ? v.toFixed(1) : "n/a");
const f2 = (v: number) => (Number.isFinite(v) ? v.toFixed(2) : "n/a");

(async () => {
  const a = await import("../src/app/dfs/nfl/actions");
  const { projectOwnershipPrior } = await import("../src/lib/nfl-dfs/ownership-prior");
  const { normalizeName } = await import("../src/lib/nfl-dfs/field-audit");
  const { db } = await import("../src/db"); const { sql } = await import("drizzle-orm");
  const rowsOf = (r: unknown) => (r as { rows: Record<string, unknown>[] }).rows;
  const contests = rowsOf(await db.execute(sql`SELECT contest_id, format, season, week, slate_upload_id, entry_count FROM nfl_dfs_field_contests ORDER BY season, week, format`));
  if (!contests.length) { console.log("No imported contests. Import a contest's standings on the Results step first."); return; }
  const summary: Array<{ label: string; n: number; r: number; rho: number; mae: number; bias: number }> = [];
  for (const c of contests) {
    const uploadId = String(c.slate_upload_id);
    const format = String(c.format) as "classic" | "showdown";
    const label = `${c.season} wk${c.week} ${format} ${c.contest_id} (${Number(c.entry_count).toLocaleString()} entries)`;
    let slate;
    try { slate = (await a.loadSavedNflWorkspace(uploadId)).slate; } catch (e) { console.log(`\n== ${label}: slate ${uploadId.slice(0, 8)} unavailable (${e instanceof Error ? e.message : e})`); continue; }
    const prior = projectOwnershipPrior(slate.players.map((p) => ({ dkPlayerId: p.dkPlayerId, position: p.position, salary: p.salary,
      projection: p.ourProj, dkAvg: p.avgFptsDk, isOut: p.isOut, dkStatus: p.dkStatus, captainSalary: p.captainSalary })), format);
    const ours = new Map(prior.players.map((p) => [p.dkPlayerId, p.ownPct]));
    const actual = new Map(rowsOf(await db.execute(sql`SELECT normalized_name, drafted_pct FROM nfl_dfs_field_ownership WHERE contest_id = ${String(c.contest_id)}`))
      .map((r) => [String(r.normalized_name), Number(r.drafted_pct)]));
    const seen = new Set<string>();
    const rows: Row[] = [];
    const slots = format === "classic" ? 9 : 6;
    const recordedMass = [...actual.values()].reduce((a, b) => a + b, 0) / (slots * 100);
    const zerosReconciled = recordedMass >= RECONCILED_ZERO_MIN_MASS;
    let unresolved = 0;
    for (const p of slate.players) {
      const key = normalizeName(p.name);
      if (seen.has(key)) continue; seen.add(key);
      const label = actual.get(key);
      // A slate player with no label is a reconciled 0 only when the contest record is
      // near-complete; otherwise the row is unresolved and excluded, never zeroed.
      if (label == null && !zerosReconciled) { unresolved += 1; continue; }
      rows.push({ name: p.name, position: p.position, team: p.team, salary: p.salary, ours: ours.get(p.dkPlayerId) ?? 0, actual: label ?? 0 });
    }
    const matchedField = [...actual.keys()].filter((k) => seen.has(k)).length;
    console.log(`\n== ${label}`);
    console.log(`  recorded mass ${(recordedMass * 100).toFixed(1)}% of ${slots * 100}% · missing labels ${zerosReconciled ? "counted as reconciled zeros" : `UNRESOLVED (${unresolved} excluded)`} · prior ${prior.version}`);
    const x = rows.map((r) => r.ours), y = rows.map((r) => r.actual);
    const err = rows.map((r) => r.ours - r.actual);
    const mae = err.reduce((t, e) => t + Math.abs(e), 0) / err.length, bias = err.reduce((t, e) => t + e, 0) / err.length;
    const r = pearson(x, y), rho = spearman(x, y);
    summary.push({ label, n: rows.length, r, rho, mae, bias });
    console.log(`  players ${rows.length} · field rows matched ${matchedField}/${actual.size} · our sum ${f1(x.reduce((a, b) => a + b, 0))}% vs actual sum ${f1(y.reduce((a, b) => a + b, 0))}%`);
    console.log(`  Pearson ${f2(r)} · Spearman ${f2(rho)} · MAE ${f2(mae)} pts · bias ${f2(bias)} pts (positive = we over-project ownership)`);
    for (const pos of ["QB", "RB", "WR", "TE", "DST"]) {
      const g = rows.filter((w) => w.position === pos); if (g.length < 3) continue;
      const gx = g.map((w) => w.ours), gy = g.map((w) => w.actual);
      console.log(`    ${pos.padEnd(3)} n=${String(g.length).padStart(3)} Spearman ${f2(spearman(gx, gy))} MAE ${f2(g.reduce((t, w) => t + Math.abs(w.ours - w.actual), 0) / g.length)} · top actual ${g.sort((p, q) => q.actual - p.actual).slice(0, 3).map((w) => `${w.name} ${f1(w.actual)}% (ours ${f1(w.ours)}%)`).join(", ")}`);
    }
    const owned = rows.filter((w) => w.actual >= 1);
    if (owned.length >= 5) console.log(`  diagnostic, actual >= 1% (outcome-selected, never a gate): n=${owned.length} MAE ${f2(owned.reduce((t, w) => t + Math.abs(w.ours - w.actual), 0) / owned.length)} Spearman ${f2(spearman(owned.map((w) => w.ours), owned.map((w) => w.actual)))}`);
    console.log("  biggest misses:");
    for (const w of [...rows].sort((p, q) => Math.abs(q.ours - q.actual) - Math.abs(p.ours - p.actual)).slice(0, 8))
      console.log(`    ${w.name.padEnd(24)} ${w.position.padEnd(3)} $${w.salary}  ours ${f1(w.ours).padStart(5)}%  actual ${f1(w.actual).padStart(5)}%`);
  }
  console.log("\n== Summary (gate in docs/nfl-ownership-model.md: Spearman >= 0.70 and MAE <= 2.0 on >= 4 held-out Classic slates)");
  for (const s of summary) console.log(`  ${s.label.padEnd(58)} n=${s.n} Spearman ${f2(s.rho)} MAE ${f2(s.mae)} bias ${f2(s.bias)}`);
  console.log("  This grades the stated prior on the slates it was designed against; it is descriptive, not a holdout result.");
})().catch((e) => { console.error(e); process.exit(1); });
