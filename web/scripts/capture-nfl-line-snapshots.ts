/**
 * Capture historical NFL line snapshots for the west-coast 1pm study.
 *
 * Pre-registration: docs/nfl-line-movement-study.md. Read it before changing
 * a snapshot time — the times are part of a frozen design, not tuning knobs.
 *
 * THIS SCRIPT SPENDS REAL MONEY. Historical calls bill at 10x, so each one is
 * 20 credits (2 markets x 1 region). The full run is 288 calls = 5,760 credits.
 * Two properties follow and both are load-bearing:
 *
 *   RESUMABLE  every (label, requested_at) already present in the database is
 *              skipped before the request is made, so an interrupted run costs
 *              nothing to restart.
 *   IDEMPOTENT rows are keyed on (snapshot_at, event_id), so a re-run cannot
 *              duplicate what it already bought.
 *
 * Storage is per event per snapshot: consensus (median) numbers for fast
 * analysis, plus the full per-book payload, because re-buying a snapshot to
 * recover a field we chose not to extract would be paying twice for the same
 * data.
 *
 * Run:  npm run capture:nfl-lines            (dry run — prints the plan, spends nothing)
 *       npm run capture:nfl-lines -- --go    (executes)
 */

import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL!);
const API = "https://api.the-odds-api.com/v4/historical/sports/americanfootball_nfl/odds";
const KEY = process.env.ODDS_API_KEY;

const SEASONS = [2022, 2023, 2024, 2025];
const MARKETS = "h2h,spreads";
const REGIONS = "us";
/** Frozen by the pre-registration. */
const COST_PER_CALL = 20;

type Label = "open" | "friday" | "sun_am" | "sun_close";

/**
 * US DST ends on the first Sunday of November. Before it the NFL runs on EDT
 * (UTC-4); after, EST (UTC-5). A fixed UTC time cannot be "11am ET" in both,
 * which is why the Sunday captures shift an hour and the midweek ones do not.
 */
function isEdt(d: Date): boolean {
  const y = d.getUTCFullYear();
  const nov = new Date(Date.UTC(y, 10, 1));
  const firstSunday = 1 + ((7 - nov.getUTCDay()) % 7);
  const changeover = Date.UTC(y, 10, firstSunday, 6, 0, 0);
  // DST starts second Sunday of March; the NFL season never reaches it.
  return d.getTime() < changeover;
}

/** Hour (UTC) for an Eastern wall-clock hour on a given date. */
function utcHourForEt(d: Date, etHour: number, etMinute: number): { h: number; m: number } {
  return { h: etHour + (isEdt(d) ? 4 : 5), m: etMinute };
}

type Plan = { season: number; label: Label; iso: string };

/**
 * Season date bounds come from nflverse, NOT from `nfl_season_games`.
 *
 * That table only holds 2023 onward, so sourcing bounds from it silently
 * dropped 2022 from the plan entirely — a whole season the study asks for,
 * missing with no error. Fetching the schedule makes the coverage explicit and
 * the run asserts all four seasons are present before spending anything.
 */
async function seasonBounds(): Promise<Array<{ season: number; first_day: string; last_day: string }>> {
  const res = await fetch(
    "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv",
  );
  if (!res.ok) throw new Error(`nflverse schedules: HTTP ${res.status}`);
  const text = await res.text();
  const lines = text.trim().split(/\r?\n/);
  const head = lines[0].split(",");
  const iSeason = head.indexOf("season");
  const iType = head.indexOf("game_type");
  const iDay = head.indexOf("gameday");
  const acc = new Map<number, { min: string; max: string }>();
  for (let i = 1; i < lines.length; i += 1) {
    const c = lines[i].split(",");
    const s = Number(c[iSeason]);
    if (!SEASONS.includes(s) || c[iType] !== "REG") continue;
    const d = c[iDay];
    if (!d) continue;
    const cur = acc.get(s);
    if (!cur) acc.set(s, { min: d, max: d });
    else { if (d < cur.min) cur.min = d; if (d > cur.max) cur.max = d; }
  }
  const missing = SEASONS.filter((s) => !acc.has(s));
  if (missing.length > 0) {
    throw new Error(`no schedule rows for season(s) ${missing.join(", ")} — refusing to spend on a partial plan`);
  }
  return SEASONS.map((s) => ({ season: s, first_day: acc.get(s)!.min, last_day: acc.get(s)!.max }));
}

/** Every snapshot the frozen design calls for, across all seasons. */
async function buildPlan(): Promise<Plan[]> {
  const bounds = await seasonBounds();
  const plan: Plan[] = [];
  for (const b of bounds) {
    const season = Number(b.season);
    // Start a week early so the first week's opener is captured.
    const start = new Date(`${b.first_day}T00:00:00Z`);
    start.setUTCDate(start.getUTCDate() - 7);
    const end = new Date(`${b.last_day}T23:59:59Z`);

    for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
      const dow = d.getUTCDay(); // 0 Sun .. 6 Sat
      const mk = (label: Label, h: number, m: number) => {
        const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), h, m, 0));
        plan.push({ season, label, iso: t.toISOString().replace(/\.\d{3}Z$/, "Z") });
      };
      if (dow === 2) mk("open", 15, 0);
      if (dow === 5) mk("friday", 15, 0);
      if (dow === 0) {
        const am = utcHourForEt(d, 11, 0);
        const close = utcHourForEt(d, 12, 45);
        mk("sun_am", am.h, am.m);
        mk("sun_close", close.h, close.m);
      }
    }
  }
  return plan;
}

const median = (xs: number[]): number | null => {
  const v = xs.filter((x) => Number.isFinite(x)).sort((a, b) => a - b);
  if (v.length === 0) return null;
  const mid = Math.floor(v.length / 2);
  return v.length % 2 ? v[mid] : (v[mid - 1] + v[mid]) / 2;
};

async function ensureTable() {
  await sql`
    CREATE TABLE IF NOT EXISTS nfl_line_snapshots (
      id BIGSERIAL PRIMARY KEY,
      season INTEGER NOT NULL,
      label TEXT NOT NULL,
      requested_at TIMESTAMPTZ NOT NULL,
      snapshot_at TIMESTAMPTZ NOT NULL,
      event_id TEXT NOT NULL,
      commence_time TIMESTAMPTZ NOT NULL,
      home_team TEXT NOT NULL,
      away_team TEXT NOT NULL,
      book_count INTEGER NOT NULL,
      home_spread DOUBLE PRECISION,
      home_ml INTEGER,
      away_ml INTEGER,
      lead_minutes DOUBLE PRECISION NOT NULL,
      books JSONB NOT NULL DEFAULT '[]'::jsonb,
      captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(snapshot_at, event_id)
    )`;
  await sql`CREATE INDEX IF NOT EXISTS idx_nfl_line_snap_evt ON nfl_line_snapshots(event_id, snapshot_at)`;
  await sql`CREATE TABLE IF NOT EXISTS nfl_line_snapshot_runs (
      requested_at TIMESTAMPTZ PRIMARY KEY,
      season INTEGER NOT NULL,
      label TEXT NOT NULL,
      snapshot_at TIMESTAMPTZ,
      events INTEGER NOT NULL,
      credits INTEGER NOT NULL,
      fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`;
}

async function main() {
  const go = process.argv.includes("--go");
  if (!KEY) { console.error("ODDS_API_KEY not set"); process.exit(1); }

  await ensureTable();
  const plan = await buildPlan();

  const doneRows = await sql`SELECT requested_at::text AS r FROM nfl_line_snapshot_runs`;
  const done = new Set(doneRows.map((r) => new Date(String(r.r)).toISOString().replace(/\.\d{3}Z$/, "Z")));
  const todo = plan.filter((p) => !done.has(p.iso));

  const bySeason: Record<number, number> = {};
  for (const p of todo) bySeason[p.season] = (bySeason[p.season] ?? 0) + 1;

  console.log("=".repeat(72));
  console.log("NFL LINE SNAPSHOT CAPTURE — docs/nfl-line-movement-study.md");
  console.log("=".repeat(72));
  console.log(`  planned snapshots : ${plan.length}`);
  console.log(`  already captured  : ${plan.length - todo.length}`);
  console.log(`  to fetch          : ${todo.length}`);
  console.log(`  per season        : ${JSON.stringify(bySeason)}`);
  console.log(`  cost              : ${todo.length * COST_PER_CALL} credits ` +
    `(${todo.length} x ${COST_PER_CALL})`);
  const byLabel: Record<string, number> = {};
  for (const p of todo) byLabel[p.label] = (byLabel[p.label] ?? 0) + 1;
  console.log(`  by label          : ${JSON.stringify(byLabel)}`);

  if (!go) {
    console.log("\n  DRY RUN — nothing spent. Re-run with `-- --go` to execute.");
    return;
  }

  let spent = 0;
  let stored = 0;
  let remaining = "?";
  for (let i = 0; i < todo.length; i += 1) {
    const p = todo[i];
    const url = `${API}?apiKey=${KEY}&regions=${REGIONS}&markets=${MARKETS}` +
      `&oddsFormat=american&date=${p.iso}`;
    // Transient 5xx and network blips are retried; 4xx is not. The provider
    // returned a single 502 mid-run on the first execution and aborting the
    // whole 300-call job over it was the wrong trade -- a rejected call costs
    // no credits, so retrying is free, while restarting a long run is not.
    // A 4xx (bad key, exhausted quota) is real and still stops immediately.
    let res: Response | null = null;
    let fatal = false;
    for (let attempt = 0; attempt < 4; attempt += 1) {
      try {
        const r = await fetch(url);
        if (r.ok) { res = r; break; }
        if (r.status >= 400 && r.status < 500) {
          console.error(`  ${p.iso} ${p.label}: HTTP ${r.status} — client error, stopping.`);
          console.error(`  body: ${(await r.text()).slice(0, 200)}`);
          fatal = true;
          break;
        }
        console.error(`  ${p.iso} ${p.label}: HTTP ${r.status}, retry ${attempt + 1}/3`);
      } catch (e) {
        console.error(`  ${p.iso} ${p.label}: network error, retry ${attempt + 1}/3 — ${String(e).slice(0, 80)}`);
      }
      await new Promise((r) => setTimeout(r, 1500 * (attempt + 1)));
    }
    if (fatal) break;
    if (!res) {
      console.error(`  ${p.iso} ${p.label}: still failing after retries — stopping. Re-run to resume free.`);
      break;
    }
    const cost = Number(res.headers.get("x-requests-last") ?? 0);
    remaining = res.headers.get("x-requests-remaining") ?? remaining;
    spent += cost;

    const body = (await res.json()) as {
      timestamp: string;
      data: Array<{
        id: string; commence_time: string; home_team: string; away_team: string;
        bookmakers: Array<{ key: string; markets: Array<{ key: string; outcomes: Array<{ name: string; price: number; point?: number }> }> }>;
      }>;
    };
    const snapAt = body.timestamp;
    const events = body.data ?? [];

    // Batched into one round trip per snapshot. Individually these are ~7,500
    // HTTP queries and the inserts would dominate the runtime, not the fetches.
    const inserts: ReturnType<typeof sql>[] = [];
    for (const ev of events) {
      const spreads: number[] = [];
      const homeMls: number[] = [];
      const awayMls: number[] = [];
      for (const bk of ev.bookmakers ?? []) {
        for (const mkt of bk.markets ?? []) {
          if (mkt.key === "spreads") {
            const h = mkt.outcomes.find((o) => o.name === ev.home_team);
            if (h?.point != null) spreads.push(h.point);
          } else if (mkt.key === "h2h") {
            const h = mkt.outcomes.find((o) => o.name === ev.home_team);
            const a = mkt.outcomes.find((o) => o.name === ev.away_team);
            if (h?.price != null) homeMls.push(h.price);
            if (a?.price != null) awayMls.push(a.price);
          }
        }
      }
      const lead = (new Date(ev.commence_time).getTime() - new Date(snapAt).getTime()) / 60000;
      const hs = median(spreads);
      const hm = median(homeMls);
      const am = median(awayMls);
      inserts.push(sql`
        INSERT INTO nfl_line_snapshots
          (season, label, requested_at, snapshot_at, event_id, commence_time,
           home_team, away_team, book_count, home_spread, home_ml, away_ml,
           lead_minutes, books)
        VALUES (${p.season}, ${p.label}, ${p.iso}::timestamptz, ${snapAt}::timestamptz,
                ${ev.id}, ${ev.commence_time}::timestamptz, ${ev.home_team}, ${ev.away_team},
                ${(ev.bookmakers ?? []).length}, ${hs}, ${hm == null ? null : Math.round(hm)},
                ${am == null ? null : Math.round(am)}, ${lead},
                ${JSON.stringify(ev.bookmakers ?? [])}::jsonb)
        ON CONFLICT (snapshot_at, event_id) DO NOTHING
      `);
      stored += 1;
    }
    if (inserts.length > 0) await sql.transaction(inserts);

    await sql`
      INSERT INTO nfl_line_snapshot_runs (requested_at, season, label, snapshot_at, events, credits)
      VALUES (${p.iso}::timestamptz, ${p.season}, ${p.label}, ${snapAt}::timestamptz, ${events.length}, ${cost})
      ON CONFLICT (requested_at) DO NOTHING
    `;

    if (i % 20 === 0 || i === todo.length - 1) {
      console.log(
        `  [${String(i + 1).padStart(3)}/${todo.length}] ${p.iso} ${p.label.padEnd(9)} ` +
        `events ${String(events.length).padStart(3)}  spent ${spent}  remaining ${remaining}`,
      );
    }
  }

  console.log(`\n  done. credits spent this run: ${spent}. rows stored: ${stored}. remaining: ${remaining}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
