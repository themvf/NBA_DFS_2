import fs from "fs";
import path from "path";

import { sql } from "drizzle-orm";

function loadDatabaseUrl() {
  if (process.env.DATABASE_URL) return;
  const envPath = path.join(process.cwd(), ".env.local");
  const line = fs.readFileSync(envPath, "utf8")
    .split(/\r?\n/)
    .find((entry) => entry.startsWith("DATABASE_URL="));
  if (!line) throw new Error("DATABASE_URL missing in web/.env.local");
  process.env.DATABASE_URL = line.slice("DATABASE_URL=".length);
}

function parseSlateIdArg(): number | null {
  const arg = process.argv.find((entry) => entry.startsWith("--slateId="));
  if (!arg) return null;
  const value = Number(arg.slice("--slateId=".length));
  return Number.isFinite(value) ? value : null;
}

async function resolveSlateId(): Promise<number> {
  const fromArg = parseSlateIdArg();
  if (fromArg) return fromArg;

  const mod = await import("../src/db/index");
  const db = ((mod as { db?: { execute: (query: unknown) => Promise<{ rows: Array<Record<string, unknown>> }> }; default?: { db?: { execute: (query: unknown) => Promise<{ rows: Array<Record<string, unknown>> }> } } }).db
    ?? (mod as { default?: { db?: { execute: (query: unknown) => Promise<{ rows: Array<Record<string, unknown>> }> } } }).default?.db);
  if (!db) throw new Error("DB module did not expose a db client");
  const result = await db.execute(sql`
    select ds.id
    from dk_slates ds
    join dk_players dp on dp.slate_id = ds.id
    where ds.sport = 'nba'
      and dp.actual_fpts is not null
    group by ds.id, ds.slate_date
    order by ds.slate_date desc, ds.id desc
    limit 1
  `);
  const value = result.rows[0]?.id;
  if (value == null) throw new Error("No NBA slate with actual results found.");
  return Number(value);
}

async function main() {
  loadDatabaseUrl();
  const slateId = await resolveSlateId();
  const { buildNbaOddsSignalReport, persistNbaOddsSignalReport } = await import("../src/lib/nba-odds-signal");

  const shouldPersist = process.argv.includes("--persist");
  const report = shouldPersist
    ? await persistNbaOddsSignalReport(slateId)
    : await buildNbaOddsSignalReport(slateId);

  console.log(JSON.stringify({ slateId, persisted: shouldPersist, report }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
