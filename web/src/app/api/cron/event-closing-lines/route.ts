import { NextRequest, NextResponse } from "next/server";
import { sql } from "drizzle-orm";
import { db } from "@/db";

const GITHUB_OWNER = "themvf";
const GITHUB_REPO = "NBA_DFS_2";
const WORKFLOW_FILE = "capture_event_closes.yml";
const WORKFLOW_REF = "main";

export const dynamic = "force-dynamic";
export const maxDuration = 15;

type WorkRow = { has_work: boolean };

async function hasDueWork() {
  try {
    const rows = await db.execute<WorkRow>(sql`
      WITH events AS (
        SELECT 'mlb'::text AS sport, m.id AS matchup_id, m.commence_time AS starts_at
        FROM mlb_matchups m
        WHERE m.commence_time BETWEEN NOW() - INTERVAL '3 minutes' AND NOW() + INTERVAL '6 hours'
          AND COALESCE(m.game_status, '') NOT IN ('Postponed', 'Cancelled')
        UNION ALL
        SELECT 'tennis', t.id, t.commence_time
        FROM tennis_matches t
        WHERE t.commence_time BETWEEN NOW() - INTERVAL '3 minutes' AND NOW() + INTERVAL '6 hours'
          AND t.completion_status = 'scheduled'
        UNION ALL
        SELECT 'cfb', m.id, m.commence_time
        FROM cfb_matchups m
        WHERE m.commence_time BETWEEN NOW() - INTERVAL '3 minutes' AND NOW() + INTERVAL '54 hours'
          AND m.completed = FALSE AND m.start_time_tbd = FALSE
          AND m.odds_event_id IS NOT NULL
      ), windows AS (
        SELECT *, starts_at - offset_minutes * INTERVAL '1 minute' AS target_at,
          starts_at - due_minutes * INTERVAL '1 minute' AS due_until
        FROM events
        CROSS JOIN (
          SELECT * FROM (VALUES
            ('cfb', 2880, 2520), ('cfb', 1440, 1200),
            ('all', 360, 330), ('all', 90, 60), ('all', 15, 5), ('all', 2, 0),
            ('cfb', 5, 0)
          ) AS base(sport, lead, due)
          UNION ALL
          SELECT 'cfb', lead, lead - 60 FROM generate_series(420, 720, 60) AS lead
          UNION ALL
          SELECT 'cfb', lead, lead - 15 FROM generate_series(30, 345, 15) AS lead WHERE lead <> 90
        ) AS w(window_sport, offset_minutes, due_minutes)
        WHERE w.window_sport='all' OR w.window_sport=events.sport
      )
      SELECT EXISTS (
        SELECT 1 FROM windows w
        WHERE w.target_at <= NOW() AND w.due_until >= NOW()
          AND NOT EXISTS (
            SELECT 1 FROM game_odds_history h
            WHERE h.sport=w.sport AND h.matchup_id=w.matchup_id
              AND h.captured_at BETWEEN w.target_at AND w.due_until
          )
        UNION ALL
        SELECT 1 FROM events e
        WHERE e.starts_at <= NOW()
          AND NOT EXISTS (
            SELECT 1 FROM event_closing_lines c
            WHERE c.sport=e.sport AND c.matchup_id=e.matchup_id
          )
      ) AS has_work
    `);
    return Boolean(rows.rows[0]?.has_work);
  } catch (error) {
    // First deployment: the Python workflow owns schema migration. Dispatch
    // once to bootstrap instead of letting a missing new table deadlock rollout.
    console.warn("event-closing-lines cron: due-work query failed; dispatching bootstrap", error);
    return true;
  }
}

export async function GET(request: NextRequest) {
  const cronSecret = process.env.CRON_SECRET;
  if (!cronSecret) {
    console.error("event-closing-lines cron: CRON_SECRET is not configured");
    return NextResponse.json({ ok: false, error: "CRON_SECRET is not configured" }, { status: 500 });
  }
  if (request.headers.get("authorization") !== `Bearer ${cronSecret}`) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }
  const token = process.env.GITHUB_DISPATCH_TOKEN;
  if (!token) {
    console.error("event-closing-lines cron: GITHUB_DISPATCH_TOKEN is not configured");
    return NextResponse.json(
      { ok: false, error: "GITHUB_DISPATCH_TOKEN is not configured" },
      { status: 500 },
    );
  }

  if (!(await hasDueWork())) {
    return NextResponse.json({ ok: true, dispatched: false, reason: "no_due_event" });
  }

  try {
    const runsResponse = await fetch(
      `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/workflows/${WORKFLOW_FILE}/runs?per_page=10`,
      {
        headers: {
          Accept: "application/vnd.github+json",
          Authorization: `Bearer ${token}`,
          "X-GitHub-Api-Version": "2022-11-28",
        },
        cache: "no-store",
      },
    );
    if (runsResponse.ok) {
      const runs = (await runsResponse.json()) as { workflow_runs?: Array<{ status?: string }> };
      if ((runs.workflow_runs ?? []).some((run) => run.status === "queued" || run.status === "in_progress")) {
        return NextResponse.json({ ok: true, dispatched: false, reason: "workflow_already_active" });
      }
    }
    const response = await fetch(
      `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/workflows/${WORKFLOW_FILE}/dispatches`,
      {
        method: "POST",
        headers: {
          Accept: "application/vnd.github+json",
          Authorization: `Bearer ${token}`,
          "X-GitHub-Api-Version": "2022-11-28",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ ref: WORKFLOW_REF }),
      },
    );
    if (response.status === 204) {
      return NextResponse.json({ ok: true, dispatchedAt: new Date().toISOString() });
    }
    const body = await response.text();
    console.error(`event-closing-lines cron: GitHub dispatch failed (${response.status}): ${body}`);
    return NextResponse.json(
      { ok: false, error: `GitHub dispatch failed (${response.status})` },
      { status: 502 },
    );
  } catch (error) {
    console.error("event-closing-lines cron: dispatch threw", error);
    return NextResponse.json({ ok: false, error: "Dispatch failed" }, { status: 502 });
  }
}
