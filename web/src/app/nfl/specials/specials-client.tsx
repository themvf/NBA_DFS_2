"use client";

/**
 * NFL slate specials: what we project for each of DK's nine questions.
 *
 * Two panel shapes, because DK asks two kinds of question.
 *
 * A "who leads" topic renders FamilyTable. It is organised around one measured
 * fact: our top-ranked name leads the slate only 11-16% of the time, and the
 * player who actually leads sits around 11th to 25th on our list
 * (docs/nfl-slate-specials-handoff.md §12). So it is deliberately NOT a pick
 * page -- every such topic renders a deep ranked list, the measured hit rate
 * sits in the panel header where it cannot be missed, and no probability is
 * shown, because the board ranks by expected stat and a mean cannot be restated
 * as P(leads).
 *
 * An "all teams to score" topic renders PropositionPanel. It is one yes/no
 * event, not a race, so it has no ranking and DOES carry a probability -- a
 * per-team rate fitted on implied total and multiplied across the window. The
 * independence that multiplication assumes is mildly optimistic (measured), so
 * the panel discloses it instead of correcting for it on n=89.
 *
 * DK's price appears beside our ranking when somebody managed to paste the
 * board, and is simply absent when nobody did. That is the normal case: these
 * markets are not on The Odds API. The ranking is the product; the price is
 * context.
 */

import { useMemo, useState } from "react";
import Link from "next/link";
import { AlertTriangle, FlaskConical, Info, Ruler, Terminal } from "lucide-react";
import {
  BOARD_IS_VALIDATED,
  FAMILY_META,
  barWidthPct,
  buildPanels,
  calibrationNote,
  formatAmerican,
  formatExpected,
  impliedProb,
  projectionAgeHours,
  stalenessWarning,
  type FamilyPanel,
  SCOPE_LABELS,
  SCOPES,
  type SpecialsBoard,
} from "@/lib/nfl/specials-board";

const GROUPS = ["Games", "Teams", "Players", "All teams to score"] as const;

function scopeLabel(scope: string): string {
  return SCOPE_LABELS[scope] ?? scope;
}

/** Team/opponent/position context, whichever the row carries. */
function contextLine(context: Record<string, unknown>): string {
  const parts: string[] = [];
  if (typeof context.position === "string") parts.push(context.position);
  if (typeof context.team === "string") parts.push(context.team);
  if (typeof context.opponent === "string") {
    parts.push(context.is_home === true ? `vs ${context.opponent}` : `@ ${context.opponent}`);
  }
  // A game row's label is already "AWAY @ HOME", so repeating the matchup here
  // reads as a rendering bug. Show the line instead, which is the thing that
  // actually explains the ranking.
  if (!parts.length && typeof context.spread === "number") {
    const spread = context.spread;
    const side = spread > 0 ? context.home : context.away;
    if (typeof side === "string" && spread !== 0) {
      parts.push(`${side} by ${Math.abs(spread)}`);
    }
  }
  if (!parts.length && context.spread_available === false) parts.push("no spread posted");
  return parts.join(" · ");
}

export default function SpecialsClient({
  board,
  initialFamily,
  loadedAt,
}: {
  board: SpecialsBoard;
  initialFamily: string | null;
  loadedAt: string;
}) {
  const panels = useMemo(() => buildPanels(board), [board]);
  const firstPopulated = panels.find((panel) => panel.ranked.length > 0)?.meta.family;
  const [activeFamily, setActiveFamily] = useState<string>(
    initialFamily && FAMILY_META.some((m) => m.family === initialFamily)
      ? initialFamily
      : firstPopulated ?? FAMILY_META[0].family,
  );
  const active = panels.find((panel) => panel.meta.family === activeFamily) ?? panels[0];
  // Checked here rather than trusted upstream: refresh_nfl_dfs_projections
  // reports FAILURE on every run for an unrelated shadow step, so its red X
  // cannot signal a real outage of the projection build this board reads.
  const stale = stalenessWarning(board.run);

  const href = (params: Record<string, string | number>) => {
    const search = new URLSearchParams({
      season: String(board.season),
      week: String(board.week),
      scope: board.scope,
      ...Object.fromEntries(Object.entries(params).map(([k, v]) => [k, String(v)])),
    });
    return `/nfl/specials?${search.toString()}`;
  };

  return (
    <div className="mx-auto max-w-[1500px] space-y-4 p-4">
      <header className="space-y-2">
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-2xl font-bold tracking-tight">Slate Specials</h1>
          {board.run && (
            <span className="rounded bg-muted px-2 py-0.5 font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
              {board.run.modelVersion}
            </span>
          )}
          {!BOARD_IS_VALIDATED && (
            <span className="inline-flex items-center gap-1 rounded border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 font-mono text-[11px] uppercase tracking-wider text-amber-600 dark:text-amber-400">
              <FlaskConical className="h-3 w-3" /> projection only
            </span>
          )}
        </div>
        <p className="max-w-4xl text-sm text-muted-foreground">
          What we project for each of DraftKings&apos; slate questions, kept week to week. The
          &ldquo;who leads&rdquo; topics are{" "}
          <strong className="font-semibold text-foreground">orderings, not picks</strong> and carry
          no probability: measured on 2023-25 the top-ranked name led the slate 11-16% of the time
          and the eventual leader usually sat 11th to 25th on the list, and a mean cannot be
          restated as a chance of winning. The &ldquo;all teams to score&rdquo; topics are the
          opposite shape — a single yes/no with one fitted probability, and no ranking.
        </p>
      </header>

      {/* ── week / scope controls ─────────────────────────────────────── */}
      <div className="flex flex-wrap items-center gap-3 rounded border bg-card p-3 text-sm">
        <span className="font-mono text-xs uppercase tracking-wider text-muted-foreground">Week</span>
        {board.weeksInScope.length === 0 && (
          <span className="text-muted-foreground">no board for this slate yet</span>
        )}
        <div className="flex flex-wrap gap-1">
          {board.weeksInScope.map((week) => (
            <Link
              key={week}
              href={href({ week })}
              className={`rounded border px-2 py-1 font-mono text-xs ${
                week === board.week
                  ? "border-foreground bg-foreground text-background"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              {week}
            </Link>
          ))}
        </div>
        <span className="ml-2 font-mono text-xs uppercase tracking-wider text-muted-foreground">
          Slate
        </span>
        <div className="flex gap-1">
          {SCOPES.map((scope) => (
            <Link
              key={scope}
              href={`/nfl/specials?season=${board.season}&week=${board.week}&scope=${scope}`}
              className={`rounded border px-2 py-1 text-xs ${
                scope === board.scope
                  ? "border-foreground bg-foreground text-background"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              {scopeLabel(scope)}
            </Link>
          ))}
        </div>
        <span className="ml-auto font-mono text-[11px] text-muted-foreground">
          {board.season} · loaded {loadedAt.slice(11, 16)}Z
        </span>
      </div>

      {!board.run ? (
        <EmptyState board={board} />
      ) : (
        <>
          {stale && (
            <div className="flex items-start gap-2 rounded border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-700 dark:text-amber-400">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <span>{stale}</span>
            </div>
          )}

          {/* ── family tabs ───────────────────────────────────────────── */}
          <nav className="space-y-2 rounded border bg-card p-3">
            {GROUPS.map((group) => (
              <div key={group} className="flex flex-wrap items-center gap-2">
                <span className="w-16 shrink-0 font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
                  {group}
                </span>
                {panels
                  .filter((panel) => panel.meta.group === group)
                  .map((panel) => {
                    const on = panel.meta.family === active.meta.family;
                    return (
                      <button
                        key={panel.meta.family}
                        type="button"
                        onClick={() => setActiveFamily(panel.meta.family)}
                        className={`rounded border px-2.5 py-1 text-xs transition-colors ${
                          on
                            ? "border-foreground bg-foreground font-semibold text-background"
                            : "text-muted-foreground hover:text-foreground"
                        }`}
                      >
                        {panel.meta.label}
                        <span className={`ml-1.5 font-mono ${on ? "opacity-70" : "opacity-50"}`}>
                          {panel.ranked.length}
                        </span>
                      </button>
                    );
                  })}
              </div>
            ))}
          </nav>

          {active.meta.kind === "proposition"
            ? <PropositionPanel panel={active} />
            : <FamilyTable panel={active} />}

          {/* ── provenance ────────────────────────────────────────────── */}
          <details className="rounded border bg-card">
            <summary className="cursor-pointer p-3 text-sm font-semibold">
              Run provenance · {board.run.games.length} games in scope
            </summary>
            <div className="space-y-3 border-t p-3 text-xs">
              <dl className="grid gap-x-6 gap-y-1 sm:grid-cols-2">
                {[
                  ["Run", board.run.runId],
                  ["Method", board.run.method],
                  ["Model version", board.run.modelVersion],
                  ["Generated", board.run.generatedAt],
                  ["Projection run", board.run.projectionRunId ?? "none — player topics are empty"],
                  [
                    "Projections as of",
                    board.run.projectionAsOf
                      ? `${board.run.projectionAsOf.slice(0, 16).replace("T", " ")}` +
                        (projectionAgeHours(board.run) !== null
                          ? ` (${Math.round(projectionAgeHours(board.run)!)}h before this board)`
                          : "")
                      : "unknown",
                  ],
                  ["Commit", board.run.gitSha?.slice(0, 12) ?? "unknown"],
                ].map(([label, value]) => (
                  <div key={label} className="flex gap-2">
                    <dt className="w-28 shrink-0 text-muted-foreground">{label}</dt>
                    <dd className="font-mono break-all">{value}</dd>
                  </div>
                ))}
              </dl>
              {board.run.games.length > 0 && (
                <table className="w-full text-left">
                  <thead className="text-muted-foreground">
                    <tr>
                      <th className="py-1 font-medium">Game</th>
                      <th className="py-1 font-medium">Kickoff</th>
                      <th className="py-1 text-right font-medium">Total</th>
                      <th className="py-1 text-right font-medium">Spread</th>
                    </tr>
                  </thead>
                  <tbody className="font-mono">
                    {board.run.games.map((game) => (
                      <tr key={game.game} className="border-t">
                        <td className="py-1">{game.game}</td>
                        <td className="py-1">{game.kickoff?.slice(0, 16).replace("T", " ") ?? "—"}</td>
                        <td className="py-1 text-right">{game.total ?? "—"}</td>
                        <td className="py-1 text-right">{game.spread ?? "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              {board.run.blockedReasons.length > 0 && (
                <div>
                  <p className="mb-1 font-semibold">
                    Excluded or blocked ({board.run.blockedReasons.length})
                  </p>
                  <table className="w-full text-left font-mono">
                    <thead className="text-muted-foreground">
                      <tr>
                        <th className="py-1 font-medium">Topic</th>
                        <th className="py-1 font-medium">Selection</th>
                        <th className="py-1 font-medium">Reason</th>
                      </tr>
                    </thead>
                    <tbody>
                      {board.run.blockedReasons.slice(0, 60).map((reason, index) => (
                        <tr key={index} className="border-t">
                          <td className="py-1 pr-3">{String(reason.family ?? "—")}</td>
                          <td className="py-1 pr-3">
                            {String(reason.selection ?? reason.player ?? reason.game ?? "—")}
                          </td>
                          <td className="py-1 text-muted-foreground">{String(reason.reason ?? "—")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </details>

          <HowToRead />
          <PasteProcedure board={board} />
        </>
      )}
    </div>
  );
}

/* ── one family ───────────────────────────────────────────────────────── */

function FamilyTable({ panel }: { panel: FamilyPanel }) {
  const { meta, ranked, blocked, capture, barMax, barMin } = panel;
  const showMarket = ranked.some((row) => row.marketAmerican !== null);

  return (
    <section className="rounded border bg-card">
      <header className="space-y-2 border-b p-3">
        <div className="flex flex-wrap items-baseline gap-2">
          <h2 className="text-lg font-semibold">{meta.label}</h2>
          <span className="text-sm text-muted-foreground">{meta.question}</span>
          {meta.isProxy && (
            <span className="inline-flex items-center gap-1 rounded border border-amber-500/40 bg-amber-500/10 px-1.5 py-0.5 font-mono text-[10px] uppercase tracking-wider text-amber-600 dark:text-amber-400">
              <AlertTriangle className="h-3 w-3" /> proxy
            </span>
          )}
        </div>
        {meta.proxyNote && <p className="text-xs text-amber-600 dark:text-amber-400">{meta.proxyNote}</p>}
        <p className="flex items-start gap-1.5 text-xs text-muted-foreground">
          <Ruler className="mt-0.5 h-3 w-3 shrink-0" />
          <span>{calibrationNote(meta)}</span>
        </p>
        {capture && (
          <p className="text-xs text-muted-foreground">
            DK board captured {capture.capturedAt.slice(0, 16).replace("T", " ")} ·{" "}
            {capture.selections} selections ·{" "}
            <strong className="font-mono font-semibold text-foreground">
              overround {capture.overround === null ? "—" : `${(capture.overround * 100).toFixed(0)}%`}
            </strong>{" "}
            — at that margin a raw DK implied percentage is a price, not a probability.
          </p>
        )}
      </header>

      {ranked.length === 0 ? (
        <p className="p-6 text-center text-sm text-muted-foreground">
          Nothing ranked for this topic. Check the blocked list and run provenance below.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs uppercase tracking-wider text-muted-foreground">
              <tr className="border-b">
                <th className="w-12 p-2 text-right font-medium">#</th>
                <th className="p-2 font-medium">Selection</th>
                <th className="p-2 font-medium">Context</th>
                <th className="w-24 p-2 text-right font-medium">
                  Expected <span className="normal-case">{meta.unit}</span>
                </th>
                <th className="w-40 p-2 font-medium">Relative</th>
                {showMarket && <th className="w-20 p-2 text-right font-medium">DK</th>}
              </tr>
            </thead>
            <tbody>
              {ranked.map((row) => {
                const width = barWidthPct(row.expectedValue, barMax, barMin);
                const implied = impliedProb(row.marketAmerican);
                return (
                  <tr key={row.selectionKey} className="border-b last:border-0 hover:bg-muted/40">
                    <td className="p-2 text-right font-mono text-xs text-muted-foreground">{row.rank}</td>
                    <td className="p-2 font-medium">{row.selectionLabel}</td>
                    <td className="p-2 text-xs text-muted-foreground">{contextLine(row.context)}</td>
                    <td className="p-2 text-right font-mono tabular-nums">
                      {formatExpected(row.expectedValue, meta)}
                    </td>
                    <td className="p-2">
                      {/* Redundant encoding of the number beside it, so it is
                          hidden from assistive tech rather than announced twice.
                          Scaled within the family's own range: read it as "how
                          far ahead is the leader", never as a probability. */}
                      <div
                        className="h-2 w-full overflow-hidden rounded bg-muted"
                        aria-hidden="true"
                        title={`${formatExpected(row.expectedValue, meta)} ${meta.unit}`}
                      >
                        <div
                          className="h-full rounded bg-sky-500/70 dark:bg-sky-400/70"
                          style={{ width: `${width}%` }}
                        />
                      </div>
                    </td>
                    {showMarket && (
                      <td className="p-2 text-right font-mono text-xs">
                        {formatAmerican(row.marketAmerican)}
                        {implied !== null && (
                          <span className="ml-1 text-muted-foreground">
                            {(implied * 100).toFixed(1)}%
                          </span>
                        )}
                      </td>
                    )}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {blocked.length > 0 && (
        <div className="border-t p-3">
          <p className="mb-1 text-xs font-semibold text-muted-foreground">
            Not ranked ({blocked.length}) — carried with a reason rather than dropped
          </p>
          <ul className="space-y-0.5 font-mono text-xs text-muted-foreground">
            {blocked.map((row) => (
              <li key={row.selectionKey}>
                {row.selectionLabel} — {row.blockReason}
              </li>
            ))}
          </ul>
        </div>
      )}
    </section>
  );
}

/* ── a yes/no proposition ─────────────────────────────────────────────── */

function PropositionPanel({ panel }: { panel: FamilyPanel }) {
  const { meta, ranked, blocked, capture } = panel;
  const row = ranked[0];
  const teams = (row?.context?.per_team as Array<{ team: string; implied: number; p: number }>) ?? [];
  const market = row?.marketAmerican ?? null;
  const dkImplied = impliedProb(market);

  return (
    <section className="rounded border bg-card">
      <header className="space-y-2 border-b p-3">
        <div className="flex flex-wrap items-baseline gap-2">
          <h2 className="text-lg font-semibold">{meta.label}</h2>
          <span className="text-sm text-muted-foreground">{meta.question}</span>
        </div>
        <p className="flex items-start gap-1.5 text-xs text-muted-foreground">
          <Ruler className="mt-0.5 h-3 w-3 shrink-0" />
          <span>{calibrationNote(meta)}</span>
        </p>
      </header>

      {!row || row.status !== "ok" || row.expectedValue === null ? (
        <p className="p-6 text-center text-sm text-muted-foreground">
          {blocked[0]?.blockReason
            ? `Cannot be answered: ${blocked[0].blockReason}`
            : "Nothing to show for this slate."}
        </p>
      ) : (
        <div className="space-y-4 p-4">
          <div className="flex flex-wrap items-end gap-8">
            <div>
              <div className="font-mono text-4xl font-semibold tabular-nums">
                {((row.expectedValue ?? 0) * 100).toFixed(1)}%
              </div>
              <div className="text-xs uppercase tracking-wider text-muted-foreground">
                our probability &middot; {String(row.context.teams ?? teams.length)} teams
              </div>
            </div>
            {market !== null && (
              <div>
                <div className="font-mono text-4xl font-semibold tabular-nums text-muted-foreground">
                  {dkImplied === null ? "—" : `${(dkImplied * 100).toFixed(1)}%`}
                </div>
                <div className="text-xs uppercase tracking-wider text-muted-foreground">
                  DK {formatAmerican(market)} &middot; before removing margin
                </div>
              </div>
            )}
          </div>

          {row.context.weakest_team ? (
            <p className="text-sm">
              It rides on{" "}
              <strong className="font-semibold">{String(row.context.weakest_team)}</strong> at{" "}
              <span className="font-mono">
                {(Number(row.context.weakest_p) * 100).toFixed(0)}%
              </span>{" "}
              — a conjunction is only as strong as its weakest member.
            </p>
          ) : null}

          <div>
            <p className="mb-1 text-xs uppercase tracking-wider text-muted-foreground">
              Per team, weakest first
            </p>
            <div className="flex flex-wrap gap-1.5">
              {teams.map((t) => (
                <span
                  key={t.team}
                  className="rounded border px-1.5 py-0.5 font-mono text-[11px]"
                  title={`implied total ${t.implied}`}
                >
                  {t.team} <span className="text-muted-foreground">{(t.p * 100).toFixed(0)}%</span>
                </span>
              ))}
            </div>
          </div>
        </div>
      )}
    </section>
  );
}

/* ── explainers ───────────────────────────────────────────────────────── */

function HowToRead() {
  return (
    <details className="rounded border bg-card">
      <summary className="cursor-pointer p-3 text-sm font-semibold">
        How to read this — why one half of the board has probabilities and the other does not
      </summary>
      <div className="space-y-3 border-t p-3 text-sm text-muted-foreground">
        <p>
          The board answers two different kinds of question, and they are not interchangeable.
        </p>
        <p>
          A <strong className="font-semibold text-foreground">&ldquo;who leads&rdquo;</strong> topic
          is ranked by an expected stat taken from the weekly NFL DFS projections (player yards and
          touchdowns) or from the schedule&apos;s total and spread (team and game points). The
          ordering is genuinely informative — but it is not a forecast of the winner, and no
          probability is shown, because the rank comes from a mean.
        </p>
        <p>
          An <strong className="font-semibold text-foreground">&ldquo;all teams to score&rdquo;</strong>{" "}
          topic has no ranking at all: it is one yes/no event, so it gets one probability, fitted
          per team from the implied total and multiplied across the window. The independence that
          multiplication assumes is not exactly true — checked against 89 real Sunday 1pm windows it
          runs mildly optimistic — so each panel says so rather than quietly fitting a correction to
          a sample that small.
        </p>
        <table className="w-full text-left text-xs">
          <thead className="uppercase tracking-wider">
            <tr className="border-b">
              <th className="py-1 font-medium">Topic</th>
              <th className="py-1 text-right font-medium">Our #1 led</th>
              <th className="py-1 text-right font-medium">Leader&apos;s median rank</th>
              <th className="py-1 text-right font-medium">Top-20 captures</th>
            </tr>
          </thead>
          <tbody className="font-mono">
            {[
              ["Most receiving yards", "11.1%", "15th", "60.0%"],
              ["Most passing yards", "15.6%", "11th", "77.8%"],
              ["Any touchdown (first-TD proxy)", "4.4%", "25th", "46.7%"],
            ].map((cells) => (
              <tr key={cells[0]} className="border-b last:border-0">
                <td className="py-1 font-sans">{cells[0]}</td>
                <td className="py-1 text-right">{cells[1]}</td>
                <td className="py-1 text-right">{cells[2]}</td>
                <td className="py-1 text-right">{cells[3]}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p>
          Measured over the 2023-25 regular seasons, walk-forward, with each week&apos;s ranking
          built only from prior weeks — 45 slates, about 840 candidates each. Picking one candidate
          at random leads roughly 0.1% of the time, so 11% is about{" "}
          <strong className="font-semibold text-foreground">90x chance</strong>: the order carries
          real signal. It is simply not decisive, which is why the lists run 32-60 deep and why no
          single name is presented as a pick.
        </p>
        <p>
          Ranking by ceiling instead of by the mean was tested — prior maximum, prior 90th
          percentile, and P(clears a slate-winning threshold). None beat the mean, so the mean
          stays. At 45 slates those gaps sit inside noise, so read that as{" "}
          <em>no evidence a variance proxy is better</em>, not as proof variance does not matter.
        </p>
        <p className="flex items-start gap-1.5">
          <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>
            A DK price appears only when somebody pasted the board — these markets are not on The
            Odds API. Where a price is shown, the panel header also shows the board&apos;s overround,
            because on a board summing far above 100% a raw implied percentage is a price with
            margin in it rather than a probability.
          </span>
        </p>
      </div>
    </details>
  );
}

function PasteProcedure({ board }: { board: SpecialsBoard }) {
  const command =
    `python -m ingest.nfl_specials_market --season ${board.season} --week ${board.week} \\\n` +
    `    --family first_td_scorer --scope ${board.scope} --file paste.txt`;
  return (
    <details className="rounded border bg-card">
      <summary className="cursor-pointer p-3 text-sm font-semibold">
        <Terminal className="mr-1.5 inline h-3.5 w-3.5" />
        Capturing DK&apos;s board (optional)
      </summary>
      <div className="space-y-2 border-t p-3 text-sm text-muted-foreground">
        <p>
          Copy a DK specials market into a text file and run this once per family. Add{" "}
          <code className="rounded bg-muted px-1 font-mono text-xs">--dry-run</code> to see the parse
          and the overround without writing anything.
        </p>
        <pre className="overflow-x-auto rounded bg-muted p-2 font-mono text-xs text-foreground">
          {command}
        </pre>
        <p>
          The tool refuses a paste it may have misread: a price between -100 and +100, a duplicated
          selection, or a board whose implied probabilities sum below 100% (which means the paste is
          truncated, and would otherwise record a flattering negative overround).
        </p>
      </div>
    </details>
  );
}

function EmptyState({ board }: { board: SpecialsBoard }) {
  const otherScope = board.scope === "sunday_all" ? "sunday_1pm" : "sunday_all";
  const existsElsewhere = board.week > 0 && board.weeksAnyScope.includes(board.week);
  const nothingAtAll = board.weeksAnyScope.length === 0;
  return (
    <div className="space-y-3 rounded border border-dashed bg-card p-6 text-sm">
      <p className="font-semibold">
        {nothingAtAll
          ? `No boards have been generated for ${board.season} yet.`
          : `No board has been generated for ${board.season} week ${board.week} (${scopeLabel(board.scope)}).`}
      </p>
      {existsElsewhere && (
        <p>
          Week {board.week} does have a board for the other slate {"\u2014"}{" "}
          <Link
            href={`/nfl/specials?season=${board.season}&week=${board.week}&scope=${otherScope}`}
            className="underline underline-offset-2"
          >
            view {scopeLabel(otherScope).toLowerCase()}
          </Link>
          .
        </p>
      )}
      <p className="text-muted-foreground">
        The board is built by a Python job from the week&apos;s schedule and the latest NFL DFS
        projection run. Nothing is rendered above because there is no run to render — the page does
        not invent a placeholder.
      </p>
      <pre className="overflow-x-auto rounded bg-muted p-2 font-mono text-xs text-foreground">
        {`python -m model.nfl_specials_board --season ${board.season}${
          board.week > 0 ? ` --week ${board.week}` : ""
        } --scope ${board.scope}`}
      </pre>
      <p className="text-xs text-muted-foreground">
        Add <code className="rounded bg-muted px-1 font-mono">--dry-run</code> to inspect what it
        would publish first.
      </p>
    </div>
  );
}
