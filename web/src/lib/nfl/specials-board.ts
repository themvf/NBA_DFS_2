/**
 * Shaping and presentation rules for the NFL slate-specials board.
 *
 * Pure: no database, no React. The page renders what these functions return so
 * the honesty rules below are testable rather than aspirational.
 *
 * The board answers DK's specials questions in two shapes. A "who leads"
 * question gets OUR RANKING and the expected stat behind it, and deliberately
 * publishes no probability, because a mean cannot be turned into one. An "all
 * teams to score" question is a single yes/no event, so it gets a probability
 * and no ranking. Measured on 2023-2025 (walk-forward, means from prior weeks
 * only, 45 slates, ~840 candidates each), the player with the highest projected
 * stat actually led the week:
 *
 *     most receiving yards   11.1%    actual leader's median rank 15
 *     most passing yards     15.6%    actual leader's median rank 11
 *     any touchdown (proxy)   4.4%    actual leader's median rank 25
 *
 * Against a ~0.1% chance baseline that is roughly 90x, so the order carries
 * real signal -- and our number one is still an underdog to lead. Hence the two
 * rules this module enforces: publish DEEP, and never render a single pick.
 *
 * See docs/nfl-slate-specials-handoff.md §12.
 */

export type SpecialsStatus = "ok" | "blocked";

export type SpecialsRow = {
  family: string;
  rank: number | null;
  selectionKey: string;
  selectionLabel: string;
  statKey: string;
  expectedValue: number | null;
  isProxy: boolean;
  pLeads: number | null;
  context: Record<string, unknown>;
  status: SpecialsStatus;
  blockReason: string | null;
  marketAmerican: number | null;
};

export type SpecialsRun = {
  runId: string;
  modelVersion: string;
  method: string;
  generatedAt: string;
  projectionRunId: string | null;
  /** Data cutoff of the projection run the player topics were ranked from. */
  projectionAsOf: string | null;
  gitSha: string | null;
  games: Array<{ game: string; kickoff: string | null; total: number | null; spread: number | null }>;
  blockedReasons: Array<Record<string, unknown>>;
};

export type SpecialsCapture = {
  family: string;
  capturedAt: string;
  selections: number;
  overround: number | null;
};

export type SpecialsBoard = {
  season: number;
  week: number;
  scope: string;
  run: SpecialsRun | null;
  rows: SpecialsRow[];
  captures: SpecialsCapture[];
  /** Weeks with a board in THIS slate scope. */
  weeksInScope: number[];
  /** Weeks with a board in any scope, so the empty state can point at one. */
  weeksAnyScope: number[];
};

/**
 * Display metadata per family, in the same order as Python's FAMILIES tuple so
 * our board and DK's are diffable line by line.
 *
 * `topOneLedPct` is NULL where it has not been measured. Games and teams were
 * never put through the study in §12, and inventing a plausible number for them
 * would be worse than leaving the column blank.
 */
export type FamilyMeta = {
  family: string;
  label: string;
  question: string;
  unit: string;
  decimals: number;
  isProxy: boolean;
  proxyNote?: string;
  ascending: boolean;
  /** "ranked" answers which selection wins; "proposition" answers yes/no. */
  kind: "ranked" | "proposition";
  group: "Games" | "Teams" | "Players" | "All teams to score";
  topOneLedPct: number | null;
  leaderMedianRank: number | null;
};

export const FAMILY_META: readonly FamilyMeta[] = [
  {
    family: "highest_scoring_game", label: "Highest scoring game",
    question: "Which game produces the most combined points?",
    unit: "pts", decimals: 1, isProxy: false, kind: "ranked", ascending: false, group: "Games",
    topOneLedPct: null, leaderMedianRank: null,
  },
  {
    family: "lowest_scoring_game", label: "Lowest scoring game",
    question: "Which game produces the fewest combined points?",
    unit: "pts", decimals: 1, isProxy: false, kind: "ranked", ascending: true, group: "Games",
    topOneLedPct: null, leaderMedianRank: null,
  },
  {
    family: "highest_scoring_team", label: "Highest scoring team",
    question: "Which team scores the most points?",
    unit: "pts", decimals: 1, isProxy: false, kind: "ranked", ascending: false, group: "Teams",
    topOneLedPct: null, leaderMedianRank: null,
  },
  {
    family: "lowest_scoring_team", label: "Lowest scoring team",
    question: "Which team scores the fewest points?",
    unit: "pts", decimals: 1, isProxy: false, kind: "ranked", ascending: true, group: "Teams",
    topOneLedPct: null, leaderMedianRank: null,
  },
  {
    family: "most_passing_yards", label: "Most passing yards",
    question: "Which quarterback throws for the most yards?",
    unit: "yds", decimals: 0, isProxy: false, kind: "ranked", ascending: false, group: "Players",
    topOneLedPct: 15.6, leaderMedianRank: 11,
  },
  {
    family: "most_receiving_yards", label: "Most receiving yards",
    question: "Which player gains the most receiving yards?",
    unit: "yds", decimals: 0, isProxy: false, kind: "ranked", ascending: false, group: "Players",
    topOneLedPct: 11.1, leaderMedianRank: 15,
  },
  {
    family: "most_rushing_yards", label: "Most rushing yards",
    question: "Which player gains the most rushing yards?",
    unit: "yds", decimals: 0, isProxy: false, kind: "ranked", ascending: false, group: "Players",
    topOneLedPct: null, leaderMedianRank: null,
  },
  {
    family: "first_td_scorer", label: "First touchdown scorer",
    question: "Who scores the first touchdown of the slate?",
    unit: "TD", decimals: 2, isProxy: true,
    proxyNote:
      "Ranked by expected touchdowns, which is not P(scores first) — that needs drive order and clock.",
    kind: "ranked", ascending: false, group: "Players", topOneLedPct: 4.4, leaderMedianRank: 25,
  },
  {
    family: "first_qb_td_pass", label: "First QB touchdown pass",
    question: "Which quarterback throws the first touchdown pass?",
    unit: "TD", decimals: 2, isProxy: true,
    proxyNote: "Ranked by expected touchdown passes, not by who is first.",
    kind: "ranked", ascending: false, group: "Players", topOneLedPct: null, leaderMedianRank: null,
  },
  {
    family: "first_qb_int", label: "First QB interception",
    question: "Which quarterback throws the first interception?",
    unit: "INT", decimals: 2, isProxy: true,
    proxyNote: "Ranked by expected interceptions, not by who is first.",
    kind: "ranked", ascending: false, group: "Players", topOneLedPct: null, leaderMedianRank: null,
  },
  ...( [
    ["all_teams_td", "All teams to score 1+ TD", "Does every team in the window score a touchdown?"],
    ["all_teams_two_td", "All teams to score 2+ TDs", "Does every team score at least twice?"],
    ["all_teams_fg", "All teams to kick 1+ FG", "Does every team make a field goal?"],
    ["all_teams_td_and_fg", "All teams: 1+ TD and 1+ FG", "Does every team manage both?"],
    ["all_teams_passing_td", "All teams to throw 1+ passing TD", "Does every team throw a touchdown?"],
    ["all_teams_rushing_td", "All teams to run in 1+ rushing TD", "Does every team run one in?"],
    ["all_teams_score", "All teams to score", "Does every team score at all?"],
  ] as const).map(([family, label, question]) => ({
    family, label, question, unit: "%", decimals: 1, isProxy: false,
    kind: "proposition" as const, ascending: false, group: "All teams to score" as const,
    topOneLedPct: null, leaderMedianRank: null,
  })),
] as const;

export const FAMILY_ORDER: readonly string[] = FAMILY_META.map((meta) => meta.family);

export function familyMeta(family: string): FamilyMeta | null {
  return FAMILY_META.find((meta) => meta.family === family) ?? null;
}

export type FamilyPanel = {
  meta: FamilyMeta;
  ranked: SpecialsRow[];
  blocked: SpecialsRow[];
  capture: SpecialsCapture | null;
  /** Largest published expected value, for scaling the magnitude bars. */
  barMax: number;
  /** Smallest published expected value. */
  barMin: number;
};

/**
 * Group a board's rows into one panel per family, in FAMILY_ORDER.
 *
 * Families with no rows at all are still returned, so a topic that silently
 * stopped producing is visible as an empty panel rather than vanishing from the
 * page. A missing row is the bug you want to see.
 */
export function buildPanels(board: SpecialsBoard): FamilyPanel[] {
  const captureByFamily = new Map(board.captures.map((c) => [c.family, c]));
  return FAMILY_META.map((meta) => {
    const mine = board.rows.filter((row) => row.family === meta.family);
    const ranked = mine
      .filter((row) => row.status === "ok")
      .sort((a, b) => (a.rank ?? Number.MAX_SAFE_INTEGER) - (b.rank ?? Number.MAX_SAFE_INTEGER));
    const values = ranked.map((row) => row.expectedValue ?? 0);
    return {
      meta,
      ranked,
      blocked: mine.filter((row) => row.status === "blocked"),
      capture: captureByFamily.get(meta.family) ?? null,
      barMax: values.length ? Math.max(...values) : 0,
      barMin: values.length ? Math.min(...values) : 0,
    };
  });
}

/**
 * Bar width as a percentage.
 *
 * Scaled against the family's own range rather than zero, because these ranges
 * are narrow by nature -- an 18-to-26 point team board anchored at zero makes
 * every bar look identical and hides the only thing the bar is for. A floor of
 * 6% keeps the last row visible.
 *
 * Read the bars as "how far ahead is the leader", never as probability.
 */
export function barWidthPct(value: number | null, max: number, min: number): number {
  if (value === null || !Number.isFinite(value) || max <= 0) return 0;
  if (max === min) return 100;
  const span = max - min;
  const floor = 6;
  return Math.max(floor, Math.min(100, floor + ((value - min) / span) * (100 - floor)));
}

export function formatExpected(value: number | null, meta: FamilyMeta): string {
  if (value === null || !Number.isFinite(value)) return "—";
  return value.toFixed(meta.decimals);
}

/** American odds as a signed string. Display only; never used to rank. */
export function formatAmerican(american: number | null): string {
  if (american === null || !Number.isFinite(american)) return "—";
  return american > 0 ? `+${american}` : `${american}`;
}

/**
 * Raw implied probability of an American price, vig included.
 *
 * Shown only alongside the family's overround, because on a board summing to
 * 250% a raw implied probability is not a probability -- it is a price with
 * margin in it. This is why the page never prints it on its own.
 */
export function impliedProb(american: number | null): number | null {
  if (american === null || !Number.isFinite(american)) return null;
  return american < 0 ? -american / (-american + 100) : 100 / (american + 100);
}

/**
 * The one-line honesty label for a family panel.
 *
 * Deliberately not a confidence score: it states the measured hit rate where we
 * have one and says the ranking is unmeasured where we do not.
 */
export function calibrationNote(meta: FamilyMeta): string {
  if (meta.kind === "proposition") {
    return (
      "A probability, not a ranking: a fitted per-team rate multiplied across every team in " +
      "the window. Checked against 89 real Sunday 1pm windows, independence runs mildly " +
      "optimistic (32.9% predicted vs 31.5% observed for touchdowns), and no correction is " +
      "fitted to a sample that small."
    );
  }
  if (meta.topOneLedPct === null) {
    return "Hit rate for this topic has not been measured — read the order as an ordering, not a forecast.";
  }
  return (
    `Historically our top-ranked name led the week ${meta.topOneLedPct.toFixed(1)}% of the time ` +
    `and the actual leader sat around ${meta.leaderMedianRank}th on this list. ` +
    `Informative ordering, not a pick.`
  );
}

export const SCOPE_LABELS: Record<string, string> = {
  sunday_all: "All Sunday games",
  sunday_1pm: "1pm ET only",
  sunday_late: "4.05 & 4.25pm ET",
  // DK's combined market; excludes Sunday Night Football, unlike sunday_all.
  sunday_main: "1pm, 4.05 & 4.25pm ET",
};

export const SCOPES = ["sunday_1pm", "sunday_late", "sunday_main", "sunday_all"] as const;

export const BOARD_IS_VALIDATED = false;

/**
 * Hours before a board's projections count as stale.
 *
 * refresh_nfl_dfs_projections runs twice a day, so a board built on the
 * Thursday or Sunday cron should be ranking projections a few hours old at
 * most. 36 hours means roughly three refreshes have been missed -- past the
 * point where a scheduled run is quietly ranking last week's numbers.
 *
 * This exists because the upstream workflow currently reports FAILURE on every
 * run for a shadow-research step that is unrelated to the projection build.
 * A permanently red workflow cannot signal a real outage, so the board checks
 * its own input rather than trusting that someone is watching the red X.
 */
export const PROJECTION_STALE_AFTER_HOURS = 36;

export function projectionAgeHours(run: SpecialsRun | null): number | null {
  if (!run?.projectionAsOf) return null;
  const asOf = Date.parse(run.projectionAsOf.replace(" ", "T"));
  const generated = Date.parse(run.generatedAt.replace(" ", "T"));
  if (!Number.isFinite(asOf) || !Number.isFinite(generated)) return null;
  return (generated - asOf) / 3_600_000;
}

/**
 * A warning when the board ranked projections that were already old, or when
 * it could not tell. Null when the input was fresh.
 */
export function stalenessWarning(run: SpecialsRun | null): string | null {
  if (!run) return null;
  if (!run.projectionRunId) {
    return "No projection run was attached, so the six player topics are empty.";
  }
  const age = projectionAgeHours(run);
  if (age === null) return null;
  if (age < PROJECTION_STALE_AFTER_HOURS) return null;
  const days = Math.floor(age / 24);
  const label = days >= 1 ? `${days} day${days === 1 ? "" : "s"}` : `${Math.round(age)} hours`;
  return (
    `Player topics were ranked from projections ${label} old. ` +
    `refresh_nfl_dfs_projections runs twice daily, so this board is not ranking current numbers.`
  );
}