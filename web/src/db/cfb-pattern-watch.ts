import { sql } from "drizzle-orm";
import { db } from "./index";
import { queryRows } from "./query-result";
import type { BookGapRow, LineMoveRow, MoveSummaryRow } from "@/app/cfb/pattern-watch/book-disagreement";
import type {
  ComebackRow, DriveFieldPositionRow, FourthDownRow, FourthDownTrendRow, RedZoneRow,
} from "@/app/cfb/pattern-watch/drive-stories";
import type { KeyMarginRow, KeyNumberRow, SpreadLandingRow, TotalLandingRow } from "@/app/cfb/pattern-watch/key-numbers";
import type { ConferenceRow, TeamAtsRow } from "@/app/cfb/pattern-watch/team-ats";
import type { RecapHookRow, RecapTotalMissRow, RecapUpsetRow, RecapWeekRow } from "@/app/cfb/pattern-watch/weekly-recap";

/**
 * Tables behind /cfb/pattern-watch. Written by the Python job
 * (model/cfb_pattern_csv.py --store, in .github/workflows/cfb_pattern_watch.yml)
 * once per distinct content; the web app only reads the newest row and never
 * recomputes a grade, so the page and the CSV/PNG exports cannot disagree.
 */
export type PatternRow = {
  Pattern: string; Rule: string; Bet: string; Graded: number; Wins: number; Losses: number; Pushes: number;
  Record: string; Units: number; "ROI (%)": number | null; "Sample floor": number;
  "Progress to floor (%)": number; "Discovery record": string; "Discovery ROI": string; "Discovery dates": number;
};
export type WeeklyRow = {
  Week: number; Games: number; "Favorites won (%)": number | null; "Price implied (%)": number | null;
  "Difference (pts)": number | null; "Favorites covered (%)": number | null; "Overs (%)": number | null;
  "Decided by 7 or less (%)": number | null;
};
export type MarketSplitRow = {
  Split: string; Games: number; Wins: number; Losses: number; Record: string;
  "Favorites won (%)": number | null; "Price implied (%)": number | null; "Difference (pts)": number | null;
  Units: number; "ROI (%)": number | null; "Median price": string | null;
  "Underdog record"?: string; "Underdog units"?: number; "Underdog ROI (%)"?: number | null;
  "Underdog median price"?: string | null;
};
export type MarketBreakdownRow = MarketSplitRow & {
  Level: number; "Level name": string; Grid: boolean; Group: string | null; Column: string | null;
  "Fav ATS record"?: string; "Fav covered (%)"?: number | null; "Fav ATS units"?: number; "Fav ATS ROI (%)"?: number | null;
  "Dog ATS units"?: number; "Dog ATS ROI (%)"?: number | null; "Games with total"?: number; "Over-under record"?: string;
  "Overs (%)"?: number | null; "Over units"?: number; "Over ROI (%)"?: number | null; "Under units"?: number;
  "Under ROI (%)"?: number | null;
};
export type SeasonSummaryRow = {
  Season: string | number; "Games with lines": number; "Favorites won (%)": number | null; "Price implied (%)": number | null;
  "Difference (pts)": number | null; "Favorite ROI (%)": number | null; "Favorites covered (%)": number | null;
  "Overs (%)": number | null; "Games with total": number;
};
export type ResultRow = {
  Date: string; Game: string; Pattern: string; Pick: string; Price: string; Final: string; Result: string; Units: number;
};
export type CumulativeRow = { Date: string; "All patterns": number } & Record<string, number | string>;
export type FrozenRow = {
  Game: string; Pattern: string; Pick: string; "Frozen home spread or total": number | null;
  "Closing home spread or total": number | null; "Points vs close": number | null; "Result at close": string;
  "Result at frozen line": string; "Units at best retail": number | null;
};

export type PatternWatchDataset = {
  generatedAt: string;
  studyVersion: string;
  patterns: PatternRow[];
  weekly: WeeklyRow[];
  marketSplits: MarketSplitRow[];
  marketBreakdown: MarketBreakdownRow[];
  marketBreakdownPrior: (MarketBreakdownRow & { Seasons: string })[];
  seasonSummary: SeasonSummaryRow[];
  teamAts: TeamAtsRow[];
  conferenceReport: ConferenceRow[];
  recapWeeks: RecapWeekRow[];
  recapUpsets: RecapUpsetRow[];
  recapHooks: RecapHookRow[];
  recapTotalMisses: RecapTotalMissRow[];
  bookGaps: BookGapRow[];
  lineMoves: LineMoveRow[];
  moveSummary: MoveSummaryRow[];
  keyMargins: KeyMarginRow[];
  keyNumbers: KeyNumberRow[];
  spreadLanding: SpreadLandingRow[];
  totalLanding: TotalLandingRow[];
  driveFieldPosition: DriveFieldPositionRow[];
  redZone: RedZoneRow[];
  fourthDown: FourthDownRow[];
  fourthDownTrend: FourthDownTrendRow[];
  comebacks: ComebackRow[];
  results: ResultRow[];
  cumulative: CumulativeRow[];
  frozenVsClose: FrozenRow[];
};

type Raw = { payload: Record<string, unknown>; generated_at: string | Date };

export async function getLatestPatternWatchDataset(): Promise<PatternWatchDataset | null> {
  const rows = queryRows<Raw>(await db.execute(sql`
    SELECT payload, generated_at FROM cfb_pattern_datasets ORDER BY generated_at DESC, id DESC LIMIT 1
  `));
  const row = rows[0];
  if (!row) return null;
  const p = row.payload;
  return {
    generatedAt: new Date(row.generated_at).toISOString(),
    studyVersion: String(p.study_version ?? ""),
    patterns: (p.patterns ?? []) as PatternRow[],
    weekly: (p.weekly_favorites ?? []) as WeeklyRow[],
    marketSplits: (p.market_splits ?? []) as MarketSplitRow[],
    marketBreakdown: (p.market_breakdown ?? []) as MarketBreakdownRow[],
    marketBreakdownPrior: (p.market_breakdown_prior ?? []) as PatternWatchDataset["marketBreakdownPrior"],
    seasonSummary: (p.season_summary ?? []) as SeasonSummaryRow[],
    teamAts: (p.team_ats ?? []) as TeamAtsRow[],
    conferenceReport: (p.conference_report ?? []) as ConferenceRow[],
    recapWeeks: (p.recap_weeks ?? []) as RecapWeekRow[],
    recapUpsets: (p.recap_upsets ?? []) as RecapUpsetRow[],
    recapHooks: (p.recap_hooks ?? []) as RecapHookRow[],
    recapTotalMisses: (p.recap_total_misses ?? []) as RecapTotalMissRow[],
    bookGaps: (p.book_gaps ?? []) as BookGapRow[],
    lineMoves: (p.line_moves ?? []) as LineMoveRow[],
    moveSummary: (p.move_summary ?? []) as MoveSummaryRow[],
    keyMargins: (p.key_margins ?? []) as KeyMarginRow[],
    keyNumbers: (p.key_numbers ?? []) as KeyNumberRow[],
    spreadLanding: (p.spread_landing ?? []) as SpreadLandingRow[],
    totalLanding: (p.total_landing ?? []) as TotalLandingRow[],
    driveFieldPosition: (p.drive_field_position ?? []) as DriveFieldPositionRow[],
    redZone: (p.red_zone ?? []) as RedZoneRow[],
    fourthDown: (p.fourth_down ?? []) as FourthDownRow[],
    fourthDownTrend: (p.fourth_down_trend ?? []) as FourthDownTrendRow[],
    comebacks: (p.comebacks ?? []) as ComebackRow[],
    results: (p.results ?? []) as ResultRow[],
    cumulative: (p.cumulative_units ?? []) as CumulativeRow[],
    frozenVsClose: (p.frozen_vs_close ?? []) as FrozenRow[],
  };
}
