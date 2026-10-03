import { sql } from "drizzle-orm";
import { db } from ".";

export type FrozenMarketEvaluation = {
  version: string;
  market: "spread" | "total" | "moneyline";
  eligibleGames: number;
  modelError: number | null;
  marketError: number | null;
  excluded: number;
};

type RawRow = {
  game_id: number; version: string; home_score: number; away_score: number;
  evidence_json: {
    forecast?: { home_points?: number; away_points?: number; home_win_probability?: number };
    markets?: Record<string, { eligible?: boolean; value?: number | null }>;
  };
};

/** Latest prospectively frozen forecast for each completed game and version.
 * The market reference is the immutable comparison row made at publication. */
export async function getCfbFrozenMarketEvaluation(): Promise<FrozenMarketEvaluation[]> {
  const result = await db.execute(sql`
    SELECT DISTINCT ON (r.version,f.game_id)
      f.game_id,r.version,m.home_score,m.away_score,
      c.evidence_json
    FROM cfb_market_comparisons c
    JOIN cfb_game_forecasts f ON f.id=c.forecast_id
    JOIN cfb_forecast_runs r ON r.id=c.run_id
    JOIN cfb_matchups m ON m.id=c.game_id
    WHERE m.completed=TRUE AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
      AND m.season=(SELECT MAX(season) FROM cfb_matchups WHERE completed=TRUE)
      AND c.forecast_at<m.commence_time AND f.kickoff=m.commence_time
    ORDER BY r.version,f.game_id,c.forecast_at DESC,c.id DESC
  `);
  const groups = new Map<string, { model: number[]; market: number[]; excluded: number }>();
  for (const raw of result.rows as unknown as RawRow[]) {
    const actualMargin = Number(raw.home_score) - Number(raw.away_score);
    const actualTotal = Number(raw.home_score) + Number(raw.away_score);
    const actualWin = actualMargin > 0 ? 1 : 0;
    for (const market of ["spread", "total", "moneyline"] as const) {
      const key = `${raw.version}:${market}`;
      const group = groups.get(key) ?? { model: [], market: [], excluded: 0 };
      const quote = raw.evidence_json?.markets?.[market];
      const forecast = raw.evidence_json?.forecast;
      if (!quote?.eligible || quote.value == null || forecast?.home_points == null
        || forecast.away_points == null || forecast.home_win_probability == null) {
        group.excluded++; groups.set(key, group); continue;
      }
      const marketValue = Number(quote.value);
      if (market === "spread") {
        group.model.push(Math.abs(Number(forecast.home_points) - Number(forecast.away_points) - actualMargin));
        group.market.push(Math.abs(marketValue - actualMargin));
      } else if (market === "total") {
        group.model.push(Math.abs(Number(forecast.home_points) + Number(forecast.away_points) - actualTotal));
        group.market.push(Math.abs(marketValue - actualTotal));
      } else {
        group.model.push((Number(forecast.home_win_probability) - actualWin) ** 2);
        group.market.push((marketValue - actualWin) ** 2);
      }
      groups.set(key, group);
    }
  }
  const mean = (values: number[]) => values.length
    ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
  return [...groups.entries()].map(([key, group]) => {
    const separator = key.lastIndexOf(":");
    return { version: key.slice(0, separator), market: key.slice(separator + 1) as FrozenMarketEvaluation["market"],
      eligibleGames: group.model.length, modelError: mean(group.model),
      marketError: mean(group.market), excluded: group.excluded };
  });
}
