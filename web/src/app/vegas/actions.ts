"use server";

import { revalidatePath } from "next/cache";
import { db } from "@/db";
import { nbaMatchups, mlbMatchups, teams, mlbTeams } from "@/db/schema";
import { sql } from "drizzle-orm";
import type { Sport } from "@/db/queries";

type OddsGame = {
  id: string;
  commence_time: string;
  home_team: string;
  away_team: string;
  bookmakers: Array<{
    markets: Array<{
      key: string;
      outcomes: Array<{ name: string; price: number; point?: number }>;
    }>;
  }>;
};

function roundHalf(v: number) {
  return Math.round(v * 2) / 2;
}

function mlToRaw(ml: number) {
  return ml >= 0 ? 100 / (ml + 100) : Math.abs(ml) / (Math.abs(ml) + 100);
}

function computeImpliedTotals(
  vegasTotal: number,
  homeMl: number,
  awayMl: number,
): { homeImplied: number; awayImplied: number } {
  const rawHome = mlToRaw(homeMl);
  const rawAway = mlToRaw(awayMl);
  const vig = rawHome + rawAway;
  const homeProb = rawHome / vig;
  const impliedSpread = Math.max(-15, Math.min(15, (homeProb - 0.5) / 0.025));
  const homeImplied = Math.round((vegasTotal / 2 + impliedSpread / 2) * 10) / 10;
  const awayImplied = Math.round((vegasTotal - homeImplied) * 10) / 10;
  return { homeImplied, awayImplied };
}

export type FetchOddsResult = {
  ok: boolean;
  message: string;
  gamesFound: number;
  upserted: number;
  updated: number;
};

export async function fetchVegasOdds(date: string, sport: Sport = "nba"): Promise<FetchOddsResult> {
  const oddsKey = process.env.ODDS_API_KEY;
  if (!oddsKey) {
    return { ok: false, message: "ODDS_API_KEY not configured", gamesFound: 0, upserted: 0, updated: 0 };
  }

  const isMlb = sport === "mlb";
  const sportKey = isMlb ? "baseball_mlb" : "basketball_nba";

  // Build full team name → team_id map from DB (sport-specific table)
  const teamRows = isMlb
    ? await db.select({ teamId: mlbTeams.teamId, name: mlbTeams.name }).from(mlbTeams)
    : await db.select({ teamId: teams.teamId, name: teams.name }).from(teams);
  const nameToId = new Map(teamRows.map((t) => [t.name, t.teamId]));

  const oddsUrl = new URL(`https://api.the-odds-api.com/v4/sports/${sportKey}/odds/`);
  oddsUrl.searchParams.set("apiKey", oddsKey);
  oddsUrl.searchParams.set("regions", "us");
  oddsUrl.searchParams.set("markets", "h2h,spreads,totals");
  oddsUrl.searchParams.set("oddsFormat", "american");

  let allGames: OddsGame[];
  try {
    const resp = await fetch(oddsUrl.toString(), { cache: "no-store" });
    if (!resp.ok) {
      return {
        ok: false,
        message: `Odds API returned ${resp.status} ${resp.statusText}`,
        gamesFound: 0,
        upserted: 0,
        updated: 0,
      };
    }
    allGames = (await resp.json()) as OddsGame[];
  } catch (e) {
    return { ok: false, message: `Network error: ${e}`, gamesFound: 0, upserted: 0, updated: 0 };
  }

  // Filter to target date: 36h window from midnight UTC (mirrors Python behavior)
  const windowStart = new Date(date + "T00:00:00Z");
  const windowEnd = new Date(windowStart.getTime() + 36 * 60 * 60 * 1000);
  const dateGames = allGames.filter((g) => {
    const t = new Date(g.commence_time);
    return t >= windowStart && t < windowEnd;
  });

  if (dateGames.length === 0) {
    return {
      ok: true,
      message: `No upcoming games found for ${date} — the Odds API only returns live/upcoming games`,
      gamesFound: 0,
      upserted: 0,
      updated: 0,
    };
  }

  // Insert matchup rows for any games not yet in DB
  const toInsert = dateGames
    .map((g) => ({
      gameDate: date,
      homeTeamId: nameToId.get(g.home_team) ?? null,
      awayTeamId: nameToId.get(g.away_team) ?? null,
    }))
    .filter((r): r is { gameDate: string; homeTeamId: number; awayTeamId: number } =>
      r.homeTeamId != null && r.awayTeamId != null,
    );

  let upserted = 0;
  if (toInsert.length > 0) {
    const inserted = isMlb
      ? await db.insert(mlbMatchups).values(toInsert).onConflictDoNothing().returning({ id: mlbMatchups.id })
      : await db.insert(nbaMatchups).values(toInsert).onConflictDoNothing().returning({ id: nbaMatchups.id });
    upserted = inserted.length;
  }

  // Reload matchup rows for this date so we can update odds by home team name
  const matchupRows = isMlb
    ? await db.execute<{ id: number; homeName: string }>(sql`
        SELECT m.id, t.name AS "homeName"
        FROM mlb_matchups m
        JOIN mlb_teams t ON t.team_id = m.home_team_id
        WHERE m.game_date = ${date}
      `)
    : await db.execute<{ id: number; homeName: string }>(sql`
        SELECT m.id, t.name AS "homeName"
        FROM nba_matchups m
        JOIN teams t ON t.team_id = m.home_team_id
        WHERE m.game_date = ${date}
      `);
  const byHome = new Map(matchupRows.rows.map((r) => [r.homeName, r.id]));

  // Update odds on each matchup
  let updated = 0;
  for (const g of dateGames) {
    const matchupId = byHome.get(g.home_team);
    if (!matchupId) continue;

    const homePrices: number[] = [];
    const awayPrices: number[] = [];
    const totalPoints: number[] = [];
    const homeSpreads: number[] = [];

    for (const bm of g.bookmakers ?? []) {
      for (const market of bm.markets ?? []) {
        if (market.key === "h2h") {
          const ho = market.outcomes.find((o) => o.name === g.home_team);
          const ao = market.outcomes.find((o) => o.name === g.away_team);
          if (ho) homePrices.push(ho.price);
          if (ao) awayPrices.push(ao.price);
        } else if (market.key === "spreads") {
          const homeO = market.outcomes.find((o) => o.name === g.home_team);
          if (homeO?.point != null) homeSpreads.push(homeO.point);
        } else if (market.key === "totals") {
          const over = market.outcomes.find((o) => o.name === "Over");
          if (over?.point != null) totalPoints.push(over.point);
        }
      }
    }

    const avg = (arr: number[]) => arr.reduce((a, b) => a + b, 0) / arr.length;
    const homeMl = homePrices.length ? Math.round(avg(homePrices)) : null;
    const awayMl = awayPrices.length ? Math.round(avg(awayPrices)) : null;
    const homeSpread = homeSpreads.length ? roundHalf(avg(homeSpreads)) : null;
    const vegasTotal = totalPoints.length ? roundHalf(avg(totalPoints)) : null;
    const homeWinProb =
      homeMl != null && awayMl != null
        ? mlToRaw(homeMl) / (mlToRaw(homeMl) + mlToRaw(awayMl))
        : null;

    let homeImplied: number | null = null;
    let awayImplied: number | null = null;
    if (vegasTotal != null && homeMl != null && awayMl != null) {
      ({ homeImplied, awayImplied } = computeImpliedTotals(vegasTotal, homeMl, awayMl));
    }

    if (isMlb) {
      await db.execute(sql`
        UPDATE mlb_matchups
        SET home_ml         = ${homeMl},
            away_ml         = ${awayMl},
            home_spread     = ${homeSpread},
            vegas_total     = ${vegasTotal},
            vegas_prob_home = ${homeWinProb},
            home_implied    = ${homeImplied},
            away_implied    = ${awayImplied}
        WHERE id = ${matchupId}
      `);
    } else {
      await db.execute(sql`
        UPDATE nba_matchups
        SET home_ml         = ${homeMl},
            away_ml         = ${awayMl},
            home_spread     = ${homeSpread},
            vegas_total     = ${vegasTotal},
            vegas_prob_home = ${homeWinProb},
            home_implied    = ${homeImplied},
            away_implied    = ${awayImplied}
        WHERE id = ${matchupId}
      `);
    }
    updated++;
  }

  revalidatePath("/vegas");

  const parts: string[] = [];
  if (upserted > 0) parts.push(`${upserted} game${upserted > 1 ? "s" : ""} added`);
  parts.push(`${updated} matchup${updated !== 1 ? "s" : ""} updated with lines`);

  return {
    ok: true,
    message: parts.join(", "),
    gamesFound: dateGames.length,
    upserted,
    updated,
  };
}
