/**
 * Game archetypes — a model of how the ROOM will read a matchup.
 *
 * These do not predict winners. `analyze-nfl-archetypes` measured all seventeen
 * across 2020-2025 (3,220 team-games) and sixteen had a market gap whose CI
 * includes zero: the closing line prices every one of them, because rest,
 * travel, kickoff slot and last week's result are public months ahead.
 *
 * What they predict is your OPPONENTS. A pool entrant is not running a model;
 * they are reacting to what they remember. So an archetype where the market is
 * calibrated but the story is loud is exactly where rivals' cards drift from
 * the price and yours does not — leverage that requires the market to be RIGHT
 * rather than wrong, which is the only kind this repo has ever found.
 *
 * `visibility` is a STATED PRIOR, not a measurement. There is no pick-share
 * feed here; it is a judgement about how loudly a thing announces itself to
 * someone who is not modelling. It is the weakest part of this file and is
 * labelled as such wherever it surfaces.
 */

export type Visibility = "loud" | "moderate" | "quiet";

export type ArchetypeCode =
  | "ALTITUDE"
  | "ALTITUDE_OFF"
  | "OFF_INTERNATIONAL"
  | "AT_INTERNATIONAL"
  | "WEST_TEAM_EARLY"
  | "CROSS_COUNTRY"
  | "OFF_MONDAY"
  | "SHORT_WEEK"
  | "OFF_BYE"
  | "REST_EDGE"
  | "OFF_BLOWOUT_WIN"
  | "OFF_BLOWOUT_LOSS"
  | "OFF_PRIMETIME_WIN"
  | "IN_PRIMETIME"
  | "HOME_DOG"
  | "DIVISIONAL"
  | "COLD_OUTDOOR_LATE"
  // Added 2026-09-09. Unmeasured: measuredGapPp/measuredN are null until
  // `analyze:archetypes` is re-run against a pre-registered plan.
  | "MARQUEE_BRAND"
  | "FLYOVER_FADE"
  | "HEAVY_FAVORITE"
  | "RECORD_GAP"
  | "UNDEFEATED"
  | "WINLESS";

export type ArchetypeDef = {
  code: ArchetypeCode;
  label: string;
  /** Chip text. Kept short enough to sit in a table cell. */
  short: string;
  visibility: Visibility;
  /** Which way the story pushes the room's opinion of the TAGGED team. */
  lean: "toward" | "against" | "neutral";
  story: string;
  /**
   * Measured market gap over 2020-2025, in percentage points, and its sample.
   * NULL means "not yet measured" and must render as such -- never as 0.0pp,
   * which would read as a measured null result rather than an absent one.
   */
  measuredGapPp: number | null;
  measuredN: number | null;
  test: (t: TeamGameContext) => boolean;
};

export type TeamGameContext = {
  team: string;
  opp: string;
  isHome: boolean;
  week: number;
  impliedWin: number;
  rest: number;
  oppRest: number;
  /** 0 = Sunday ... 4 = Thursday, matching Date#getDay in ET. */
  weekday: number;
  /** Kickoff hour in ET, or -1 when unknown. */
  hourEt: number;
  neutralSite: boolean;
  div: boolean;
  roof: string | null;
  prev?: {
    neutralSite: boolean;
    weekday: number;
    margin: number;
    won: boolean;
    hourEt: number;
  };
  /** Opponent and venue of the team's previous game, for hangover archetypes. */
  prevOpp?: string;
  prevWasAway?: boolean;
  /**
   * Season-to-date record for each side as of the START of this week, from
   * `buildStandings`. Absent means unknown, and every record archetype fails
   * closed on absence rather than assuming 0-0.
   */
  record?: StandingsRow;
  oppRecord?: StandingsRow;
};

// ---------------------------------------------------------------------------
// Standings -- the input the room actually starts from
// ---------------------------------------------------------------------------

export type StandingsRow = {
  wins: number;
  losses: number;
  ties: number;
  games: number;
  pointsFor: number;
  pointsAgainst: number;
};

export type StandingsGame = {
  week: number;
  home: string;
  away: string;
  homeScore: number | null;
  awayScore: number | null;
};

/**
 * Season-to-date records from completed games in weeks STRICTLY BEFORE
 * `throughWeekExclusive`.
 *
 * The cutoff is a week boundary, not a kickoff timestamp, and that is the
 * correct cutoff for this tool rather than a convenient one: a pick'em card is
 * submitted before the week's first kickoff, so the standings the entrant reads
 * are exactly the ones through the completed prior week. Using the game's own
 * week would leak its result into the tag that is supposed to explain why the
 * room picked it.
 *
 * A team with no completed games is ABSENT from the map rather than 0-0, so a
 * caller cannot silently treat "unknown" as "even".
 */
export function buildStandings(
  games: StandingsGame[],
  throughWeekExclusive: number,
): Map<string, StandingsRow> {
  const out = new Map<string, StandingsRow>();
  const row = (team: string): StandingsRow => {
    let r = out.get(team);
    if (!r) {
      r = { wins: 0, losses: 0, ties: 0, games: 0, pointsFor: 0, pointsAgainst: 0 };
      out.set(team, r);
    }
    return r;
  };

  for (const g of games) {
    if (g.week >= throughWeekExclusive) continue;
    if (g.homeScore == null || g.awayScore == null) continue;
    const h = row(g.home);
    const a = row(g.away);
    h.games += 1;
    a.games += 1;
    h.pointsFor += g.homeScore;
    h.pointsAgainst += g.awayScore;
    a.pointsFor += g.awayScore;
    a.pointsAgainst += g.homeScore;
    if (g.homeScore > g.awayScore) {
      h.wins += 1;
      a.losses += 1;
    } else if (g.homeScore < g.awayScore) {
      h.losses += 1;
      a.wins += 1;
    } else {
      h.ties += 1;
      a.ties += 1;
    }
  }
  return out;
}

/** Win differential as a standings table shows it, ties counting as neither. */
function winDiff(r: StandingsRow): number {
  return r.wins - r.losses;
}

/**
 * Brand sets: franchises the room over- and under-backs irrespective of
 * quality, by national television share and fanbase size.
 *
 * These are a STATED PRIOR and a blunt one -- the same caveat that covers
 * `visibility`, doubly. They are frozen deliberately and must not be tuned
 * against outcomes: a set fitted to results stops measuring the room and
 * starts measuring the season. A franchise's national profile does drift over
 * a decade, so revisit by judgement and version the change, never by fitting.
 */
export const MARQUEE_TEAMS: readonly string[] = ["DAL", "KC", "SF", "GB", "PIT", "PHI", "BUF", "BAL", "NE", "NYG"];
export const FLYOVER_TEAMS: readonly string[] = ["JAX", "CAR", "ARI", "TEN", "WSH", "LV", "HOU", "IND"];
const MARQUEE = new Set<string>(MARQUEE_TEAMS);
const FLYOVER = new Set<string>(FLYOVER_TEAMS);

const NORTHERN = new Set([
  "GB", "CHI", "BUF", "NE", "CLE", "PIT", "DEN", "NYJ", "NYG", "PHI", "WSH", "BAL", "CIN", "KC",
]);

const PACIFIC = new Set(["LAR", "LAC", "SF", "SEA", "LV"]);
const MOUNTAIN = new Set(["ARI", "DEN"]);
const CENTRAL = new Set(["DAL", "HOU", "CHI", "GB", "MIN", "KC", "NO", "TEN"]);

/** 0 Eastern, 1 Central, 2 Mountain, 3 Pacific. */
export function timezoneOf(abbrev: string): number {
  if (PACIFIC.has(abbrev)) return 3;
  if (MOUNTAIN.has(abbrev)) return 2;
  if (CENTRAL.has(abbrev)) return 1;
  return 0;
}

/**
 * The measured gaps below come from `analyze-nfl-archetypes` over 2020-2025.
 * They are carried here so the UI can show what the market did with each
 * archetype rather than asserting the situation matters. Only CROSS_COUNTRY
 * had a CI excluding zero, and it is flagged as unconfirmed everywhere it
 * appears — one survivor out of seventeen.
 */
export const ARCHETYPES: ArchetypeDef[] = [
  {
    code: "OFF_PRIMETIME_WIN",
    label: "Off a primetime win",
    short: "off PT win",
    visibility: "loud",
    lean: "toward",
    story: "Everyone watched it. The cleanest availability bias on the board.",
    measuredGapPp: 3.8,
    measuredN: 313,
    test: (t) => t.prev?.won === true && (t.prev?.hourEt ?? 0) >= 20,
  },
  {
    code: "OFF_BLOWOUT_WIN",
    label: "Off a 17+ point win",
    short: "off blowout W",
    visibility: "loud",
    lean: "toward",
    story: "Recency bias in its purest form.",
    measuredGapPp: 1.1,
    measuredN: 394,
    test: (t) => (t.prev?.margin ?? 0) >= 17,
  },
  {
    code: "OFF_BLOWOUT_LOSS",
    label: "Off a 17+ point loss",
    short: "off blowout L",
    visibility: "loud",
    lean: "against",
    story: "Teams get written off on one bad Sunday.",
    measuredGapPp: 2.0,
    measuredN: 394,
    test: (t) => (t.prev?.margin ?? 0) <= -17,
  },
  {
    code: "OFF_BYE",
    label: "Off a bye",
    short: "off bye",
    visibility: "loud",
    lean: "toward",
    story: "Universally cited, and in both directions — rested, or rusty.",
    measuredGapPp: 1.8,
    measuredN: 191,
    test: (t) => t.rest >= 13,
  },
  {
    code: "IN_PRIMETIME",
    label: "Primetime game",
    short: "primetime",
    visibility: "loud",
    lean: "neutral",
    story: "The game is the story of the week; both sides get over-thought.",
    measuredGapPp: 0.9,
    measuredN: 324,
    test: (t) => t.hourEt >= 20,
  },
  {
    code: "SHORT_WEEK",
    label: "Thursday short week",
    short: "short week",
    visibility: "loud",
    lean: "against",
    story: "Flagged by the schedule itself, so nobody misses it.",
    measuredGapPp: -0.2,
    measuredN: 98,
    test: (t) => t.weekday === 4 && t.rest <= 4,
  },
  {
    code: "HOME_DOG",
    label: "Home underdog",
    short: "home dog",
    visibility: "loud",
    lean: "toward",
    story: "Home crowd plus a plus number is the classic upset pick.",
    measuredGapPp: -1.9,
    measuredN: 644,
    test: (t) => t.isHome && t.impliedWin < 0.5,
  },
  {
    code: "DIVISIONAL",
    label: "Divisional game",
    short: "divisional",
    visibility: "loud",
    lean: "neutral",
    story: "\"Throw the records out\" gets said about every one of them.",
    measuredGapPp: -1.0,
    measuredN: 574,
    test: (t) => t.div,
  },
  {
    code: "ALTITUDE",
    label: "Visiting Denver (5,280 ft)",
    short: "altitude",
    visibility: "loud",
    lean: "against",
    story:
      "Altitude is cited in every Denver home broadcast and has been for decades. " +
      "The market prices it: visitors went 43.1% against a 44.5% price.",
    measuredGapPp: -1.4,
    measuredN: 51,
    test: (t) => !t.isHome && t.opp === "DEN",
  },
  {
    code: "ALTITUDE_OFF",
    label: "Week after visiting Denver",
    short: "post-altitude",
    visibility: "quiet",
    lean: "against",
    story:
      "A hangover nobody tracks, because it needs looking two games back. " +
      "Quiet by construction, so it moves the room very little.",
    measuredGapPp: 5.9,
    measuredN: 46,
    test: (t) => t.prevOpp === "DEN" && t.prevWasAway === true,
  },
  {
    code: "AT_INTERNATIONAL",
    label: "Neutral / international site",
    short: "international",
    visibility: "loud",
    lean: "neutral",
    story: "Announced months ahead; the novelty invites over-thinking.",
    measuredGapPp: -0.7,
    measuredN: 29,
    test: (t) => t.neutralSite,
  },
  {
    code: "WEST_TEAM_EARLY",
    label: "Pacific team at 1pm ET",
    short: "west @ 1pm",
    visibility: "loud",
    lean: "against",
    story: "The single most repeated angle in football media.",
    measuredGapPp: 7.0,
    measuredN: 115,
    test: (t) => timezoneOf(t.team) === 3 && !t.isHome && t.hourEt === 13,
  },
  {
    code: "CROSS_COUNTRY",
    label: "Travelling three time zones",
    short: "cross-country",
    visibility: "moderate",
    lean: "against",
    story:
      "The only archetype whose market gap excluded zero (+8.0pp, and +9.6pp in " +
      "2020-2022 which no earlier study had seen). Travellers beat their price, " +
      "opposite to the folklore. UNCONFIRMED — one survivor out of fifteen.",
    measuredGapPp: 8.0,
    measuredN: 211,
    test: (t) => !t.isHome && Math.abs(timezoneOf(t.team) - timezoneOf(t.opp)) === 3,
  },
  {
    code: "OFF_MONDAY",
    label: "Sunday after a Monday night game",
    short: "off MNF",
    visibility: "moderate",
    lean: "against",
    story: "A short week that is easy to miss on a Sunday-morning card.",
    measuredGapPp: 1.8,
    measuredN: 230,
    test: (t) => t.prev?.weekday === 1 && t.weekday === 0,
  },
  {
    code: "OFF_INTERNATIONAL",
    label: "Game after an international game",
    short: "off intl",
    visibility: "moderate",
    lean: "against",
    story: "Talked about; whether it matters is not obvious to anyone.",
    measuredGapPp: -4.5,
    measuredN: 56,
    test: (t) => t.prev?.neutralSite === true,
  },
  {
    code: "COLD_OUTDOOR_LATE",
    label: "Outdoors, week 14+, northern venue",
    short: "cold/late",
    visibility: "moderate",
    lean: "neutral",
    story: "Weather narratives spike late in the season.",
    measuredGapPp: -1.0,
    measuredN: 334,
    test: (t) => t.week >= 14 && t.roof === "outdoors" && NORTHERN.has(t.opp),
  },
  {
    code: "REST_EDGE",
    label: "3+ days more rest than the opponent",
    short: "rest edge",
    visibility: "quiet",
    lean: "toward",
    story: "Requires comparing both schedules. Most entrants will not.",
    measuredGapPp: 0.5,
    measuredN: 342,
    test: (t) => t.rest - t.oppRest >= 3,
  },

  // -------------------------------------------------------------------------
  // Added 2026-09-09. UNMEASURED -- measuredGapPp/measuredN are null until
  // `analyze:archetypes` is re-run under a pre-registered plan. Note that
  // re-running it grows the family from 17 to 23 tests over the same 3,220
  // team-games, so any new survivor deserves LESS trust than CROSS_COUNTRY,
  // which already sits at roughly 54% false-positive odds. A calibrated market
  // (CI including zero) is the desired outcome here, not a disappointing one:
  // it is the precondition for the leverage these tags exist to capture.
  // -------------------------------------------------------------------------

  {
    code: "MARQUEE_BRAND",
    label: "Marquee franchise",
    short: "marquee",
    visibility: "loud",
    lean: "toward",
    story:
      "The only archetype here with no schedule or market correlation at all -- " +
      "a pure statement about the room. If brand does not shift picks, the " +
      "visibility model underpinning this whole file is suspect, which is worth " +
      "knowing either way. Two marquee teams meeting cancels to no signal.",
    measuredGapPp: null,
    measuredN: null,
    test: (t) => MARQUEE.has(t.team),
  },
  {
    code: "FLYOVER_FADE",
    label: "Low-profile franchise",
    short: "low profile",
    visibility: "moderate",
    lean: "against",
    story: "The complement of the marquee set: quality the room does not watch and does not back.",
    measuredGapPp: null,
    measuredN: null,
    test: (t) => FLYOVER.has(t.team),
  },
  {
    code: "HEAVY_FAVORITE",
    label: "Heavy favourite (80%+)",
    short: "heavy fav",
    visibility: "loud",
    lean: "toward",
    story:
      "Roughly a 9.5-point spread -- where a game stops reading as a contest and " +
      "confidence entrants start stacking their top points. This tag is defined " +
      "ON price, so a market-gap measurement of it is close to meaningless; it " +
      "earns its place as a strategy input, not as an observation about the market.",
    measuredGapPp: null,
    measuredN: null,
    test: (t) => t.impliedWin >= 0.8,
  },
  {
    code: "UNDEFEATED",
    label: "Undefeated",
    short: "undefeated",
    visibility: "loud",
    lean: "toward",
    story:
      "An unbeaten team gets picked on reputation for weeks after the price has " +
      "caught up. Three wins is where the national column gets written; at 2-0 " +
      "nobody has noticed yet. A tie breaks the framing in the room's eyes too.",
    measuredGapPp: null,
    measuredN: null,
    test: (t) => !!t.record && t.record.losses === 0 && t.record.ties === 0 && t.record.wins >= 3,
  },
  {
    code: "WINLESS",
    label: "Winless",
    short: "winless",
    visibility: "loud",
    lean: "against",
    story: "Faded well past the point where the price has it as a live dog.",
    measuredGapPp: null,
    measuredN: null,
    test: (t) => !!t.record && t.record.wins === 0 && t.record.ties === 0 && t.record.losses >= 3,
  },
  {
    code: "RECORD_GAP",
    label: "Much better record",
    short: "record edge",
    visibility: "loud",
    lean: "toward",
    story:
      "The record is the first and often the only number a casual entrant looks " +
      "at, and they read it as a strength rating rather than the schedule-and- " +
      "variance artefact the line has already digested. Win differential rather " +
      "than win percentage because that is what the standings graphic shows; a " +
      "4-game gap is about two games of separation, where 2 fires on half the " +
      "board by midseason and 6 waits until week 9. Week 5 floor because the " +
      "room already treats a 3-0 vs 1-2 split as noise.",
    measuredGapPp: null,
    measuredN: null,
    test: (t) =>
      t.week >= 5 && !!t.record && !!t.oppRecord && winDiff(t.record) - winDiff(t.oppRecord) >= 4,
  },
];

const BY_CODE = new Map(ARCHETYPES.map((a) => [a.code, a]));
export function archetype(code: ArchetypeCode): ArchetypeDef {
  const a = BY_CODE.get(code);
  if (!a) throw new Error(`unknown archetype ${code}`);
  return a;
}

/**
 * Pairs where the second tag restates the first. `narrativeRead` SUMS tag
 * weights, so leaving both in would double-count one story and inflate the
 * room's heat -- the failure this file warns about and then has to enforce.
 */
const SUPPRESSES: ReadonlyArray<readonly [ArchetypeCode, ArchetypeCode]> = [
  // An undefeated team almost always also has a large record gap. One story.
  ["UNDEFEATED", "RECORD_GAP"],
];

export function tagArchetypes(ctx: TeamGameContext): ArchetypeCode[] {
  const tags = ARCHETYPES.filter((a) => a.test(ctx)).map((a) => a.code);
  const present = new Set(tags);
  const dropped = new Set<ArchetypeCode>();
  for (const [winner, loser] of SUPPRESSES) {
    if (present.has(winner)) dropped.add(loser);
  }
  return tags.filter((c) => !dropped.has(c));
}

// ---------------------------------------------------------------------------
// The read: which way will the room lean, and does that help or hurt a flip?
// ---------------------------------------------------------------------------

export type NarrativeRead = {
  /** Net story pressure on the FAVOURITE, in loud-archetype units. */
  favouriteHeat: number;
  /** Net story pressure on the UNDERDOG. */
  underdogHeat: number;
  /**
   * `crowded`  the room is piling onto the favourite -> a poor flip target,
   *            expensive by price AND contested on the other side.
   * `contrarian` the room is drawn to the UNDERDOG -> flipping here is cheap
   *            by price but no longer differentiating.
   * `quiet`    no loud story either way -> the flip is as contrarian as its
   *            price says, which is the clean case.
   */
  verdict: "crowded" | "contrarian" | "quiet";
  note: string;
};

/** Loud archetypes count double; quiet ones do not move the room at all. */
function weight(v: Visibility): number {
  return v === "loud" ? 2 : v === "moderate" ? 1 : 0;
}

/**
 * Score how the room will read one game.
 *
 * Deliberately coarse. The inputs are a stated-prior visibility and a
 * stated-prior lean, so a precise number here would be false precision — the
 * output is a three-way verdict, not a percentage.
 */
export function narrativeRead(
  favouriteTags: ArchetypeCode[],
  underdogTags: ArchetypeCode[],
): NarrativeRead {
  const heat = (tags: ArchetypeCode[]) =>
    tags.reduce((acc, c) => {
      const a = archetype(c);
      if (a.lean === "neutral") return acc;
      return acc + weight(a.visibility) * (a.lean === "toward" ? 1 : -1);
    }, 0);

  const favouriteHeat = heat(favouriteTags);
  const underdogHeat = heat(underdogTags);
  const swing = favouriteHeat - underdogHeat;

  if (swing >= 2) {
    return {
      favouriteHeat,
      underdogHeat,
      verdict: "crowded",
      note:
        "The room has a loud reason to back the favourite here. Flipping is both " +
        "expensive by price and contested — a poor differentiation target.",
    };
  }
  if (swing <= -2) {
    return {
      favouriteHeat,
      underdogHeat,
      verdict: "contrarian",
      note:
        "The room has a loud reason to like the underdog. The flip is cheap by " +
        "price but no longer differentiating — others will be there too.",
    };
  }
  return {
    favouriteHeat,
    underdogHeat,
    verdict: "quiet",
    note:
      "No loud story either way, so this flip is exactly as contrarian as its " +
      "price suggests. This is the clean case.",
  };
}
