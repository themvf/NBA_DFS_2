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
  | "COLD_OUTDOOR_LATE";

export type ArchetypeDef = {
  code: ArchetypeCode;
  label: string;
  /** Chip text. Kept short enough to sit in a table cell. */
  short: string;
  visibility: Visibility;
  /** Which way the story pushes the room's opinion of the TAGGED team. */
  lean: "toward" | "against" | "neutral";
  story: string;
  /** Measured market gap over 2020-2025, in percentage points. */
  measuredGapPp: number;
  measuredN: number;
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
};

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
];

const BY_CODE = new Map(ARCHETYPES.map((a) => [a.code, a]));
export function archetype(code: ArchetypeCode): ArchetypeDef {
  const a = BY_CODE.get(code);
  if (!a) throw new Error(`unknown archetype ${code}`);
  return a;
}

export function tagArchetypes(ctx: TeamGameContext): ArchetypeCode[] {
  return ARCHETYPES.filter((a) => a.test(ctx)).map((a) => a.code);
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
