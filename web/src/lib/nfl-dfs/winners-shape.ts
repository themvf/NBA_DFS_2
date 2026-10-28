/**
 * Winners shape (`nfl-winners-shape-v1`): a build preset that reproduces how
 * the top finishers of the 2026 week 1-5 flagship contests built, measured
 * from stat-api's full lineups (`nfl_dfs_field_user_builds`, reviewed in
 * `research/nfl_top_builds.py`). It is a SHAPE, not a validated edge: the
 * same projections, rearranged. The weekly review grades it against the
 * standard build; nothing here changes a projection or an ownership number.
 *
 * Classic (Millionaire top-5, n=20 lineups; the 300-odd below them agree):
 * - min unique 4 (ours was 2), most-used player in no more than 55% of
 *   lineups, about 1.6 distinct quarterbacks per 10 lineups, bring-back in
 *   about half the lineups (ours: every one), median salary $49,874;
 * - no pool rules since v11 (v1-v10 kept backs with top-quartile touches or
 *   4+ targets a game, receivers with 25%+ of their team's targets or under
 *   $5,000, tight ends with 4+ targets a game, defenses under $3,000; the
 *   rules remain behind `poolRules: true` for the frozen research cohorts).
 * Showdown (248 Thursday/Monday winners):
 * - no kicker (6% vs 36% lower down); the team split is free since v10 (the
 *   4-2 ban of v1-v9 was measured wrong, see the v10 note);
 *   the captain a QB, RB or WR (346 of 346 top-20 lineups);
 *   winning captains sat at ~6% captain ownership, so no captain above 30%.
 *
 * Every rule below is applied through an ordinary optimizer setting, so a
 * saved run records exactly what was enforced; when the pool rules run (research
 * only since v11) every player they leave out is named with the number that
 * left him out.
 */
import type { NflOptimizerPlayer, NflOptimizerSettings } from "@/app/dfs/nfl/nfl-optimizer";

/**
 * v2 (2026-10-11): the RB rule keeps a back on top-quartile touches OR 4+
 * targets a game. v1's targets-only line kept 10 of 112 backs on the week-6
 * Classic and left out Jonathan Taylor (3.7 targets a game) and every other
 * bell-cow with a thin receiving role; the winners' backs were bell-cows
 * first (top-quartile touches) and pass catchers second.
 */
/**
 * v3 (2026-10-11): Showdown captains are QB, RB or WR only. In 346 top-20
 * showdown lineups from the stored flagship builds the captain was a running
 * back 163 times, a receiver 98 and a quarterback 85; never a defense, tight
 * end or kicker. The first v1 build captained the Jaguars DST three times and
 * a tight end five times, which no winner did.
 */
/**
 * v4 (2026-10-11): Classic plays a DST against the lineup's own QB/RB/WR/TE
 * in at most 5% of lineups. Across the week 1-4 Millionaires, 16 of 392
 * checkable top-100 lineups did (4%), and 1 of 40 top-10 lineups; the first
 * v3 build on the week-5 Classic did it in 40 of 100.
 */
/**
 * v5 (2026-10-17): every Showdown lineup rosters at least one quarterback.
 * Across the 11 stored 2026 flagship Showdowns (weeks 1-6, distinct lineups),
 * 11 of 12 first-place, 38 of 41 top-20 and 157 of 167 top-100 lineups had one
 * (92-94%); the v4 HOU@JAX build left the quarterback out of 17 of 50,
 * because the ceiling objective and the ownership fade both cut the two
 * chalk quarterbacks. Owner's decision, before the first factorial slate.
 */
/**
 * v6 (2026-10-17, owner's decision, same evening as v5): (a) Showdown
 * captain stack: a WR/TE captain has his own QB, a QB captain one of his own
 * WR/TE. Distinct top-100 lineups of the 11 Showdowns: about 80% of WR
 * captains had their own QB (72 of 89) and 40 of 42 QB captains had an own
 * pass catcher; the v4 HOU@JAX build paired a WR captain with his QB in 6 of
 * 29. (b) Salary floor $49,500 -> $48,100 (both formats): 51% of distinct
 * top-100 Showdown winners spent under $49,500; 17% under $48,100 (2% of top-20).
 */
/**
 * v7 (2026-10-17, owner's decision: "we need to match the winners"): Showdown
 * builds target the winners' distributions across the run, from the distinct
 * top-20 / top-100 lineups of the 11 stored 2026 flagship Showdowns:
 * both quarterbacks in 40% of lineups (41% / 44%), a quarterback captain with
 * no second quarterback in at most 7% (5% / 7%), a running-back captain in
 * 20% (24% / 20%). Each quarterback may appear in up to 75% of lineups unless
 * the user set his own overall range, since 40% two-QB lineups need ~70%
 * apiece. A lineup that cannot meet a target is built with it relaxed and
 * counted in the notes.
 */
/**
 * v8 (2026-10-18, owner's decision: Classic "as close to the winners' shape"
 * as Showdown). From the 484 distinct top-100 lineups of the 2026 week 1-5
 * Millionaires: the QB stacked with 2+ of his own WR/TE in 40% of lineups
 * (winners 45%, weekly 12-79%; the whole field 30%; our week-5 build 10%), the
 * rest with exactly one; a 4th WR in at most 35% (2026 winners 24%, weekly
 * 9-35%; 2023-25 first-place winners 39%, n=38; ours 82%); 2+ RB/WR/TE from a team outside the QB's game in at most 35%
 * (winners 30%, weekly 22-48%; ours 68%). All scheduled across the run like
 * the bring-back share.
 */
/**
 * v9 (2026-10-18, owner's decision: 50-lineup Classic builds must include the
 * winners' 2nd TE). A 2nd TE in 30% of lineups, two-sided (2026 top-100
 * winners 45%, 2023-25 first place 13%), and the TE pool rule drops its
 * $4,000 ceiling: in the 220 winning two-TE lineups the pricier TE cost a
 * median $4,600 and was $4,000+ in 75% of them, and 36% of one-TE winners used
 * a $4,000+ TE. The 4+ targets a game requirement stays.
 */
/**
 * v10 (2026-10-28, owner's decision): the Showdown 4-2 ban is lifted. Across
 * the 12 stored 2026 flagship Showdowns the top-100 split 3-3 / 4-2 / 5-1 as
 * 50% / 35% / 15% against a field of 33 / 47 / 19: 4-2 and 5-1 have the same
 * lift (0.74 and 0.79) and 3-3 is the shape that wins above its share (1.5),
 * so banning 4-2 while allowing 5-1 removed a third of the winning lineups
 * for nothing. It cost most in lopsided scripts: the week-6 and week-7
 * Monday top-100s were 91% and 93% 4-2, which the preset could not build at
 * all. No target mix replaces it (12 slates cannot fit shares); the
 * projections and the spread decide. The ghost factorial's house keeps its
 * own frozen "banned" level (`ghost/design.ts`), so its cohort is unchanged.
 */
/**
 * v11 (2026-10-28, owner's decision: "fix classic builds"): the Classic pool
 * rules no longer apply to a user's build. Measured on the week 5-7
 * Millionaire top-100 entries against the saved Winners builds (the preset
 * was live on all three): 613 of 2,700 winning roster slots (23%) were
 * players a pool rule had left out, worth 47 points per winning lineup (week
 * 5: 93), and the best lineup reachable from the kept pool trailed the slate's
 * by 38 points in week 5. The WR target-share rule did most of it (315 slots:
 * Chase, Higgins, Nabers, Watson, Golden), the TE rule the rest of week 5
 * (Likely, 81 of 100 lineups). The rules described the winners' MOST USED
 * players; as exclusions they removed the ones who won. The registered test
 * (docs/nfl-classic-te-wr-pool-rule-study.md) has enrolled no slate because
 * stat-api stopped serving score curves on 2026-10-19, so this is an owner's
 * decision on discovery evidence, recorded as such. The rules still exist
 * behind `poolRules: true`: the ghost factorial's house (`ghost/arm.ts`) and
 * the pool-rule study runner call them so their frozen cohorts are unchanged.
 */
export const WINNERS_SHAPE_VERSION = "nfl-winners-shape-v11";

export type BuildShape = "standard" | "winners";
export const BUILD_SHAPES: readonly BuildShape[] = ["standard", "winners"];

/** Prior-week opportunity (play-by-play, the three weeks before the slate), per player. */
export type PriorOpportunity = {
  games: number;
  targets: number;
  carries: number;
  /** Player targets over team targets in the same window; null when the team total is unknown. */
  targetShare: number | null;
};

export const WINNERS_SHAPE = {
  minUnique: 4,
  maxExposure: 0.55,
  minSalary: 48_100,
  /** A quarterback never above this share of lineups, whatever the lineup count. */
  qbExposureFloor: 0.15,
  /** Distinct quarterbacks the winners used per ten lineups. */
  distinctQbsPerTenLineups: 1.6,
  bringBackShare: 0.5,
  /** Classic: share of lineups stacking the QB with 2+ of his own pass catchers. */
  qbDoubleStackShare: 0.4,
  /** Classic: at most this share of lineups roster a 4th WR. */
  maxFourWrShare: 0.35,
  /** Classic: at most this share of lineups carry a 2-player stack from another game. */
  maxOtherGameStackShare: 0.35,
  /** Classic: share of lineups rostering a 2nd TE. */
  twoTeShare: 0.3,
  /** Classic: share of lineups that may play a DST against their own offense. */
  dstVsOffenseMaxShare: 0.05,
  rbMinTargetsPerGame: 4,
  /** A back whose touches a game rank in the slate's top quarter qualifies without the targets. */
  rbTouchesTopShare: 0.25,
  wrMinTargetShare: 0.25,
  wrCheapSalary: 5_000,
  teMinTargetsPerGame: 4,
  dstMaxSalary: 3_000,
  captainMaxExposure: 0.3,
  captainPositions: ["QB", "RB", "WR"] as NflOptimizerPlayer["position"][],
  /** Showdown: quarterbacks every lineup rosters, Captain or Flex. */
  showdownMinQb: 1,
  showdownTwoQbShare: 0.4,
  showdownMaxLoneQbCaptainShare: 0.07,
  showdownRbCaptainShare: 0.2,
  showdownQbMinCap: 0.75,
} as const;

/**
 * The fewest eligible players a position needs once the 55% cap applies:
 * each slot wants at least two names, and a filter that leaves fewer is not
 * applied for that position (said in the notes), never silently loosened.
 */
const MIN_KEPT: Record<"RB" | "WR" | "TE" | "DST", number> = { RB: 4, WR: 6, TE: 2, DST: 2 };

/**
 * Per-quarterback exposure cap for a build of `nLineups`: 1.6 distinct
 * quarterbacks per 10 lineups (at least two), never below 15%. 20 lineups
 * -> 3 quarterbacks -> 35%; 100 lineups -> 16 -> 15% floor.
 */
export function qbExposureCap(nLineups: number): number {
  const n = Math.max(1, Math.floor(nLineups));
  const distinct = Math.max(2, Math.round(WINNERS_SHAPE.distinctQbsPerTenLineups * n / 10));
  return Math.min(1, Math.max(WINNERS_SHAPE.qbExposureFloor, Math.ceil(n / distinct) / n));
}

/**
 * The positions a Showdown build may put at Captain, or null when any may.
 * Mirrors what applyWinnersShape enforces, so the page can respect it before
 * a build (the "Suggest CPT ranges" button gave a TE a captain minimum the
 * Winners shape then rejected, 2026-10-26).
 */
export function showdownCaptainPositions(s: { format: "classic" | "showdown"; mode: string; buildShape?: BuildShape;
  captainPositions?: readonly NflOptimizerPlayer["position"][] }): NflOptimizerPlayer["position"][] | null {
  if (s.format !== "showdown") return null;
  if (s.mode === "gpp" && s.buildShape === "winners") return [...WINNERS_SHAPE.captainPositions];
  return s.captainPositions?.length ? [...s.captainPositions] : null;
}

/** Positions "Suggest CPT ranges" proposes in any shape (owner's rule, 2026-10-26): never a TE, DST or kicker. */
export const SUGGESTED_CAPTAIN_POSITIONS: readonly NflOptimizerPlayer["position"][] = ["QB", "RB", "WR"];

/** The positions to suggest captain ranges for: QB/RB/WR, narrowed further by the build's own captain rule. */
export function suggestedCaptainPositions(s: Parameters<typeof showdownCaptainPositions>[0]): NflOptimizerPlayer["position"][] {
  const allowed = showdownCaptainPositions(s);
  return SUGGESTED_CAPTAIN_POSITIONS.filter((position) => !allowed || allowed.includes(position));
}

type ShapePlayer = Pick<NflOptimizerPlayer, "dkPlayerId" | "name" | "position" | "salary" | "isOut" | "projectionStatus" | "priorOpportunity">;

export type WinnersShapeResult = {
  settings: NflOptimizerSettings;
  /** dkPlayerId -> why a pool rule left him out. Empty unless `poolRules` was asked for (v11). Locked and manually excluded players are never here. */
  excluded: Map<number, string>;
  notes: string[];
};

const perGame = (o: PriorOpportunity, key: "targets" | "carries") => (o.games > 0 ? o[key] / o.games : 0);
const touches = (o: PriorOpportunity) => perGame(o, "carries") + perGame(o, "targets");
const fmtShare = (share: number | null) => (share == null ? "an unknown" : `${Math.round(share * 100)}%`);
const money = (n: number) => `$${n.toLocaleString()}`;

/**
 * The portfolio limits the preset actually enforces, given the user's own
 * Lineup rules. `applyWinnersShape` uses this, and so does the page's
 * Lineup rules summary, so the summary can never show a rule the build will
 * not apply.
 */
export function winnersShapeLimits(base: { format: "classic" | "showdown"; nLineups: number; minSalary: number; maxExposure: number; minUnique: number }) {
  const classic = base.format === "classic";
  return {
    minSalary: Math.max(base.minSalary, WINNERS_SHAPE.minSalary),
    maxExposure: classic ? Math.min(base.maxExposure, WINNERS_SHAPE.maxExposure) : base.maxExposure,
    minUnique: classic ? Math.max(base.minUnique, WINNERS_SHAPE.minUnique) : base.minUnique,
    maxQbExposure: classic ? qbExposureCap(base.nLineups) : null,
    maxCaptainExposure: classic ? null : WINNERS_SHAPE.captainMaxExposure,
  };
}

/**
 * The settings the preset enforces, applied to one slate. GPP only: a cash
 * build is returned unchanged with a note saying so. `poolRules: true` also
 * runs the v1-v10 Classic pool rules; no user build asks for that since v11,
 * the ghost house and the pool-rule study runner do.
 */
export function applyWinnersShape(settings: NflOptimizerSettings, players: readonly ShapePlayer[], options: { poolRules?: boolean } = {}): WinnersShapeResult {
  const notes: string[] = [];
  const excluded = new Map<number, string>();
  if (settings.mode !== "gpp") {
    notes.push(`Winners shape (${WINNERS_SHAPE_VERSION}) applies to GPP builds only; this cash build is unchanged.`);
    return { settings, excluded, notes };
  }
  const n = settings.nLineups;
  const limits = winnersShapeLimits(settings);
  const shaped: NflOptimizerSettings = { ...settings, minSalary: limits.minSalary };
  if (settings.format === "classic") {
    const qbCap = limits.maxQbExposure!;
    Object.assign(shaped, {
      minUnique: limits.minUnique,
      maxExposure: limits.maxExposure,
      stackPassCatchers: Math.max(1, settings.stackPassCatchers) as 1 | 2,
      bringBack: true,
      bringBackShare: WINNERS_SHAPE.bringBackShare,
      maxQbExposure: qbCap,
      maxDstVsOffenseShare: Math.min(settings.maxDstVsOffenseShare ?? 1, WINNERS_SHAPE.dstVsOffenseMaxShare),
      qbDoubleStackShare: settings.qbDoubleStackShare ?? WINNERS_SHAPE.qbDoubleStackShare,
      maxFourWrShare: Math.min(settings.maxFourWrShare ?? 1, WINNERS_SHAPE.maxFourWrShare),
      maxOtherGameStackShare: Math.min(settings.maxOtherGameStackShare ?? 1, WINNERS_SHAPE.maxOtherGameStackShare),
      twoTeShare: settings.twoTeShare ?? WINNERS_SHAPE.twoTeShare,
    });
    notes.push(`Winners shape (${WINNERS_SHAPE_VERSION}, classic): min unique ${shaped.minUnique}, max exposure ${Math.round(shaped.maxExposure * 100)}%, `
      + `each quarterback in at most ${Math.round(qbCap * 100)}% of lineups (${Math.max(1, Math.floor(qbCap * n))} of ${n}), `
      + `a bring-back in ${Math.round(WINNERS_SHAPE.bringBackShare * 100)}% of lineups, at least one stacked pass catcher (two in ${Math.round(WINNERS_SHAPE.qbDoubleStackShare * 100)}% of lineups), a 4th WR in at most ${Math.round(WINNERS_SHAPE.maxFourWrShare * 100)}%, a 2-player stack from another game in at most ${Math.round(WINNERS_SHAPE.maxOtherGameStackShare * 100)}%, a 2nd TE in ${Math.round(WINNERS_SHAPE.twoTeShare * 100)}%, `
      + `a DST against its own players in at most ${Math.round(shaped.maxDstVsOffenseShare! * 100)}% of lineups (${Math.floor(shaped.maxDstVsOffenseShare! * n + 1e-9)} of ${n}), min salary ${money(shaped.minSalary)}` + (options.poolRules ? "." : ", no pool rules (v11)."));
    if (options.poolRules) applyPoolRules(settings, players, excluded, notes);
  } else {
    Object.assign(shaped, {
      maxCaptainExposure: limits.maxCaptainExposure!,
      excludeKickers: true,
      captainPositions: [...WINNERS_SHAPE.captainPositions],
      minQbPerLineup: Math.max(settings.minQbPerLineup ?? 0, WINNERS_SHAPE.showdownMinQb),
      captainStack: true,
      twoQbShare: settings.twoQbShare ?? WINNERS_SHAPE.showdownTwoQbShare,
      maxLoneQbCaptainShare: settings.maxLoneQbCaptainShare ?? WINNERS_SHAPE.showdownMaxLoneQbCaptainShare,
      rbCaptainShare: settings.rbCaptainShare ?? WINNERS_SHAPE.showdownRbCaptainShare,
      showdownQbMinCap: Math.max(settings.showdownQbMinCap ?? 0, WINNERS_SHAPE.showdownQbMinCap),
    });
    notes.push(`Winners shape (${WINNERS_SHAPE_VERSION}, showdown): any team split (v10 lifted the 4-2 ban), at least ${WINNERS_SHAPE.showdownMinQb} quarterback, a WR/TE captain with his own QB and a QB captain with his own WR/TE, both QBs in ${Math.round(WINNERS_SHAPE.showdownTwoQbShare * 100)}% of lineups, a lone-QB captain in at most ${Math.round(WINNERS_SHAPE.showdownMaxLoneQbCaptainShare * 100)}%, an RB captain in ${Math.round(WINNERS_SHAPE.showdownRbCaptainShare * 100)}%, each QB in up to ${Math.round(WINNERS_SHAPE.showdownQbMinCap * 100)}% of lineups, no kicker, the captain a ${WINNERS_SHAPE.captainPositions.join("/")}, `
      + `no captain in more than ${Math.round(WINNERS_SHAPE.captainMaxExposure * 100)}% of lineups, min salary ${money(shaped.minSalary)}. `
      + `Captain ranges you set are kept.`);
  }
  return { settings: shaped, excluded, notes };
}

function applyPoolRules(settings: NflOptimizerSettings, players: readonly ShapePlayer[], excluded: Map<number, string>, notes: string[]): void {
  const keep = new Set([...settings.lockedPlayerIds]);
  const manual = new Set(settings.excludedPlayerIds);
  const active = players.filter(p => !p.isOut && p.projectionStatus !== "out" && !manual.has(p.dkPlayerId));
  // The slate's own touches-a-game quartile among backs with a record, so the
  // rule reads the week's pool rather than a constant carried from another one.
  const rbTouches = active.filter(p => p.position === "RB" && p.priorOpportunity && p.priorOpportunity.games > 0)
    .map(p => touches(p.priorOpportunity!)).sort((a, b) => b - a);
  const touchFloor = rbTouches.length ? rbTouches[Math.max(0, Math.ceil(rbTouches.length * WINNERS_SHAPE.rbTouchesTopShare) - 1)] : null;
  const rules: Array<{ position: "RB" | "WR" | "TE" | "DST"; label: string; needsPrior: boolean;
    qualifies: (p: ShapePlayer) => boolean; why: (p: ShapePlayer) => string }> = [
    { position: "RB", label: `backs with touches a game in the slate's top quarter (${touchFloor == null ? "n/a" : touchFloor.toFixed(1)}+) or ${WINNERS_SHAPE.rbMinTargetsPerGame}+ targets a game`, needsPrior: true,
      qualifies: p => !!p.priorOpportunity && (perGame(p.priorOpportunity, "targets") >= WINNERS_SHAPE.rbMinTargetsPerGame
        || (touchFloor != null && touches(p.priorOpportunity) >= touchFloor)),
      why: p => p.priorOpportunity
        ? `${touches(p.priorOpportunity).toFixed(1)} touches and ${perGame(p.priorOpportunity, "targets").toFixed(1)} targets a game over the prior ${p.priorOpportunity.games} game(s); the preset keeps backs at ${touchFloor == null ? "n/a" : touchFloor.toFixed(1)}+ touches or ${WINNERS_SHAPE.rbMinTargetsPerGame}+ targets.`
        : `no prior-week opportunity record; the preset keeps backs with top-quartile touches or ${WINNERS_SHAPE.rbMinTargetsPerGame}+ targets a game.` },
    { position: "WR", label: `receivers with ${Math.round(WINNERS_SHAPE.wrMinTargetShare * 100)}%+ of their team's targets, or under ${money(WINNERS_SHAPE.wrCheapSalary)}`, needsPrior: true,
      qualifies: p => p.salary < WINNERS_SHAPE.wrCheapSalary || (p.priorOpportunity?.targetShare ?? -1) >= WINNERS_SHAPE.wrMinTargetShare,
      why: p => `${fmtShare(p.priorOpportunity?.targetShare ?? null)} target share at ${money(p.salary)}; the preset keeps receivers at ${Math.round(WINNERS_SHAPE.wrMinTargetShare * 100)}%+ or under ${money(WINNERS_SHAPE.wrCheapSalary)}.` },
    { position: "TE", label: `tight ends with ${WINNERS_SHAPE.teMinTargetsPerGame}+ targets a game`, needsPrior: true,
      qualifies: p => !!p.priorOpportunity && perGame(p.priorOpportunity, "targets") >= WINNERS_SHAPE.teMinTargetsPerGame,
      why: p => `${money(p.salary)}${p.priorOpportunity ? ` with ${perGame(p.priorOpportunity, "targets").toFixed(1)} targets a game` : ", no prior-week opportunity record"}; the preset keeps tight ends with ${WINNERS_SHAPE.teMinTargetsPerGame}+ targets a game.` },
    { position: "DST", label: `defenses under ${money(WINNERS_SHAPE.dstMaxSalary)}`, needsPrior: false,
      qualifies: p => p.salary < WINNERS_SHAPE.dstMaxSalary,
      why: p => `${money(p.salary)}; the preset keeps defenses under ${money(WINNERS_SHAPE.dstMaxSalary)}.` },
  ];
  for (const rule of rules) {
    const candidates = active.filter(p => p.position === rule.position);
    if (!candidates.length) continue;
    if (rule.needsPrior && !candidates.some(p => p.priorOpportunity)) {
      notes.push(`Winners shape: no prior-week opportunity data for any ${rule.position}, so the ${rule.position} rule (${rule.label}) was not applied.`);
      continue;
    }
    const kept = candidates.filter(p => keep.has(p.dkPlayerId) || rule.qualifies(p));
    if (kept.length < MIN_KEPT[rule.position]) {
      notes.push(`Winners shape: only ${kept.length} ${rule.position}(s) meet the rule (${rule.label}); at least ${MIN_KEPT[rule.position]} are needed under the exposure cap, so the ${rule.position} rule was not applied.`);
      continue;
    }
    let dropped = 0;
    for (const p of candidates) {
      if (keep.has(p.dkPlayerId) || rule.qualifies(p)) continue;
      excluded.set(p.dkPlayerId, `Winners shape: ${rule.why(p)}`);
      dropped++;
    }
    notes.push(`Winners shape: ${rule.position} pool kept ${kept.length} of ${candidates.length} (${rule.label}); ${dropped} left out, each named in the build's eligibility list.`);
  }
}
