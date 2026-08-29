// One-time seed for the RB ADP VALUE note list (16 running backs).
//
// A third exercise, and the first that is about the PRICE rather than the
// player: where the market has each back versus where he should go. RB-only by
// construction. It coexists with the draft-board and PPR-consistency lists
// because ff_player_notes is keyed per category -- e.g. Bhayshul Tuten is
// "Target" on the draft board and "Too high" here, which is the disagreement
// worth seeing side by side.
//
//   npm run seed:rb-adp-value-notes             # insert only where absent
//   npm run seed:rb-adp-value-notes -- --force  # also overwrite edited notes
//
// TWO REPRESENTATION CHOICES, recorded because both lose a little on purpose:
//
// 1. `source_adp` is left NULL. This list quotes POSITIONAL rank ("RB18"), not
//    an overall ADP number, and the tooltip renders that field as "listed ADP
//    N" -- storing 18 there would read as ADP 18, which is wrong by ~40 picks.
//    The market rank and the target range are carried as the note's first line
//    instead, where they render verbatim and cannot be misread.
//
// 2. The "going too late" half of the list supplies a target range rather than
//    a verdict word, so the verdict label is written as "Value at RB10-12"
//    rather than being invented. Those all map to `target`; the "too high"
//    half maps by its own color, red -> fade and amber -> caution.
//
// `listRank` is null: this list is two ranked halves, not one 1..N sequence, so
// a synthetic position would imply an ordering the source does not have.

import { runSeed, seedNotes, type SeedNote } from "./seed-notes-lib";

/** Prefix the market context so it renders above the reasoning in the tooltip. */
function withMarket(marketRank: string, myValue: string, why: string): string {
  return `Market: ${marketRank} · My value: ${myValue}\n\n${why}`;
}

const NOTES: SeedNote[] = [
  // --- Going too high -------------------------------------------------------
  {
    name: "Javonte Williams", position: "RB", team: "DAL",
    verdict: "fade", verdictLabel: "Too high",
    note: withMarket("RB18", "below RB18", "His 2025 production faded after the midpoint, and Jaydon Blue is earning a larger role. FantasyPros specifically argues his current price assumes too much volume."),
  },
  {
    name: "Bucky Irving", position: "RB", team: "TB",
    verdict: "fade", verdictLabel: "Too high",
    note: withMarket("RB20", "below RB20", "Gainwell threatens passing-down work, while Sean Tucker remains involved; that matters a lot in full PPR. His floor is much shakier if receptions disappear."),
  },
  {
    name: "Bhayshul Tuten", position: "RB", team: "JAX",
    verdict: "fade", verdictLabel: "Too high (strongest)",
    note: withMarket("RB23", "below RB23", "The one I feel strongest about. The market is pricing a breakout before we know he owns goal-line or receiving work. Chris Rodriguez and LeQuint Allen give Jacksonville realistic alternatives in those high-value situations. RB23 is paying for the optimistic scenario, while the consistency strategy specifically says to avoid backs whose touch distribution isn't yet predictable."),
  },
  {
    name: "David Montgomery", position: "RB", team: "HOU",
    verdict: "caution", verdictLabel: "Slightly high",
    note: withMarket("RB24", "slightly below RB24", "Houston increasingly looks like a genuine Montgomery/Woody Marks split. He can still produce touchdowns, but that is exactly the type of profile that can become volatile week to week."),
  },
  {
    name: "TreVeyon Henderson", position: "RB", team: "NE",
    verdict: "caution", verdictLabel: "Slightly high",
    note: withMarket("RB27", "slightly below RB27", "Tremendous upside, but Rhamondre Stevenson remains directly alongside him at RB26 in ADP. You're paying for Henderson winning the committee before he has actually done it."),
  },
  {
    name: "Tony Pollard", position: "RB", team: "TEN",
    verdict: "caution", verdictLabel: "Too high for consistency",
    note: withMarket("RB28", "below RB28 for floor", "Spears creates enough receiving/touch competition that Pollard's week-to-week workload is harder to trust. I'd rather take backs with clearer receiving roles or wait another tier."),
  },
  {
    name: "RJ Harvey", position: "RB", team: "DEN",
    verdict: "caution", verdictLabel: "Slightly high",
    note: withMarket("RB35", "slightly below RB35", "Denver has Dobbins and rookie Jonah Coleman, and Coleman is getting legitimate positive buzz. FantasyPros specifically argues that Harvey may not inherit a huge role even if Dobbins misses time."),
  },

  // --- Going too late -------------------------------------------------------
  {
    name: "Breece Hall", position: "RB", team: "NYJ",
    verdict: "target", verdictLabel: "Value at RB10-12",
    note: withMarket("RB16", "RB10-12", "Biggest value among established backs. His receiving volume has room to rebound significantly, and FantasyPros also identifies him as undervalued at current cost."),
  },
  {
    name: "Saquon Barkley", position: "RB", team: "PHI",
    verdict: "target", verdictLabel: "Value at RB7-8",
    note: withMarket("RB10", "RB7-8", "The market may have overcorrected after 2025 regression. Philadelphia's healthier line and new offensive direction create a credible rebound while his workload remains enormous."),
  },
  {
    name: "Jeremiyah Love", position: "RB", team: "ARI",
    verdict: "target", verdictLabel: "Value at RB8-10 when healthy",
    note: withMarket("RB13", "RB8-10 when healthy", "The high-ankle sprain is suppressing ADP, but the long-term talent/workload upside is substantially higher. This is more of an injury discount than a talent discount."),
  },
  {
    name: "Jaylen Warren", position: "RB", team: "PIT",
    verdict: "target", verdictLabel: "Value at RB19-21",
    note: withMarket("RB25", "RB19-21", "Particularly undervalued for a PPR-floor approach because receiving work can rescue low-rushing-volume weeks. I'd rather roster him than several backs currently going 5-8 spots earlier."),
  },
  {
    name: "Rhamondre Stevenson", position: "RB", team: "NE",
    verdict: "target", verdictLabel: "Value at RB22-24",
    note: withMarket("RB26", "RB22-24", "His price assumes Henderson takes over substantially. If the Patriots maintain a real committee, Stevenson can beat RB26 through touches and receptions alone."),
  },
  {
    name: "Jordan Mason", position: "RB", team: "MIN",
    verdict: "target", verdictLabel: "Value at RB28-32",
    note: withMarket("RB40", "RB28-32", "This looks like one of the strongest late-round discrepancies. Camp reports suggest a larger role, his career efficiency is excellent, and FantasyPros has repeatedly highlighted him as undervalued."),
  },
  {
    name: "Jacory Croskey-Merritt", position: "RB", team: "WSH",
    verdict: "target", verdictLabel: "Value at RB30-34",
    note: withMarket("RB38", "RB30-34", "Washington didn't bring in overwhelming competition, and he already produced 805 rushing yards and eight TDs as a rookie. Even modest receiving improvement would raise his PPR value substantially."),
  },
  {
    name: "Blake Corum", position: "RB", team: "LAR",
    verdict: "target", verdictLabel: "Value at RB33-36",
    note: withMarket("RB41", "RB33-36", "His 2025 efficiency was excellent, and there has been talk of moving the Rams closer to a 50/50 split. He also has huge contingent value if Kyren misses time."),
  },
  {
    name: "Dylan Sampson", position: "RB", team: "CLE",
    verdict: "target", verdictLabel: "Deep PPR sleeper (RB45-ish)",
    note: withMarket("RB61", "RB45-ish in PPR", "Deep sleeper specifically for this format: he caught 33 passes as a rookie and was extremely efficient as a receiver. Negative game scripts could create a lot of inexpensive PPR points."),
  },
];

runSeed(seedNotes({ category: "rb-adp-value", author: "seed:rb-adp-value", notes: NOTES }));
