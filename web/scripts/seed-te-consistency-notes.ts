// One-time seed for the TE CONSISTENCY note list (12 ranked tight ends, plus
// one unranked entry -- see KELCE below).
//
// Same question as the WR consistency list (weekly floor rather than season
// total), asked at tight end. It gets its own category rather than joining
// `ppr-consistency` because the two are separately ranked sequences: this list
// runs 1-12 within tight ends while the WR list runs 26-50 within a larger
// board, so pooling them would make "#1" and "#26" look comparable when they
// are not.
//
//   npm run seed:te-consistency-notes             # insert only where absent
//   npm run seed:te-consistency-notes -- --force  # also overwrite edited notes
//
// The 2025 hit/bust rates from the supplied comparison table are appended to
// the six players it covers, marked as coming from that table so they read as
// evidence rather than as part of the written take.
//
// KELCE is NOT one of the 12 ranked tight ends. He appears only in the
// comparison table, with no verdict of his own, so his note is labelled "Not in
// the ranked 12" and carries the neutral verdict -- the alternative was either
// dropping a real data point or inventing a ranking that was never given.
// Delete him from the admin page if he should not be on the board at all.
//
// GOEDERT CARRIES A CONFLICT FROM THE SOURCE, left unresolved on purpose: the
// written take says "top-12 in 47% of 2025 games and top six in 33%", while the
// comparison table lists "TE12+ in 47%, bust 33%". The same 33% is a top-six
// rate in one and a bust rate in the other. Both are reproduced verbatim and
// attributed, rather than silently picking one.

import { runSeed, seedNotes, type SeedNote } from "./seed-notes-lib";

/** Append the comparison-table rates, attributed so they read as evidence. */
function withRates(take: string, hitPct: number, bustPct: number): string {
  return `${take}\n\n2025 comparison table: TE12+ in ${hitPct}% of games, bust ${bustPct}%.`;
}

const NOTES: SeedNote[] = [
  {
    listRank: 1, name: "Trey McBride", position: "TE", team: "ARI",
    verdict: "target", verdictLabel: "Elite",
    note: withRates("The safest TE for our particular strategy. His production comes from receptions and targets rather than needing touchdowns. One of only two tight ends I consider a truly elite weekly-floor asset.", 81, 6),
  },
  {
    listRank: 2, name: "Brock Bowers", position: "TE", team: "LV",
    verdict: "target", verdictLabel: "Elite",
    note: withRates("His 2025 injury-shortened season still produced only an 8% bust rate. If healthy, he's essentially a WR playing TE. The other of the two truly elite weekly-floor assets at the position.", 58, 8),
  },
  {
    listRank: 3, name: "Colston Loveland", position: "TE", team: "CHI",
    verdict: "target", verdictLabel: "Very high",
    note: "Chicago increasingly uses him as a featured receiver, and current consensus has him TE3. I like the target profile much more than touchdown-dependent alternatives.",
  },
  {
    listRank: 4, name: "Tyler Warren", position: "TE", team: "IND",
    verdict: "target", verdictLabel: "Very high",
    note: withRates("He posted a top-12 TE week in 50% of games as a rookie despite zero huge “boom” weeks -- that's actually appealing for what we're measuring.", 50, 31),
  },
  {
    listRank: 5, name: "Harold Fannin Jr.", position: "TE", team: "CLE",
    verdict: "target", verdictLabel: "High",
    note: withRates("Also hit TE1 territory 50% of the time in 2025. His route/target involvement gives him a much better floor than the average TE.", 50, 38),
  },
  {
    listRank: 6, name: "Tucker Kraft", position: "TE", team: "GB",
    verdict: "target", verdictLabel: "High",
    note: "Green Bay can spread targets around, but Kraft has become one of the more integrated receiving TEs in the league.",
  },
  {
    listRank: 7, name: "Sam LaPorta", position: "TE", team: "DET",
    verdict: "target", verdictLabel: "High",
    note: "Strong offense and predictable route participation give him a relatively stable baseline. Detroit's abundance of weapons does cap target dominance.",
  },
  {
    listRank: 8, name: "George Kittle", position: "TE", team: "SF",
    verdict: "caution", verdictLabel: "High ceiling / lower floor",
    note: "Kittle can destroy a week, but his scoring has historically contained more variance because San Francisco doesn't always need to feed him.",
  },
  {
    listRank: 9, name: "Kyle Pitts", position: "TE", team: "ATL",
    verdict: "caution", verdictLabel: "Moderate-high",
    note: withRates("Interestingly, Pitts delivered a TE1-level score in 50% of games last season. But he also busted in 50%, which perfectly illustrates why season totals can be deceptive.", 50, 50),
  },
  {
    listRank: 10, name: "Dallas Goedert", position: "TE", team: "PHI",
    verdict: "target", verdictLabel: "Sneaky floor",
    // See the header: the written take and the comparison table disagree about
    // what the 33% is. Both are kept, attributed, rather than reconciled here.
    note: withRates("A top-12 TE in 47% of 2025 games and top six in 33%. His role is boring but relatively predictable.", 47, 33),
  },
  {
    listRank: 11, name: "Dalton Kincaid", position: "TE", team: "BUF",
    verdict: "caution", verdictLabel: "Moderate",
    note: "Josh Allen gives him touchdown opportunity, but target distribution in Buffalo creates weekly uncertainty.",
  },
  {
    listRank: 12, name: "Isaiah Likely", position: "TE", team: "NYG",
    verdict: "caution", verdictLabel: "Moderate",
    note: "Huge upside in New York, but his new role creates projection risk. I'd rather bet on him for ceiling than proven consistency.",
  },

  // Unranked -- comparison-table data only. See the header.
  {
    name: "Travis Kelce", position: "TE", team: "KC",
    verdict: "fair", verdictLabel: "Not in the ranked 12",
    note: withRates("Not one of the 12 ranked tight ends -- carried here only because the 2025 comparison table covers him. The rates put him in the Warren/Fannin/Goedert band rather than the McBride/Bowers tier.", 50, 38),
  },
];

runSeed(seedNotes({ category: "te-consistency", author: "seed:te-consistency", notes: NOTES }));
