// One-time seed for the PPR CONSISTENCY note list (ranks 26-50 as supplied).
//
// A different exercise from the draft-board list: this one ranks weekly floor
// and reception volume rather than value against ADP, so the same player can
// carry a different verdict in each. Jameson Williams is "Caution" on the draft
// board and "Volatile" here; both are true of different questions, which is why
// notes are per-category rather than one-per-player.
//
//   npm run seed:ppr-consistency-notes             # insert only where absent
//   npm run seed:ppr-consistency-notes -- --force  # also overwrite edited notes
//
// VERDICT MAPPING. The supplied list uses four colors; the schema has four
// verdicts but a different middle. 🟢 -> target, 🟡 -> caution, 🔴 -> fade, and
// 🟠 (DK Metcalf, "Volatile") folds into caution by explicit decision --
// mapping it to fade would have made him look as severe as Jameson Williams,
// which overstates how he was ranked. The nuance survives in the verdict label,
// which is free text and is what the chip actually displays.
//
// `team` and `adp` are not supplied by this list, so they are recorded as the
// player's team and the list position; the tooltip renders whatever is present.

import { runSeed, seedNotes, type SeedNote } from "./seed-notes-lib";

const NOTES: SeedNote[] = [
  { listRank: 26, name: "Ladd McConkey", position: "WR", team: "LAC", adp: 26, verdict: "target", verdictLabel: "Excellent", note: "Keenan Allen is gone, and McConkey already had 106 targets last year. His underneath/intermediate role is exactly what we want for weekly PPR stability." },
  { listRank: 27, name: "Tetairoa McMillan", position: "WR", team: "CAR", adp: 27, verdict: "target", verdictLabel: "Excellent", note: "Carolina already treated him like a No. 1 with 122 targets as a rookie. Target volume should be much more dependable than his overall ADP suggests." },
  { listRank: 28, name: "DeVonta Smith", position: "WR", team: "PHI", adp: 28, verdict: "target", verdictLabel: "Very good", note: "Route participation and separation ability should produce steady opportunities even if Philadelphia remains balanced. I trust his 7-10 target pathway much more than many WRs around him." },
  { listRank: 29, name: "Jaylen Waddle", position: "WR", team: "DEN", adp: 29, verdict: "target", verdictLabel: "Very good", note: "Denver gives him a good QB and an offense capable of supporting intermediate-volume production. His speed provides upside, but he doesn't need deep touchdowns to return value." },
  { listRank: 30, name: "Garrett Wilson", position: "WR", team: "NYJ", adp: 30, verdict: "target", verdictLabel: "Very good", note: "Whatever the Jets' ceiling is, Wilson should remain near the top of their target tree. Volume protects him from becoming completely touchdown-dependent." },
  { listRank: 31, name: "Terry McLaurin", position: "WR", team: "WAS", adp: 31, verdict: "target", verdictLabel: "Good", note: "Jayden Daniels gives McLaurin unusually efficient targets, and Washington's target hierarchy is uncomplicated. Reception volume isn't elite, but weekly involvement should be." },
  { listRank: 32, name: "Rome Odunze", position: "WR", team: "CHI", adp: 32, verdict: "target", verdictLabel: "Good", note: "Caleb Williams' development should raise Chicago's passing floor. Odunze should be less volatile as his route and target share mature." },
  { listRank: 33, name: "Wan'Dale Robinson", position: "WR", team: "TEN", adp: 33, verdict: "target", verdictLabel: "Very good PPR floor", note: "This is one of my favorites for this exercise: he saw 140 targets and 8.8 per game in 2025. Tennessee may reduce that slightly, but his slot role gives him one of the cleanest reception-based floors outside the expensive receivers." },
  { listRank: 34, name: "Michael Pittman Jr.", position: "WR", team: "PIT", adp: 34, verdict: "target", verdictLabel: "Good", note: "Pittman's game is inherently consistency-friendly: possession routes, intermediate targets and catches rather than requiring 50-yard plays. FantasyPros is also identifying him as a 2026 sleeper." },
  { listRank: 35, name: "Christian Watson", position: "WR", team: "GB", adp: 35, verdict: "caution", verdictLabel: "Good with volatility", note: "This is higher than I would have put him historically because his role changed. FantasyPros notes he was WR21 in PPG from Weeks 8-18 last season and should see increased playing time/first-read usage in 2026." },
  { listRank: 36, name: "DJ Moore", position: "WR", team: "BUF", adp: 36, verdict: "target", verdictLabel: "Good", note: "Buffalo and Josh Allen should generate enough quality opportunities to prevent truly dead weeks. My concern is that Buffalo may spread the ball around enough to cap the target floor." },
  { listRank: 37, name: "Chris Godwin", position: "WR", team: "TB", adp: 37, verdict: "target", verdictLabel: "Good if healthy", note: "His slot/intermediate game has always translated beautifully to PPR consistency. Egbuka lowers his target ceiling, but Godwin doesn't need huge plays to score 12-15." },
  { listRank: 38, name: "Parker Washington", position: "WR", team: "JAX", adp: 38, verdict: "target", verdictLabel: "Sneaky good", note: "Washington actually led Jacksonville with 95 targets last year, and his catch rate improved late in the season. FantasyPros specifically highlights him as a consistency sleeper." },
  { listRank: 39, name: "Jayden Reed", position: "WR", team: "GB", adp: 39, verdict: "target", verdictLabel: "Sneaky good", note: "Green Bay can manufacture touches for Reed with motion, RPOs, pop passes and carries. FantasyPros expects roughly six-to-seven targets per game with Romeo Doubs gone, which makes his floor much more attractive." },
  { listRank: 40, name: "Courtland Sutton", position: "WR", team: "DEN", adp: 40, verdict: "caution", verdictLabel: "Moderate-good", note: "Sutton should remain involved every week, but touchdowns still comprise a meaningful portion of his fantasy production. Waddle's addition also makes 8+ target games harder to project." },
  { listRank: 41, name: "Brian Thomas Jr.", position: "WR", team: "JAX", adp: 41, verdict: "caution", verdictLabel: "Moderate", note: "Thomas has enormous talent, but I expect more scoring variance than the receivers above him. He profiles better as an upside pick than a pure floor selection." },
  { listRank: 42, name: "Carnell Tate", position: "WR", team: "TEN", adp: 42, verdict: "caution", verdictLabel: "Projected good", note: "The attraction is opportunity: Tennessee could quickly make him a major part of the offense. The problem is we're projecting consistency before seeing an NFL season of it." },
  { listRank: 43, name: "Alec Pierce", position: "WR", team: "IND", adp: 43, verdict: "caution", verdictLabel: "Moderate", note: "His game has expanded beyond pure deep shots, which raises the floor. But he still gains a larger percentage of his value from chunk plays than a Pittman/Wan'Dale type." },
  { listRank: 44, name: "Stefon Diggs", position: "WR", team: "WAS", adp: 44, verdict: "caution", verdictLabel: "Moderate", note: "Route savvy should still generate catches, but age and Washington's existing target structure create uncertainty. I wouldn't expect old Diggs-level target dominance." },
  { listRank: 45, name: "Jordan Addison", position: "WR", team: "MIN", adp: 45, verdict: "caution", verdictLabel: "Moderate", note: "Very talented and capable of big fantasy weeks, but Jefferson inherently makes Addison's target volume less predictable. Better ceiling play than floor play." },
  { listRank: 46, name: "Quentin Johnston", position: "WR", team: "LAC", adp: 46, verdict: "caution", verdictLabel: "Moderate-low", note: "He has become a legitimate NFL receiver, but McConkey should lead the Chargers in the high-percentage targets we value here. Johnston is still more touchdown/explosive-play dependent." },
  // Supplied as 🟠 -- folded into caution, label carries the nuance. See header.
  { listRank: 47, name: "DK Metcalf", position: "WR", team: "PIT", adp: 47, verdict: "caution", verdictLabel: "Volatile", note: "Metcalf will score touchdowns and produce some excellent weeks. For this particular ranking, his lower catch-volume profile pushes him down significantly." },
  { listRank: 48, name: "Jameson Williams", position: "WR", team: "DET", adp: 48, verdict: "fade", verdictLabel: "Volatile", note: "Exactly the player we're trying to distinguish from the others: he can score 24 one week and disappoint the next because so much value comes from explosive plays. FantasyPros also lists him among its 2026 bust candidates." },
  { listRank: 49, name: "Michael Wilson", position: "WR", team: "ARI", adp: 49, verdict: "fade", verdictLabel: "Volatile", note: "Harrison should remain Arizona's primary target, leaving Wilson dependent on secondary volume and touchdowns. FantasyPros currently lists him among its WR bust candidates." },
  { listRank: 50, name: "Mike Evans", position: "WR", team: "SF", adp: 50, verdict: "fade", verdictLabel: "Floor declining", note: "He can absolutely outperform this ranking in total points because touchdowns remain his superpower. But at this stage of his career, on a new team, he's much more attractive for ceiling than weekly PPR stability." },
];

runSeed(seedNotes({ category: "ppr-consistency", author: "seed:ppr-consistency", notes: NOTES }));
