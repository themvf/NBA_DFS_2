// One-time seed for the INJURY WATCH note list (12 players).
//
// Unlike the other four lists, this one DECAYS. A camp injury read is worth
// little a month later, and worth nothing once the player has played three
// games. The tooltip's "last edited" date is the staleness signal; re-run this
// with --force after refreshing the text, or edit individual notes in the
// admin page.
//
//   npm run seed:injury-watch-notes             # insert only where absent
//   npm run seed:injury-watch-notes -- --force  # also overwrite edited notes
//
// THIS IS NOT THE INJURY FEED. The boards already render an InjuryMarker chip
// driven by the live Sleeper/FantasyPros status (all 12 of these players are
// currently "Questionable" there). That badge answers "what does the provider
// say right now"; this note answers "what do I think it means for the draft".
// They are deliberately separate, and the note going stale cannot corrupt the
// live status.
//
// VERDICT MAPPING, following the decision already made for the WR list: green
// maps to target, amber and orange both map to caution, red maps to fade.
// Orange is heavier here than it was there -- six of these twelve are orange --
// so amber and orange share a chip color, and the concern word ("Moderate" vs
// "Low" vs "Improving") is what separates them. It is carried in the verdict
// label, which is what the chip actually displays. There is no green in this
// list: no entry here is a reason to draft someone.
//
// listRank is null throughout -- this is a status list, not a ranked one.

import { runSeed, seedNotes, type SeedNote } from "./seed-notes-lib";

/** Lead with the diagnosis so the tooltip opens with what is actually wrong. */
function withIssue(issue: string, draftImpact: string): string {
  return `Issue: ${issue}\n\n${draftImpact}`;
}

const NOTES: SeedNote[] = [
  {
    name: "Ashton Jeanty", position: "RB", team: "LV",
    verdict: "caution", verdictLabel: "Moderate concern",
    note: withIssue("Ankle sprain", "Still expected to have a shot at Week 1, but there is no firm return date yet. I would not pay full early-second-round price until we get another positive practice report."),
  },
  {
    name: "Breece Hall", position: "RB", team: "NYJ",
    verdict: "caution", verdictLabel: "Moderate concern",
    note: withIssue("Groin strain", "Jets expect him back for Week 1, but groin injuries can recur and directly affect burst/cutting. This doesn't make me avoid him, but it adds some risk to our “consistency” thesis."),
  },
  {
    name: "Puka Nacua", position: "WR", team: "LAR",
    verdict: "caution", verdictLabel: "Moderate concern",
    note: withIssue("Groin/psoas soreness", "He has missed roughly two weeks of practice, although the Rams still expect him ready for Week 1. At a top-three overall price, even a small health concern matters more because the opportunity cost is enormous."),
  },
  {
    name: "Malik Nabers", position: "WR", team: "NYG",
    verdict: "fade", verdictLabel: "Significant concern",
    note: withIssue("ACL/meniscus recovery", "He may be available Week 1, but this is a much more serious situation than a routine camp strain. I'd be much happier drafting him in Round 4 than treating him like a healthy second-round WR."),
  },
  {
    name: "Emeka Egbuka", position: "WR", team: "TB",
    verdict: "caution", verdictLabel: "Moderate concern",
    note: withIssue("Toe sprain", "Tampa is only saying it is “hopeful” he'll be ready for Week 1, and he hasn't fully returned to team work. Toe injuries worry me with WRs because they can linger and compromise acceleration even when the player is active."),
  },
  {
    name: "Tyler Warren", position: "TE", team: "IND",
    verdict: "caution", verdictLabel: "Mild/moderate concern",
    note: withIssue("Groin", "Doesn't currently sound serious, but he's still limited and is being drafted as a premium TE. I'm monitoring rather than downgrading substantially."),
  },
  {
    name: "George Kittle", position: "TE", team: "SF",
    verdict: "fade", verdictLabel: "Significant concern",
    note: withIssue("Achilles recovery", "He's only about seven months removed from a torn Achilles and recently returned to limited practice. Even if he plays Week 1, I would expect workload/efficiency risk early in the year and wouldn't draft him based on his old weekly ceiling."),
  },
  {
    name: "Josh Jacobs", position: "RB", team: "GB",
    verdict: "caution", verdictLabel: "Moderate concern",
    note: withIssue("Groin", "He's missed time since early August and also has separate off-field uncertainty. That's enough combined risk that I don't want to reach for him."),
  },
  {
    name: "TreVeyon Henderson", position: "RB", team: "NE",
    verdict: "caution", verdictLabel: "Low concern",
    note: withIssue("Ankle", "Recent reports are encouraging and he is expected to be ready for Week 1. I wouldn't significantly move him down, though the injury adds one more reason not to overpay for his already uncertain workload."),
  },
  {
    name: "Luther Burden III", position: "WR", team: "CHI",
    verdict: "caution", verdictLabel: "Improving",
    note: withIssue("Groin", "He's returned to limited work and Chicago is optimistic about Week 1. Still worth monitoring because he hasn't resumed a completely normal workload yet."),
  },
  {
    name: "Tucker Kraft", position: "TE", team: "GB",
    verdict: "caution", verdictLabel: "Improving",
    note: withIssue("ACL recovery", "Very positive trajectory: he's back doing 11-on-11 work and currently expected to participate fully in Week 1. I would not fade him heavily at his current TE price."),
  },
  {
    name: "Alec Pierce", position: "WR", team: "IND",
    verdict: "caution", verdictLabel: "Moderate concern",
    note: withIssue("Ankle surgery recovery", "He's finally off PUP, but Indianapolis plans to ramp him gradually. That's particularly relevant because his fantasy value depends heavily on speed and explosive routes."),
  },
];

runSeed(seedNotes({ category: "injury-watch", author: "seed:injury-watch", notes: NOTES }));
