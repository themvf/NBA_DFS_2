export type StudyMode = "match" | "diagnostic" | "awaiting";
type Study = { title: string; mode: StudyMode };

/** Each published study has an explicit policy for the daily game-matching view. */
export const studyRegistry = {
  "NBA-01": { title: "Spread size", mode: "match" },
  "NBA-02": { title: "Rest", mode: "match" },
  "NBA-03": { title: "Venue", mode: "match" },
  "NBA-04": { title: "Line movement", mode: "match" },
  "NBA-05": { title: "Full-game totals", mode: "match" },
  "NBA-06": { title: "Team margin", mode: "match" },
  "NBA-07": { title: "Quote timing", mode: "diagnostic" },
  "NBA-08": { title: "Same-line price shopping", mode: "diagnostic" },
  "NBA-09": { title: "Totals model", mode: "awaiting" },
  "NBA-10": { title: "Moneyline calibration", mode: "awaiting" },
  "NBA-11": { title: "Rest and prior venue", mode: "match" },
  "NBA-12": { title: "Injury news and repricing", mode: "awaiting" },
  "NBA-13": { title: "First-half markets", mode: "awaiting" },
  "NFL-01": { title: "Spread size and key numbers", mode: "match" },
  "NFL-02": { title: "Favorite rest days", mode: "match" },
  "NFL-03": { title: "Listed home and away favorites", mode: "match" },
  "NFL-04": { title: "Spread movement", mode: "match" },
  "NFL-05": { title: "Full-game totals", mode: "match" },
  "NFL-06": { title: "Team against the spread", mode: "match" },
  "NFL-07": { title: "First vs. last archive accuracy", mode: "diagnostic" },
  "NFL-08": { title: "Same-line price shopping", mode: "diagnostic" },
  "NFL-09": { title: "Totals model", mode: "awaiting" },
  "NFL-10": { title: "Two-way moneyline", mode: "match" },
  "NFL-11": { title: "Relative rest advantage", mode: "match" },
  "NFL-12": { title: "Injury news and repricing", mode: "awaiting" },
  "NFL-13": { title: "First-half markets", mode: "awaiting" },
  T1: { title: "G5 mid-range favorite moneyline", mode: "match" },
  T2: { title: "FBS favorite vs. non-FBS", mode: "match" },
  T3: { title: "Small underdog spread", mode: "match" },
  T4: { title: "Over with a 14–20.5 point favorite", mode: "match" },
  T5: { title: "Saturday evening G5 favorite moneyline", mode: "match" },
  T6: { title: "Mid-range favorite spread", mode: "match" },
  T7: { title: "Spread against the better turnover margin", mode: "match" },
} as const satisfies Record<string, Study>;

export type StudyId = keyof typeof studyRegistry;
export type MatchStudyId = { [K in StudyId]: typeof studyRegistry[K]["mode"] extends "match" ? K : never }[StudyId];

export function studyHeading(id: StudyId) {
  return id + " · " + studyRegistry[id].title;
}
