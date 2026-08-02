export type FantasyBadge = { code: string; class: string };

const codeStyles: Array<[string, string]> = [
  ["INJURY", "bg-red-100 text-red-800 ring-red-200"],
  ["OUR_FADE", "bg-rose-100 text-rose-800 ring-rose-200"],
  ["OUR_BUY", "bg-emerald-100 text-emerald-800 ring-emerald-200"],
  ["ROOKIE", "bg-lime-100 text-lime-800 ring-lime-200"],
  ["NEW_TEAM", "bg-amber-100 text-amber-900 ring-amber-200"],
  ["TEAM_TARGET_LEADER", "bg-cyan-100 text-cyan-900 ring-cyan-200"],
  ["NFL_TOP_10_TARGETS", "bg-blue-100 text-blue-800 ring-blue-200"],
  ["NFL_TOP_10_RUSH_TDS", "bg-orange-100 text-orange-900 ring-orange-200"],
  ["TEAM_WR", "bg-violet-100 text-violet-800 ring-violet-200"],
  ["TEAM_RB", "bg-purple-100 text-purple-800 ring-purple-200"],
  ["HANDCUFF", "bg-indigo-100 text-indigo-800 ring-indigo-200"],
];

const classStyles: Record<string, string> = {
  fact: "bg-sky-100 text-sky-800 ring-sky-200",
  role: "bg-violet-100 text-violet-800 ring-violet-200",
  risk: "bg-red-100 text-red-800 ring-red-200",
  model: "bg-emerald-100 text-emerald-800 ring-emerald-200",
};

export function fantasyBadgeClass(badge: FantasyBadge): string {
  return codeStyles.find(([prefix]) => badge.code.startsWith(prefix))?.[1]
    ?? classStyles[badge.class]
    ?? "bg-slate-100 text-slate-800 ring-slate-200";
}
