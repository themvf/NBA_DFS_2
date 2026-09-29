/**
 * The daily failure sweep: every FAIL row of the /health checklist
 * (lib/health-checklist), delivered to a person instead of waiting to be
 * looked at.
 *
 * Delivered as one GitHub issue that @mentions the owner: opened or commented
 * on when something is wrong (new, still failing with how long, resolved),
 * closed with "all clear" when nothing is. At most one comment per UTC day
 * unless something new appears. Pure: this module decides and renders; the
 * script (web/scripts/daily-failure-sweep.ts) reads and writes.
 */
import type { HealthGroup, HealthItem } from "@/lib/health-checklist";

export interface SweepProblem {
  /** Stable identity across days: the checklist item key. */
  key: string;
  group: HealthGroup;
  title: string;
  detail: string;
  url: string | null;
}

const SITE = "https://nbadfs.vercel.app";

const day = (iso: string) => iso.slice(0, 10);
const et = (iso: string) => {
  const t = new Date(iso);
  return Number.isFinite(t.getTime())
    ? t.toLocaleString("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }) + " ET"
    : iso;
};

/** The checklist's FAIL rows, as sweep problems. */
export function problemsFromChecklist(items: HealthItem[]): SweepProblem[] {
  return items.filter((i) => i.status === "fail").map((i) => ({
    key: i.key, group: i.group, title: i.label, detail: i.detail,
    url: i.url == null ? null : i.url.startsWith("/") ? `${SITE}${i.url}` : i.url,
  }));
}

export interface SweepState {
  /** Problem key -> ISO time the sweep first saw it. */
  firstSeen: Record<string, string>;
  /** UTC day of the last comment, so a second run that day does not email again. */
  lastCommentDay: string | null;
}

const MARKER = /<!-- sweep-state: (\{[\s\S]*?\}) -->/;

export function parseState(body: string | null | undefined): SweepState | null {
  const match = body ? MARKER.exec(body) : null;
  if (!match) return null;
  try {
    const parsed = JSON.parse(match[1]) as Partial<SweepState>;
    return { firstSeen: parsed.firstSeen ?? {}, lastCommentDay: parsed.lastCommentDay ?? null };
  } catch { return null; }
}

export interface SweepDiff { added: SweepProblem[]; still: SweepProblem[]; resolved: string[]; state: SweepState }

export function diffState(previous: SweepState | null, problems: SweepProblem[], nowIso: string): SweepDiff {
  const before = previous?.firstSeen ?? {};
  const firstSeen: Record<string, string> = {};
  const added: SweepProblem[] = [], still: SweepProblem[] = [];
  for (const p of problems) {
    if (before[p.key]) { firstSeen[p.key] = before[p.key]; still.push(p); }
    else { firstSeen[p.key] = nowIso; added.push(p); }
  }
  const resolved = Object.keys(before).filter((k) => !firstSeen[k]);
  return { added, still, resolved, state: { firstSeen, lastCommentDay: previous?.lastCommentDay ?? null } };
}

const GROUP_ORDER: HealthGroup[] = ["NFL DFS", "Checklist", "Clocks", "Scheduled jobs", "Data freshness"];

function line(p: SweepProblem, firstSeen?: string): string {
  const since = firstSeen ? ` _(first seen ${et(firstSeen)})_` : "";
  return `- **${p.title}**: ${p.detail}${p.url ? ` [open](${p.url})` : ""}${since}`;
}

export function renderIssueBody(problems: SweepProblem[], state: SweepState, nowIso: string): string {
  const sections = GROUP_ORDER.map((group) => {
    const items = problems.filter((p) => p.group === group);
    return items.length ? `### ${group} (${items.length})\n${items.map((p) => line(p, state.firstSeen[p.key])).join("\n")}` : null;
  }).filter(Boolean);
  return [
    `Daily failure sweep, updated ${et(nowIso)}. ${problems.length} problem${problems.length === 1 ? "" : "s"} open.`,
    "",
    ...sections.flatMap((s) => [s as string, ""]),
    `Full status: ${SITE}/health · This issue closes itself when the sweep finds nothing wrong.`,
    "",
    `<!-- sweep-state: ${JSON.stringify(state)} -->`,
  ].join("\n");
}

export function renderComment(diff: SweepDiff, nowIso: string, mention: string): string {
  const parts = [`@${mention} daily failure sweep, ${et(nowIso)}:`];
  if (diff.added.length) parts.push(`**New (${diff.added.length})**\n${diff.added.map((p) => line(p)).join("\n")}`);
  if (diff.still.length) parts.push(`**Still failing (${diff.still.length})**\n${diff.still.map((p) => line(p, diff.state.firstSeen[p.key])).join("\n")}`);
  if (diff.resolved.length) parts.push(`**Resolved (${diff.resolved.length})**: ${diff.resolved.map((k) => `\`${k}\``).join(", ")}`);
  return parts.join("\n\n");
}

export type SweepAction =
  | { kind: "open"; title: string; body: string; comment: string }
  | { kind: "comment"; body: string; comment: string }
  | { kind: "update"; body: string }
  | { kind: "close"; comment: string }
  | { kind: "none" };

export const ISSUE_TITLE = "Daily failure sweep: problems found";

/**
 * What to do with the tracking issue. At most one comment per UTC day unless
 * something new appeared, so a fallback second run does not email twice.
 */
export function planSweep(openIssueBody: string | null, problems: SweepProblem[], nowIso: string, mention: string): SweepAction {
  const previous = parseState(openIssueBody);
  if (!problems.length) {
    if (openIssueBody == null) return { kind: "none" };
    return { kind: "close", comment: `@${mention} daily failure sweep, ${et(nowIso)}: all clear. Every scheduled job, clock and dataset checked out${previous ? `; resolved ${Object.keys(previous.firstSeen).length} problem(s)` : ""}.` };
  }
  const diff = diffState(previous, problems, nowIso);
  const commentToday = diff.added.length > 0 || diff.resolved.length > 0 || previous?.lastCommentDay !== day(nowIso);
  const state = { ...diff.state, lastCommentDay: commentToday ? day(nowIso) : diff.state.lastCommentDay };
  const body = renderIssueBody(problems, state, nowIso);
  if (openIssueBody == null) return { kind: "open", title: ISSUE_TITLE, body, comment: renderComment(diff, nowIso, mention) };
  return commentToday ? { kind: "comment", body, comment: renderComment(diff, nowIso, mention) } : { kind: "update", body };
}
