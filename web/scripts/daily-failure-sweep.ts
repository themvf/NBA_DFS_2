/**
 * Daily failure sweep (see .github/workflows/daily_failure_sweep.yml).
 *
 * Runs the /health checklist, stores it, and manages one GitHub issue labelled
 * `failure-sweep` that @mentions the owner (GitHub emails the mention). Prints
 * a markdown summary for the job summary.
 *
 *   GITHUB_TOKEN=... node -r ./scripts/server-only-stub.cjs --env-file=.env.local --import tsx ./scripts/daily-failure-sweep.ts [--dry-run]
 *
 * --dry-run reads everything and prints what it would do, writing nothing.
 */
import { collectHealth, storeHealth } from "../src/lib/health-collector";
import { recordCronRun } from "../src/lib/cron-heartbeat";
import { planSweep, problemsFromChecklist, type SweepAction } from "../src/lib/failure-sweep";

const LABEL = "failure-sweep";
const repo = process.env.GITHUB_REPOSITORY ?? "themvf/NBA_DFS_2";
const token = process.env.GITHUB_TOKEN ?? "";
const mention = process.env.SWEEP_MENTION ?? "themvf";
const dryRun = process.argv.includes("--dry-run");

async function gh(method: string, path: string, body?: unknown): Promise<Record<string, unknown> | Record<string, unknown>[]> {
  const response = await fetch(`https://api.github.com/repos/${repo}${path}`, {
    method,
    headers: { Accept: "application/vnd.github+json", Authorization: `Bearer ${token}`, "X-GitHub-Api-Version": "2022-11-28",
      ...(body ? { "Content-Type": "application/json" } : {}) },
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(15_000),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`GitHub ${method} ${path} answered ${response.status}: ${text.slice(0, 300)}`);
  return text ? JSON.parse(text) : {};
}

async function apply(action: SweepAction, issueNumber: number | null): Promise<string> {
  switch (action.kind) {
    case "none": return "nothing to do (no problems, no open issue)";
    case "open": {
      try { await gh("POST", "/labels", { name: LABEL, color: "d73a4a", description: "Opened by the daily failure sweep" }); }
      catch (error) { if (!String(error).includes("422")) throw error; } // 422 = the label already exists
      const issue = await gh("POST", "/issues", { title: action.title, body: action.body, labels: [LABEL] }) as Record<string, unknown>;
      await gh("POST", `/issues/${issue.number}/comments`, { body: action.comment });
      return `opened issue #${issue.number}`;
    }
    case "comment":
      await gh("PATCH", `/issues/${issueNumber}`, { body: action.body });
      await gh("POST", `/issues/${issueNumber}/comments`, { body: action.comment });
      return `updated and commented on issue #${issueNumber}`;
    case "update":
      await gh("PATCH", `/issues/${issueNumber}`, { body: action.body });
      return `updated issue #${issueNumber} (already commented today; no new problems)`;
    case "close":
      await gh("POST", `/issues/${issueNumber}/comments`, { body: action.comment });
      await gh("PATCH", `/issues/${issueNumber}`, { state: "closed", state_reason: "completed" });
      return `all clear; closed issue #${issueNumber}`;
  }
}

async function main() {
  if (!token) throw new Error("GITHUB_TOKEN is required (the workflow passes github.token).");
  const now = new Date();
  const items = await collectHealth({ githubToken: token, now });
  const problems = problemsFromChecklist(items);
  const counts = { pass: items.filter((i) => i.status === "pass").length, fail: problems.length, info: items.filter((i) => i.status === "info").length };
  console.log(`## Daily failure sweep, ${now.toISOString().slice(0, 16).replace("T", " ")} UTC`);
  console.log(`${items.length} items checked: ${counts.pass} pass, ${counts.fail} fail, ${counts.info} info.\n`);
  for (const p of problems) console.log(`- FAIL **${p.title}** (${p.group}): ${p.detail}`);

  const open = await gh("GET", `/issues?labels=${LABEL}&state=open&per_page=5`) as Record<string, unknown>[];
  const issue = open.find((i) => !i.pull_request) ?? null;
  const action = planSweep(issue ? String(issue.body ?? "") : null, problems, now.toISOString(), mention);
  if (dryRun) {
    console.log(`\n[dry run] would ${action.kind}${issue ? ` issue #${issue.number}` : ""}; nothing written.`);
    if (action.kind !== "none" && action.kind !== "update" && "comment" in action) console.log(`\n[dry run] comment:\n${action.comment}`);
    return;
  }
  await storeHealth(items, now);
  const outcome = await apply(action, issue ? Number(issue.number) : null);
  console.log(`\n${outcome}.`);
  await recordCronRun("daily-failure-sweep", true, `${counts.fail} failing of ${items.length}; ${outcome}`);
}

main().then(() => process.exit(0)).catch(async (error) => {
  console.error(error);
  if (!dryRun) await recordCronRun("daily-failure-sweep", false, error instanceof Error ? error.message : String(error));
  process.exit(1);
});
