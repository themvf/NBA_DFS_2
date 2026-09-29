#!/usr/bin/env node
// Runs every web test and fails loudly if any of them fails.
//
// What counts as a test:
//   1. every `test:*` script in package.json, and
//   2. every scripts/test-*.ts file that no package.json script mentions
//      (an "unscripted" test file). These used to be run by nobody.
//
// Nothing is skipped silently. A test that cannot run in CI goes in EXCLUDED
// below with the reason, and the summary lists it. The runner also refuses to
// start when the list goes stale (an excluded name no longer exists) or when a
// new script needs a local .env file but nobody excluded it, so the list has to
// be kept honest by hand.
//
// Why this exists: on 2026-09-28 test-nfl-availability-coverage turned out to
// have been failing since 2026-09-24 without anyone noticing, because nothing
// ran it. See docs/nfl-dfs-reliability-program.md, item C4.
//
// Usage (from web/):
//   node scripts/run-test-scripts.mjs            run everything
//   node scripts/run-test-scripts.mjs --list     print the plan and exit
//   node scripts/run-test-scripts.mjs --jobs 2   limit parallelism
//   node scripts/run-test-scripts.mjs --only nfl-gpp   run names containing "nfl-gpp"
//
// On GitHub Actions it also writes a pass/fail table to $GITHUB_STEP_SUMMARY.

import { spawn, spawnSync } from "node:child_process";
import { appendFileSync, readdirSync, readFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const WEB_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// Tests that cannot run in CI, and why. Keys are `test:*` script names or
// unscripted test file names. Each one needs a reason a reader can act on;
// "flaky" is not a reason. Remove an entry as soon as its test can run offline.
const EXCLUDED = {
  // Integration test that writes real pick'em rows through the server action
  // (it loads .env.local and deliberately does NOT stub the database). CI has
  // no database credentials, and it also asserts against live rows that change
  // week to week: on 2026-09-28 it failed against changed live data. Run it by
  // hand against a database you are allowed to write to.
  "test:pickem-freeze-evidence":
    "Needs the live database (--env-file=.env.local, writes real pick'em rows) and asserts against live data that changes weekly. Run by hand.",
};

const DEFAULT_TIMEOUT_MS = 10 * 60 * 1000;
const UNSCRIPTED_COMMAND = ["node", "-r", "./scripts/server-only-stub.cjs", "--import", "tsx"];

function parseArgs(argv) {
  const args = { list: false, jobs: null, only: null, timeoutMs: DEFAULT_TIMEOUT_MS };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--list") args.list = true;
    else if (arg === "--jobs") args.jobs = Number(argv[++i]);
    else if (arg === "--only") args.only = argv[++i];
    else if (arg === "--timeout-seconds") args.timeoutMs = Number(argv[++i]) * 1000;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  if (args.jobs !== null && !(Number.isInteger(args.jobs) && args.jobs > 0)) {
    throw new Error("--jobs needs a positive integer");
  }
  return args;
}

function discover() {
  const pkg = JSON.parse(readFileSync(path.join(WEB_DIR, "package.json"), "utf8"));
  const scripts = pkg.scripts ?? {};
  const allCommands = Object.values(scripts).join("\n");

  const tests = Object.entries(scripts)
    .filter(([name]) => name.startsWith("test:"))
    .map(([name, command]) => ({
      name,
      kind: "npm script",
      command,
      argv: ["npm", "run", "--silent", name],
    }));

  const unscripted = readdirSync(path.join(WEB_DIR, "scripts"))
    .filter((file) => /^test-.*\.tsx?$/.test(file))
    .filter((file) => !allCommands.includes(`scripts/${file}`))
    .sort()
    .map((file) => ({
      name: file,
      kind: "unscripted file",
      command: [...UNSCRIPTED_COMMAND, `./scripts/${file}`].join(" "),
      argv: [...UNSCRIPTED_COMMAND, `./scripts/${file}`],
    }));

  return [...tests, ...unscripted];
}

function validate(tests) {
  const problems = [];
  const names = new Set(tests.map((test) => test.name));
  for (const name of Object.keys(EXCLUDED)) {
    if (!names.has(name)) {
      problems.push(`EXCLUDED lists "${name}", which no longer exists. Remove the entry.`);
    }
  }
  for (const [name, reason] of Object.entries(EXCLUDED)) {
    if (typeof reason !== "string" || reason.trim().length < 20) {
      problems.push(`EXCLUDED entry "${name}" needs a real reason.`);
    }
  }
  for (const test of tests) {
    if (EXCLUDED[test.name]) continue;
    if (test.command.includes("--env-file")) {
      problems.push(
        `"${test.name}" loads a local .env file (${test.command}). CI has none. ` +
          "Either stub what it needs so it runs offline, or add it to EXCLUDED in " +
          "scripts/run-test-scripts.mjs with the reason.",
      );
    }
  }
  return problems;
}

function killTree(child) {
  if (!child.pid) return;
  if (process.platform === "win32") {
    spawnSync("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore" });
  } else {
    try {
      process.kill(-child.pid, "SIGKILL");
    } catch {
      child.kill("SIGKILL");
    }
  }
}

function runOne(test, timeoutMs) {
  return new Promise((resolve) => {
    const started = Date.now();
    const chunks = [];
    const [command, ...rest] = test.argv;
    const child = spawn(command, rest, {
      cwd: WEB_DIR,
      env: { ...process.env, FORCE_COLOR: "0", NO_COLOR: "1" },
      shell: process.platform === "win32",
      detached: process.platform !== "win32",
      stdio: ["ignore", "pipe", "pipe"],
    });
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      killTree(child);
    }, timeoutMs);
    child.stdout.on("data", (chunk) => chunks.push(chunk));
    child.stderr.on("data", (chunk) => chunks.push(chunk));
    const finish = (code, error) => {
      clearTimeout(timer);
      const output = Buffer.concat(chunks).toString("utf8") + (error ? `\n${error.stack ?? error}` : "");
      // Every test prints a result line when it finishes. An exit 0 with no
      // output is what a test looks like when it stopped before its checks ran
      // (e.g. an async test whose pending promise did not keep Node alive), so
      // it is a failure, not a pass.
      const silent = !timedOut && code === 0 && output.trim() === "";
      resolve({
        ...test,
        status: !timedOut && code === 0 && !silent ? "pass" : "fail",
        exitCode: code,
        timedOut,
        silent,
        seconds: (Date.now() - started) / 1000,
        output,
      });
    };
    child.on("error", (error) => finish(null, error));
    child.on("close", (code) => finish(code, null));
  });
}

async function runAll(tests, jobs, timeoutMs, onDone) {
  const queue = [...tests];
  const results = [];
  async function worker() {
    while (queue.length > 0) {
      const test = queue.shift();
      const result = await runOne(test, timeoutMs);
      results.push(result);
      onDone(result);
    }
  }
  await Promise.all(Array.from({ length: Math.min(jobs, tests.length) }, worker));
  return results;
}

const onActions = process.env.GITHUB_ACTIONS === "true";

function escapeAnnotation(text) {
  return text.replace(/%/g, "%25").replace(/\r/g, "%0D").replace(/\n/g, "%0A");
}

function report(result) {
  const time = `${result.seconds.toFixed(1)}s`;
  if (result.status === "pass") {
    console.log(`PASS  ${result.name}  (${time})`);
    return;
  }
  const why = result.timedOut ? "timed out" : result.silent ? "exit 0 but printed nothing: it may have stopped before its checks ran" : `exit ${result.exitCode}`;
  console.log(`FAIL  ${result.name}  (${why}, ${time})`);
  if (onActions) console.log(`::group::Output of ${result.name}`);
  console.log(result.output.trimEnd());
  if (onActions) {
    console.log("::endgroup::");
    const tail = result.output.trimEnd().split("\n").slice(-15).join("\n");
    console.log(`::error title=Test failed: ${result.name}::${escapeAnnotation(`${why}\n${tail}`)}`);
  }
}

function writeSummary(results, excluded, totalSeconds) {
  const file = process.env.GITHUB_STEP_SUMMARY;
  if (!file) return;
  const failed = results.filter((r) => r.status === "fail");
  const passed = results.filter((r) => r.status === "pass");
  const unscripted = results.filter((r) => r.kind === "unscripted file");
  const lines = [];
  lines.push(`## Web test scripts: ${failed.length === 0 ? "all passed" : `${failed.length} failed`}`);
  lines.push("");
  lines.push(
    `${passed.length} passed, ${failed.length} failed, ${excluded.length} excluded ` +
      `(${results.length + excluded.length} total, ${totalSeconds.toFixed(0)}s wall clock).`,
  );
  lines.push("");
  lines.push("| Result | Test | Kind | Time |");
  lines.push("|---|---|---|---:|");
  const ordered = [...failed, ...passed.sort((a, b) => a.name.localeCompare(b.name))];
  for (const r of ordered) {
    const result = r.status === "pass" ? "✅ pass" : r.timedOut ? "❌ timed out" : `❌ fail (exit ${r.exitCode})`;
    lines.push(`| ${result} | \`${r.name}\` | ${r.kind} | ${r.seconds.toFixed(1)}s |`);
  }
  for (const test of excluded) {
    lines.push(`| ⏭️ excluded | \`${test.name}\` | ${test.kind} | — |`);
  }
  if (excluded.length > 0) {
    lines.push("");
    lines.push("### Excluded (not run in CI)");
    lines.push("");
    for (const test of excluded) lines.push(`- \`${test.name}\`: ${EXCLUDED[test.name]}`);
  }
  if (unscripted.length > 0) {
    lines.push("");
    lines.push(
      `${unscripted.length} test file(s) have no \`test:*\` script in package.json. ` +
        "They run here anyway; give each a script so it can be run by name.",
    );
  }
  appendFileSync(file, `${lines.join("\n")}\n`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  let tests = discover();
  const problems = validate(tests);
  if (problems.length > 0) {
    for (const problem of problems) {
      console.error(`ERROR: ${problem}`);
      if (onActions) console.log(`::error title=Test runner configuration::${escapeAnnotation(problem)}`);
    }
    process.exit(2);
  }
  if (args.only) tests = tests.filter((test) => test.name.includes(args.only));

  const excluded = tests.filter((test) => EXCLUDED[test.name]);
  const runnable = tests.filter((test) => !EXCLUDED[test.name]);
  const jobs = args.jobs ?? Math.max(1, os.availableParallelism?.() ?? os.cpus().length);

  if (args.list) {
    for (const test of runnable) console.log(`run       ${test.name}  [${test.kind}]  ${test.command}`);
    for (const test of excluded) console.log(`excluded  ${test.name}  ${EXCLUDED[test.name]}`);
    console.log(`\n${runnable.length} to run, ${excluded.length} excluded.`);
    return;
  }

  console.log(`Running ${runnable.length} tests (${excluded.length} excluded) with ${jobs} parallel jobs.\n`);
  const started = Date.now();
  const results = await runAll(runnable, jobs, args.timeoutMs, report);
  const totalSeconds = (Date.now() - started) / 1000;
  writeSummary(results, excluded, totalSeconds);

  const failed = results.filter((r) => r.status === "fail");
  console.log(
    `\n${results.length - failed.length} passed, ${failed.length} failed, ${excluded.length} excluded ` +
      `in ${totalSeconds.toFixed(0)}s.`,
  );
  for (const test of excluded) console.log(`  excluded: ${test.name} (${EXCLUDED[test.name]})`);
  if (failed.length > 0) {
    console.log("\nFailed:");
    for (const r of failed) console.log(`  ${r.name}`);
    process.exit(1);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(2);
});
