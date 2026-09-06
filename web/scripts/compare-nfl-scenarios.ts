import { readFile, writeFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import { parseNflDkSalaryCsv, type NflSlateFormat } from "../src/lib/nfl-dfs/dk-salary-csv";
import { compareNflCandidates } from "../src/lib/nfl-dfs/experiment";
import { generateNflCandidates } from "../src/lib/nfl-dfs/lineups";
import { nflDemoBank, nflDemoSlate } from "./fixtures/nfl-scenario-demo";

async function main() {
  const args = process.argv.slice(2);
  const options = new Map<string, string>();
  const valueFlags = new Set(["--format", "--seed", "--draws", "--count", "--target", "--output", "--salary-csv", "--selection-bank", "--evaluation-bank"]);
  let demo = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--demo" && !demo) { demo = true; continue; }
    if (!valueFlags.has(args[i]) || options.has(args[i]) || args[i + 1] === undefined || args[i + 1].startsWith("--")) {
      throw new Error(`Invalid, duplicate, or incomplete argument: ${args[i]}`);
    }
    options.set(args[i], args[++i]);
  }
  const number = (flag: string, fallback: number) => {
    const value = options.has(flag) ? Number(options.get(flag)) : fallback;
    if (!Number.isFinite(value)) throw new Error(`Invalid numeric argument ${flag}`);
    return value;
  };
  const seed = number("--seed", 20260905);
  const count = number("--count", 100);
  const draws = number("--draws", 1000);
  if (!Number.isSafeInteger(count) || count < 1 || count > 1000) throw new Error("--count must be 1..1000");
  if (!Number.isSafeInteger(draws) || draws < 2 || draws > 100_000) throw new Error("--draws must be 2..100000");
  if (!Number.isInteger(seed) || seed < 0 || seed > 0xffff_ffff) throw new Error("--seed must be an unsigned 32-bit integer");
  const format = options.get("--format") ?? "classic";
  if (format !== "classic" && format !== "showdown") throw new Error("--format must be classic or showdown");
  const realFlags = ["--salary-csv", "--selection-bank", "--evaluation-bank"];
  if (demo && realFlags.some((flag) => options.has(flag))) throw new Error("Do not mix --demo and external input files");
  if (!demo && (!realFlags.every((flag) => options.has(flag)) || !options.has("--target"))) {
    throw new Error("Use --demo, or provide --salary-csv, --selection-bank, --evaluation-bank, and --target");
  }
  if (!demo && (options.has("--format") || options.has("--draws"))) throw new Error("External files determine format and draw counts");
  const started = performance.now();
  const slate = demo ? nflDemoSlate(format as NflSlateFormat) : parseNflDkSalaryCsv(await readFile(options.get("--salary-csv")!, "utf8"));
  const selection = demo ? nflDemoBank(slate, seed, draws, "selection") : JSON.parse(await readFile(options.get("--selection-bank")!, "utf8"));
  const evaluation = demo ? nflDemoBank(slate, (seed ^ 0xa5a5a5a5) >>> 0, draws, "evaluation") : JSON.parse(await readFile(options.get("--evaluation-bank")!, "utf8"));
  const search = generateNflCandidates(slate, { count, seed: (seed ^ 0x12345678) >>> 0 });
  if (!search.lineups.length) {
    throw new Error(`No legal candidates found within ${search.attempts} attempts / ${search.nodes} nodes; this is not a proof of infeasibility`);
  }
  const report = compareNflCandidates({ slate, candidates: search.lineups, selection, evaluation,
    target: number("--target", slate.format === "classic" ? 150 : 90), ablationSeed: (seed ^ 0x87654321) >>> 0 });
  const output = {
    ...report,
    execution: { elapsedMs: performance.now() - started, heapUsedBytes: process.memoryUsage().heapUsed, nodeVersion: process.version,
      candidateSearch: { requested: count, seed: (seed ^ 0x12345678) >>> 0, status: search.status, attempts: search.attempts, nodes: search.nodes } },
    poolWarnings: slate.warnings,
  };
  if (options.has("--output")) await writeFile(options.get("--output")!, JSON.stringify(output, null, 2) + "\n", { flag: "wx" });
  console.log(JSON.stringify({ kind: report.kind, source: report.source, candidateCount: report.manifest.candidateCount,
    selectionDraws: report.manifest.selectionDraws, evaluationDraws: report.manifest.evaluationDraws,
    selected: report.selected, execution: output.execution, limitations: report.limitations, output: options.get("--output") ?? null }, null, 2));
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
