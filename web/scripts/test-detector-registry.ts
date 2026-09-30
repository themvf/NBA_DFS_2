// The detector registry is duplicated by hand between model/line_alerts.py
// (source of truth for the CLI report) and web/src/db/queries.ts (source of
// truth for the /vegas/detectors page and the per-sport Detector Health
// panels). CLAUDE.md records that they are "kept in sync by hand". On
// 2026-09-29 eight detectors registered in Python (seven NFL market-structure
// types and tennis pinnacle_favorite_forward) were missing from the web copy,
// so the page could never show them dead or alive. This test reads both
// files and fails on any drift in either direction, including dates.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";

const ROOT = path.resolve(__dirname, "..", "..");
const py = readFileSync(path.join(ROOT, "model", "line_alerts.py"), "utf8");
const ts = readFileSync(path.join(ROOT, "web", "src", "db", "queries.ts"), "utf8");

type Entry = { sport: string; alertType: string; deployedAt: string };
const key = (e: Entry) => `${e.sport}/${e.alertType}@${e.deployedAt}`;
const iso = (y: string, m: string, d: string) => `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;

function pythonRegistry(): Entry[] {
  const start = py.indexOf("DETECTOR_REGISTRY: list[dict] = [");
  assert.ok(start >= 0, "Python DETECTOR_REGISTRY not found");
  const block = py.slice(start, py.indexOf("\n]", start));
  // Module-level string constants used as alert_type values, e.g. _TENNIS_PIN_FORWARD_TYPE.
  const constants = new Map<string, string>();
  for (const m of py.matchAll(/^(_[A-Z_]+_TYPE)\s*=\s*"(\w+)"/gm)) constants.set(m[1], m[2]);
  const entries: Entry[] = [];
  // Literal rows: {"sport": "mlb", "alert_type": "steam", "deployed_at": date(2026, 7, 2)}
  for (const m of block.matchAll(/\{"sport":\s*"(\w+)",\s*"alert_type":\s*("(\w+)"|(\w+)),\s*"deployed_at":\s*date\((\d+),\s*(\d+),\s*(\d+)\)\}/g)) {
    const [, sport, , literal, constant, y, mo, d] = m;
    // A comprehension row ("alert_type": kind) is handled below, not here.
    if (constant === "kind") continue;
    const alertType = literal ?? constants.get(constant!);
    assert.ok(alertType, `unresolved alert_type constant ${constant}`);
    entries.push({ sport, alertType, deployedAt: iso(y, mo, d) });
  }
  // Comprehension rows: *[{"sport": "nhl", "alert_type": kind, "deployed_at": date(...)} for kind in ("a", "b")]
  for (const m of block.matchAll(/\*\[\{"sport":\s*"(\w+)",\s*"alert_type":\s*kind,\s*"deployed_at":\s*date\((\d+),\s*(\d+),\s*(\d+)\)\}\s*for kind in \(([^)]*)\)\]/g)) {
    const [, sport, y, mo, d, kinds] = m;
    for (const k of kinds.matchAll(/"(\w+)"/g)) entries.push({ sport, alertType: k[1], deployedAt: iso(y, mo, d) });
  }
  return entries;
}

function webRegistry(): Entry[] {
  const start = ts.indexOf("const DETECTOR_REGISTRY");
  assert.ok(start >= 0, "web DETECTOR_REGISTRY not found");
  const block = ts.slice(start, ts.indexOf("\n];", start));
  return [...block.matchAll(/sport:\s*"(\w+)",\s*alertType:\s*"(\w+)",\s*deployedAt:\s*"([\d-]+)"/g)]
    .map(([, sport, alertType, deployedAt]) => ({ sport, alertType, deployedAt }));
}

const python = pythonRegistry();
const web = webRegistry();
assert.ok(python.length >= 60, `Python parse looks incomplete: ${python.length} entries`);
assert.ok(web.length >= 60, `web parse looks incomplete: ${web.length} entries`);

const pySet = new Set(python.map(key));
const webSet = new Set(web.map(key));
const onlyPython = [...pySet].filter((k) => !webSet.has(k)).sort();
const onlyWeb = [...webSet].filter((k) => !pySet.has(k)).sort();
assert.deepEqual(onlyPython, [], `registered in Python but invisible on the web detector pages: ${onlyPython.join(", ")}`);
assert.deepEqual(onlyWeb, [], `registered on the web but not in Python: ${onlyWeb.join(", ")}`);
assert.equal(new Set(web.map((e) => `${e.sport}/${e.alertType}`)).size, web.length, "duplicate web entries");

// Every sport in the registry must be rendered by the cross-sport status page.
const page = readFileSync(path.join(ROOT, "web", "src", "app", "vegas", "detectors", "page.tsx"), "utf8");
const order = page.match(/const SPORT_ORDER = \[([^\]]*)\]/);
assert.ok(order, "SPORT_ORDER not found on the detectors page");
const rendered = new Set([...order![1].matchAll(/"(\w+)"/g)].map((m) => m[1]));
const registrySports = [...new Set(web.map((e) => e.sport))].sort();
const hidden = registrySports.filter((s) => !rendered.has(s));
assert.deepEqual(hidden, [], `sports with registered detectors that /vegas/detectors never renders: ${hidden.join(", ")}`);

console.log(`RESULT: detector-registry checks passed (${web.length} detectors, ${registrySports.length} sports)`);
