import { NFL_OPTIMIZER_VERSION } from "@/app/dfs/nfl/nfl-optimizer";
import { NFL_SCENARIO_SCHEMA, NFL_SCORER_VERSION } from "@/lib/nfl-dfs/scenarios";

/**
 * Phase 0 (spec P0-AC1): a single, versioned identity for the NFL DFS build so
 * the live page and every generated run can be traced back to exact code.
 *
 * The settings-schema version is bumped whenever the shape of
 * NflOptimizerSettings changes in a way that alters how a saved run must be
 * interpreted. Legacy runs keep their own recorded version and are never
 * silently reinterpreted under a newer default (spec §16).
 */
export const NFL_SETTINGS_SCHEMA_VERSION = "nfl_gpp_portfolio_v1";

/** Resolved once per process. Vercel bakes these env vars in at build time. */
function resolveCommitSha(): string {
  return (
    process.env.VERCEL_GIT_COMMIT_SHA ??
    process.env.NFL_POOL_CODE_REVISION ??
    process.env.NEXT_PUBLIC_COMMIT_SHA ??
    "local-uncommitted"
  );
}

function resolveBuildTime(): string {
  // Set at build time on deploy. When absent (local dev), say "unknown" rather
  // than fabricate a timestamp the build cannot actually support.
  const value = process.env.NEXT_PUBLIC_BUILD_TIME;
  return value && Number.isFinite(Date.parse(value)) ? value : "unknown";
}

export type NflBuildInfo = {
  /** Deployed commit SHA, or a local/unknown sentinel — never fabricated. */
  commitSha: string;
  /** ISO build timestamp when available, else "unknown". */
  buildTime: string;
  /** Shape version for NflOptimizerSettings and the persisted run record. */
  settingsSchemaVersion: string;
  /** The ILP optimizer implementation version. */
  optimizerVersion: string;
  /** Scenario bank schema + scorer identity used by scenario scoring. */
  scenarioSchemaVersion: number;
  scorerVersion: string;
};

export function nflBuildInfo(): NflBuildInfo {
  return {
    commitSha: resolveCommitSha(),
    buildTime: resolveBuildTime(),
    settingsSchemaVersion: NFL_SETTINGS_SCHEMA_VERSION,
    optimizerVersion: NFL_OPTIMIZER_VERSION,
    scenarioSchemaVersion: NFL_SCENARIO_SCHEMA,
    scorerVersion: NFL_SCORER_VERSION,
  };
}

/** Short display form for the page footer: first 7 chars of the SHA. */
export function shortCommitSha(sha: string): string {
  return sha === "local-uncommitted" || sha === "unknown" ? sha : sha.slice(0, 7);
}
