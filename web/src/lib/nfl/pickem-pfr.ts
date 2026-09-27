/** Descriptive game charting, never a probability adjustment. */
export type PfrPlayer = { name: string; id: string; team: string; section: string; stats: Record<string, number | null>;
  gsisId: string | null; identityStatus: string; sourceTeam: string };
export type PfrGame = { gameId: string; week: number; capturedAt: string | null; sourceUrl: string | null;
  missing: string[]; players: PfrPlayer[]; manifest: {
    snapshotId: number | null; recordedAt: string | null; parserVersion: string | null;
    schemaVersion: string | null; provider: string | null; sourceSha256: string | null;
    sourceFiles: unknown[]; identity: unknown; identityResolution: "frozen" | "unresolved";
    teamAliasVersion: "pfr-team-alias-v1";
  } };
export type PfrEvidence = { team: string; games: PfrGame[] };
export const pfrTeam = (s: string) => ({ LA: "LAR", WAS: "WSH", JAC: "JAX", AZ: "ARI" }[s] ?? s);
const sections = ["passing_advanced", "rushing_advanced", "receiving_advanced", "defense_advanced"];
const fields: Record<string, string[]> = {
  passing_advanced: ["times_pressured", "times_pressured_pct", "times_sacked", "times_blitzed", "passing_drops", "passing_bad_throws"],
  rushing_advanced: ["carries", "rushing_yards_before_contact", "rushing_yards_after_contact"],
  receiving_advanced: ["receiving_drop", "receiving_broken_tackles"],
  defense_advanced: ["def_pressures", "def_targets", "def_missed_tackles"],
};

export function pfrGame(gameId: string, week: number, capturedAt: string | null, raw: unknown,
  snapshot?: { snapshotId: number; recordedAt: string | null; parserVersion: string | null; sourceSha256: string | null }): PfrGame {
  const payload = raw as { stats_schema?: string; source_url?: string; coverage?: Record<string, {status?: string}>;
    source_provider?: string; source_files?: unknown[]; source_sha256?: string; parser_version?: string;
    identity_manifest?: unknown;
    rows?: Array<{player_name: string; pfr_player_id: string; team: string; section: string; stats: Record<string, unknown>;
      gsis_id?: string; identity_status?: string}> } | null;
  const valid = payload?.stats_schema === "nflverse_pfr_fields_percentage_points";
  const players = valid ? (payload.rows ?? []).filter(r => sections.includes(r.section)).map(r => ({
    name: r.player_name, id: r.pfr_player_id, team: pfrTeam(r.team), section: r.section,
    sourceTeam: r.team, gsisId: payload?.identity_manifest && r.identity_status === "resolved" ? r.gsis_id ?? null : null,
    identityStatus: payload?.identity_manifest ? r.identity_status ?? "unresolved" : "unresolved",
    stats: Object.fromEntries(fields[r.section].map(k => [k, typeof r.stats?.[k] === "number" && Number.isFinite(r.stats[k]) ? r.stats[k] : null])),
  })) : [];
  return { gameId, week, capturedAt, sourceUrl: valid && payload?.source_url?.startsWith("https://") ? payload.source_url : null,
    manifest: { snapshotId: snapshot?.snapshotId ?? null, recordedAt: snapshot?.recordedAt ?? null,
      parserVersion: snapshot?.parserVersion ?? payload?.parser_version ?? null,
      schemaVersion: payload?.stats_schema ?? null, provider: payload?.source_provider ?? null,
      sourceSha256: snapshot?.sourceSha256 ?? payload?.source_sha256 ?? null,
      sourceFiles: payload?.source_files ?? [], identity: payload?.identity_manifest ?? null,
      identityResolution: payload?.identity_manifest ? "frozen" : "unresolved", teamAliasVersion: "pfr-team-alias-v1" },
    missing: sections.filter(s => !valid || payload.coverage?.[s]?.status !== "available"), players };
}
