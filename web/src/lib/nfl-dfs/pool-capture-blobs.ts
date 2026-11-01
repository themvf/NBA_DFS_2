import { createHash } from 'node:crypto';
import { canonicalAuditJson } from './audit-json';

/**
 * Content-addressed storage for the slate-wide part of a pool capture.
 *
 * Measured 2026-10-31: `context.situationTeams` (every team's workload and
 * situation evidence for the whole slate) was ~77% of each capture's JSON and
 * was copied into every game's capture, every minute of the last 20 before
 * kickoff: 3,261 copies of 293 distinct values, ~1.2 GB of the 1.66 GB table.
 *
 * A capture now stores `{"$poolBlob": <sha256>}` in its place and the value
 * itself once in `nfl_dfs_pool_capture_blobs`. A capture's digest is still the
 * sha256 of the FULL payload, so readers rehydrate before verifying it and an
 * old (inline) capture and a new (compacted) one verify the same way.
 */
export const POOL_BLOB_MARKER = '$poolBlob';
export const POOL_BLOB_DDL = `CREATE TABLE IF NOT EXISTS nfl_dfs_pool_capture_blobs (
  blob_digest TEXT PRIMARY KEY, content JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now())`;

type Json = Record<string, unknown>;
export type PoolBlob = { digest: string; content: unknown };

export const poolBlobDigest = (value: unknown) => createHash('sha256').update(canonicalAuditJson(value)).digest('hex');
const isRef = (v: unknown): v is { [POOL_BLOB_MARKER]: string } =>
  v !== null && typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === 1
  && typeof (v as Json)[POOL_BLOB_MARKER] === 'string';
const context = (payload: unknown): Json | null => {
  const c = (payload as Json | null)?.context;
  return c !== null && typeof c === 'object' && !Array.isArray(c) ? c as Json : null;
};

/** Stored form plus the blobs it names. A payload with nothing to share is returned unchanged. */
export function compactPoolPayload<T>(payload: T): { stored: T; blobs: PoolBlob[] } {
  const ctx = context(payload);
  const teams = ctx?.situationTeams;
  if (!Array.isArray(teams) || teams.length === 0) return { stored: payload, blobs: [] };
  const digest = poolBlobDigest(teams);
  return { stored: { ...payload, context: { ...ctx, situationTeams: { [POOL_BLOB_MARKER]: digest } } } as T,
    blobs: [{ digest, content: teams }] };
}

export function poolBlobRefs(payload: unknown): string[] {
  const teams = context(payload)?.situationTeams;
  return isRef(teams) ? [teams[POOL_BLOB_MARKER]] : [];
}

/** The original payload. Throws when a named blob is missing. */
export function rehydratePoolPayload<T>(payload: T, blobs: ReadonlyMap<string, unknown>): T {
  const ctx = context(payload);
  const teams = ctx?.situationTeams;
  if (!isRef(teams)) return payload;
  const key = teams[POOL_BLOB_MARKER];
  if (!blobs.has(key)) throw new Error(`Pool capture blob ${key} is missing`);
  return { ...payload, context: { ...ctx, situationTeams: blobs.get(key) } } as T;
}
