export type AuditCell = {
  field_id: string; position: string; season: number; n: number; present: number;
  valid: number; missing: number; invalid: number; zero: number; captured: number;
  latest_capture: string | null; status: string;
};
export type FeatureAudit = {
  version: string; evaluated_at: string; study_id: string; production_changed: false;
  fields: { id: string; key: string; label: string; group: string; positions: string[]; unit: string; supported: boolean; aliases: string[] }[];
  datasets: {
    dataset: string; scanned: number; eligible: number; excluded: Record<string, number>;
    canonical_position_fallback: number; seasons: number[]; sources: Record<string, number>; normalization_warning?: string | null;
    input_digest: string; latest_capture: string | null; cells: AuditCell[];
    cohorts: { position: string; rows: number; complete: number; required: string[] }[];
  }[];
  limits: string[]; implementation: Record<string, string>;
};

export function coverage(cell: AuditCell | undefined): number | null {
  return !cell || !cell.n || cell.status === "unsupported" ? null : 100 * cell.valid / cell.n;
}

export function captureAgeHours(captured: string | null, viewedAt: number): number | null {
  if (!captured) return null;
  const time = Date.parse(captured);
  return Number.isFinite(time) && time <= viewedAt ? (viewedAt - time) / 3_600_000 : null;
}
