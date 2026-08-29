export function normalizeProjectionModelVersion(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}

export function requireProjectionModelVersion(value: unknown): string {
  const normalized = normalizeProjectionModelVersion(value);
  if (!normalized) throw new Error("The ranking snapshot does not identify its projection model.");
  return normalized;
}

export function formatProjectionModelLabel(value: unknown): string {
  const normalized = normalizeProjectionModelVersion(value);
  if (!normalized) return "Unversioned";
  const independent = /^ff-independent-(v\d+(?:\.\d+)*)$/i.exec(normalized);
  return independent ? independent[1].toUpperCase() : normalized;
}
