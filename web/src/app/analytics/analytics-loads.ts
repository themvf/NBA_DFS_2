/**
 * Every analytics section is loaded independently so one slow or broken
 * query cannot take the page down. That isolation used to be `catch {
 * return null }` followed by `?? []`, which rendered a failed query as an
 * empty table: the page said "No NBA accuracy data yet" when the truth was
 * "the accuracy query threw". A missing table and a failed read must look
 * different on screen, so each load now records its outcome and the page
 * lists every section that could not be read, by name, with the reason.
 */
export type SectionLoad<T> =
  | { label: string; ok: true; value: T }
  | { label: string; ok: false; error: string };

export type SectionLoadError = { label: string; error: string };

export async function loadSection<T>(label: string, fn: () => Promise<T>): Promise<SectionLoad<T>> {
  try {
    return { label, ok: true, value: await fn() };
  } catch (error) {
    return { label, ok: false, error: error instanceof Error ? error.message : String(error) };
  }
}

/** A section this sport does not have is neither loaded nor failed. */
export function skippedSection<T>(label: string, empty: T): SectionLoad<T> {
  return { label, ok: true, value: empty };
}

export function sectionValue<T>(load: SectionLoad<T>, fallback: T): T {
  return load.ok ? load.value : fallback;
}

export function sectionErrors(loads: SectionLoad<unknown>[]): SectionLoadError[] {
  return loads.flatMap((load) => (load.ok ? [] : [{ label: load.label, error: load.error }]));
}
