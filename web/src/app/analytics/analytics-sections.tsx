import type { ReactNode } from "react";

const SECTION_TIMEOUT_MS = 25_000;

export const PERFECT_LINEUP_TIMEOUT_MS = 30_000;

export async function safeAnalyticsSection(
  label: string,
  render: () => Promise<ReactNode>,
  timeoutMs = SECTION_TIMEOUT_MS,
): Promise<ReactNode> {
  const start = Date.now();
  try {
    const result = await Promise.race([
      render(),
      new Promise<never>((_, reject) =>
        setTimeout(
          () => reject(new Error(`Timed out after ${timeoutMs / 1000}s`)),
          timeoutMs,
        )
      ),
    ]);
    console.log(`[analytics] ${label} OK ${Date.now() - start}ms`);
    return result ?? null;
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    const stack = error instanceof Error ? (error.stack ?? "") : "";
    console.error(`[analytics] ${label} FAILED ${Date.now() - start}ms — ${msg}\n${stack}`);
    return (
      <div className="mx-auto mt-4 max-w-5xl rounded border border-amber-200 bg-amber-50 px-4 py-2 text-xs text-amber-700">
        <span className="font-semibold">{label}</span>
        {": "}
        <span className="font-mono">{msg}</span>
      </div>
    );
  }
}

export async function AnalyticsSection({
  label,
  render,
  timeoutMs = SECTION_TIMEOUT_MS,
}: {
  label: string;
  render: () => Promise<ReactNode>;
  timeoutMs?: number;
}) {
  return safeAnalyticsSection(label, render, timeoutMs);
}

export function SectionFallback({
  label,
}: {
  label: string;
}) {
  return (
    <div className="mx-auto mt-4 max-w-5xl rounded border border-slate-200 bg-white px-4 py-3 text-xs text-slate-500">
      <span className="font-semibold text-slate-700">{label}</span>
      {": "}
      <span>Loading...</span>
    </div>
  );
}
