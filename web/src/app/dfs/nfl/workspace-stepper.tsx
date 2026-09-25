"use client";

import { STAGE_LABELS, WORKSPACE_STAGES, type WorkspaceStage } from "@/lib/nfl-dfs/workspace-stage";

/**
 * The four moments of a slate, in order. The page opens on the step the slate
 * is in (see `recommendedStage`); any step can still be opened by hand.
 */
export default function WorkspaceStepper({ stage, onChange, notes, done }: {
  stage: WorkspaceStage;
  onChange: (stage: WorkspaceStage) => void;
  notes: Record<WorkspaceStage, string>;
  done: Record<WorkspaceStage, boolean>;
}) {
  return <nav aria-label="Workspace steps" className="grid grid-cols-2 gap-2 md:grid-cols-4">
    {WORKSPACE_STAGES.map((id, index) => {
      const current = id === stage;
      return <button key={id} type="button" aria-current={current ? "step" : undefined} onClick={() => onChange(id)}
        className={`rounded-xl border p-3 text-left transition ${current
          ? "border-blue-600 bg-blue-50 ring-1 ring-blue-600"
          : "border-slate-200 bg-white hover:border-slate-300"}`}>
        <span className={`text-xs font-bold ${current ? "text-blue-800" : done[id] ? "text-emerald-700" : "text-slate-500"}`}>
          {done[id] && !current ? "✓ " : ""}{index + 1} · {STAGE_LABELS[id]}
        </span>
        <span className={`mt-0.5 block text-sm ${current ? "text-blue-950" : "text-slate-600"}`}>{notes[id]}</span>
      </button>;
    })}
  </nav>;
}
