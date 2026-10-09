"use client";

import { useState, useTransition } from "react";
import { refreshNflPbp } from "./actions";
import p from "./pbp-archetype.module.css";

// "Ingest latest" -- dispatches the GitHub Actions ingest in stale mode, which
// on a game day picks up whatever nflverse has published so far. This replaces
// a terminal `python -m ingest.nfl_pbp_archetypes --relabel-stale` run. The
// work happens in Actions, not here, so success means "dispatched", and the
// new game appears after the run finishes and you reload.
export default function RefreshButton() {
  const [pending, startTransition] = useTransition();
  const [status, setStatus] = useState<{ tone: "good" | "bad"; text: string; url?: string } | null>(null);

  const onClick = () => {
    setStatus(null);
    startTransition(async () => {
      const result = await refreshNflPbp();
      if (result.ok) {
        setStatus({ tone: "good", text: result.message, url: result.runsUrl });
      } else {
        setStatus({ tone: "bad", text: result.error });
      }
    });
  };

  return (
    <div className={p.refresh}>
      <button
        type="button"
        className={p.refreshBtn}
        onClick={onClick}
        disabled={pending}
        aria-busy={pending}
      >
        {pending ? "Dispatching…" : "Ingest latest"}
      </button>
      {status && (
        <span className={p.refreshStatus} data-tone={status.tone} role="status">
          {status.text}
          {status.url && (
            <>
              {" "}
              <a href={status.url} target="_blank" rel="noreferrer">View run</a>
            </>
          )}
        </span>
      )}
    </div>
  );
}
