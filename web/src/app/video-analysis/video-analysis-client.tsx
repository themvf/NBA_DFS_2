"use client";

import { useState, useTransition } from "react";
import { analyzeVideo, type RecentVideoAnalysis, type VideoAnalysisResult } from "./actions";

const SPORT_ICON: Record<string, string> = {
  nba: "🏀",
  mlb: "⚾",
  nfl: "🏈",
  nhl: "🏒",
  soccer: "⚽",
  tennis: "🎾",
};

function SubjectCard({ subject }: { subject: VideoAnalysisResult["subjects"][number] }) {
  return (
    <div className="rounded-lg border bg-card p-3">
      <div className="mb-1 flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5">
          {subject.sport && <span aria-hidden="true">{SPORT_ICON[subject.sport] ?? "🏆"}</span>}
          <span className="font-medium">{subject.name}</span>
          <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] uppercase text-muted-foreground">
            {subject.type}
          </span>
        </div>
        {subject.timestamp && (
          <span className="text-xs text-muted-foreground">@ {subject.timestamp}</span>
        )}
      </div>
      <p className="text-sm text-muted-foreground">{subject.summary}</p>
    </div>
  );
}

export function VideoAnalysisClient({ initialRecent }: { initialRecent: RecentVideoAnalysis[] }) {
  const [url, setUrl] = useState("");
  const [isPending, startTransition] = useTransition();
  const [message, setMessage] = useState<{ ok: boolean; text: string } | null>(null);
  const [result, setResult] = useState<
    (VideoAnalysisResult & { title: string | null; videoId: string; cached: boolean }) | null
  >(null);
  const [recent, setRecent] = useState(initialRecent);

  function handleAnalyze(forceRefresh = false) {
    if (!url.trim()) return;
    startTransition(async () => {
      setMessage(null);
      const res = await analyzeVideo(url.trim(), forceRefresh);
      setMessage({ ok: res.ok, text: res.message });
      if (res.ok && res.result) {
        setResult(res.result);
        setRecent((prev) => {
          const withoutDup = prev.filter((r) => r.videoId !== res.result!.videoId);
          return [
            {
              videoId: res.result!.videoId,
              videoUrl: url.trim(),
              title: res.result!.title,
              channelName: null,
              createdAt: new Date(),
              subjectCount: res.result!.subjects.length,
            },
            ...withoutDup,
          ].slice(0, 20);
        });
      }
    });
  }

  return (
    <main className="mx-auto max-w-4xl space-y-6 p-4">
      <div>
        <h1 className="text-xl font-bold">📺 Video Analysis</h1>
        <p className="text-sm text-muted-foreground">
          Paste a YouTube video URL to get a per-team/per-player breakdown of what was
          discussed. Works across any sport. Results are cached per video.
        </p>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <input
          type="text"
          placeholder="https://www.youtube.com/watch?v=..."
          value={url}
          onChange={(e) => setUrl(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleAnalyze(false)}
          className="min-w-[300px] flex-1 rounded border bg-background px-3 py-2 text-sm"
        />
        <button
          onClick={() => handleAnalyze(false)}
          disabled={isPending || !url.trim()}
          className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white disabled:opacity-50"
        >
          {isPending ? "Analyzing…" : "Analyze"}
        </button>
      </div>

      {message && (
        <div
          className={`rounded-lg border p-3 text-sm ${
            message.ok
              ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
              : "border-rose-500/40 bg-rose-500/10 text-rose-700 dark:text-rose-300"
          }`}
        >
          {message.text}
        </div>
      )}

      {result && (
        <div className="space-y-3">
          <div className="flex items-center justify-between gap-2">
            <h2 className="font-semibold">{result.title ?? "Video"}</h2>
            {result.cached && (
              <button
                onClick={() => handleAnalyze(true)}
                disabled={isPending}
                className="text-xs text-muted-foreground underline hover:text-foreground"
              >
                Re-analyze (ignore cache)
              </button>
            )}
          </div>
          <p className="text-sm text-muted-foreground">{result.videoSummary}</p>
          {result.subjects.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No teams or players were clearly identified in this video.
            </p>
          ) : (
            <div className="grid gap-2 sm:grid-cols-2">
              {result.subjects.map((s, i) => (
                <SubjectCard key={i} subject={s} />
              ))}
            </div>
          )}
        </div>
      )}

      {recent.length > 0 && (
        <div>
          <h3 className="mb-2 text-sm font-semibold text-muted-foreground uppercase tracking-wide">
            Recently analyzed
          </h3>
          <ul className="space-y-1">
            {recent.map((r) => (
              <li key={r.videoId}>
                <button
                  onClick={() => {
                    setUrl(r.videoUrl);
                    handleAnalyze(false);
                  }}
                  className="w-full rounded border bg-card px-3 py-2 text-left text-sm hover:bg-accent/40"
                >
                  <span className="font-medium">{r.title ?? r.videoId}</span>{" "}
                  <span className="text-muted-foreground">
                    — {r.subjectCount} subject{r.subjectCount === 1 ? "" : "s"}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
    </main>
  );
}
