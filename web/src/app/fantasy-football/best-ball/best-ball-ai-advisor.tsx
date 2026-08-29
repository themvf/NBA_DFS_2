"use client";

import { useEffect, useRef, useState } from "react";
import {
  bestBallAdvisorDraftSignature,
  type BestBallAdvisorProvider,
  type BestBallAdvisorResult,
} from "@/lib/fantasy-football/ai-draft-advisor";
import { requestBestBallAdvice } from "./advisor-actions";
import { formatProjectionModelLabel } from "@/lib/fantasy-football/projection-model";

type ProviderState = {
  loading: boolean;
  loadingMode: "base" | "news" | null;
  error: string | null;
  result: BestBallAdvisorResult | null;
  resultUsedNews: boolean;
};

const EMPTY_STATE: ProviderState = { loading: false, loadingMode: null, error: null, result: null, resultUsedNews: false };

function formatProjection(value: number | null): string {
  return value === null ? "—" : value.toFixed(1);
}

function formatNewsDate(value: string | null): string {
  if (!value) return "date unknown";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleDateString();
}

function RecommendationCard({
  provider,
  state,
  configured,
  showNewsButton,
  onRequest,
  onDraft,
  canDraftRecommendation,
  projectionModel,
}: {
  provider: BestBallAdvisorProvider;
  state: ProviderState;
  configured: boolean;
  showNewsButton: boolean;
  onRequest: (provider: BestBallAdvisorProvider, withNews: boolean) => void;
  onDraft: (playerId: number) => void;
  canDraftRecommendation: boolean;
  projectionModel: string;
}) {
  const label = provider === "openai" ? "OpenAI" : "DeepSeek";
  const model = provider === "openai" ? "GPT-5.6 Luna" : "DeepSeek V4 Flash";
  const projectionLabel = formatProjectionModelLabel(projectionModel);
  const accent = provider === "openai"
    ? "border-emerald-300 bg-emerald-50 text-emerald-950"
    : "border-violet-300 bg-violet-50 text-violet-950";
  return <article className={`rounded-2xl border p-5 ${accent}`}>
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div>
        <p className="text-xs font-black uppercase tracking-widest opacity-70">{label} recommendation</p>
        <h3 className="mt-1 text-xl font-black">{model}</h3>
        <p className="text-xs opacity-70">Independent analysis · {projectionLabel} evidence</p>
      </div>
      <div className="flex flex-wrap justify-end gap-2">
        <button
          type="button"
          disabled={state.loading || !configured}
          onClick={() => onRequest(provider, false)}
          className="rounded-xl bg-slate-950 px-4 py-2.5 text-sm font-black text-white shadow-sm disabled:cursor-wait disabled:opacity-60"
        >
          {!configured ? "Connection needed" : state.loading && state.loadingMode === "base" ? "Analyzing…" : state.result && !showNewsButton ? `Ask ${label} again` : `Ask ${label}`}
        </button>
        {showNewsButton && <button
          type="button"
          disabled={state.loading || !configured}
          onClick={() => onRequest(provider, true)}
          title="Slower: checks recent news for the top candidates before answering"
          className="rounded-xl border border-current/30 bg-white/60 px-4 py-2.5 text-sm font-black shadow-sm disabled:cursor-wait disabled:opacity-60"
        >
          {state.loading && state.loadingMode === "news" ? "Reading news…" : "Ask with news"}
        </button>}
      </div>
    </div>

    {!configured && <div className="mt-4 rounded-xl border border-amber-300 bg-amber-50 p-3 text-sm text-amber-950"><b>{label} needs a one-time connection.</b><p className="mt-1">Add <code>{provider === "openai" ? "OPENAI_API_KEY (or OPENAI_API)" : "DEEPSEEK_API_KEY"}</code> to Vercel Production and Preview, then redeploy.</p></div>}
    {configured && state.error && <div role="alert" className="mt-4 rounded-xl border border-red-300 bg-white/80 p-3 text-sm font-semibold text-red-800">{state.error}</div>}
    {configured && !state.loading && !state.error && !state.result && <p className="mt-5 rounded-xl bg-white/60 p-4 text-sm">Press a button when you want this model to evaluate your next pick.{showNewsButton && " \"Ask with news\" also checks recent news for the top candidates first, but takes longer."}</p>}

    {state.result && <div className="mt-5 space-y-4">
      <div className="rounded-2xl bg-white p-4 shadow-sm">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="text-xs font-bold uppercase tracking-wide opacity-60">Pick {state.result.targetOverallPick ?? "—"} · {state.result.confidenceProvided ? `${state.result.confidence}% confidence` : "confidence not stated"}</p>
            <p className="mt-1 text-2xl font-black">{state.result.recommendation.name}</p>
            <p className="text-sm opacity-75">{state.result.recommendation.position} · {state.result.recommendation.team ?? "FA"} · Bye {state.result.recommendation.byeWeek ?? "—"}</p>
          </div>
          <button type="button" disabled={!canDraftRecommendation} onClick={() => onDraft(state.result!.recommendation.playerId)} className="rounded-xl bg-blue-600 px-4 py-2.5 text-sm font-black text-white disabled:cursor-not-allowed disabled:opacity-40">{canDraftRecommendation ? "Add this player" : "Waiting for your pick"}</button>
        </div>
        <div className="mt-3 grid grid-cols-3 gap-2 text-center text-xs">
          <div className="rounded-lg bg-slate-100 p-2"><span className="block opacity-60">Our {projectionLabel}</span><b>{formatProjection(state.result.recommendation.ourProjectedPoints)}</b></div>
          <div className="rounded-lg bg-slate-100 p-2"><span className="block opacity-60">FantasyPros</span><b>{formatProjection(state.result.recommendation.fantasyProsProjectedPoints)}</b></div>
          <div className="rounded-lg bg-slate-100 p-2"><span className="block opacity-60">ADP</span><b>{formatProjection(state.result.recommendation.adp)}</b></div>
        </div>
      </div>

      <div><p className="text-xs font-black uppercase tracking-wide opacity-60">Why now</p><p className="mt-1 text-sm leading-6">{state.result.whyNow}</p></div>
      <div><p className="text-xs font-black uppercase tracking-wide opacity-60">How this fits your roster</p><p className="mt-1 text-sm leading-6">{state.result.rosterFit}</p></div>

      <div className="grid gap-3 md:grid-cols-2">
        <div className="rounded-xl bg-white/70 p-3"><p className="text-xs font-black uppercase opacity-60">Evidence used</p><ul className="mt-2 space-y-1 text-sm">{state.result.evidence.map((item) => <li key={item}>• {item}</li>)}</ul></div>
        <div className="rounded-xl bg-white/70 p-3"><p className="text-xs font-black uppercase opacity-60">Risks</p><ul className="mt-2 space-y-1 text-sm">{state.result.risks.map((item) => <li key={item}>• {item}</li>)}</ul></div>
      </div>

      {state.resultUsedNews && <div className="rounded-xl bg-white/70 p-3">
        <p className="text-xs font-black uppercase opacity-60">News checked</p>
        {state.result.newsSources.length === 0
          ? <p className="mt-2 text-sm opacity-80">Searched for recent news on the top candidates; nothing relevant was found.</p>
          : <ul className="mt-2 space-y-2 text-sm">{state.result.newsSources.map((source) => <li key={source.url}>
            <a href={source.url} target="_blank" rel="noopener noreferrer" className="font-semibold underline underline-offset-2">{source.title}</a>
            <span className="ml-2 opacity-60">{source.player} · {formatNewsDate(source.publishedAt)}</span>
            {source.summary && <p className="mt-0.5 opacity-80">{source.summary}</p>}
          </li>)}</ul>}
      </div>}

      <div>
        <p className="text-xs font-black uppercase tracking-wide opacity-60">If he goes, consider</p>
        <div className="mt-2 space-y-2">{state.result.alternatives.map((player, index) => <div key={player.playerId} className="rounded-xl bg-white/70 p-3 text-sm"><b>{index + 2}. {player.name}</b><span className="ml-2 opacity-70">{player.position} · ADP {formatProjection(player.adp)}</span><p className="mt-1 opacity-80">{player.reason}</p></div>)}</div>
      </div>
      <div className="rounded-xl border border-current/15 p-3 text-sm"><b>Until your next pick:</b> {state.result.strategyUntilNextTurn}</div>
      <details className="text-sm"><summary className="cursor-pointer font-bold">What would change this answer?</summary><p className="mt-2 leading-6">{state.result.whatWouldChange}</p></details>
      <p className="text-[11px] opacity-60">{state.result.model} · {state.result.projectionModel} · snapshot after {state.result.draftedCount} picks · generated {new Date(state.result.generatedAt).toLocaleTimeString()}</p>
    </div>}
  </article>;
}

export default function BestBallAiAdvisor({
  rankingSetId,
  userSlot,
  playerIds,
  availability,
  onDraft,
  canDraftRecommendation,
  projectionModel,
}: {
  rankingSetId: number;
  userSlot: number;
  playerIds: number[];
  availability: Record<BestBallAdvisorProvider, boolean>;
  onDraft: (playerId: number) => void;
  canDraftRecommendation: boolean;
  projectionModel: string;
}) {
  const projectionLabel = formatProjectionModelLabel(projectionModel);
  const signature = bestBallAdvisorDraftSignature({ rankingSetId, userSlot, playerIds });
  const signatureRef = useRef(signature);
  useEffect(() => {
    signatureRef.current = signature;
  }, [signature]);
  const [states, setStates] = useState<Record<BestBallAdvisorProvider, ProviderState>>({
    openai: EMPTY_STATE,
    deepseek: EMPTY_STATE,
  });

  const onRequest = async (provider: BestBallAdvisorProvider, withNews: boolean) => {
    const requestedSignature = signature;
    const loadingMode = withNews ? "news" : "base";
    setStates((current) => ({ ...current, [provider]: { ...current[provider], loading: true, loadingMode, error: null } }));
    const response = await requestBestBallAdvice({ provider, rankingSetId, userSlot, playerIds, withNews });
    if (signatureRef.current !== requestedSignature) {
      // The draft moved on while this request was in flight -- discard the
      // now-stale response, but still clear loading so the button doesn't
      // stay stuck on "Analyzing..." forever.
      setStates((current) => ({ ...current, [provider]: { ...current[provider], loading: false, loadingMode: null } }));
      return;
    }
    setStates((current) => ({
      ...current,
      [provider]: response.ok
        ? { loading: false, loadingMode: null, error: null, result: response.result, resultUsedNews: withNews }
        : { loading: false, loadingMode: null, error: response.message, result: null, resultUsedNews: false },
    }));
  };

  return <section className="rounded-3xl border bg-card p-5 shadow-sm">
    <div className="mb-5">
      <p className="text-xs font-black uppercase tracking-[0.22em] text-blue-700">Your next pick</p>
      <h2 className="mt-1 text-2xl font-black">Ask two independent draft advisors</h2>
      <p className="mt-1 max-w-4xl text-sm text-muted-foreground">Both models receive the same rules, your draft slot and roster, every recorded pick, bye weeks, ADP, and the legal {projectionLabel} projection board. Their answers stay separate so you can compare their reasoning. OpenAI can also optionally check recent news for the top candidates before answering; DeepSeek has no equivalent web-search capability.</p>
    </div>
    <div className="grid gap-4 xl:grid-cols-2">
      <RecommendationCard provider="openai" state={states.openai} configured={availability.openai} showNewsButton onRequest={onRequest} onDraft={onDraft} canDraftRecommendation={canDraftRecommendation} projectionModel={projectionModel} />
      <RecommendationCard provider="deepseek" state={states.deepseek} configured={availability.deepseek} showNewsButton={false} onRequest={onRequest} onDraft={onDraft} canDraftRecommendation={canDraftRecommendation} projectionModel={projectionModel} />
    </div>
  </section>;
}
