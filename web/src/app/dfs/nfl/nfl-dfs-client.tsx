"use client";

import { generationSettings, sameGenerationSettings } from "@/lib/nfl-dfs/generation-settings";
import { nflIdentityLabel } from "@/lib/nfl-dfs/identity";

import ProjectionAuditPanel from './projection-audit-panel';
import {DEFAULT_SITUATIONS} from '@/lib/nfl-dfs/projection-audit';
import AvailabilityPanel from './availability-panel';
import FieldAuditPanel from './field-audit-panel';
import LiveStatusBanner from './live-status-banner';
import CaptainRangeInput from './captain-range-input';
import type { CaptainTarget } from '@/lib/nfl-dfs/generation-settings';
import { availabilityCoverage } from '@/lib/nfl-dfs/availability-coverage';
import AbsencePreview from './absence-preview';
import CompetitorPanel from './competitor-panel';
import { AlertTriangle, BarChart3, CheckCircle2, Download, FileUp, HelpCircle, Lock, Play, Search, ShieldCheck, Unlock, XCircle } from "lucide-react";
import { useEffect, useMemo, useRef, useState, useTransition } from "react";
import { refreshNflSlateProjections, listSavedNflSlates, loadSavedNflWorkspace, loadSavedNflLineups, readNflOptimizerAudit, applyNflComparison, loadNflSalaryCsv, runNflOptimizer, type NflComparisonSource, type NflWorkspaceSlate } from "./actions";
import type { NflGeneratedLineup, NflOptimizerSettings, NflProjectionSource } from "./nfl-optimizer";
import { DEFAULT_NFL_PUNT_POLICY } from "@/lib/nfl-dfs/punt-policy";
import { PUNT_PRESETS, resolvePuntPreset, describePuntPolicy, type PuntPresetKey } from "@/lib/nfl-dfs/punt-presets";
import { objectiveLabel } from "@/lib/nfl-dfs/ownership-capability";
import { ARCHETYPE_LABELS, type ArchetypeId, type ArchetypeQuota } from "@/lib/nfl-dfs/archetypes";
import { runNflPreExportQa, NFL_QA_RULESET_VERSION, type QaReport, type QaOverride } from "@/lib/nfl-dfs/pre-export-qa";
import { parseNflComparisonCsv } from "@/lib/nfl-dfs/comparison-csv";
import { exportNflDkEntries } from "@/lib/nfl-dfs/entry-export";
import {DEFAULT_WORKLOAD_POSITIONS} from "@/lib/nfl-dfs/workload-selection";
import WorkloadProjections from "./workload-projections";
import CalibratedProjections from "./calibrated-projections";
import NflLineupVisualizations from "./nfl-lineup-visualizations";
import PlayerExplanationPanel from "./player-explanation-panel";
import type { NflWorkspacePlayer } from "./actions";
import { POOL_POSITION_FILTERS, POOL_FILTER_LABELS, isPoolPositionFilter, matchesPoolPosition, poolPositionCounts, type PoolPositionFilter } from "@/lib/nfl-dfs/player-pool-filter";
import { NFL_FLEX_POSITIONS } from "@/lib/nfl-dfs/dk-salary-csv";
import { buildValueIndex } from "@/lib/nfl-dfs/salary-value";
import { ValueChip, ValueLegend } from "./value-chip";
import { DEFAULT_POOL_SORT, nextPoolSort, sortPlayerPool, type PoolSort, type PoolSortKey } from "@/lib/nfl-dfs/player-pool-sort";
import { PoolTableHeader } from "./pool-table-header";
import LineupReview from "./lineup-review";
import "./nfl-workspace.css";

// Position filter options, including FLEX (RB/WR/TE). The membership rule lives
// in `player-pool-filter.ts` and reads the optimizer's own NFL_FLEX_POSITIONS,
// so the dropdown and the FLEX roster slot can never disagree.
const SOURCE_LABELS: Record<NflProjectionSource, string> = { our: "Our historical model", workload: "Position workload (experimental)", calibrated: "Calibrated QB/DST (experimental)", dk_avg: "DK average", fantasypros: "FantasyPros", linestar: "LineStar", custom: "Custom" };
const points = (value: number | null) => value == null ? "—" : value.toFixed(1);
const pct = (value: number | null) => value == null ? "—" : `${value.toFixed(1)}%`;
const dollars = (value: number) => `$${value.toLocaleString()}`;

function downloadText(name: string, content: string) {
  const url = URL.createObjectURL(new Blob([content], { type: name.endsWith(".json")?"application/json":"text/csv;charset=utf-8" }));
  const anchor = document.createElement("a"); anchor.href = url; anchor.download = name; anchor.click(); URL.revokeObjectURL(url);
}

export default function NflDfsClient() {
  const salaryRef = useRef<HTMLInputElement>(null), comparisonRef = useRef<HTMLInputElement>(null), entryRef = useRef<HTMLInputElement>(null);
  const [pending, startTransition] = useTransition();
  const [savedSlates, setSavedSlates] = useState<Awaited<ReturnType<typeof listSavedNflSlates>>>([]);
  const [savedRuns, setSavedRuns] = useState<Awaited<ReturnType<typeof loadSavedNflWorkspace>>['runs']>([]);
  const [libraryId, setLibraryId] = useState('');
  const [libraryLoading, setLibraryLoading] = useState(true);
  const [slate, setSlate] = useState<NflWorkspaceSlate | null>(null);
  const [lineups, setLineups] = useState<NflGeneratedLineup[]>([]);
  const [runId, setRunId] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null), [error, setError] = useState<string | null>(null);
  const [playerPage, setPlayerPage] = useState(1);
  const [query, setQuery] = useState(""), [position, setPosition] = useState<PoolPositionFilter>("ALL");
  const [sort, setSort] = useState<PoolSort>(DEFAULT_POOL_SORT);
  const [comparisonSource, setComparisonSource] = useState<NflComparisonSource>("fantasypros");
  const [entryFile, setEntryFile] = useState<File | null>(null);
  const [locked, setLocked] = useState<number[]>([]), [excluded, setExcluded] = useState<number[]>([]);
  const [targetExposure, setTargetExposure] = useState<Record<string, number>>({});
  // Showdown captain ranges, in percent. See `captainExposurePolicies`.
  const [captainTargets, setCaptainTargets] = useState<Record<string, CaptainTarget>>({});
  const [completedSettings, setCompletedSettings] = useState<NflOptimizerSettings | null>(null);
  const [showVisuals, setShowVisuals] = useState(false);
  const [workspaceView, setWorkspaceView] = useState("players");
  const [columnView, setColumnView] = useState("essential");
  const [showBuilder, setShowBuilder] = useState(false);
  const [explainPlayer, setExplainPlayer] = useState<NflWorkspacePlayer | null>(null);
  const [eligibility, setEligibility] = useState<import("./nfl-optimizer").NflEligibilityDecision[]>([]);
  const [ownership, setOwnership] = useState<import("@/lib/nfl-dfs/ownership-capability").OwnershipAssessment | null>(null);
  const [exposureReport, setExposureReport] = useState<import("./nfl-optimizer").NflExposureReport[]>([]);
  const [archetypeQuotas, setArchetypeQuotas] = useState<ArchetypeQuota[]>([]);
  const [salaryBands, setSalaryBands] = useState<import("@/lib/nfl-dfs/salary-duplication").SalaryBandReport[]>([]);
  const [duplication, setDuplication] = useState<import("@/lib/nfl-dfs/salary-duplication").LineupDuplication[]>([]);
  const [qaOverrides, setQaOverrides] = useState<QaOverride[]>([]);
  // Phase 4 archetype configuration. "balanced" (default) lets the generator
  // allocate the archetype mix, pick fade targets, and read the Vegas favorite
  // from the game's moneyline; "custom" exposes the manual controls below.
  const [archetypePlanMode, setArchetypePlanMode] = useState<"balanced" | "standard" | "custom" | "chalk_leverage">("balanced");
  const [archetypeFavorite, setArchetypeFavorite] = useState<string>("");
  const [archetypeFades, setArchetypeFades] = useState<number[]>([]);
  // Merge note: main's #216/#217 defaults (minPlayerSalary + observed-history
  // gate) are kept alongside the punt policy — the policy owns cheap-player
  // roles, while requireObservedHistory still removes any-priced players whose
  // projection is a position average, not theirs.
  const [settings, setSettings] = useState({ mode: "gpp" as "cash" | "gpp", projectionSource: "our" as NflProjectionSource, allowDkFallback: false, workloadPositions:{...DEFAULT_WORKLOAD_POSITIONS}, situations:DEFAULT_SITUATIONS, nLineups: 20, minSalary: 45000, minPlayerSalary: 1000, requireObservedHistory: true, maxExposure: .6, minUnique: 2, stackPassCatchers: 1 as 0 | 1 | 2, bringBack: true, randomness: .08, useHeuristicOwnershipLeverage: true, puntPolicy: {...DEFAULT_NFL_PUNT_POLICY} as import("@/lib/nfl-dfs/punt-policy").NflPuntPolicy, puntOverrides: [] as import("@/lib/nfl-dfs/punt-policy").PuntOverride[] });

  const slateTeams = useMemo(() => [...new Set((slate?.players ?? []).map((p) => p.team))].sort(), [slate]);
  const archetypeUnderdog = archetypeFavorite ? slateTeams.find((t) => t !== archetypeFavorite) ?? null : null;
  const fadeConfig = archetypeFades.length ? { fadePlayerIds: archetypeFades } : undefined;
  // Balanced mode sends only the mode: quotas, fade targets and the favorite
  // are resolved by the generator/server. Custom mode sends the manual plan.
  const custom = archetypePlanMode === "custom";
  const currentSettings = { ...generationSettings(settings, slate?.format ?? 'classic', locked, excluded, targetExposure, captainTargets),
    archetypeMode: archetypePlanMode,
    archetypeQuotas: custom && archetypeQuotas.length ? archetypeQuotas : undefined,
    favoriteTeam: custom && archetypeFavorite ? archetypeFavorite : undefined,
    underdogTeam: custom && archetypeFavorite ? archetypeUnderdog ?? undefined : undefined,
    archetypeConfigs: custom && fadeConfig ? { single_chalk_fade: { fadePlayerIds: archetypeFades.slice(0, 1) }, double_fade: { fadePlayerIds: archetypeFades.slice(0, 2) } } : undefined };
  const settingsChanged = Boolean(completedSettings && !sameGenerationSettings(completedSettings, currentSettings));

  const matching = useMemo(() => (slate?.players ?? []).filter((p) => matchesPoolPosition(p.position, position) && (!query.trim() || `${p.name} ${p.team} ${p.opponent ?? ""}`.toLowerCase().includes(query.trim().toLowerCase()))), [slate, position, query]);
  // Counted over the unfiltered pool, so the dropdown describes slate composition
  // rather than moving with every keystroke in the search box.
  const positionCounts = useMemo(() => poolPositionCounts(slate?.players ?? []), [slate]);
  // Availability coverage sits next to the Generate button, not behind a tab:
  // a slate where nothing is known about who is playing built 20 lineups
  // unchallenged on 2026 week 2. See `availability-coverage.ts`.
  const coverage = useMemo(() => availabilityCoverage(slate?.players ?? []), [slate]);
  // Per-position reference distributions are built once per slate, not per row:
  // the pool re-renders on every keystroke in the search box.
  const valueIndex = useMemo(() => buildValueIndex(slate?.players ?? []), [slate]);
  // Sorting runs through the tested module so the ordering rules -- blanks
  // sink, and a row carrying no value verdict sinks below every row that
  // does -- cannot drift away from what the header claims.
  const filtered = useMemo(() => sortPlayerPool(matching, sort, (p) => valueIndex.assess(p as NflWorkspacePlayer)), [matching, sort, valueIndex]);
  function applySort(key: PoolSortKey) { setSort((current) => nextPoolSort(current, key)); setPlayerPage(1); }
  const pages = Math.max(1, Math.ceil(filtered.length / 50));
  const currentPage = Math.min(playerPage, pages);
  const exposures = useMemo(() => {
    const map = new Map<number, { name: string; team: string; n: number }>();
    lineups.forEach((lineup) => lineup.slots.forEach(({ player }) => { const row = map.get(player.dkPlayerId) ?? { name: player.name, team: player.team, n: 0 }; row.n++; map.set(player.dkPlayerId, row); }));
    return [...map.values()].sort((a, b) => b.n - a.n || a.name.localeCompare(b.name));
  }, [lineups]);

  // Phase 6: single pre-export QA decision. Recomputed whenever the run, its
  // reports, or the overrides change — a new run resets overrides (P6-AC3).
  const qaReport: QaReport | null = useMemo(() => {
    if (!lineups.length) return null;
    return runNflPreExportQa({
      format: (completedSettings?.format ?? slate?.format ?? "showdown") as "classic" | "showdown",
      requestedLineups: completedSettings?.nLineups ?? lineups.length,
      lineups: lineups.map((l) => ({ lineupNumber: l.lineupNumber, playerIds: l.playerIds, totalSalary: l.totalSalary, slots: l.slots.map((s) => ({ slot: s.slot, playerId: s.player.dkPlayerId })), archetype: l.archetype ?? null })),
      eligibility: eligibility.map((e) => ({ dkPlayerId: e.dkPlayerId, name: e.name, eligible: e.eligible, reasonCode: e.reasonCode, overridden: e.overridden })),
      exposureReport,
      salaryBandReport: salaryBands,
      duplication,
      ownership: ownership ? { capability: ownership.capability, errors: ownership.errors, features: { leverage: ownership.features.leverage } } : undefined,
      newerRunAvailable: Boolean(slate?.refreshAvailable),
      availabilityCoverage: coverage,
    }, qaOverrides);
  }, [lineups, eligibility, exposureReport, salaryBands, duplication, ownership, completedSettings, slate, coverage, qaOverrides]);

  async function refreshLibrary() {
    const saved = await listSavedNflSlates(); setSavedSlates(saved); return saved;
  }
  // A run's QA inputs belong to THAT run. Loading a different run (or a fresh
  // slate) must never leave a previous generate's eligibility/exposure/QA
  // overrides behind, or the export gate would judge run B against run A.
  function clearRunReports() {
    setEligibility([]); setOwnership(null); setExposureReport([]); setSalaryBands([]); setDuplication([]); setQaOverrides([]);
  }
  function openSaved(uploadId: string, restoreLineups = true) {
    // The chooser's placeholder option carries an empty value. Posting that to
    // the server just to be told it is not a slate turns a no-op into an error
    // banner, so it never leaves the browser.
    if (!uploadId) return;
    setError(null); setMessage(null);
    startTransition(async () => {
      try {
        const next = await loadSavedNflWorkspace(uploadId);
        setSlate(next.slate); setLibraryId(uploadId); setSavedRuns(next.runs);
        setLineups([]); setRunId(null); setCompletedSettings(null); setShowVisuals(false); clearRunReports();
        setLocked([]); setExcluded([]); setTargetExposure({}); setCaptainTargets({}); setEntryFile(null); setQuery(''); setPosition('ALL'); setPlayerPage(1);
        try { localStorage.setItem('nfl-saved-slate', uploadId); } catch { /* Selection memory is optional. */ }
        setMessage('Saved player pool loaded. Availability refreshed.');
        if (restoreLineups && next.runs[0]) {
          try {
            const saved = await loadSavedNflLineups(uploadId, next.runs[0].runId);
            setLineups(saved.lineups); setRunId(saved.runId); setCompletedSettings(saved.settings);
            setMessage(`Restored ${saved.lineups.length} saved lineups. Scores reflect their original run; review current availability before exporting.`);
          } catch (reason) { setError(reason instanceof Error ? reason.message : 'Saved lineups could not be restored.'); }
        }
      } catch (reason) { setError(reason instanceof Error ? reason.message : 'Saved slate could not be loaded.'); }
    });
  }
  useEffect(() => {
    let canceled = false;
    listSavedNflSlates().then(saved => {
      if (canceled) return;
      setSavedSlates(saved); setLibraryLoading(false);
      let remembered: string | null = null;
      try { remembered = localStorage.getItem('nfl-saved-slate'); } catch { /* Use latest. */ }
      const selected = saved.find(s => s.uploadId === remembered) ?? saved[0];
      if (selected) openSaved(selected.uploadId);
    }).catch(() => { if (!canceled) { setLibraryLoading(false); setError('Saved slates could not be listed. Retry or upload a salary file.'); } });
    return () => { canceled = true; };
  }, []);
  function openRun(id: string) {
    if (!slate || !id) return;
    startTransition(async () => {
      try {
        const saved = await loadSavedNflLineups(slate.uploadId, id);
        clearRunReports();
        setLineups(saved.lineups); setRunId(saved.runId); setCompletedSettings(saved.settings); setShowVisuals(false);
        setMessage('Saved lineups restored with their original scores. Review current availability before exporting.'); setError(null);
      } catch (reason) { setError(reason instanceof Error ? reason.message : 'Saved lineups could not be loaded.'); }
    });
  }
  function loadSalary(file: File | null) {
    if (!file) return; setError(null); setMessage(null); setLineups([]); setShowVisuals(false); setTargetExposure({}); setCaptainTargets({}); setLocked([]); setExcluded([]); setRunId(null); clearRunReports();
    const form = new FormData(); form.set("file", file);
    startTransition(async () => { try { const result = await loadNflSalaryCsv(form); setSlate(result); setLibraryId(result.uploadId); setCompletedSettings(null); setEntryFile(null); setSavedRuns((await loadSavedNflWorkspace(result.uploadId)).runs); await refreshLibrary(); try { localStorage.setItem("nfl-saved-slate", result.uploadId); } catch {} setMessage(`${file.name} saved with ${result.players.length} players and linked to the latest projection run.`); } catch (reason) { setError(reason instanceof Error ? reason.message : "Salary upload failed."); } });
  }
  function refreshProjections() {
    if (!slate) return;
    setError(null);
    startTransition(async () => {
      try {
        const next = await refreshNflSlateProjections(slate.uploadId);
        setSlate(next); setLibraryId(next.uploadId); setLineups([]); setRunId(null); setCompletedSettings(null);
        setShowVisuals(false); setEntryFile(null); setExplainPlayer(null);
        setSavedRuns((await loadSavedNflWorkspace(next.uploadId)).runs);
        await refreshLibrary();
        try { localStorage.setItem('nfl-saved-slate', next.uploadId); } catch {}
        setMessage('Created a refreshed projection snapshot. Generate new lineups; previous lineup audits are preserved.');
      } catch (error) { setError(error instanceof Error ? error.message : 'Projection refresh failed.'); }
    });
  }
  function importComparison(file: File | null) {
    if (!file || !slate) return; setError(null); setMessage(null);
    startTransition(async () => { try { const parsed = parseNflComparisonCsv(await file.text()); const result = await applyNflComparison(slate.uploadId, comparisonSource, parsed.rows, file.name); setSlate(result.slate); setMessage(`${SOURCE_LABELS[comparisonSource]}: matched ${result.matched}/${parsed.rows.length}; ${result.unmatched.length} unmatched.${parsed.warnings.length ? ` ${parsed.warnings.length} rows warned.` : ""}`); } catch (reason) { setError(reason instanceof Error ? reason.message : "Comparison import failed."); } });
  }
  function generate() {
    if (!slate) return; setError(null); setMessage(null);
    const payload = currentSettings;
    setShowVisuals(false);
    startTransition(async () => { try { const response = await runNflOptimizer(slate.uploadId, payload); setLineups(response.result.lineups); setEligibility(response.result.eligibility ?? []); setOwnership(response.ownership ?? null); setExposureReport(response.result.exposureReport ?? []); setSalaryBands(response.result.salaryBandReport ?? []); setDuplication(response.result.duplication ?? []); setQaOverrides([]); setSlate(response.slate); setCompletedSettings(payload); setRunId(response.runId); setSavedRuns((await loadSavedNflWorkspace(slate.uploadId)).runs); setMessage(`Saved optimizer run ${response.runId.slice(0, 8)} with ${response.result.lineups.length}/${settings.nLineups} lineups. ${response.result.warnings.join(" ")}`); } catch (reason) { setError(reason instanceof Error ? reason.message : "Optimizer failed."); } });
  }
  function allowCheapPlayer(dkPlayerId: number, name: string) {
    // Salary is never a valid reason — the spec requires a role reason (§8.1).
    const reason = window.prompt(`Allow ${name} for this run. Record why (role/opportunity evidence — salary alone is not valid):`)?.trim();
    if (!reason) return;
    setSettings((s) => ({
      ...s,
      puntPolicy: { ...s.puntPolicy, allowlistedPlayerIds: [...new Set([...s.puntPolicy.allowlistedPlayerIds, dkPlayerId])] },
      puntOverrides: [...s.puntOverrides.filter((o) => o.playerId !== dkPlayerId), { playerId: dkPlayerId, reason, user: "local", at: new Date().toISOString(), slot: "FLEX" as const }],
    }));
    setMessage(`${name} allowed for this run (Flex-only). Regenerate to apply.`);
  }
  function overrideQaCheck(checkId: string) {
    const reason = window.prompt(`Override QA check "${checkId}". Record why:`)?.trim();
    if (!reason) return;
    setQaOverrides((cur) => [...cur.filter((o) => o.checkId !== checkId), { checkId, reason, user: "local", at: new Date().toISOString(), rulesetVersion: NFL_QA_RULESET_VERSION, runId: runId ?? "unsaved" }]);
  }
  async function exportEntries() {
    if (!entryFile || !lineups.length) return;
    // Phase 6 (P6-AC1): export is blocked while any un-overridden blocker remains.
    if (qaReport && qaReport.decision === "blocked") { setError(`Export blocked: ${qaReport.openBlockers.join(", ")}. Resolve or override each blocker first.`); return; }
    try { downloadText(`nfl-lineups-${runId?.slice(0, 8) ?? "export"}.csv`, exportNflDkEntries(await entryFile.text(), lineups)); } catch (reason) { setError(reason instanceof Error ? reason.message : "Entry export failed."); }
  }

  return <div className="nfl-dfs-workspace mx-auto max-w-[1600px] space-y-4">
    <header className="nfl-workspace-heading"><div><p className="text-xs font-semibold uppercase tracking-widest text-emerald-700">DraftKings · NFL</p><h1 className="mt-1 text-2xl font-bold tracking-tight">Lineup workspace</h1><p className="mt-1 text-sm text-slate-500">Research your pool. Build your portfolio. Review and export.</p></div><span className="rounded-full border border-amber-200 bg-amber-50 px-3 py-1 text-xs font-semibold text-amber-900">Experimental model · research use</span></header>
    {message ? <Notice good>{message}</Notice> : null}{error ? <Notice>{error}</Notice> : null}
    {slate?.refreshAvailable && <section className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-amber-300 bg-amber-50 p-4"><p>A newer projection run is available for these games. Existing lineups retain their original projections.</p><button disabled={pending} onClick={refreshProjections} className="rounded-lg bg-blue-700 px-4 py-2 font-bold text-white disabled:opacity-50">Refresh projections</button></section>}
    <details className="nfl-data-tools rounded-xl border bg-white p-4" open={!slate}><summary className="cursor-pointer text-sm font-semibold">Manage slate & data <span className="ml-2 font-normal text-slate-500">Saved slates, salary uploads and source files</span></summary><div className="mt-4 space-y-4">
    <details className="rounded-xl border bg-white p-4"><summary className="cursor-pointer text-sm font-semibold">How this workspace saves and scores lineups</summary><section className="mt-4 grid gap-3 md:grid-cols-4"><Status icon={<FileUp className="h-5 w-5" />} title="Persisted intake" text="Salary pools and source files are fingerprinted and saved." good /><Status icon={<BarChart3 className="h-5 w-5" />} title="Source comparison" text="Our model, DK, FantasyPros, LineStar, or custom—always labeled." good /><Status icon={<ShieldCheck className="h-5 w-5" />} title="Auditable optimizer" text="Settings, input snapshot, digest, and every lineup are retained." good /><Status icon={<AlertTriangle className="h-5 w-5" />} title="No claimed edge yet" text="The historical model has not beaten its recency baseline." /></section></details>
    <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm"><div className="grid gap-4 lg:grid-cols-[1fr_auto] lg:items-end"><div><h2 className="font-bold text-slate-950">1. Choose a saved slate</h2><p className="mt-1 text-sm text-slate-600">Slates and generated lineups are saved in the database. Upload to add or update salaries; refresh projections when a newer run is available.</p></div><input ref={salaryRef} type="file" accept=".csv,text/csv" className="hidden" onChange={(e) => loadSalary(e.target.files?.[0] ?? null)} /><button disabled={pending} onClick={() => salaryRef.current?.click()} className="inline-flex min-h-11 items-center justify-center gap-2 rounded-lg bg-blue-600 px-4 text-sm font-bold text-white disabled:opacity-50"><FileUp className="h-4 w-4" />{slate ? "Replace salaries" : "Upload salaries"}</button></div></section>

    {!!slate?.redistribution?.withheld?.length && <details className="rounded-xl border border-amber-300 bg-amber-50 p-4"><summary className="cursor-pointer text-sm font-semibold">Unresolved absence adjustments — baseline workload retained</summary><p className="mt-2 text-sm">Historical averages below are evidence to review, not incremental vacated work or a team budget. No carries or receptions are added from these pools. Receptions are not targets.</p><div className="mt-3 space-y-3">{slate.redistribution.withheld.map(p => <div key={`${p.team}:${p.pool}`}><strong>{p.team} · {p.unit}</strong><p className="text-xs">{p.reason}</p><ul className="text-sm">{p.donors.map(d => <li key={d.key}>{d.name}: {d.historicalUnits.toFixed(2)} historical {p.unit}; assigned 0</li>)}</ul></div>)}</div><p className="mt-2 text-xs">{slate.redistribution.version}</p></details>}
    {slate?.redistribution?.pools && <details className="rounded-xl border bg-white p-4"><summary className="cursor-pointer text-sm font-semibold">Workload transfers: offered, assigned, and unassigned</summary><table className="mt-3 w-full text-sm"><thead><tr><th>Team / pool</th><th>Offered</th><th>Assigned</th><th>Unassigned</th></tr></thead><tbody>{slate.redistribution.pools.map(p => <tr key={`${p.team}:${p.pool}`}><td>{p.team} / {p.pool}</td><td>{p.offered.toFixed(2)}</td><td>{p.assigned.toFixed(2)}</td><td>{p.unassigned.toFixed(2)}</td></tr>)}</tbody></table><p className="mt-2 text-xs">Unassigned workload includes capped transfers and pools with no eligible recipient.</p></details>}
    <section className="grid gap-4 rounded-xl border border-teal-200 bg-teal-50 p-5 md:grid-cols-2" aria-label="Saved slates and lineups">
      <Field label="Saved slate"><select className="control" value={libraryId} disabled={pending || libraryLoading} onChange={e => openSaved(e.target.value)}><option value="" disabled>{libraryLoading ? 'Loading saved slates…' : 'Choose a slate'}</option>{savedSlates.map(s => <option key={s.uploadId} value={s.uploadId}>{s.label}</option>)}</select></Field>
      <Field label="Saved lineup sets"><select className="control" value={runId ?? ''} disabled={pending || !savedRuns.length} onChange={e => openRun(e.target.value)}><option value="" disabled>{savedRuns.length ? 'Choose a saved run' : 'No generated lineups for this slate yet'}</option>{savedRuns.map(r => <option key={r.runId} value={r.runId}>{new Date(r.createdAt).toLocaleString()} · {r.count} lineups · {r.mode.toUpperCase()} · {SOURCE_LABELS[r.source as NflProjectionSource] ?? r.source}</option>)}</select></Field>
      <p className="text-sm text-teal-900">Your last selected slate reopens automatically. “Generate &amp; save” adds a lineup set to its history.</p>
      <button disabled={pending || libraryLoading} className="justify-self-start rounded-lg border border-teal-700 px-3 py-2 text-sm font-semibold" onClick={() => { if (libraryId) openSaved(libraryId, false); else startTransition(async () => { try { await refreshLibrary(); setError(null); } catch { setError('Saved slate list could not be refreshed.'); } }); }}>Refresh saved player pool</button>
    </section>
        <section className="rounded-xl border bg-white p-4 shadow-sm"><h2 className="font-bold">Comparison sources</h2><p className="mt-1 text-xs text-slate-500">CSV: Name, optional Team, Projection and/or Ownership.</p><div className="mt-3 flex gap-2"><select value={comparisonSource} onChange={(e) => setComparisonSource(e.target.value as NflComparisonSource)} className="control"><option value="fantasypros">FantasyPros</option><option value="linestar">LineStar</option><option value="custom">Custom</option></select><input ref={comparisonRef} type="file" accept=".csv,text/csv" className="hidden" onChange={(e) => importComparison(e.target.files?.[0] ?? null)} /><button aria-label="Import comparison CSV" disabled={pending} onClick={() => comparisonRef.current?.click()} className="rounded-lg border px-3"><FileUp className="h-4 w-4" /></button></div></section>
    </div></details>
    {slate ? <>
      {/* The slate has always carried warnings; nothing rendered them, so a
          stale-model notice had nowhere to go. */}
      {coverage.state !== "adequate" ? <section role="alert" className={`rounded-xl border p-4 ${coverage.state === "blind" ? "border-red-300 bg-red-50 text-red-900" : "border-amber-300 bg-amber-50 text-amber-900"}`}>
        <h2 className="font-bold">{coverage.state === "blind" ? "We do not know who is playing" : "Availability evidence is thin"}</h2>
        <p className="mt-1 text-sm">{coverage.headline}</p>
        <button type="button" onClick={() => setWorkspaceView("research")} className="mt-2 rounded-lg border border-current px-3 py-1.5 text-sm font-semibold">Open roles and evidence</button>
      </section> : null}
      {slate.warnings.length ? <div className="space-y-2">{slate.warnings.map((warning) => <Notice key={warning}>{warning}</Notice>)}</div> : null}
      <LiveStatusBanner live={slate.liveDkStatus} />
      <section className="nfl-slate-status grid grid-cols-2 gap-3 md:grid-cols-6"><Metric label="Format" value={slate.format.toUpperCase()} /><Metric label="Players" value={String(slate.players.length)} /><Metric label="Games" value={String(slate.games.length)} /><Metric label="Our model" value={`${slate.players.filter((p) => p.ourProj != null).length}/${slate.players.length}`} /><Metric label="Availability" value={coverage.metric} /><Metric label="Model" value={slate.modelVersion ?? "None"} small /><Metric label="As of" value={slate.modelAsOf ? new Date(slate.modelAsOf).toLocaleString() : "No run"} small /></section>
      <a href={`/dfs/nfl/pool-review?upload=${slate.uploadId}`} className="inline-block text-sm font-semibold text-blue-700 underline">Open full pool audit and results</a>
      <nav aria-label="NFL workspace" className="nfl-workspace-tabs">{["players", "lineups", "research"].map(view => <button key={view} type="button" aria-current={workspaceView === view ? "page" : undefined} onClick={() => setWorkspaceView(view)}>{view === "players" ? "Players" : view === "lineups" ? `Lineups (${lineups.length})` : "Research & audit"}</button>)}</nav>
      <div hidden={workspaceView !== "research"} className="space-y-4">    <details className="rounded-xl border bg-white p-4"><summary className="cursor-pointer font-semibold">Advanced projection tools</summary><div className="mt-4 space-y-4"><ProjectionAuditPanel slate={slate} settings={{...settings,format:slate?.format??"classic",lockedPlayerIds:locked,excludedPlayerIds:excluded,minExposureByPlayer:{},maxExposureByPlayer:{}}} onChange={situations=>setSettings(s=>({...s,situations}))}/><WorkloadProjections key={slate?.uploadId??'no-slate'} slate={slate} active={settings.projectionSource === 'workload'} onChoose={()=>setSettings({...settings,projectionSource:'workload'})} onPositionsChange={workloadPositions=>setSettings({...settings,workloadPositions})} settings={{...settings,format:slate?.format??'classic',lockedPlayerIds:locked,excludedPlayerIds:excluded,minExposureByPlayer:Object.fromEntries(Object.entries(targetExposure).map(([id,pct])=>[id,Math.round(pct/100*Math.min(5,settings.nLineups))/Math.min(5,settings.nLineups)])),maxExposureByPlayer:Object.fromEntries(Object.entries(targetExposure).map(([id,pct])=>[id,Math.round(pct/100*Math.min(5,settings.nLineups))/Math.min(5,settings.nLineups)]))}} />
    <CalibratedProjections slate={slate} active={settings.projectionSource === "calibrated"} onChoose={() => setSettings({ ...settings, projectionSource: "calibrated" })} /></div></details>
      <FieldAuditPanel uploadId={slate.uploadId} />
      <AvailabilityPanel slate={slate} />
      <details className="rounded-xl border bg-white p-4"><summary className="cursor-pointer font-semibold">Compare sources and explore player absences</summary><div className="mt-4 space-y-4"><CompetitorPanel key={`${slate.uploadId}-benchmark`} slate={slate} />
      <AbsencePreview key={`${slate.uploadId}-${slate.players[0]?.availability?.evaluatedAt}`} slate={slate} /></div></details>
</div>
      <button hidden={workspaceView === "research"} type="button" aria-expanded={showBuilder} aria-controls="nfl-lineup-builder" onClick={() => setShowBuilder(value => !value)} className="nfl-mobile-builder-toggle rounded-lg border bg-white px-4 py-2 text-sm font-semibold">{showBuilder ? "Hide build settings" : "Build settings & export"}</button>
      <section hidden={workspaceView === "research"} className="nfl-work-grid grid gap-5 xl:grid-cols-[minmax(0,1fr)_320px]"><div className="space-y-5">
        <section hidden={workspaceView !== "players"} data-columns={columnView} className="nfl-player-pool rounded-xl border border-slate-200 bg-white shadow-sm"><div className="flex flex-wrap items-end gap-3 border-b p-4"><div className="mr-auto"><h2 className="font-bold">Player pool</h2><p className="text-xs text-slate-500">Select a player for projection details. OUT/IR players are excluded automatically. Our projection and player tails reflect the historical model.</p></div><label className="flex min-h-10 min-w-56 items-center gap-2 rounded-lg border px-3"><Search className="h-4 w-4 text-slate-400" /><input className="w-full text-sm outline-none" aria-label="Search players or teams" placeholder="Search player or team" value={query} onChange={(e) => { setQuery(e.target.value); setPlayerPage(1); }} /></label><select aria-label="Filter the player pool by position" title={`FLEX shows ${NFL_FLEX_POSITIONS.join(", ")} together — the positions eligible for the DK Classic FLEX slot.`} className="min-h-10 rounded-lg border bg-white px-3 text-sm" value={position} onChange={(e) => { if (isPoolPositionFilter(e.target.value)) { setPosition(e.target.value); setPlayerPage(1); } }}>{POOL_POSITION_FILTERS.map((p) => <option key={p} value={p}>{POOL_FILTER_LABELS[p]} ({positionCounts[p]})</option>)}</select><label className="text-xs font-semibold text-slate-600">Columns<select aria-label="Player column view" value={columnView} onChange={e => { setColumnView(e.target.value); if(e.target.value === "essential") { setSort(DEFAULT_POOL_SORT); setPlayerPage(1); } }} className="ml-2 min-h-10 rounded-lg border bg-white px-3 text-sm"><option value="essential">Essential</option><option value="research">All research</option></select></label></div>
          {position === "FLEX" ? <p className="border-b bg-blue-50 px-4 py-2 text-[11px] text-blue-900">Showing {NFL_FLEX_POSITIONS.join(", ")} together &mdash; the positions eligible for the Classic FLEX slot.{slate.format === "showdown" ? " On a Showdown slate every rostered player fills a FLEX slot, so this is a skill-position view rather than DK's slot rule." : ""}</p> : null}
          <p className="border-b px-4 py-2 text-xs text-slate-600">{locked.length} locked · {excluded.length} excluded · {Object.keys(targetExposure).length} exposure targets{slate.format === "showdown" ? ` · ${Object.keys(captainTargets).length} captain ranges` : ""} <span className="text-slate-400">(edit targets in All research)</span></p>
          <details className="border-b bg-slate-50 px-4 py-2"><summary className="cursor-pointer text-[11px] font-bold text-slate-600">How the Value column works</summary><ValueLegend className="mt-2" /></details>
          <div className="max-h-[620px] overflow-auto"><table className="w-full nfl-pool-table text-sm"><PoolTableHeader sort={sort} onSort={applySort} /><tbody>{filtered.length === 0 && <tr><td colSpan={14} className="p-8 text-center text-slate-500">No players match this search and position. Clear the search or choose another position.</td></tr>}{filtered.slice((currentPage - 1) * 50, currentPage * 50).map((player) => <tr key={player.dkPlayerId} className={`border-t ${player.isOut ? "bg-red-50 opacity-60" : locked.includes(player.dkPlayerId) ? "bg-emerald-50" : excluded.includes(player.dkPlayerId) ? "bg-slate-100 opacity-60" : ""}`}><td className="p-2"><div className="flex gap-1"><button aria-label={`Lock ${player.name}`} aria-pressed={locked.includes(player.dkPlayerId)} title="Lock" onClick={() => { setLocked((v) => v.includes(player.dkPlayerId) ? v.filter((id) => id !== player.dkPlayerId) : [...v, player.dkPlayerId]); setExcluded((v) => v.filter((id) => id !== player.dkPlayerId)); }} className="rounded border p-1.5">{locked.includes(player.dkPlayerId) ? <Lock className="h-3.5 w-3.5 text-emerald-700" /> : <Unlock className="h-3.5 w-3.5" />}</button><button aria-label={`Exclude ${player.name}`} aria-pressed={excluded.includes(player.dkPlayerId)} title="Exclude" onClick={() => { setExcluded((v) => v.includes(player.dkPlayerId) ? v.filter((id) => id !== player.dkPlayerId) : [...v, player.dkPlayerId]); setLocked((v) => v.filter((id) => id !== player.dkPlayerId)); }} className="rounded border p-1.5"><XCircle className={`h-3.5 w-3.5 ${excluded.includes(player.dkPlayerId) ? "text-red-600" : ""}`} /></button></div></td><td className="p-2 text-center"><div className="inline-flex items-center"><input aria-label={`${player.name} target exposure percentage`} disabled={player.isOut || excluded.includes(player.dkPlayerId)} type="number" min={0} max={100} step={1} placeholder="Auto" value={targetExposure[String(player.dkPlayerId)] ?? ""} onChange={(event) => { const raw = event.target.value; setTargetExposure((current) => { const next = { ...current }; if (raw === "") delete next[String(player.dkPlayerId)]; else next[String(player.dkPlayerId)] = Math.max(0, Math.min(100, Number(raw))); return next; }); }} className="h-8 w-14 rounded-l border px-1 text-right text-xs disabled:bg-slate-100" /><span className="flex h-8 items-center rounded-r border border-l-0 bg-slate-50 px-1 text-slate-500">%</span></div>{targetExposure[String(player.dkPlayerId)] != null ? <div className="mt-1 text-[9px] font-bold text-blue-700">{Math.round(targetExposure[String(player.dkPlayerId)] / 100 * settings.nLineups)} of {settings.nLineups}</div> : null}{slate.format === "showdown" ? <CaptainRangeInput name={player.name} disabled={player.isOut || excluded.includes(player.dkPlayerId) || player.captainDkPlayerId == null} value={captainTargets[String(player.dkPlayerId)]} nLineups={settings.nLineups} onChange={(next) => setCaptainTargets((current) => { const copy = { ...current }; if (next.min == null && next.max == null) delete copy[String(player.dkPlayerId)]; else copy[String(player.dkPlayerId)] = next; return copy; })} /> : null}</td><td className="p-3 font-bold"><button type="button" onClick={() => setExplainPlayer(player)} className="text-left text-blue-700 underline decoration-blue-400 underline-offset-2 hover:text-blue-900 hover:decoration-blue-700" title="Why this projection? Opens the full breakdown.">{player.name}</button><button data-research-detail type="button" onClick={() => setExplainPlayer(player)} title="Why this projection? Opens the full breakdown." className="ml-2 inline-flex items-center gap-1 rounded-full border border-blue-300 bg-blue-50 px-2 py-0.5 align-middle text-[10px] font-bold text-blue-800 hover:bg-blue-100"><HelpCircle className="h-3 w-3" />Why?</button>{["QB", "WR", "TE"].includes(player.position) && <a data-research-detail className="ml-2 text-[10px] font-normal text-teal-700 underline" href={`/dfs/nfl/history?name=${encodeURIComponent(player.name)}`} target="_blank" rel="noopener noreferrer">2025 context ↗</a>}<div data-research-detail className="font-normal text-[10px] text-slate-600">{nflIdentityLabel(player.identityMethod)}</div>{player.identityEvidence ? <details data-research-detail className="mt-1 font-normal text-[10px]"><summary className="cursor-pointer text-teal-700">Matching evidence</summary><pre className="max-h-48 max-w-sm overflow-auto whitespace-pre-wrap rounded bg-slate-50 p-2">{JSON.stringify(player.identityEvidence,null,2)}</pre></details> : null}<div data-research-detail className="font-normal text-[10px] text-slate-500">{player.projectionStatus} · {player.historyGames ?? 0} games</div><div className={`mt-1 rounded px-1 py-0.5 text-[10px] ${player.availability?.blockedReason ? "bg-red-100 text-red-800" : "bg-amber-50 text-amber-900"}`} title={`${player.availability?.source ?? "Unresolved"} / ${player.availability?.capturedAt ?? "No timestamp"}`}>{player.availability?.blockedReason ?? player.availability?.role ?? "Role unresolved"} / {player.availability?.status ?? "UNKNOWN"}</div></td><td>{player.position}</td><td>{player.team}<span className="text-slate-400"> {player.opponent ? `vs ${player.opponent}` : ""}</span></td><td className="text-right">{dollars(player.salary)}</td><td className="whitespace-nowrap p-1 text-right"><ValueChip assessment={valueIndex.assess(player)} position={player.position} /></td><td className="text-right font-bold">{points(player.ourProj)}</td><td className="text-right">{points(player.floorFpts)}</td><td className="text-right">{points(player.ceilingFpts)}</td><td className="text-right">{points(player.avgFptsDk)}</td><td className="text-right">{points(player.fantasyprosProj)}</td><td className="text-right">{points(player.linestarProj)}</td><td className="p-3 text-right">{pct(player.linestarOwnPct)}</td></tr>)}</tbody></table></div><div className="flex items-center justify-between gap-3 border-t p-3 text-sm"><span>{filtered.length} players · Page {currentPage} of {pages}</span><div className="flex gap-2"><button className="rounded border px-3 py-2 disabled:opacity-40" disabled={currentPage <= 1} onClick={() => setPlayerPage(currentPage - 1)}>Previous players</button><button className="rounded border px-3 py-2 disabled:opacity-40" disabled={currentPage >= pages} onClick={() => setPlayerPage(currentPage + 1)}>Next players</button></div></div></section>
        <div hidden={workspaceView !== "lineups"} className="space-y-4">
        {!lineups.length && <section className="rounded-xl border border-dashed bg-white p-8"><h2 className="font-semibold">Your lineups will appear here</h2><p className="mt-2 text-sm text-slate-500">Choose a projection source and generate your portfolio using the builder.</p></section>}
        {lineups.length ? <>{completedSettings && settingsChanged && <Notice>Settings changed. Displayed lineups still use {SOURCE_LABELS[completedSettings.projectionSource]} / {completedSettings.mode}. Generate again to apply changes to settings, locks, exclusions, or exposure.</Notice>}<section className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-blue-200 bg-gradient-to-r from-blue-50 to-cyan-50 p-4"><div><h2 className="font-bold text-blue-950">Lineup visual analysis</h2><p className="text-xs text-blue-800">Compare additive player-tail scores, roster construction, and portfolio overlap. Tail sums are not lineup percentiles.</p></div><button type="button" onClick={() => setShowVisuals((visible) => !visible)} className="inline-flex min-h-11 items-center gap-2 rounded-lg bg-blue-700 px-4 text-sm font-bold text-white hover:bg-blue-600"><BarChart3 className="h-4 w-4" />{showVisuals ? "Hide Visuals" : "Generate Visuals"}</button></section>{showVisuals ? <NflLineupVisualizations lineups={lineups} mode={completedSettings?.mode ?? settings.mode} projectionSource={completedSettings?.projectionSource ?? settings.projectionSource} /> : null}<button className="rounded border bg-white px-3 py-2 text-sm" onClick={()=>{if(runId)startTransition(async()=>{try{downloadText(`nfl-frozen-audit-${runId}.json`,JSON.stringify(await readNflOptimizerAudit(runId),null,2));}catch{setError("Saved audit could not be downloaded.");}});}}>Download frozen lineup explanations</button><LineupReview lineups={lineups} runId={runId} /></> : null}
        </div>
      </div><aside id="nfl-lineup-builder" data-mobile-open={showBuilder} className="nfl-builder space-y-4">

        <section className="rounded-xl border bg-white p-4 shadow-sm"><h2 className="font-bold">Build lineups</h2><div className="mt-4 space-y-3"><Field label="Objective"><select value={settings.mode} onChange={(e) => setSettings({ ...settings, mode: e.target.value as "cash" | "gpp" })} className="control"><option value="gpp">GPP ceiling</option><option value="cash">Cash floor</option></select></Field><Field label="Projection source"><select value={settings.projectionSource} onChange={(e) => setSettings({ ...settings, projectionSource: e.target.value as NflProjectionSource })} className="control">{Object.entries(SOURCE_LABELS).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></Field>
          <div className="grid grid-cols-2 gap-2"><Field label="Lineups"><input type="number" min={1} max={150} value={settings.nLineups} onChange={(e) => setSettings({ ...settings, nLineups: Number(e.target.value) })} className="control" /></Field><Field label="Min lineup salary"><input type="number" step={100} value={settings.minSalary} onChange={(e) => setSettings({ ...settings, minSalary: Number(e.target.value) })} className="control" /></Field><Field label="Max exposure %"><input type="number" min={1} max={100} value={Math.round(settings.maxExposure * 100)} onChange={(e) => setSettings({ ...settings, maxExposure: Number(e.target.value) / 100 })} className="control" /></Field><Field label="Min unique"><input type="number" min={1} max={9} value={settings.minUnique} onChange={(e) => setSettings({ ...settings, minUnique: Number(e.target.value) })} className="control" /></Field></div>
          <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
            <div className="flex items-center justify-between"><h3 className="text-sm font-bold text-slate-800">Cheap-player policy</h3></div>
            <Field label="Preset"><select className="control" value={settings.puntPolicy.mode} onChange={(e) => setSettings({ ...settings, puntPolicy: resolvePuntPreset(e.target.value as PuntPresetKey, settings.puntPolicy) })}>{PUNT_PRESETS.map((p) => <option key={p.key} value={p.key}>{p.label}</option>)}</select></Field>
            <ul className="mt-2 list-disc space-y-1 pl-4 text-[11px] text-slate-600">{describePuntPolicy(settings.puntPolicy).map((line) => <li key={line}>{line}</li>)}</ul>
            {eligibility.some((e) => !e.eligible && e.reasonCode === "DOUBTFUL") ? <div className="mt-2 rounded border border-amber-300 bg-amber-50 p-2 text-[11px]"><p className="font-bold text-amber-900">{eligibility.filter((e) => !e.eligible && e.reasonCode === "DOUBTFUL").length} player(s) left out &mdash; DraftKings lists them Doubtful</p><p className="mt-0.5 text-amber-800">Every Doubtful player on a 2026 slate so far has scored 0.0. Lock one to use him anyway.</p><div className="mt-1 max-h-40 space-y-1 overflow-auto">{eligibility.filter((e) => !e.eligible && e.reasonCode === "DOUBTFUL").map((e) => <div key={e.dkPlayerId} className="flex items-center justify-between gap-2 rounded border bg-white p-1.5"><span><b>{e.name}</b> <span className="text-slate-400">{dollars(e.salary)}</span></span><button type="button" className="shrink-0 rounded border border-emerald-300 bg-emerald-50 px-1.5 py-0.5 text-[10px] font-bold text-emerald-800" onClick={() => { setLocked((v) => v.includes(e.dkPlayerId) ? v : [...v, e.dkPlayerId]); setExcluded((v) => v.filter((id) => id !== e.dkPlayerId)); }}>Lock him</button></div>)}</div></div> : null}
            {eligibility.some((e) => !e.eligible && e.reasonCode !== "INACTIVE" && e.reasonCode !== "MANUAL_EXCLUSION" && e.reasonCode !== "DOUBTFUL") ? <details className="mt-2"><summary className="cursor-pointer text-[11px] font-bold text-amber-800">{eligibility.filter((e) => !e.eligible && e.reasonCode !== "INACTIVE" && e.reasonCode !== "MANUAL_EXCLUSION" && e.reasonCode !== "DOUBTFUL").length} cheap player(s) blocked — review</summary><div className="mt-1 max-h-40 space-y-1 overflow-auto">{eligibility.filter((e) => !e.eligible && e.reasonCode !== "INACTIVE" && e.reasonCode !== "MANUAL_EXCLUSION" && e.reasonCode !== "DOUBTFUL").map((e) => <div key={e.dkPlayerId} className="flex items-start justify-between gap-2 rounded border bg-white p-1.5 text-[11px]"><span><b>{e.name}</b> <span className="text-slate-400">{dollars(e.salary)}</span><span className="block text-slate-500">{e.reason}</span></span><button type="button" className="shrink-0 rounded border border-emerald-300 bg-emerald-50 px-1.5 py-0.5 text-[10px] font-bold text-emerald-800" onClick={() => allowCheapPlayer(e.dkPlayerId, e.name)}>Allow for run</button></div>)}</div></details> : null}
          </div>
          {slate.format === "showdown" ? <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-[11px]">
            <h3 className="text-sm font-bold text-slate-800">Portfolio plan (archetypes)</h3>
            <Field label="Plan"><select className="control" value={archetypePlanMode} onChange={(e) => setArchetypePlanMode(e.target.value as "balanced" | "standard" | "custom" | "chalk_leverage")}>
              <option value="balanced">Balanced mix (recommended)</option>
              <option value="chalk_leverage">Chalk captain, rotating leverage</option>
              <option value="standard">Standard ceiling only</option>
              <option value="custom">Custom…</option>
            </select></Field>
            {archetypePlanMode === "balanced" ? <p className="mt-1 text-slate-500">Automatically spreads lineups across ceiling, fade, game-script and K/DST strategies. Fade targets are the chalkiest players (by projected ownership, or projection when no ownership feed exists); the Vegas favorite comes from this game&apos;s moneyline. Anything the slate can&apos;t support folds into Standard ceiling — each run&apos;s notes say exactly what was chosen. Names describe strategy, not expected profit.</p> : null}
            {archetypePlanMode === "standard" ? <p className="mt-1 text-slate-500">Strongest evaluated lineups only — no forced fades or game scripts.</p> : null}
            {archetypePlanMode === "chalk_leverage" ? <p className="mt-1 text-slate-500">Captains come from the obvious plays — the top three by projected ownership, or by projection when there is no ownership feed — plus anyone you give a CPT minimum. Every lineup must also carry one leverage player from outside the six chalkiest names, and the leverage position rotates lineup to lineup (WR → TE → RB → K/DST → QB). Week 2&apos;s winners were chalkier than us, so this takes differentiation in the flex, not at captain. A portfolio shape, not a validated edge.</p> : null}
            {archetypePlanMode === "custom" ? <><p className="mt-0.5 text-slate-500">Assign lineup quotas by strategy. Leave all off for Standard ceiling only. Names describe strategy, not expected profit.</p>
            <div className="mt-2 space-y-1">{(Object.keys(ARCHETYPE_LABELS) as ArchetypeId[]).filter((id) => id !== "chalk_captain_leverage").map((id) => {
              const q = archetypeQuotas.find((x) => x.archetypeId === id);
              return <div key={id} className="flex items-center gap-2"><label className="flex flex-1 items-center gap-1.5"><input type="checkbox" checked={Boolean(q?.enabled)} onChange={(e) => setArchetypeQuotas((cur) => { const others = cur.filter((x) => x.archetypeId !== id); return e.target.checked ? [...others, { archetypeId: id, minLineups: 1, maxLineups: settings.nLineups, enabled: true }] : others; })} />{ARCHETYPE_LABELS[id]}</label>{q?.enabled ? <><input aria-label={`${ARCHETYPE_LABELS[id]} min`} type="number" min={0} max={settings.nLineups} value={q.minLineups} onChange={(e) => setArchetypeQuotas((cur) => cur.map((x) => x.archetypeId === id ? { ...x, minLineups: Number(e.target.value) } : x))} className="h-7 w-12 rounded border px-1 text-right" /><span>–</span><input aria-label={`${ARCHETYPE_LABELS[id]} max`} type="number" min={0} max={settings.nLineups} value={q.maxLineups} onChange={(e) => setArchetypeQuotas((cur) => cur.map((x) => x.archetypeId === id ? { ...x, maxLineups: Number(e.target.value) } : x))} className="h-7 w-12 rounded border px-1 text-right" /></> : null}</div>;
            })}</div>
            {archetypeQuotas.some((q) => q.enabled && (q.archetypeId === "favorite_onslaught" || q.archetypeId === "underdog_comeback")) ? <div className="mt-2">
              <Field label="Favorite team (required by game-script archetypes)"><select className="control" value={archetypeFavorite} onChange={(e) => setArchetypeFavorite(e.target.value)}><option value="">Choose…</option>{slateTeams.map((t) => <option key={t} value={t}>{t}</option>)}</select></Field>
              {archetypeFavorite ? <p className="mt-0.5 text-slate-500">Underdog: {archetypeUnderdog ?? "—"}</p> : <p className="mt-0.5 text-amber-800">Pick the Vegas favorite; generation fails without it.</p>}
            </div> : null}
            {archetypeQuotas.some((q) => q.enabled && (q.archetypeId === "single_chalk_fade" || q.archetypeId === "double_fade")) ? <div className="mt-2 space-y-1">
              <p className="font-bold text-slate-700">Faded players (fades are applied per-lineup; teammates form the default beneficiary path)</p>
              {[0, 1].slice(0, archetypeQuotas.some((q) => q.enabled && q.archetypeId === "double_fade") ? 2 : 1).map((i) => <select key={i} aria-label={`Fade ${i + 1}`} className="control" value={archetypeFades[i] ?? ""} onChange={(e) => setArchetypeFades((cur) => { const next = [...cur]; if (e.target.value) next[i] = Number(e.target.value); else next.splice(i, 1); return [...new Set(next)]; })}>
                <option value="">Fade {i + 1}: choose…</option>
                {[...(slate?.players ?? [])].sort((a, b) => (b.ourProj ?? 0) - (a.ourProj ?? 0)).slice(0, 30).map((p) => <option key={p.dkPlayerId} value={p.dkPlayerId}>{p.name} ({p.team} · {dollars(p.salary)})</option>)}
              </select>)}
              {!archetypeFades.length ? <p className="text-amber-800">A fade archetype fails generation until a faded player is chosen.</p> : null}
            </div> : null}</> : null}
          </div> : null}
          {ownership ? <div className={`rounded-lg border p-3 text-[11px] ${ownership.capability === "validated" ? "border-emerald-200 bg-emerald-50" : ownership.capability === "heuristic_uncalibrated" ? "border-amber-200 bg-amber-50" : "border-slate-200 bg-slate-50"}`}>
            <div className="flex items-center justify-between"><h3 className="text-sm font-bold text-slate-800">Ownership</h3><span className="rounded-full border bg-white px-2 py-0.5 font-bold uppercase">{ownership.capability.replace(/_/g, " ")}</span></div>
            <p className="mt-1 font-semibold text-slate-700">Objective: {objectiveLabel(ownership.capability)}</p>
            <div className="mt-1 grid grid-cols-2 gap-x-3 text-slate-600"><span>Source: {ownership.source ?? "none"}</span><span>Coverage: {(ownership.coverage * 100).toFixed(0)}%</span><span>CPT total: {(ownership.captainTotal * 100).toFixed(0)}%</span><span>FLEX total: {(ownership.flexTotal * 100).toFixed(0)}%</span></div>
            <p className="mt-1 text-slate-600">Leverage {ownership.features.leverage ? "on" : "off"} · Ownership fade {ownership.features.ownershipFade ? "on" : "off"} · Duplication model {ownership.features.duplicationModel ? "on" : "off"}</p>
            {ownership.errors.length ? <ul className="mt-1 list-disc pl-4 text-red-700">{ownership.errors.map((e) => <li key={e}>{e}</li>)}</ul> : null}
            {ownership.warnings.length ? <ul className="mt-1 list-disc pl-4 text-amber-800">{ownership.warnings.map((w) => <li key={w}>{w}</li>)}</ul> : null}
          </div> : null}
          <details className="rounded-lg border p-3"><summary className="cursor-pointer text-sm font-semibold">Advanced settings</summary><div className="mt-3 space-y-3">          {settings.mode === "gpp" && slate.format === "classic" ? <><Field label="QB pass catchers"><select value={settings.stackPassCatchers} onChange={(e) => setSettings({ ...settings, stackPassCatchers: Number(e.target.value) as 0 | 1 | 2 })} className="control"><option value={0}>No requirement</option><option value={1}>At least 1</option><option value={2}>At least 2</option></select></Field><label className="check"><input type="checkbox" checked={settings.bringBack} onChange={(e) => setSettings({ ...settings, bringBack: e.target.checked })} />Require opposing bring-back</label></> : null}
          <Field label={`Randomness (${Math.round(settings.randomness * 100)}%)`}><input type="range" min={0} max={.25} step={.01} value={settings.randomness} onChange={(e) => setSettings({ ...settings, randomness: Number(e.target.value) })} className="w-full" /></Field><label className="check"><input type="checkbox" checked={settings.requireObservedHistory} onChange={(e) => setSettings({ ...settings, requireObservedHistory: e.target.checked })} />Require observed games (drops players projected purely from position averages)</label><label className="check"><input type="checkbox" checked={settings.allowDkFallback} onChange={(e) => setSettings({ ...settings, allowDkFallback: e.target.checked })} />Fall back to DK's season average when we have no projection (off: the player is excluded instead)</label>{settings.mode === "gpp" ? <label className="check"><input type="checkbox" checked={settings.useHeuristicOwnershipLeverage} onChange={(e) => setSettings({ ...settings, useHeuristicOwnershipLeverage: e.target.checked })} />Ownership leverage from LineStar (uncalibrated estimate — the server labels it; off = projection-only GPP)</label> : null}</div></details><button disabled={pending} onClick={generate} className="inline-flex min-h-11 w-full items-center justify-center gap-2 rounded-lg bg-emerald-700 text-sm font-bold text-white disabled:opacity-40"><Play className="h-4 w-4" />{pending ? "Working…" : "Generate & save"}</button><button type="button" onClick={() => setWorkspaceView(workspaceView === "lineups" ? "players" : "lineups")} className="min-h-10 w-full rounded-lg border text-sm font-semibold">{workspaceView === "lineups" ? "Back to player pool" : `Review ${lineups.length} lineups →`}</button></div></section>
        <div hidden={workspaceView !== "lineups"} className="space-y-4">        {lineups.length ? <>{completedSettings && settingsChanged && <Notice>Settings changed. Displayed lineups still use {SOURCE_LABELS[completedSettings.projectionSource]} / {completedSettings.mode}. Generate again to apply changes to settings, locks, exclusions, or exposure.</Notice>}<Exposure rows={exposures} total={lineups.length} report={exposureReport} format={completedSettings?.format ?? slate.format} />{salaryBands.length ? <section className="rounded-xl border bg-white p-4 shadow-sm"><h2 className="font-bold">Salary-left distribution</h2><div className="mt-2 space-y-1 text-[11px]">{salaryBands.map((b) => <div key={`${b.band.min}-${b.band.max}`} className={`flex justify-between ${b.withinPlan ? "" : "text-red-700"}`}><span>${b.band.min.toLocaleString()}–${b.band.max.toLocaleString()}</span><span>{b.count} lineup(s) · plan {b.minCount}–{b.maxCount}{b.withinPlan ? "" : " ⚠"}</span></div>)}</div>{duplication.length ? <p className="mt-2 text-[11px] text-slate-500">Duplication: {duplication[0].basis === "model" ? "field-model expected counts" : duplication[0].basis === "heuristic" ? "uncalibrated concentration estimate (not a duplicate count)" : "unavailable"}.</p> : null}</section> : null}<section className="rounded-xl border bg-white p-4 shadow-sm"><h2 className="font-bold">DraftKings export</h2><p className="mt-1 text-xs text-slate-500">Your DK entries template supplies the entry IDs; generated rosters fill its slot columns.</p><input ref={entryRef} type="file" accept=".csv,text/csv" className="hidden" onChange={(e) => setEntryFile(e.target.files?.[0] ?? null)} /><button onClick={() => entryRef.current?.click()} className="mt-3 min-h-10 w-full rounded-lg border text-sm font-bold">{entryFile?.name ?? "Select entry template"}</button>{qaReport ? <div className={`mt-3 rounded-lg border p-2 text-[11px] ${qaReport.decision === "blocked" ? "border-red-300 bg-red-50" : qaReport.decision === "ready_with_warnings" ? "border-amber-300 bg-amber-50" : "border-emerald-300 bg-emerald-50"}`}><div className="flex items-center justify-between font-bold"><span>Pre-export QA: {qaReport.decision === "blocked" ? "Blocked" : qaReport.decision === "ready_with_warnings" ? "Ready with warnings" : "Ready"}</span><span className="text-slate-500">{qaReport.counts.blocker}B · {qaReport.counts.warning}W · {qaReport.counts.info}i</span></div><div className="mt-1 max-h-40 space-y-1 overflow-auto">{qaReport.checks.filter((c) => !c.passed).map((c) => <div key={c.id} className={`flex items-start justify-between gap-2 rounded border bg-white p-1 ${c.severity === "blocker" && qaReport.openBlockers.includes(c.id) ? "border-red-200" : "border-slate-200"}`}><span><b>{c.title}</b><span className="block text-slate-500">{c.detail}</span></span>{c.overridable && qaReport.openBlockers.includes(c.id) ? <button type="button" className="shrink-0 rounded border px-1.5 py-0.5 text-[10px] font-bold" onClick={() => overrideQaCheck(c.id)}>Override</button> : null}</div>)}</div></div> : null}<button disabled={!entryFile || (qaReport?.decision === "blocked")} onClick={() => void exportEntries()} className="mt-2 inline-flex min-h-10 w-full items-center justify-center gap-2 rounded-lg bg-blue-600 text-sm font-bold text-white disabled:opacity-40"><Download className="h-4 w-4" />{qaReport?.decision === "blocked" ? "Export blocked" : "Export lineups"}</button></section></> : null}
</div>
      </aside></section>
    </> : <section className="rounded-xl border border-dashed border-slate-300 bg-slate-50 p-14 text-center"><FileUp className="mx-auto h-8 w-8 text-slate-400" /><h2 className="mt-3 font-bold">No NFL slate loaded</h2><p className="mt-1 text-sm text-slate-600">Upload a DraftKings NFL Classic or Showdown salary CSV to begin.</p></section>}
    {slate ? <PlayerExplanationPanel uploadId={slate.uploadId} player={explainPlayer} slatePlayers={slate.players} valueIndex={valueIndex} onClose={() => setExplainPlayer(null)} /> : null}
    <style jsx global>{`.nfl-dfs-workspace .control{min-height:40px;width:100%;border:1px solid #cbd5e1;border-radius:.5rem;background:white;padding:0 .65rem;font-size:.875rem}.nfl-dfs-workspace .check{display:flex;align-items:flex-start;gap:.5rem;font-size:.75rem;color:#334155}`}</style>
  </div>;
}

function Status({ icon, title, text, good = false }: { icon: React.ReactNode; title: string; text: string; good?: boolean }) { return <div className={`rounded-xl border p-4 ${good ? "border-emerald-200 bg-emerald-50 text-emerald-950" : "border-amber-200 bg-amber-50 text-amber-950"}`}>{icon}<h2 className="mt-2 font-bold">{title}</h2><p className="mt-1 text-xs">{text}</p></div>; }
function Metric({ label, value, small = false }: { label: string; value: string; small?: boolean }) { return <div className="rounded-xl border bg-white p-4 shadow-sm"><div className="text-[10px] font-bold uppercase text-slate-500">{label}</div><div className={`mt-1 font-black ${small ? "text-sm" : "text-2xl"}`}>{value}</div></div>; }
function Field({ label, children }: { label: string; children: React.ReactNode }) { return <label className="block text-xs font-bold text-slate-700"><span className="mb-1 block">{label}</span>{children}</label>; }
function Notice({ children, good = false }: { children: React.ReactNode; good?: boolean }) { return <div role={good ? undefined : "alert"} className={`mt-4 flex items-start gap-2 rounded-lg border p-3 text-sm ${good ? "border-emerald-200 bg-emerald-50 text-emerald-900" : "border-red-200 bg-red-50 text-red-900"}`}>{good ? <CheckCircle2 className="h-4 w-4 shrink-0" /> : <AlertTriangle className="h-4 w-4 shrink-0" />}{children}</div>; }
function Exposure({ rows, total, report, format }: { rows: { name: string; team: string; n: number }[]; total: number; report?: import("./nfl-optimizer").NflExposureReport[]; format?: "classic" | "showdown" }) {
  // Phase 3: when a slot-aware report exists, show realized vs requested for
  // Overall / CPT / Flex and flag any binding constraint (P3-AC2/AC5).
  const byId = new Map((report ?? []).map((r) => [r.name, r]));
  if (report && report.length) {
    return <section className="rounded-xl border bg-white p-4 shadow-sm"><h2 className="font-bold">Exposure <span className="text-xs font-normal text-slate-400">realized / requested over {total}</span></h2><div className="mt-3 max-h-72 space-y-2 overflow-auto">{[...report].sort((a, b) => b.overall - a.overall).slice(0, 40).map((r) => <div key={r.dkPlayerId} className={`rounded border p-1.5 text-[11px] ${r.binding && /missed/.test(r.binding) ? "border-red-200 bg-red-50" : ""}`}><div className="flex justify-between font-semibold"><span>{r.name}</span><span>{r.overall}/{r.overallMax === total ? "∞" : r.overallMax}{r.overallMin ? ` (min ${r.overallMin})` : ""}</span></div>{format === "showdown" ? <div className="mt-0.5 flex gap-3 text-slate-500"><span>CPT {r.captain}{r.captainMax < total ? `/${r.captainMax}` : ""}</span><span>FLEX {r.flex}{r.flexMax < total ? `/${r.flexMax}` : ""}</span></div> : null}{r.binding ? <div className={`mt-0.5 ${/missed/.test(r.binding) ? "text-red-700" : "text-slate-400"}`}>{r.binding}</div> : null}</div>)}</div></section>;
  }
  void byId;
  return <section className="rounded-xl border bg-white p-4 shadow-sm"><h2 className="font-bold">Exposure</h2><div className="mt-3 max-h-64 space-y-2 overflow-auto">{rows.slice(0, 30).map((row) => <div key={`${row.name}-${row.team}`}><div className="flex justify-between text-xs"><span>{row.name} <span className="text-slate-400">{row.team}</span></span><b>{Math.round(row.n / total * 100)}%</b></div><div className="mt-1 h-1.5 rounded bg-slate-100"><div className="h-full rounded bg-blue-600" style={{ width: `${row.n / total * 100}%` }} /></div></div>)}</div></section>; }
