# Spec — Soccer Bet Backtest, Broken Down by Bet Type

**Status:** Draft for review
**Date:** 2026-06-28
**Author:** Claude (Opus 4.8) + Josh
**Related:** `model/soccer_game_bets.py`, `model/soccer_bet_rating.py`, memory `soccer-totals-no-edge`

---

## 1. Problem

The soccer Vegas page is used to **evaluate betting models**. The current backtest
panel ("Results by star rating") computes calibration with a single query,
`getSoccerBetBacktest()` (`web/src/db/queries.ts:6381`), which groups settled bets
**by `stars` only** — aggregating across:

- every **bet type** (`moneyline`, `total`, `draw_no_bet`, `first_scorer`,
  `outright_winner`, `group_winner`), and
- every **`model_version`** (`gameline-v1/v2/v3`, `firstscorer-v*`, `futures-v1`).

This produces two false impressions:

1. **Type pollution.** A settled "3★" row mixes 3★ moneyline bets with historical
   3★ totals (rated by the now-removed overfit uplift model). Totals were just
   shown to have **no out-of-sample edge** (walk-forward hit rate < −110 breakeven
   at every damping factor), yet their old 3★ settled rows still sit in the
   aggregate 3★ calibration line, making that tier read better/worse than the
   market we actually have signal on (moneyline).

2. **No per-market verdict.** You cannot answer the core evaluation question —
   *"which of our betting models actually has edge?"* — because every model's bets
   are blended into one stars ladder.

The live Bets panel was already fixed (totals capped at 2★, commit `fe47ce5`).
This spec fixes the **backtest panel** so model evaluation is honest end-to-end.

---

## 2. Goal

Let the user see calibration + ROI **per bet type**, so each betting model can be
judged on its own merits, while preserving the existing aggregate as an explicit
rollup. No change to how bets are rated or settled — this is a read/display change.

### Non-goals
- No change to `record_bet`, star rubric, or settlement.
- No new bet types.
- CLV (`getSoccerClv`) is already per-type-capable and out of scope here.

---

## 3. Current state (ground truth)

**Query** — `getSoccerBetBacktest()` (`queries.ts:6381`):
```sql
SELECT b.stars, COUNT(*) AS n,
       AVG(b.our_prob)                                   AS "expectedWinRate",
       AVG(CASE WHEN b.status='won' THEN 1.0 ELSE 0.0 END) AS "realizedWinRate",
       COUNT(*) FILTER (WHERE b.market_decimal IS NOT NULL) AS "marketBets",
       SUM(CASE WHEN b.market_decimal IS NULL THEN 0
                WHEN b.status='won' THEN b.market_decimal - 1 ELSE -1 END) AS "profitUnits"
FROM soccer_bets b
WHERE b.status IN ('won','lost')          -- NB: excludes 'void'
GROUP BY b.stars
ORDER BY b.stars DESC
```

**Type** — `SoccerBacktestRow` (`queries.ts:6316`):
```ts
{ stars, n, expectedWinRate, realizedWinRate, roi: number|null, marketBets }
```

**UI** — `ResultsPanel` in `soccer-vegas-client.tsx:1009-1074`: a single table,
one row per star tier, columns: Tier / Settled / Won / Win% / Expected / ROI /
Calibration. Footer note already admits "first-scorer pool dominates … moneyline/
totals carry the meaningful signal at higher tiers."

**Data flow:** `vegas-content.tsx` (`getSoccerBetBacktest()`) → `SoccerVegasClient`
prop `backtest` → `ResultsPanel` prop `backtest`.

**Known minor bug to fix in passing:** the UI labels `realizedWinRate −
expectedWinRate` as "Brier" in a variable (`const brier = …`); it is a calibration
gap, not a Brier score. Rename to `calibGap` (display text already says "above/
below expected", so no user-facing copy change).

---

## 4. Design

### 4.1 New query: `getSoccerBetBacktestByType()`

Returns calibration grouped by **(bet_type, stars)**, plus a synthetic
`bet_type='all'` rollup so the existing aggregate is preserved in one payload.

```ts
export type SoccerBacktestTypeRow = {
  betType: string;          // 'moneyline' | 'total' | 'draw_no_bet' |
                            // 'first_scorer' | 'outright_winner' |
                            // 'group_winner' | 'all'
  stars: number;            // 1..5
  n: number;                // settled won+lost in this (type, stars) cell
  expectedWinRate: number;  // AVG(our_prob)
  realizedWinRate: number;  // wins / n
  roi: number | null;       // profitUnits / marketBets, null if no market bets
  marketBets: number;
  modelVersions: string[];  // distinct model_versions contributing (traceability)
};
```

SQL (single round-trip via `GROUPING SETS` so per-type and the `all` rollup come
back together):
```sql
SELECT
  COALESCE(b.bet_type, 'all') AS "betType",   -- NULL bucket from GROUPING SETS = rollup
  b.stars,
  COUNT(*) AS n,
  AVG(b.our_prob) AS "expectedWinRate",
  AVG(CASE WHEN b.status='won' THEN 1.0 ELSE 0.0 END) AS "realizedWinRate",
  COUNT(*) FILTER (WHERE b.market_decimal IS NOT NULL) AS "marketBets",
  SUM(CASE WHEN b.market_decimal IS NULL THEN 0
           WHEN b.status='won' THEN b.market_decimal - 1 ELSE -1 END) AS "profitUnits",
  ARRAY_AGG(DISTINCT b.model_version) AS "modelVersions"
FROM soccer_bets b
WHERE b.status IN ('won','lost')
GROUP BY GROUPING SETS ((b.bet_type, b.stars), (b.stars))
ORDER BY "betType", b.stars DESC
```
- `GROUPING SETS ((bet_type,stars),(stars))` yields per-type rows **and** a
  `bet_type IS NULL` set → mapped to `'all'` via `COALESCE`. Identical math to the
  existing query for the `'all'` rows, so the rollup stays consistent.
- `void` excluded (matches current behavior; DNB/totals pushes don't count as
  won/lost). Documented in the panel footer.

**Decision — model_version scoping:** do **not** filter by version. Show full
history, but expose `modelVersions[]` per cell so a tier rated under a retired
model is visible (e.g. totals 3★ → `["gameline-v1","gameline-v2"]`). Rationale:
the WC is a 2-week event; version-filtering would empty most cells. Surfacing the
contributing versions is enough for honest attribution. (If you'd rather hard-scope
to the current model, that's a one-line `WHERE b.model_version = (latest)` — call
it out in review.)

### 4.2 Keep `getSoccerBetBacktest()` or replace?

**Replace consumers, keep the function** during transition: `getSoccerBetBacktestByType()`
supersedes it. `vegas-content.tsx` switches to the new call. Leave the old function
defined but unreferenced for one commit, then delete in a follow-up (avoids a
risky big-bang diff). The `'all'` rows of the new query == old output.

### 4.3 UI changes (`ResultsPanel`)

Replace the single table with a **bet-type selector + one table**, defaulting to a
view that makes the verdict obvious.

**Layout:**
```
Results by star rating          [ All ▾ ] [ Moneyline ] [ Win(DNB) ] [ O/U ] [ First scorer ] …
┌─────────────────────────────────────────────────────────────────────────┐
│ Tier │ Settled │ Won │ Win% │ Expected │ ROI │ Models │ Calibration       │
│ ★★★★★ │   …                                                               │
└─────────────────────────────────────────────────────────────────────────┘
Verdict chip per type:  Moneyline ✅ edge   |   O/U ⚠️ no edge (capped 2★)
```

- **Selector**: pill/segmented control over the bet types **present in the data**
  (don't show empty types), plus "All". Reuse `BET_TYPE_LABEL` (already has
  `draw_no_bet → "Win (DNB)"`, `total → "Over/Under"`).
- **New column "Models"**: small muted text listing `modelVersions` (e.g. "v3" or
  "v1,v2"). Truncate `gameline-` / `firstscorer-` prefixes for width.
- **Verdict chip** (per selected type, above the table): a deterministic summary
  so the user doesn't have to read tiers:
  - `total` → always "⚠️ No out-of-sample edge — capped at 2★" (hard-coded, cites
    the walk-forward finding; this is the anti-false-impression banner).
  - other types → derived: if the type's market-bet tiers (3★+) have **realized ≥
    expected AND ROI > 0** with `n ≥ 10` → "✅ edge holds"; if `n < 10` →
    "🔎 sample too small (n)"; else "⚠️ underperforming".
- **Breakeven note**: ROI (not win%) is the profitability truth since soccer odds
  vary by selection. Keep win% vs expected as the calibration check. Footer states
  this explicitly.
- Preserve existing empty-state (`backtest.length === 0`).

### 4.4 Props threading

- `vegas-content.tsx`: replace `getSoccerBetBacktest()` with
  `getSoccerBetBacktestByType()` in the `Promise.all`; pass `backtestByType`.
- `SoccerVegasClient` + `ResultsPanel`: change prop type
  `backtest: SoccerBacktestRow[]` → `backtest: SoccerBacktestTypeRow[]`; add client
  state `const [btType, setBtType] = useState<string>('all')`; filter rows by
  `btType` for the table; compute the verdict chip from the selected type's rows.

---

## 5. Edge cases

| Case | Handling |
|---|---|
| Type with only `void` settled (e.g. all DNB pushed) | Excluded from won/lost → type absent from selector. Acceptable. |
| Type with no market odds (`group_winner`) | `roi = null`, render "—"; calibration still shown via edge/our_prob. |
| `first_scorer` huge volume, all 1★ | Visible as its own type now → no longer drags the aggregate's high tiers. Footer note about it can be removed. |
| Totals going forward | Only populate 1–2★ cells (cap). Historical 3★ totals cell shows `modelVersions=["gameline-v1/v2"]` — honest provenance. |
| Empty data (fresh DB) | `backtest.length===0` → existing empty state. |
| `n` small per cell after splitting | Verdict chip shows "sample too small"; tiers still render with raw counts. This is the honest trade-off of disaggregation. |

---

## 6. Testing / acceptance

**Data checks (run against live Neon before & after):**
1. Sum of `n` over all per-type cells == sum of `n` over `'all'` cells == old
   `getSoccerBetBacktest()` total. (Conservation.)
2. `'all'` rows of new query are numerically identical to old query output
   (expected, realized, roi) per star tier.
3. `total` bet type shows **no cell above 2★ for any `gameline-v3` contribution**;
   any 3★ total cell lists only retired versions in `modelVersions`.
4. `moneyline` per-type ROI/realized matches a hand SQL spot-check.

**UI checks:**
5. Selector lists only present types; switching updates table + verdict chip.
6. `total` always shows the "no edge / capped" verdict regardless of tier stats.
7. `tsc --noEmit` clean; no refs to removed `brier` var.

**Acceptance:** From the soccer Vegas → Results tab, a reviewer can select
"Over/Under" and "Moneyline" and see clearly that totals are a non-edge market and
moneyline carries the signal — with per-type ROI and the contributing model
versions visible.

---

## 7. Rollout

1. Branch `feat/soccer-backtest-by-bettype`.
2. Add `getSoccerBetBacktestByType()` + type (queries.ts).
3. Switch `vegas-content.tsx`; update `SoccerVegasClient` + `ResultsPanel`.
4. Verify data conservation queries + `tsc`.
5. Commit, push, merge (ff-only), as per session convention.
6. Follow-up commit: delete the now-unused `getSoccerBetBacktest()` +
   `SoccerBacktestRow` if nothing else imports them.

**No DB migration. No model re-run. Pure read/display.**

---

## 8. Decisions (locked 2026-06-28)

1. **Version scoping** — **show all history with `modelVersions[]` provenance.**
   No version filter; the WC is ~2 weeks so hard-scoping would empty most cells.
   Old 3★ totals are stamped `["gameline-v1","v2"]`, which is the disclosure.
2. **Default selected type** — **open on "Moneyline"**, not "All". The aggregate
   reintroduces the blended false impression on first glance; open on the market
   that carries edge. "All" stays one click away.
3. **Verdict thresholds** — **two-tier confidence:**
   - `n ≥ 20` AND ROI > 0 AND realized ≥ expected → ✅ "edge holds"
   - `10 ≤ n < 20` AND ROI > 0 AND realized ≥ expected → 🔎 "leaning positive (small sample)"
   - `n < 10` → "insufficient sample (n)"
   - else → ⚠️ "underperforming"
   `n` here = settled market bets in the type's 3★+ tiers (the bets we'd place).
4. **First-scorer footer note** — **drop it**; replace with one general line:
   *"ROI is the profitability metric; win% vs expected is the calibration check.
   Voids excluded."*
5. **Totals verdict is HARDCODED** (most important line for model-evaluation use):
   `total` always renders "⚠️ No out-of-sample edge — capped at 2★" regardless of
   what its small settled sample happens to show. A lucky positive ROI must never
   re-create the false impression. Derived verdicts apply to all other types.
