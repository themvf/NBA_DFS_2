# Scoring & Roster Construction Impact Analysis

## Current State Assessment

### ✅ What Already Works

The system **already handles scoring-specific projections and ADP**:

1. **Projections are scoring-specific**
   - Backend (`ingest/ff_independent.py`) generates projections for **STD, HALF, and PPR** separately
   - Database stores `projected_points` per scoring type in `ff_player_source_projections`
   - Query layer (`getFantasyRankings`) correctly fetches projections matching the draft's scoring preset
   
2. **ADP is scoring-specific**
   - Fetched from Fantasy Football Calculator for each format: `standard`, `half-ppr`, `ppr`
   - Stored separately by scoring type
   - Correctly displayed based on the ranking set's scoring profile

3. **Rankings are pre-computed per scoring format**
   - Each `ff_ranking_sets` row has a `scoring_profile` (STD/HALF/PPR)
   - Rankings are not computed on-the-fly but loaded from pre-generated sets
   - The draft form lets users select which ranking set (which implicitly includes its scoring)

### ⚠️ Current Limitation: Roster Construction Not Considered

**What's missing:** The system does NOT adjust projections or ADP based on **roster construction** differences:

- **Hood Rivals** (2 FLEX) vs **Standard** (1 FLEX) uses the same projections/ADP
- **Deep Bench** (3 WR) vs **Standard** (2 WR) uses the same projections/ADP
- Roster construction significantly impacts player value:
  - More FLEX spots → RB/WR depth more valuable
  - 3 WR starters → WR3/WR4 tier more valuable
  - Superflex → QB value dramatically changes

## The Impact Question

### How Much Does Roster Construction Matter?

**Significant Impact on Relative Value:**

1. **FLEX Slots (1 vs 2)**
   - 2 FLEX leagues: Increases demand for top-50 RBs/WRs by 12 roster spots
   - RB/WR "replacement level" shifts up ~5-10 ADP spots
   - Late-round RBs gain ~0.5-1.0 points of relative value

2. **Starting WR Requirements (2 vs 3)**
   - 3 WR leagues: WR30-WR60 tier becomes starter-eligible
   - WR ADP compresses in rounds 7-12
   - Zero-RB strategies become more viable

3. **Superflex (most extreme)**
   - QB value nearly doubles (QB2 becomes equivalent to RB1)
   - Completely different draft strategy required
   - ADP shifts by 50+ spots for QB2-QB20

4. **Bench Depth (6 vs 7-8)**
   - Deeper benches: Handcuff RBs gain value
   - Late-round fliers become more viable
   - Waiver wire becomes shallower

### Example: Christian McCaffrey

**Standard 12-team (1 FLEX, 6 BN):**
- Half-PPR Projection: 285 pts
- Value over Replacement (VORP): +95 pts
- ADP: 1.01

**Hood Rivals 10-team (2 FLEX, 7 BN):**
- Half-PPR Projection: **Same 285 pts** (scoring didn't change)
- VORP: +87 pts (replacement level shifted up due to shallower player pool + more FLEX spots)
- Expected ADP: Still 1.01 (elite players are always elite)

**The issue:** Mid-tier players (RB15-RB30, WR20-WR40) are where roster construction creates the biggest value shifts, not superstars.

## Solutions (By Implementation Complexity)

### Option 1: **Accept Current System** (No Changes)

**Rationale:**
- Scoring format is already handled correctly (the big factor)
- Roster construction primarily affects **relative value**, not **absolute projections**
- ADP from Fantasy Football Calculator reflects 12-team leagues with standard rosters
- Users can mentally adjust for their roster differences

**When this works:**
- Roster construction is close to standard (1-2 FLEX, 6-7 BN)
- Users understand draft strategy adjusts for roster needs
- The rankings/projections are guidance, not gospel

**Trade-offs:**
- Hood Rivals (10-team, 2 FLEX) will show 12-team standard ADP
- Deep WR leagues will undervalue WR depth
- Superflex leagues would be completely wrong (but that's a different format entirely)

### Option 2: **Roster-Aware ADP Adjustment** (Medium Complexity)

**Implementation:**
Create a position-specific ADP adjustment formula based on roster construction:

```typescript
function adjustAdpForRoster(
  baseAdp: number,
  position: string,
  baseRoster: RosterConfig,
  targetRoster: RosterConfig
): number {
  // Calculate demand shift
  const baseDemand = calculatePositionDemand(position, baseRoster);
  const targetDemand = calculatePositionDemand(position, targetRoster);
  const demandRatio = targetDemand / baseDemand;
  
  // Apply tier-specific adjustment (mid-rounds more sensitive)
  const tier = Math.ceil(baseAdp / 12); // Round number
  const sensitivity = getTierSensitivity(position, tier);
  
  return baseAdp * (1 + (demandRatio - 1) * sensitivity);
}
```

**Position Demand Calculation:**
```typescript
function calculatePositionDemand(pos: string, roster: RosterConfig): number {
  let demand = 0;
  
  // Direct starters
  demand += roster[pos as keyof RosterConfig] || 0;
  
  // FLEX eligibility
  if (pos === "RB" || pos === "WR" || pos === "TE") {
    demand += roster.FLEX * (pos === "TE" ? 0.15 : 0.425); // TE rarely fills FLEX
  }
  
  // Bench factor (deeper benches = more demand for depth)
  const benchFactor = 1 + (roster.BN / 10);
  
  return demand * benchFactor;
}
```

**Files to modify:**
- `web/src/lib/fantasy-football/adp-adjustment.ts` (new file)
- `web/src/db/queries-fantasy-football.ts` - add `adjustedAdp` to rankings query
- `web/src/app/fantasy-football/draft/[draftId]/page.tsx` - pass roster config to adjustment

**Pros:**
- Reflects roster construction impact on player value
- Relatively straightforward math
- No new data sources required

**Cons:**
- ADP adjustment formula needs calibration
- Still using 12-team ADP as the base (might not fit 10-team leagues)
- Doesn't capture league-specific draft tendencies

### Option 3: **Multi-Format ADP Ingestion** (High Complexity)

**Implementation:**
Fetch ADP for multiple league sizes and roster formats from Fantasy Football Calculator's API:

```python
# In ingest/ff_independent.py
FFC_ADP_VARIANTS = [
    {"teams": 10, "roster": "standard"},
    {"teams": 12, "roster": "standard"},
    {"teams": 12, "roster": "2flex"},
    # ... more variants
]
```

Store multiple ADP values per player, select the closest match to the draft's config.

**Database changes:**
```sql
-- Add roster_key to ADP storage
ALTER TABLE ff_player_rankings 
  ADD COLUMN adp_variants JSONB; -- {"12team_std": 45.2, "10team_2flex": 52.1}
```

**Pros:**
- Real market data for different formats
- No adjustment formulas to calibrate
- Most accurate to actual draft behavior

**Cons:**
- Requires finding/scraping ADP for each variant
- Fantasy Football Calculator may not have all roster variants
- Increased storage and ingestion complexity
- Still doesn't capture every possible roster construction

### Option 4: **Value-Based Rankings (VORP)** (Highest Complexity)

**Implementation:**
Calculate **Value Over Replacement Player** based on the specific roster construction:

```typescript
function calculateVORP(
  player: Player,
  roster: RosterConfig,
  leagueSize: number
): number {
  // Determine replacement level for this position in this format
  const replacementRank = calculateReplacementLevel(
    player.position,
    roster,
    leagueSize
  );
  
  const replacementPoints = getProjectionAtRank(
    player.position,
    replacementRank
  );
  
  return player.projectedPoints - replacementPoints;
}
```

**Example Replacement Levels:**
```
12-team, 1 FLEX:
  QB12 is replacement (12 starters)
  RB24 is replacement (24 starters in RB+FLEX)
  WR36 is replacement (36 starters in WR+FLEX)

10-team, 2 FLEX (Hood Rivals):
  QB10 is replacement
  RB30 is replacement (20 RB + 10 FLEX × 50% RB fill rate)
  WR40 is replacement (20 WR + 10 FLEX × 50% WR fill rate)
```

**Pros:**
- Theoretically most accurate
- Captures true positional scarcity
- Adjusts automatically to any roster construction

**Cons:**
- Complex to implement correctly
- Requires full position-by-position projection distribution
- VORP can produce counter-intuitive rankings early in draft
- Doesn't match how most fantasy players actually draft

## Recommendation

### **Implement Option 2: Roster-Aware ADP Adjustment**

**Why:**
1. **Balanced effort/impact** - Medium complexity, meaningful improvement
2. **Leverages existing data** - Uses current projections and ADP
3. **Transparent to users** - Clear "12-team standard ADP: 45 → Hood Rivals adjusted: 48"
4. **Handles the common cases** - FLEX slots, bench depth, WR requirements
5. **Fallback is graceful** - If adjustment fails, shows base ADP

**What to build:**
1. `league-config.ts` already has roster presets ✅
2. New file: `adp-adjustment.ts` with adjustment logic
3. Update `getFantasyRankings()` to include `adjustedAdp` field
4. Draft board displays both base ADP and adjusted ADP
5. Add "ADP adjusted for 2-FLEX format" badge in UI

**What NOT to build (yet):**
- Multi-format ADP ingestion (Option 3) - only if we find real data sources
- Full VORP system (Option 4) - academic overkill for draft assistance
- Superflex support - that's a fundamentally different format, needs separate presets

## Implementation Plan

### Phase 1: Adjustment Formula (Backend)
1. Create `web/src/lib/fantasy-football/adp-adjustment.ts`
2. Implement `calculatePositionDemand()`
3. Implement `adjustAdpForRoster()`
4. Add unit tests for adjustment logic

### Phase 2: Database Integration
1. Modify `getFantasyRankings()` to compute `adjustedAdp`
2. Pass draft's `roster_config` through the query
3. Return both `adp` (base) and `adjustedAdp` (roster-aware)

### Phase 3: UI Display
1. Update draft board to show adjusted ADP
2. Add tooltip: "Adjusted for [roster format]"
3. Show delta: "Base ADP 45 → +3 (2-FLEX)"

### Phase 4: Validation
1. Manual spot-checks: Does RB depth shift correctly in 2-FLEX?
2. Sanity bounds: Adjustments shouldn't exceed ±20 ADP spots
3. User feedback: Does it match draft experience?

## Open Questions

1. **League size impact:** Should 10-team vs 12-team also adjust ADP?
   - **Recommendation:** Yes, but as a separate multiplier (simpler than full multi-format)
   
2. **Bench depth sensitivity:** How much does BN:7 vs BN:6 actually matter?
   - **Recommendation:** Small factor (1.05x per extra bench spot), mainly affects late rounds

3. **TE premium in FLEX:** TEs rarely fill FLEX in practice, but should they count?
   - **Recommendation:** Use 15% fill rate for TE in FLEX demand calculation

4. **When to show base vs adjusted:** Always show both, or hide base?
   - **Recommendation:** Show both with delta for transparency

## Success Metrics

**How we'll know this worked:**
1. Hood Rivals (2 FLEX) shows RB/WR depth 3-5 ADP spots higher than standard
2. Deep WR league shows WR3-WR4 tier compressed upward
3. Draft recommendations feel more aligned with actual roster needs
4. User feedback: "ADP adjustments match my league's draft patterns"

**What won't change:**
- Top-12 players (superstars are format-agnostic)
- Kickers and Defenses (late-round, minimal strategy)
- Projected points (scoring format handles that, roster construction doesn't)
