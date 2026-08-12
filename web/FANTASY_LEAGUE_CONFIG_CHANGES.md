# Fantasy Football League Configuration System - Changes Summary

## Overview

Refactored the NFL fantasy football league settings from hardcoded values to a flexible preset-based configuration system. This makes it easy to support multiple league formats and customize settings without code changes.

## Changes Made

### 1. New Configuration File: `league-config.ts`

**Location:** `web/src/lib/fantasy-football/league-config.ts`

**What it provides:**
- **Roster Presets** - Predefined roster structures (Hood Rivals, Standard 10/12-team, Deep Bench, Superflex)
- **Scoring Presets** - Standard, Half-PPR, Full PPR with complete point values
- **League Format Presets** - Team count, playoff structure, waiver settings
- **Helper Functions** - Get presets, validate configs, calculate roster sizes
- **TypeScript Types** - Full type safety for all configurations

**Key Presets:**

```typescript
// Roster Presets
ROSTER_PRESETS = {
  "hood-rivals": {
    config: { QB: 1, RB: 2, WR: 2, TE: 1, FLEX: 2, K: 1, DST: 1, BN: 7, IR: 1 }
  },
  "standard-12team": {
    config: { QB: 1, RB: 2, WR: 2, TE: 1, FLEX: 1, K: 1, DST: 1, BN: 6 }
  },
  // ... more presets
}

// Scoring Presets
SCORING_PRESETS = {
  "HALF": {
    config: {
      preset: "HALF",
      receptions: 0.5,
      passingYardsPerPoint: 25,
      passingTD: 4,
      // ... complete scoring rules
    }
  },
  // ... STD, PPR presets
}
```

### 2. Updated Server Actions: `actions.ts`

**Location:** `web/src/app/fantasy-football/actions.ts`

**Changes:**
- Removed hardcoded `DEFAULT_ROSTER` constant
- Added imports for `getRosterPreset()` and `getScoringPreset()`
- Updated `createFantasyDraft()` to:
  - Accept `roster` and `scoring` preset keys from form
  - Load full configurations from presets
  - Store complete config objects in database

**Before:**
```typescript
const DEFAULT_ROSTER = { QB: 1, RB: 2, WR: 2, TE: 1, FLEX: 1, K: 1, DST: 1, BN: 6 };
const scoring = String(formData.get("scoring") || "PPR");
```

**After:**
```typescript
const rosterPreset = String(formData.get("roster") || "hood-rivals");
const scoringPreset = String(formData.get("scoring") || "HALF");
const rosterConfig = getRosterPreset(rosterPreset);
const scoringConfig = getScoringPreset(scoringPreset);
```

### 3. Updated Draft Creation Form: `draft/new/page.tsx`

**Location:** `web/src/app/fantasy-football/draft/new/page.tsx`

**Changes:**
- Added import for preset constants
- Changed default team count from 12 → 10 (Hood Rivals)
- Added **Roster Format** dropdown with all roster presets
- Updated **Scoring** dropdown to use preset constants
- Changed default scoring from PPR → HALF (0.5 PPR)
- Added descriptive text below each dropdown

**New Form Fields:**
```tsx
<label>
  <span>Roster format</span>
  <select name="roster" defaultValue="hood-rivals">
    {Object.entries(ROSTER_PRESETS).map(([key, preset]) => (
      <option key={key} value={key}>{preset.name}</option>
    ))}
  </select>
  <p className="text-xs text-muted-foreground">{preset.description}</p>
</label>

<label>
  <span>Scoring</span>
  <select name="scoring" defaultValue="HALF">
    {Object.entries(SCORING_PRESETS).map(([key, preset]) => (
      <option key={key} value={key}>{preset.name}</option>
    ))}
  </select>
</label>
```

### 4. Documentation: `README.md`

**Location:** `web/src/lib/fantasy-football/README.md`

Comprehensive guide covering:
- Available presets and their configurations
- Usage examples in server actions and components
- Helper functions reference
- How to add new presets
- Matching external league settings (Yahoo, ESPN, Sleeper)
- TypeScript types and future enhancements

## Benefits

### 1. **Flexibility**
- Support multiple league formats without code changes
- Easy to add new presets (e.g., Yahoo 2027, ESPN Custom, etc.)
- Users can select their preferred format at draft creation

### 2. **Maintainability**
- Single source of truth for all league configurations
- No more scattered hardcoded values
- Easy to update scoring rules across all formats

### 3. **Type Safety**
- Full TypeScript types for all configurations
- Compile-time validation of roster and scoring structures
- IntelliSense support in editors

### 4. **User Experience**
- Clear preset names ("Hood Rivals", "Standard 12-Team")
- Descriptive text explaining each option
- Defaults match the most common use case

### 5. **Extensibility**
- Helper functions for validation and calculations
- Structured format for import/export
- Easy to add league format settings (playoffs, waivers, trades)

## Current Default Settings

Based on the "Hood Rivals" Yahoo league:

**Roster:**
- 1 QB, 2 RB, 2 WR, 1 TE, 2 FLEX (W/R/T)
- 1 K, 1 DST
- 7 Bench, 1 IR

**Scoring:**
- Half PPR (0.5 points per reception)
- Standard passing (25 yards/pt, 4pt TD)
- Standard rushing/receiving (10 yards/pt, 6pt TD)
- Standard kicking and defense

**League:**
- 10 teams
- 15 rounds
- Snake draft

## Future Enhancements

Potential additions mentioned in the README:
- Custom scoring rules UI
- Import/export functionality
- Dynasty/keeper league support
- IDP (Individual Defensive Player) configurations
- League format presets with waiver/trade rules
- Validation for compatible roster/scoring combinations

## Testing Recommendations

1. **Create a draft** with different roster presets
2. **Verify database storage** - roster_config and scoring_config should be full JSONB objects
3. **Test validation** - Use helper functions to validate custom configs
4. **Check UI rendering** - Ensure dropdowns populate correctly
5. **Backwards compatibility** - Existing drafts should continue to work

## Migration Notes

**No database migration required** - The database already stores `roster_config` and `scoring_config` as JSONB, so it can handle both old and new formats:
- Old: Simple object like `{ QB: 1, RB: 2, ... }`
- New: Same structure, just populated from presets

**Existing drafts** will continue to work - they have their configs frozen at creation time.
