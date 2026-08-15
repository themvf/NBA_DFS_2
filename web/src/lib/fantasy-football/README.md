# Fantasy Football League Configuration System

This directory contains the configuration system for NFL fantasy football leagues, including roster formats, scoring rules, and league settings.

## Configuration Files

### `league-config.ts`

Central configuration file that defines all league presets. This makes it easy to:
- Add new league formats
- Modify existing presets
- Maintain consistency across the application
- Support multiple league types (Yahoo, ESPN, Sleeper, etc.)

## Available Presets

### Roster Presets

Each roster preset defines the number of players at each position:

- **`hood-rivals`** (Default)
  - 1 QB, 2 RB, 2 WR, 1 TE, 2 FLEX, 1 K, 1 DST
  - 7 Bench, 1 IR
  - Matches the Hood Rivals Yahoo league

- **`standard-10team`** / **`standard-12team`**
  - 1 QB, 2 RB, 2 WR, 1 TE, 1 FLEX, 1 K, 1 DST
  - 6 Bench
  - Classic fantasy format

- **`deep-bench`**
  - 1 QB, 2 RB, 3 WR, 1 TE, 2 FLEX, 1 K, 1 DST
  - 8 Bench
  - For larger rosters / deeper leagues

### Scoring Presets

Three main scoring formats based on reception scoring:

- **`STD`** - Standard (0 points per reception)
- **`HALF`** - Half PPR (0.5 points per reception) - **Default**
- **`PPR`** - Full PPR (1.0 point per reception)

Each preset includes complete scoring rules for:
- Passing (yards, TDs, INTs, bonuses)
- Rushing (yards, TDs, bonuses)
- Receiving (receptions, yards, TDs, bonuses)
- Kicking (field goals by distance, PATs)
- Defense/Special Teams (sacks, turnovers, TDs, points allowed tiers)

### League Format Presets

Define league structure settings:
- Number of teams
- Draft rounds
- Playoff structure (teams, weeks)
- Waiver type (FAB, rolling, reverse)
- Trade deadline

## Usage

### In Server Actions

```typescript
import { getRosterPreset, getScoringPreset } from "@/lib/fantasy-football/league-config";

// Get configurations from user input
const rosterConfig = getRosterPreset(formData.get("roster"));
const scoringConfig = getScoringPreset(formData.get("scoring"));
```

### In Components

```typescript
import { ROSTER_PRESETS, SCORING_PRESETS } from "@/lib/fantasy-football/league-config";

// Render preset options in a form
<select name="roster">
  {Object.entries(ROSTER_PRESETS).map(([key, preset]) => (
    <option key={key} value={key}>{preset.name}</option>
  ))}
</select>
```

### Helper Functions

```typescript
import { 
  calculateRosterSize,
  getScoringDescription,
  validateRosterConfig 
} from "@/lib/fantasy-football/league-config";

// Calculate total roster spots (excluding IR)
const size = calculateRosterSize(roster); // e.g., 16

// Get human-readable scoring format
const desc = getScoringDescription(scoring); // e.g., "Half PPR (0.5 per reception)"

// Validate roster configuration
const { valid, errors } = validateRosterConfig(roster);
if (!valid) {
  console.error(errors);
}
```

## Adding New Presets

To add a new league preset:

1. **Add to `ROSTER_PRESETS`** if it has a unique roster structure:

```typescript
"my-league": {
  name: "My Custom League",
  description: "Description of roster format",
  config: { QB: 2, RB: 2, WR: 3, TE: 1, FLEX: 1, K: 1, DST: 1, BN: 6, IR: 1 },
}
```

2. **Add to `SCORING_PRESETS`** if it has unique scoring rules:

```typescript
"my-scoring": {
  name: "My Custom Scoring",
  description: "Brief description",
  config: {
    preset: "CUSTOM",
    // Define all scoring values...
  },
}
```

3. **Update the form** in `draft/new/page.tsx` if the new preset should be the default

## Matching External League Settings

When importing settings from platforms like Yahoo, ESPN, or Sleeper:

1. Create a new preset with a descriptive key (e.g., `"yahoo-2026"`)
2. Map their roster positions to ours:
   - Yahoo "W/R/T" → `FLEX`
   - ESPN "D/ST" → `DST`
   - Sleeper "SUPER_FLEX" → requires special handling
3. Copy scoring values exactly, including:
   - Points per yard ratios
   - Touchdown values
   - Bonus thresholds
   - Defensive scoring tiers

## Types

All configuration types are exported and can be used throughout the application:

```typescript
import type { 
  RosterConfig, 
  ScoringConfig, 
  LeagueFormatConfig,
  RosterPosition 
} from "@/lib/fantasy-football/league-config";
```

## Future Enhancements

Potential additions to consider:

- Custom scoring rules UI (allow users to modify individual scoring values)
- League format presets with waiver/trade rules
- Import/export functionality for league settings
- Validation for compatible roster/scoring combinations
- Dynasty/keeper league support
- IDP (Individual Defensive Player) configurations
