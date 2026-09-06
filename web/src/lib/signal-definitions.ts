/** Display reference for the current detectors; thresholds can differ by sport/version. */
export type SignalDefinition = { name: string; key: string; definition: string };
export const movementDefinitions: SignalDefinition[] = [
  {
    "name": "Steam",
    "key": "steam",
    "definition": "Several sportsbooks move their moneyline toward the same side between consecutive captures. The standard trigger requires at least three books moving at least 1.5 percentage points each."
  },
  {
    "name": "Walking",
    "key": "walking",
    "definition": "A slower moneyline drift toward a side since the first recorded price. The standard threshold is 2 percentage points; every intermediate move does not have to be in the same direction."
  },
  {
    "name": "Spread steam",
    "key": "spread_steam",
    "definition": "Multiple sportsbooks move the point spread together over a short interval. Thresholds differ between NFL and CFB."
  },
  {
    "name": "Spread walking",
    "key": "spread_walking",
    "definition": "The spread gradually moves away from its first recorded number."
  },
  {
    "name": "Total steam",
    "key": "total_steam",
    "definition": "Multiple sportsbooks move the game total together over a short interval."
  },
  {
    "name": "Total walking",
    "key": "total_walking",
    "definition": "The game total drifts materially from its first recorded number."
  },
  {
    "name": "Reversal",
    "key": "reversal",
    "definition": "The market first moves one way, then retraces materially in the opposite direction. The recorded side follows the reversal."
  },
  {
    "name": "Reference led",
    "key": "reference_led",
    "definition": "Pinnacle moves first while retail books remain relatively quiet, then retail books follow in the same direction."
  },
  {
    "name": "Price pressure",
    "key": "price_pressure",
    "definition": "Coordinated price movement before—or without—a change in the spread or total. Moneyline versions detect broad movement smaller than the usual steam threshold."
  },
  {
    "name": "Key-number crossing",
    "key": "key_cross",
    "definition": "A football spread reaches or crosses an important scoring margin: 3 or 7 for NFL, and 3, 7, 10 or 14 for CFB."
  },
  {
    "name": "Late move",
    "key": "late_move",
    "definition": "A meaningful moneyline move during the final hour before scheduled start, measured against an earlier observation."
  },
  {
    "name": "Favorite flip",
    "key": "favorite_flip",
    "definition": "A side crosses the 50% fair-probability boundary, changing which side the market favors. Requires meaningful movement, not just a tiny crossing."
  }
];
export const valueDefinitions: SignalDefinition[] = [
  {
    "name": "Pinnacle divergence",
    "key": "pinnacle_divergence",
    "definition": "Pinnacle assigns a side at least 2 percentage points more probability than the retail consensus."
  },
  {
    "name": "Pinnacle favorite forward",
    "key": "pinnacle_favorite_forward",
    "definition": "A separately versioned tennis research cohort for qualifying favorite-side Pinnacle-divergence observations. Kept separate from ordinary divergence for evaluation."
  },
  {
    "name": "Pinnacle–Polymarket difference",
    "key": "pinnacle_polymarket_delta",
    "definition": "Pinnacle assigns a side at least 2 percentage points more probability than Polymarket. Compares a sportsbook reference with an exchange source."
  },
  {
    "name": "DraftKings value",
    "key": "dk_value",
    "definition": "DraftKings’ offered price implies at least 2% estimated expected value using Pinnacle’s fair probability. The estimate depends on Pinnacle being a useful reference."
  },
  {
    "name": "Book disagreement",
    "key": "book_disagreement",
    "definition": "Retail books show a wide probability range—at least 6 percentage points in the moneyline detector. Observation only."
  },
  {
    "name": "Market convergence",
    "key": "market_convergence",
    "definition": "Previously wide sportsbook disagreement narrows substantially. The moneyline detector looks for dispersion shrinking from at least 6 percentage points to 2 or less."
  }
];
export const mlbDefinitions: SignalDefinition[] = [
  {
    "name": "MLB total steam",
    "key": "mlb_total_steam",
    "definition": "Movement in the run total itself, such as 8.5 to 9. Consecutive captures no more than 40 minutes apart."
  },
  {
    "name": "MLB total walking",
    "key": "mlb_total_walking",
    "definition": "Movement in the run total itself, such as 8.5 to 9. At least three observations over 40 minutes to six hours, moving consistently in one direction."
  },
  {
    "name": "MLB total reversal",
    "key": "mlb_total_reversal",
    "definition": "Movement in the run total itself, such as 8.5 to 9. A qualifying initial move followed by a qualifying retracement, using at least three observations over 40 minutes to six hours."
  },
  {
    "name": "MLB total price steam",
    "key": "mlb_total_price_steam",
    "definition": "Price movement while comparing the same total, such as Over 8.5 becoming more expensive. Consecutive captures no more than 40 minutes apart."
  },
  {
    "name": "MLB total price walking",
    "key": "mlb_total_price_walking",
    "definition": "Price movement while comparing the same total, such as Over 8.5 becoming more expensive. At least three observations over 40 minutes to six hours, moving consistently in one direction."
  },
  {
    "name": "MLB total price reversal",
    "key": "mlb_total_price_reversal",
    "definition": "Price movement while comparing the same total, such as Over 8.5 becoming more expensive. A qualifying initial move followed by a qualifying retracement, using at least three observations over 40 minutes to six hours."
  },
  {
    "name": "MLB run-line price steam",
    "key": "mlb_run_line_steam",
    "definition": "Price movement at the same handicap, such as −1.5 moving from +120 toward +100. Consecutive captures no more than 40 minutes apart."
  },
  {
    "name": "MLB run-line price walking",
    "key": "mlb_run_line_walking",
    "definition": "Price movement at the same handicap, such as −1.5 moving from +120 toward +100. At least three observations over 40 minutes to six hours, moving consistently in one direction."
  },
  {
    "name": "MLB run-line price reversal",
    "key": "mlb_run_line_reversal",
    "definition": "Price movement at the same handicap, such as −1.5 moving from +120 toward +100. A qualifying initial move followed by a qualifying retracement, using at least three observations over 40 minutes to six hours."
  },
  {
    "name": "MLB run-line handicap steam",
    "key": "mlb_run_line_points_steam",
    "definition": "Movement in the handicap itself, rather than just its price. Consecutive captures no more than 40 minutes apart."
  },
  {
    "name": "MLB run-line handicap walking",
    "key": "mlb_run_line_points_walking",
    "definition": "Movement in the handicap itself, rather than just its price. At least three observations over 40 minutes to six hours, moving consistently in one direction."
  },
  {
    "name": "MLB run-line handicap reversal",
    "key": "mlb_run_line_points_reversal",
    "definition": "Movement in the handicap itself, rather than just its price. A qualifying initial move followed by a qualifying retracement, using at least three observations over 40 minutes to six hours."
  },
  {
    "name": "MLB moneyline reversal",
    "key": "mlb_moneyline_reversal",
    "definition": "An MLB moneyline moves one way and then materially reverses. Existing steam and walking signals handle the other MLB moneyline patterns. Requires at least three observations over 40 minutes to six hours."
  }
];
export const otherDefinitions: SignalDefinition[] = [
  {
    "name": "Prop value",
    "key": "dk_prop_value",
    "definition": "DraftKings offers estimated value against Pinnacle at the same prop line, generally requiring 3% estimated expected value. Also used by the separate tennis-total scanner. Outside Sports → Tracking."
  },
  {
    "name": "Prop line gap",
    "key": "prop_line_gap",
    "definition": "DraftKings and Pinnacle post different lines for the same proposition. The MLB version is maintained as a control, not a recommended play. Outside Sports → Tracking."
  },
  {
    "name": "Prop outlier",
    "key": "prop_outlier",
    "definition": "Historical soccer signal identifying an unusually generous DraftKings price relative to other books. Retired; historical records remain. Outside Sports → Tracking."
  }
];
