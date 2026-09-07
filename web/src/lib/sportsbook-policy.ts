// Keep aligned with ingest/sportsbook_policy.py; parity is regression-tested.
export const SPORTSBOOK_KEYS = ["pinnacle", "fanduel", "fanatics", "draftkings", "williamhill_us", "betmgm"];
export const SPORTSBOOK_QUERY = SPORTSBOOK_KEYS.join(",");
export const SPORTSBOOK_NAMES: Record<string,string> = {pinnacle:"Pinnacle", fanduel:"FanDuel", fanatics:"Fanatics", draftkings:"DraftKings", williamhill_us:"Caesars", betmgm:"BetMGM"};
export function selectedSportsbooks<T>(books: Record<string,T> | null | undefined): Record<string,T> {
 const source = {...books};
 if ("caesars" in source && !("williamhill_us" in source)) source.williamhill_us = source.caesars;
 return Object.fromEntries(SPORTSBOOK_KEYS.filter(key=>key in source).map(key=>[key,source[key]]));
}
