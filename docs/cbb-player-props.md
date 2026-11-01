# CBB player props

The CBB player-props board is `/cbb/props`. It is linked from sport navigation and the CBB landing page, and also opens inside the CBB DFS page. The first implementation supports named-player **points, rebounds, and assists** lines and their American over/under prices across returned US sportsbooks. DraftKings appears when the provider returns it; absence is explicitly stated for the selected game.

## Source and scope

Source: [The Odds API NCAAB feed](https://the-odds-api.com/sports/ncaab-odds.html), sport `basketball_ncaab`. The existing server-side `ODDS_API_KEY` is used; no key is sent to the browser. Supported market names and event endpoint follow the [provider documentation](https://the-odds-api.com/liveapi/guides/v4/#get-event-odds).

- `/events` loads the game directory; the response is cached for five minutes. The directory includes in-play and upcoming games, so the UI keeps events with scheduled start no more than six hours ago and labels games whose scheduled start has passed. It does not imply an official live/final game status.
- `/events/{eventId}/odds` requests `player_points,player_rebounds,player_assists`, `regions=us`, `oddsFormat=american`. The shared cache is keyed by event ID and revalidates after one minute. Only one selected game is requested, with no automatic polling of paid endpoints. Reload/refresh does not bypass the cache. Normal Next.js stale-while-revalidate behavior can return an older snapshot during refresh; source/fetch timestamps remain attached to that snapshot.
- The standalone board loads the earliest returned eligible game initially. The DFS panel loads only when expanded. Users can select a different game and filter by player, stat, or book.
- IDs are validated and must appear in the current CBB directory before a game request is made. The returned event ID, home team, and away team must match the selected source event. Provider event start-time changes are retained. Explicit non-CBB sport keys are rejected.
- A line pairs over/under selections only for the identical **event, bookmaker key, exact player description, market, and threshold**. Different books, suffixes, thresholds, or stat types are not merged. Conflicting duplicate prices or timestamps cause the conflicting row to be omitted with a coverage warning. A missing side remains null and displays a dash.
- Returned player descriptions are shown verbatim as text. No source-to-DraftKings identity, team, availability, or roster match is assumed. Selecting a game scopes source player names to that event. The board is separate from the DFS player projection calculation.
- A response with no books/lines is shown as no returned coverage, not zero player expectations or proof that all books lack props. Missing markets and DraftKings coverage are visible. Provider authentication, rate-limit, connection, and malformed-response failures have explicit states; request URLs and credentials are not included in public errors.

## Provenance and interpretation

Each row retains the source player description, market, threshold, book key/name, over and under prices, and provider market update timestamp (book update time as fallback). Missing source times remain unknown. The fetch timestamp is separate and is created when an actual response is fetched, not on every cache hit.

A source timestamp older than 15 minutes is labeled "Older than 15m". Missing, invalid, or more than one minute future source time is labeled unverified. The 15-minute threshold is a display convention, not a claim that a more recent line is still available to bet. Labels update every 30 seconds without making odds requests.

The downloadable source snapshot includes the exact returned payload, normalized lines, selected event, requested region, source name, capture timestamp, skipped count, version, and export timestamp. A snapshot captured after scheduled tipoff is not described as frozen pregame evidence. Browser downloads do not create a server-side historical archive; no historical movement or pre-lock coverage claim is made.

An over/under threshold is not an expected average. These three stat markets omit made threes, steals, blocks, turnovers, and joint double/triple-double bonus probabilities needed for a full DraftKings projection. No automatic fantasy-point estimate, edge, calibrated probability, or ownership estimate is produced from these lines.

## Verification

`node --import tsx scripts/test-cbb-props.ts` exercises exact pairing and missing sides, conflicting/duplicate outcomes, book and threshold separation, suffix preservation, event and sport checks, malformed values, missing coverage, and source-time freshness. `scripts/test-cbb-dfs.ts` remains the separate scoring/lineup check.

Release validation also checks real provider data through the page, filters, empty-game coverage, and access from DFS, plus full TypeScript/lint/build and repository CI. Actual posted markets and names vary by game and bookmaker; synthetic tests cannot establish provider coverage for every slate.
