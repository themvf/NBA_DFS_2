# Browser Performance Improvement

## Scope

This document records the browser-performance improvements implemented for the DraftKings NFL Best Ball workspace at `/fantasy-football/best-ball`.

Implementation commit: `8d5512b` (`speed up best ball draft interactions`)

## Original problem

The Best Ball player-selection screen became noticeably slow when adding or removing players. Three browser-side costs were contributing to the lag:

1. The page mounted the complete player board—up to 260 detailed rows—even though only a small number were visible.
2. Every row calculated and mounted its full **How projected** explanation before the user opened it.
3. Draft actions read and wrote browser storage synchronously and kept the draft-state subscription coupled to the component rendering the player board.

The underlying snake-draft calculations were small. The primary problem was unnecessary browser rendering and component work.

## Implemented improvements

### 1. Viewport-virtualized player board

The player board now uses `@tanstack/react-virtual` to mount only the rows in or near the visible viewport.

- The full filtered player list remains searchable and scrollable.
- Six rows are rendered as overscan above and below the visible area to keep scrolling smooth.
- Player rows are dynamically measured, so opening a projection explanation can increase a row's height without overlapping the next row.
- The column header remains visible within the player-board scrolling area.
- Filtering resets the virtual list to its first row.
- Drafted players continue to disappear from the available-player list.

Rendered verification with 258 available players mounted 14 player rows instead of 258.

Primary file: `web/src/app/fantasy-football/best-ball/best-ball-player-board.tsx`

### 2. Lazy projection explanations

`ProjectionNotation` no longer calls `buildProjectionExplanation()` for every player during the initial render.

- The closed row mounts only the **How projected** control.
- Explanation calculations and detailed markup are created after the user opens the control.
- Closing the control removes the detailed explanation from the mounted page.

Rendered verification showed zero explanation panels before interaction and one panel after opening a player explanation.

Primary file: `web/src/app/fantasy-football/rankings/projection-notation.tsx`

### 3. Immediate in-memory draft updates with deferred persistence

The active draft is now maintained in React memory during the session.

- Add, Undo, Reset, and draft-position changes update in-memory state first.
- The updated draft is saved to `localStorage` with `requestIdleCallback`, using a 400 ms maximum wait.
- Browsers without idle-callback support use a zero-delay timer fallback.
- A pending save is replaced when another draft action occurs, preventing redundant storage writes.
- Pending state is flushed when the component unmounts.
- The browser `storage` event keeps another open tab synchronized.
- The existing per-ranking-set storage key and saved draft format remain unchanged.

Primary file: `web/src/app/fantasy-football/best-ball/best-ball-client.tsx`

## Verification evidence

The implementation passed the following checks:

- Focused ESLint checks for all modified React components.
- `npm run test:fantasy-football`.
- Full `npm run build`, including TypeScript validation.
- Browser verification of Add, Undo, filtering, and projection-detail interactions.
- Draft-state restoration after a browser reload.
- Dynamic virtual-row remeasurement after opening projection details.
- 14 mounted player rows with 258 available players.
- No browser console warnings or errors during the tested workflow.
- Successful Vercel deployment and HTTP 200 verification on the production route.

Production route: <https://nbadfs.vercel.app/fantasy-football/best-ball>

## Performance guardrails

Future changes to this page should preserve the following requirements:

1. Do not replace the virtualized board with a direct `.map()` that mounts the complete player pool.
2. Do not perform projection-explanation calculations until the explanation is opened.
3. Do not add synchronous network or browser-storage work to Add, Undo, or Reset handlers.
4. Keep expensive filters and lookup structures memoized or indexed.
5. Ensure expanded or variable-height row content is measured by the virtualizer.
6. Verify the mounted `[data-index]` row count remains close to the visible row count—not the total player count.
7. Test both an early draft and a nearly complete 240-pick draft.
8. Confirm draft persistence with a reload after changing the state.
9. Inspect the rendered page and browser console before reporting the performance work complete.

## Relevant files

- `web/src/app/fantasy-football/best-ball/best-ball-client.tsx`
- `web/src/app/fantasy-football/best-ball/best-ball-player-board.tsx`
- `web/src/app/fantasy-football/rankings/projection-notation.tsx`
- `web/package.json`
- `web/package-lock.json`
- `CLAUDE.md`
