# Defensive optimizer connection: September 27 verification

Status: implemented on the draft PR and exercised against the saved pregame Classic slate. The Vercel preview deployment passed; browser acceptance remains open. The active default remains **Off**; neither defensive profile has a qualified production verdict.

## Consumer contract

- Explicit Experimental mode supports separate PFR efficiency and allowed-rushing-volume profiles on an exact saved historical-v5 baseline. Unsupported source combinations fail. An Approved selection uses only a server-controlled, exact PASS policy; absent or incompatible policy retains baseline. The default is Off.
- The optimizer reads a frozen per-player bundle for mean, P10, median, P90, boom and stat means. GPP/cash objectives and saved slot summaries use the selected tails. Player audit and saved input digest contain the selected bundle and fallback reason.
- Saved runs restore their original bundles and QA inputs. Experimental entry rewrites reload the server's saved roster, validate its input digest and QA, and recheck every salary game before export. New adjusted generation and entry rewriting stop at the first kickoff.
- The PFR publisher continues to use the frozen pressure/contact models. Its capture is now pinned to the salary upload's baseline run. The volume publisher reproduces the registered full draw loop and scores every scaled stat draw, without modifying historical production rows or the compact research payload.

## Pregame Classic evidence

The pregame server-action checks ran on the working checkout before integration onto current main. Saved salary upload `d5d97cc7-0574-491b-ae89-efad4b836b37`; historical-v5 baseline `1f066a50-d1ee-5115-b32b-26723d8a572a`. PFR capture `b621112180f8eddc2527fa359589fac8bc4bbd7f0be6b628f13b6e0bd6f27e6f` had 643 matched rows and 149 research adjustments. The stricter optimizer bundle checks applied 115 players; two were in the tested selected lineup. The Off and Experimental rosters had the same player IDs in a different slot order.

The full allowed-rushing-volume capture `4ab2735c2d7c8c9a1c092c2894c9c76a0d4e1c94d4fcbb9d04889a0d9cf5ead0` had 643 matched rows and 493 research adjustments. The optimizer applied 368 players; seven were in the tested selected lineup. The paired Off and Experimental 20-entry runs each generated the requested count. Experimental run `ea34d346-fe57-4de3-916c-97d56b24eb0b` reopened with unchanged roster IDs and input digest `b1b1e73334adfea116d434d5457eb23fc3b26c88660dcbcead0860486338508e`. Its server export preserved 20 distinct synthetic entry IDs and used the 20 saved rosters. A separate PFR three-entry paired run also completed.

The 149/493 research counts are **not** optimizer applied counts. Players with unreproduced saved availability, missing draws, mismatched identity or incomplete distributions retained baseline. No realized contest return is claimed.

## Verification and remaining gates

- TypeScript compile and a complete Next.js Webpack production build passed on current main after integration. Controlled tests passed again there: GPP ranking changes with equal means but different P90/boom, cash ranking responds to P10, and a near-tie Classic roster changes. Two archived Showdown replay fixtures passed legal Captain pricing, one 1.5× multiplier, three-lineup generation and CSV identities. The full volume draw loop reproduced its prior compact mean, tails and boom in a fixture.
- After the 17:00 UTC first kickoff, server actions rejected both new adjusted generation and a rewrite of an earlier saved entry run. Historical read/download remains available.
- All saved Showdown slates inspected on September 27 were already started; no two upcoming Showdown cases were available for live production-action acceptance. Vercel deployed the draft PR preview successfully, but a browser check of the page did not run because the browser inspection tool failed before opening it. The browser flow and a deployed end-to-end run remain unverified. Those gates remain open before calling the overall handoff complete.
- Approved mode has no active PASS record; the server policy falls back to baseline. A real default promotion remains subject to the existing forward qualification contracts. The environment controls are `NFL_DEFENSIVE_ACTIVATION`, `NFL_DEFENSIVE_QUALIFICATION`, and the immediate `NFL_DEFENSIVE_REVOKE=true` kill switch.
