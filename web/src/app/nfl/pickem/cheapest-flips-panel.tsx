"use client";

import type { NarrativeRead } from "@/lib/nfl/pickem-archetypes";
import { DEVIATION_MIN_POOL } from "@/lib/nfl/pickem-policy";
import type {
  FlipCandidate,
  FlipLadderStep,
  PickemGame,
  PoolFormat,
  flipsFromChalk,
} from "@/lib/nfl/pickem-strategy";

function pct(x: number, digits = 1): string {
  return `${(x * 100).toFixed(digits)}%`;
}

/**
 * Which favourites are cheapest to fade, what each fade costs, which side of
 * the pool it leaves, and how many to make. Rendering only: every number is
 * computed in `pickem-strategy.ts` (cheapestFlips, flipLadder, flipsFromChalk),
 * where the exact parts are tested against brute force.
 */
export function CheapestFlipsPanel({
  games,
  format,
  candidates,
  ladder,
  cardVsChalk,
  readByGame,
  cardLocked,
  poolEntries,
  chalkFraction,
  chalkRivals,
  rivalCount,
  sims,
}: {
  games: PickemGame[];
  format: PoolFormat;
  candidates: FlipCandidate[];
  ladder: FlipLadderStep[];
  cardVsChalk: ReturnType<typeof flipsFromChalk> | null;
  readByGame: Map<number, NarrativeRead["verdict"]>;
  cardLocked: boolean;
  poolEntries: number;
  chalkFraction: number;
  chalkRivals: number | null;
  rivalCount: number | null;
  sims: number;
}) {
  return (
    <section className="rounded-lg border bg-card">
      <header className="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
        <h2 className="text-sm font-semibold">
          Cheapest flips this week
          <span className="ml-2 font-normal text-muted-foreground">
            cost and ties are exact · the payoff is simulated
          </span>
        </h2>
        <span className="font-mono text-[10px] uppercase text-muted-foreground">ranked by price, not by story</span>
      </header>
      <p className="border-b px-3 py-2 text-xs text-muted-foreground">
        The cheapest favourite to fade is the one nearest a coin flip. Five pre-registered studies
        here found nothing that picks upsets better than the betting line, so a flip is a bet on
        standing apart from the pool, not a claim that the underdog is better.
      </p>

      {cardVsChalk && (
        <p className={`border-b px-3 py-2 text-xs ${cardVsChalk.canTieChalk && cardVsChalk.flipped.length > 0
          ? "bg-amber-500/5 text-amber-800 dark:text-amber-300" : "text-muted-foreground"}`}>
          <strong className="text-foreground">Your card: </strong>
          {cardVsChalk.canTieChalk === null
            ? `${cardVsChalk.flipped.length} flip${cardVsChalk.flipped.length === 1 ? "" : "s"} and a reordered confidence ranking, so it differs from the all-favourites card beyond its flips and the tie check does not apply.`
            : cardVsChalk.flipped.length === 0
              ? "this is the all-favourites card, so it ties every rival who submits it."
              : cardVsChalk.canTieChalk
                ? `${cardVsChalk.flipped.length} flips (${cardVsChalk.flipped.map((i) => games[i].pHome >= 0.5 ? games[i].awayAbbrev : games[i].homeAbbrev).join(", ")}) can finish exactly level with the all-favourites card, which splits the prize with every rival holding it.`
                : `${cardVsChalk.flipped.length} flip${cardVsChalk.flipped.length === 1 ? "" : "s"} (${cardVsChalk.flipped.map((i) => games[i].pHome >= 0.5 ? games[i].awayAbbrev : games[i].homeAbbrev).join(", ")}) — it can never finish level with the all-favourites card.`}
        </p>
      )}

      {cardLocked ? (
        <p className="p-3 text-sm text-muted-foreground">
          The card locked at the week&apos;s first kickoff, so no flips are available. If your pool
          locks game by game, set that in the pool rules.
        </p>
      ) : candidates.length === 0 ? (
        <p className="p-3 text-sm text-muted-foreground">Every game this week has started.</p>
      ) : (
        <>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-muted/50 text-left text-xs text-muted-foreground">
                <tr>
                  <th className="px-3 py-2 font-medium">Flip</th>
                  <th className="px-3 py-2 text-right font-medium">Favourite wins</th>
                  <th className="px-3 py-2 text-right font-medium">Costs (pts)</th>
                  <th className="px-3 py-2 text-right font-medium">Pool on favourite</th>
                  <th className="px-3 py-2 text-right font-medium">Leverage</th>
                  <th className="px-3 py-2 font-medium">Room read</th>
                </tr>
              </thead>
              <tbody>
                {candidates.slice(0, 6).map((c) => {
                  const onCard = cardVsChalk?.flipped.includes(c.i) ?? false;
                  const read = readByGame.get(games[c.i].gameId);
                  return (
                    <tr key={c.i} className="border-t">
                      <td className="px-3 py-2">
                        <span className="font-semibold">{c.toAbbrev}</span>
                        <span className="text-muted-foreground"> over {c.fromAbbrev}</span>
                        {format === "confidence" && (
                          <span className="ml-1.5 font-mono text-[10px] text-muted-foreground">({c.confidence})</span>
                        )}
                        {onCard && (
                          <span className="ml-2 rounded border px-1 font-mono text-[10px] uppercase text-muted-foreground">on your card</span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums">{pct(c.pFrom)}</td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums text-rose-600 dark:text-rose-400">
                        −{c.evCost.toFixed(3)}
                      </td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums">
                        {pct(c.fieldOnFrom)}
                        <span className="ml-1 text-[10px] text-muted-foreground">
                          {c.fieldSource === "observed" ? "entered" : "modeled"}
                        </span>
                      </td>
                      <td className={`px-3 py-2 text-right font-mono tabular-nums ${c.fieldSource === "modeled" ? "text-muted-foreground" : ""}`}>
                        {c.leverage >= 0 ? "+" : ""}{(c.leverage * 100).toFixed(1)}pp
                      </td>
                      <td className="px-3 py-2 text-xs">
                        {read ? (
                          <span className={`rounded border px-1.5 py-0.5 ${read === "crowded" ? "border-rose-500/40"
                            : read === "contrarian" ? "border-amber-500/40" : "border-emerald-500/40"}`}>
                            {read}
                          </span>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {ladder.length > 1 && (() => {
            const best = ladder.reduce((a, b) => (b.prizeShare > a.prizeShare ? b : a));
            return (
              <div className="border-t">
                <div className="px-3 pt-2 text-xs font-semibold">
                  How many to flip
                  <span className="ml-2 font-normal text-muted-foreground">
                    the cheapest k, scored on the same {sims.toLocaleString()} simulated weeks
                  </span>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="text-left text-xs text-muted-foreground">
                      <tr>
                        <th className="px-3 py-2 font-medium">Flips</th>
                        <th className="px-3 py-2 text-right font-medium">Costs (pts)</th>
                        <th className="px-3 py-2 text-right font-medium">Win the pool</th>
                        <th className="px-3 py-2 text-right font-medium">vs all favourites</th>
                        <th className="px-3 py-2 font-medium">Ties all-favourites card?</th>
                      </tr>
                    </thead>
                    <tbody>
                      {ladder.map((step) => (
                        <tr key={step.k} className={`border-t ${step === best ? "bg-muted/40" : ""}`}>
                          <td className="px-3 py-2">
                            <span className="font-mono tabular-nums">{step.k}</span>
                            {step.k > 0 && (
                              <span className="ml-2 text-xs text-muted-foreground">
                                {step.flips.map((i) => games[i].pHome >= 0.5 ? games[i].awayAbbrev : games[i].homeAbbrev).join(", ")}
                              </span>
                            )}
                            {step === best && (
                              <span className="ml-2 font-mono text-[10px] uppercase text-muted-foreground">highest simulated</span>
                            )}
                          </td>
                          <td className="px-3 py-2 text-right font-mono tabular-nums">
                            {step.k === 0 ? "0" : `−${step.evCost.toFixed(3)}`}
                          </td>
                          <td className="px-3 py-2 text-right font-mono tabular-nums">{pct(step.prizeShare, 2)}</td>
                          <td className="px-3 py-2 text-right font-mono tabular-nums text-muted-foreground">
                            {step.k === 0 ? "—" : `${step.gainVsChalk >= 0 ? "+" : ""}${(step.gainVsChalk * 100).toFixed(2)}pp ± ${(2 * step.gainStdErr * 100).toFixed(2)}`}
                          </td>
                          <td className="px-3 py-2 text-xs text-muted-foreground">
                            {step.k === 0
                              ? "Always (it is that card)"
                              : step.canTieChalk
                                ? "Yes — prize splits with all of them"
                                : "Never"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            );
          })()}
        </>
      )}

      <div className="space-y-1.5 border-t px-3 py-2 text-xs text-muted-foreground">
        <p>
          <strong className="text-foreground">Proved:</strong>{" "}
          {format === "straight"
            ? "your score minus the all-favourites card's score is 2 × (flips that win) − (number of flips). With an even number of flips that can be zero, and tying that card means tying every rival who submitted it. With an odd number it never can."
            : "you finish level with the all-favourites card only when the confidence on flips that win equals the confidence on flips that lose. Two flips with different weights never can; three can when two weights add up to the third."}{" "}
          The cost column is <code className="font-mono">c(2p − 1)</code>; in a straight pool a point is one correct pick.
        </p>
        <p>
          <strong className="text-foreground">Modeled:</strong> whether any flip beats none, and how
          many, depends on the pool — the win column assumes {poolEntries} entries with{" "}
          {chalkRivals != null && rivalCount != null ? `${chalkRivals} of ${rivalCount}` : "a share of"} rivals taking every
          favourite ({Math.round(chalkFraction * 100)}%, a stated prior). Differences inside the ± band
          (two Monte Carlo standard errors) are simulation noise, and the band says nothing about
          whether the field model is right.
          {poolEntries < DEVIATION_MIN_POOL && ` At ${poolEntries} entries flipping usually works against you.`}{" "}
          This ladder only tries the cheapest flips. With pick shares entered, a pricier flip can buy
          more separation, and the Max win chance search weighs every one.
        </p>
        {candidates.length > 0 && candidates.every((c) => c.fieldSource === "modeled") && (
          <p>
            Pool shares are modeled from the win probability, so leverage here only restates the
            price. Enter your pool&apos;s pick percentages on the card and it becomes real information.
          </p>
        )}
      </div>
    </section>
  );
}
