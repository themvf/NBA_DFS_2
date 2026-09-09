/**
 * The method page: what this tool believes, why, and how much of it is proved.
 *
 * Written in plain language first, formulas second. The archetype table is
 * generated from the shared ARCHETYPES constant rather than transcribed, so the
 * numbers shown here cannot drift from the numbers the board actually uses or
 * the analysis actually measured.
 */

import { ARCHETYPES, type Visibility } from "@/lib/nfl/pickem-archetypes";
import { DEVIATION_MIN_POOL, FIELD_CHALK_FRACTION } from "@/lib/nfl/pickem-policy";
import PickemTabs from "../pickem-tabs";

export const metadata = {
  title: "Pick'em Method & Archetypes",
  description:
    "What the pick'em tool believes, the arithmetic behind it, the game archetypes it tags, and an honest split of what is proved, modelled, and unmeasured.",
};

const VIS_STYLE: Record<Visibility, string> = {
  loud: "bg-amber-500/10 text-amber-700 dark:text-amber-400",
  moderate: "bg-muted text-muted-foreground",
  quiet: "bg-muted/50 text-muted-foreground",
};

function Section({
  n,
  title,
  sub,
  children,
}: {
  n: string;
  title: string;
  sub?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-lg border bg-card">
      <header className="border-b px-4 py-2.5">
        <h2 className="text-sm font-semibold">
          <span className="mr-2 font-mono text-muted-foreground">{n}</span>
          {title}
        </h2>
        {sub && <p className="mt-0.5 text-xs text-muted-foreground">{sub}</p>}
      </header>
      <div className="space-y-3 p-4 text-sm leading-relaxed text-muted-foreground">{children}</div>
    </section>
  );
}

const F = ({ children }: { children: React.ReactNode }) => (
  <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-[13px] text-foreground">
    {children}
  </code>
);

export default function MethodPage() {
  const studies: Array<[string, string, string]> = [
    ["10-segment calibration screen", "analyze:pickem-2025", "0 of 10 confirmed. Late-season favourites REVERSED between periods (+8.3pp then −8.5pp) — a textbook false positive."],
    ["Underdog selection from lines", "analyze:underdogs", "Dead at the mechanism. Margin spread is flat across totals (corr −0.036), so a moneyline built from the spread alone is already right."],
    ["Situational angles", "analyze:situational", "Unanswerable, not merely unanswered. A 2pp effect needs 4,901 games; international games are 15 in three seasons."],
    ["Upset anatomy", "analyze:upsets", "Underdogs win by OUTPLAYING favourites — 80% also won the EPA battle. Only 9.7% 'stole it' on turnovers."],
    ["Form divergence", "analyze:form", "Dies before the market is even involved: a hot streak does not predict the next game (corr −0.005 over 1,152 team-weeks)."],
    ["Archetype pricing", "analyze:archetype-pricing", "6 of 16 archetypes already move the closing spread. The market prices the story, and prices it about right."],
    ["West-coast 1pm line movement", "analyze:line-movement", "All three hypotheses died. The line does not drift against west-coast teams; if anything it drifts toward them."],
  ];

  return (
    <div className="mx-auto max-w-[1100px] space-y-4 p-4">
      <header className="space-y-2">
        <h1 className="text-2xl font-bold tracking-tight">Method &amp; archetypes</h1>
        <p className="max-w-3xl text-sm text-muted-foreground">
          Everything this tool believes, in the order it matters: the one idea, the arithmetic
          behind it, the archetypes it tags, and an honest accounting of which parts are proved and
          which are guesses.
        </p>
      </header>
      <PickemTabs active="method" />

      {/* ------------------------------------------------------------------ */}
      <Section
        n="1"
        title="The one idea"
        sub="Scoring the most points and winning the pool are different goals."
      >
        <p>
          Pick every favourite and you will score well. You will also have almost exactly the same
          sheet as everyone else who did the sensible thing — so the pool gets decided by whose coin
          flips landed, not by whose analysis was better.
        </p>
        <p>
          Measured on real 2025 slates, in a 50-person pool where a quarter of entrants play straight
          chalk, the all-favourites card won <strong className="text-foreground">0.65%</strong> of
          weeks. Your fair share is 2%. It is not that chalk fails to win — it does{" "}
          <em>worse than an average entry</em>, because when favourites hold you split with everyone
          who also played chalk, and when they do not, whoever differentiated wins outright.
        </p>
        <p>
          So the goal is not to be right more often. It is to be{" "}
          <strong className="text-foreground">slightly different, as cheaply as possible</strong>.
        </p>
      </Section>

      {/* ------------------------------------------------------------------ */}
      <Section n="2" title="The simple math" sub="Three formulas. None of them are estimates.">
        <div>
          <p className="font-medium text-foreground">a. The best-scoring card is just a sort.</p>
          <p className="mt-1">
            Expected points are <F>Σ cᵢ × pᵢ</F> — each game&apos;s confidence weight times its win
            probability. The rearrangement inequality says that sum is largest when the biggest
            weight meets the biggest probability. So &quot;rank by win probability&quot; is not a
            heuristic, it is provably the answer. There is nothing else to find — which is exactly
            why everyone finds it.
          </p>
        </div>

        <div>
          <p className="font-medium text-foreground">b. Every deviation has an exact price.</p>
          <p className="mt-1">
            Flipping a game to the other side costs <F>c × (2p − 1)</F> expected points. That is all
            it is: if a favourite is 52%, you are right 52 times in 100 instead of 48, so you give up{" "}
            <F>0.04</F> wins. If the favourite is 82%, you give up <F>0.64</F>. Swapping two
            confidence weights costs <F>(cᵢ − cⱼ) × (pᵢ − pⱼ)</F>.
          </p>
          <p className="mt-1">
            Nothing there is estimated, which is why every move the board suggests arrives with its
            bill attached.
          </p>
        </div>

        <div>
          <p className="font-medium text-foreground">c. Winning the pool has a closed form.</p>
          <p className="mt-1">
            Given one rival&apos;s score distribution, your expected share against{" "}
            <F>R</F> rivals is{" "}
            <F>((x+y)^(R+1) − y^(R+1)) / ((R+1)·x)</F>, where <F>y</F> is the chance a rival scores
            below you and <F>x</F> the chance they tie. That is why a 5,000-entry pool costs no more
            to evaluate than a 20-entry one — the rivals never have to be simulated, only one
            rival&apos;s distribution does.
          </p>
        </div>

        <div className="rounded border border-emerald-500/40 bg-emerald-500/5 p-2.5 text-foreground">
          <p className="font-medium">The practical upshot</p>
          <p className="mt-1 text-muted-foreground">
            Take every favourite, then flip the <strong className="text-foreground">cheapest</strong>{" "}
            game — the one closest to a coin flip, not the one that feels due. Across all of 2025
            that cost about <strong className="text-foreground">one correct pick for the season</strong>.
          </p>
        </div>
      </Section>

      {/* ------------------------------------------------------------------ */}
      <Section
        n="3"
        title="Flip one or three, never two"
        sub="This one is arithmetic, not a preference."
      >
        <p>
          With <F>k</F> flips, the gap between your score and the chalk crowd&apos;s is{" "}
          <F>2 × (dogs that hit) − k</F>. That can equal <strong className="text-foreground">zero
          only when k is even</strong>.
        </p>
        <p>
          So with two flips there is a real chance you go 1-for-2 and land{" "}
          <em>exactly level with everyone playing chalk</em> — splitting the prize with all of them.
          With one flip you either beat them by one or lose by one. You can never tie into the pile.
        </p>
        <p>
          Measured on a 2025-shaped slate, 50 entries, 25% chalk rivals: k=0 wins 0.65%, k=1 3.84%,
          k=2 3.47%, <strong className="text-foreground">k=3 4.62%</strong>. The dip at two is the
          parity effect, and it holds at every pool size and every assumption about how many rivals
          play chalk.
        </p>
        <p className="text-xs">
          The board is not told this rule. Once the field model includes a chalk block, the simulator
          reproduces the sawtooth on its own — a hard-coded constant would be a second source of
          truth free to drift from the model.
        </p>
      </Section>

      {/* ------------------------------------------------------------------ */}
      <Section
        n="4"
        title="Archetypes — they predict your opponents, not the winner"
        sub="Measured across 3,220 team-games, 2020–2025."
      >
        <p>
          <strong className="text-foreground">16 of 17 archetypes have a market gap whose
          confidence interval includes zero.</strong>{" "}
          The closing line already prices rest, travel, kickoff slot and last week&apos;s result —
          all of it is public months ahead. So these do not tell you who wins.
        </p>
        <p>
          What they tell you is how the <em>room</em> will read a game. Your rivals are not running a
          model; they are reacting to what they remember. An archetype the market has already priced
          but the room will still react to is where their card drifts from the price and yours does
          not — leverage that needs the market to be <strong className="text-foreground">right</strong>,
          which is the only kind this project has ever found.
        </p>

        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="bg-muted/50 text-left text-muted-foreground">
              <tr>
                <th className="px-2 py-1.5 font-medium">Archetype</th>
                <th className="px-2 py-1.5 font-medium">Room</th>
                <th className="px-2 py-1.5 font-medium">Pushes</th>
                <th className="px-2 py-1.5 text-right font-medium">Market gap</th>
                <th className="px-2 py-1.5 text-right font-medium">n</th>
              </tr>
            </thead>
            <tbody>
              {[...ARCHETYPES]
                .sort((a, b) => {
                  const r = { loud: 0, moderate: 1, quiet: 2 } as const;
                  return r[a.visibility] - r[b.visibility] || a.label.localeCompare(b.label);
                })
                .map((a) => (
                  <tr key={a.code} className="border-t align-top">
                    <td className="px-2 py-1.5">
                      <div className="font-medium text-foreground">{a.label}</div>
                      <div className="mt-0.5 max-w-md text-[11px]">{a.story}</div>
                    </td>
                    <td className="px-2 py-1.5">
                      <span className={`rounded px-1.5 py-0.5 font-mono text-[10px] uppercase ${VIS_STYLE[a.visibility]}`}>
                        {a.visibility}
                      </span>
                    </td>
                    <td className="px-2 py-1.5 text-[11px]">
                      {a.lean === "toward" ? "toward the tagged team" : a.lean === "against" ? "against the tagged team" : "neither side"}
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono tabular-nums">
                      {a.measuredGapPp == null ? (
                        <span className="text-muted-foreground" title="Added after the 2020-2025 measurement pass. Not yet measured -- which is not the same as measured at zero.">
                          not measured
                        </span>
                      ) : (
                        <>
                          {a.measuredGapPp >= 0 ? "+" : ""}
                          {a.measuredGapPp.toFixed(1)}pp
                        </>
                      )}
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono tabular-nums">
                      {a.measuredN ?? <span className="text-muted-foreground">&mdash;</span>}
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>

        <p className="text-xs">
          <strong className="text-foreground">&quot;Room&quot; is a stated prior, not a
          measurement.</strong>{" "}
          There is no pick-share feed here, so how loudly an archetype announces itself is a
          judgement. It is the weakest column on this page.
        </p>

        <div className="rounded border p-2.5">
          <p className="font-medium text-foreground">How to actually use them</p>
          <p className="mt-1">
            A <strong className="text-foreground">favourite with a loud positive story</strong> is a
            poor flip target — expensive by price <em>and</em> crowded on the other side. An{" "}
            <strong className="text-foreground">underdog whose opponent carries that story</strong>{" "}
            is where the room is most over-committed; if that game is also near a coin flip, it is
            the cheapest flip on the board and the most contrarian. Price still decides{" "}
            <em>whether</em> to flip. Archetypes only break ties between similarly-priced games.
          </p>
        </div>
      </Section>

      {/* ------------------------------------------------------------------ */}
      <Section n="5" title="What is proved, modelled, and unmeasured" sub="In that order of trust.">
        <div className="grid gap-3 sm:grid-cols-3">
          <div className="rounded border border-emerald-500/40 p-2.5">
            <div className="font-mono text-[10px] uppercase tracking-wider text-emerald-700 dark:text-emerald-400">
              proved
            </div>
            <ul className="mt-1.5 list-disc space-y-1 pl-4 text-xs">
              <li>The best-scoring card is the probability sort.</li>
              <li>The exact cost of any flip or swap.</li>
              <li>The prize-share formula.</li>
              <li>The odd/even parity effect.</li>
            </ul>
            <p className="mt-1.5 text-[11px]">Cannot be wrong. No data required.</p>
          </div>
          <div className="rounded border border-amber-500/40 p-2.5">
            <div className="font-mono text-[10px] uppercase tracking-wider text-amber-700 dark:text-amber-400">
              modelled
            </div>
            <ul className="mt-1.5 list-disc space-y-1 pl-4 text-xs">
              <li>How much the field over-backs favourites.</li>
              <li>That {(FIELD_CHALK_FRACTION * 100).toFixed(0)}% of rivals play straight chalk.</li>
              <li>That deviation pays above ~{DEVIATION_MIN_POOL} entries.</li>
            </ul>
            <p className="mt-1.5 text-[11px]">
              Correct given its assumptions. The assumptions are stated priors.
            </p>
          </div>
          <div className="rounded border border-rose-500/40 p-2.5">
            <div className="font-mono text-[10px] uppercase tracking-wider text-rose-700 dark:text-rose-400">
              unmeasured
            </div>
            <ul className="mt-1.5 list-disc space-y-1 pl-4 text-xs">
              <li>What your pool actually picks.</li>
              <li>Whether any of this beats naive chalk in a real pool.</li>
            </ul>
            <p className="mt-1.5 text-[11px]">
              No pick&apos;em entry has ever been settled here. That is what the ledger is for.
            </p>
          </div>
        </div>
        <p>
          The single change that would improve this most is not a better model — it is typing your
          pool&apos;s real pick percentages into the board. That turns the leverage column from a
          modelled guess into a measurement.
        </p>
      </Section>

      {/* ------------------------------------------------------------------ */}
      <Section
        n="6"
        title="What we looked for and did not find"
        sub="Seven studies, all pre-registered before the confirmation data was examined."
      >
        <p>
          None of these found a way to beat the closing line. That is the expected result and it is
          recorded rather than buried — the honest reason the tool sells{" "}
          <em>cheap differentiation</em> rather than <em>better predictions</em>.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <tbody>
              {studies.map(([name, cmd, result]) => (
                <tr key={cmd} className="border-t align-top">
                  <td className="px-2 py-1.5 font-medium text-foreground">{name}</td>
                  <td className="px-2 py-1.5">
                    <code className="font-mono text-[11px]">{cmd}</code>
                  </td>
                  <td className="px-2 py-1.5">{result}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="rounded border border-amber-500/40 bg-amber-500/5 p-2.5 text-foreground">
          <strong>Why upsets are unpredictable, in one number.</strong>{" "}
          <span className="text-muted-foreground">
            Weekly team performance is <strong className="text-foreground">21.9%</strong> between
            teams and <strong className="text-foreground">78.1%</strong> within a team, week to
            week. The stable part is exactly what the closing line is built from. A good team plays
            like a bad one roughly one week in four and nobody knows which — that is a complete
            explanation of the seven nulls above, not an excuse for them.
          </span>
        </p>
        <p className="text-xs">
          Cumulative testing is tracked deliberately: about twenty comparisons now sit on the same
          games, and one has already produced a sign-flipping false positive. A new idea needs its
          own pre-registration and a stated mechanism, and any lone survivor should be trusted less
          than its own confidence interval suggests.
        </p>
      </Section>
    </div>
  );
}
