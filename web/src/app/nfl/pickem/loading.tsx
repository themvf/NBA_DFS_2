/**
 * Shown the moment a pick'em tab is clicked, while the server builds the page.
 * Without it the click looked dead: the Board page reads the season's odds,
 * injury reports and matchup forecasts before it can render anything.
 */
export default function PickemLoading() {
  return (
    <div className="mx-auto max-w-6xl space-y-4 p-6" aria-busy="true" aria-live="polite">
      <h1 className="text-2xl font-semibold">Pick&apos;em Pools</h1>
      <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
        Loading this week&apos;s card, lines and injury reports…
      </div>
    </div>
  );
}
