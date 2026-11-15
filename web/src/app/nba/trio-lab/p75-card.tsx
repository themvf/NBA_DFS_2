import type { getNbaP75Card } from "@/db/nba-p75-card";
import type { AvailabilityEvidence } from "@/lib/nba-availability";

const time = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short",
});

export function P75CardSection({ data, date, availability }: {
  data: Awaited<ReturnType<typeof getNbaP75Card>>;
  date: string;
  availability: AvailabilityEvidence | null;
}) {
  return <section id="p75-trio-card" aria-labelledby="p75-trio-card-title" className="scroll-mt-20 space-y-3 rounded-lg border bg-card p-4">
    <div>
      <h2 id="p75-trio-card-title" className="text-lg font-semibold">Lead trio and P75 alternatives</h2>
      <p className="text-sm text-muted-foreground">The lead has the highest expected score. The four alternatives are ranked by P75 of their combined score across shared draws.</p>
    </div>
    {data ? <>
      <p className="text-xs text-muted-foreground">Frozen {time.format(new Date(data.card.frozen_at))} · Cutoff {time.format(new Date(data.card.lock_at))} · Experimental scoring card</p>
      <ol className="divide-y">
        {data.card.entries.map((entry, i) => <li className="py-3" key={entry.players.map(p => p.event_id + ":" + p.player_key).join("|")}>
          <p className="text-xs font-medium text-muted-foreground">{i === 0 ? "Lead · Expected points" : "Alternative " + i + " · Joint P75"}</p>
          <p className="text-sm font-semibold">{entry.players.map(p => p.name).join(" · ")}</p>
          <p className="text-sm tabular-nums">Expected {entry.mean.toFixed(1)} · Joint P75 {entry.p75.toFixed(1)}
            {data.grade?.totals[i] != null && " · Actual " + data.grade.totals[i]}</p>
          <ReportedOut players={entry.players} availability={availability} />
        </li>)}
      </ol>
      <p className="text-sm">Actual best entry <b>{data.grade?.best ?? "Pending"}</b> · Combined five <b>{data.grade?.combined ?? "Pending"}</b></p>
      <p className="text-xs text-muted-foreground">Both totals require a final slate and verified scores for every selection. Forecasts are conditional on playing.</p>
    </> : <p className="text-sm text-muted-foreground">No frozen five-entry P75 card is available for {date}.</p>}
  </section>;
}

function ReportedOut({ players, availability }: {
  players: NonNullable<Awaited<ReturnType<typeof getNbaP75Card>>>["card"]["entries"][number]["players"];
  availability: AvailabilityEvidence | null;
}) {
  const names = players.filter(p =>
    availability?.states[p.event_id + ":" + p.player_key]?.status.toLowerCase() === "out").map(p => p.name);
  return names.length ? <p className="text-sm font-medium text-red-700">Latest report lists {names.join(", ")} as Out.</p> : null;
}
