"use client";

import { Fragment, useMemo, useState } from "react";
import type { NflArchetypeGameRow, NflArchetypePlayRow, NflArchetypeParticipantRow } from "@/db/queries";
import s from "../../cfb/cfb-terminal.module.css";
import n from "../nfl-terminal.module.css";
import p from "./pbp-archetype.module.css";

type Props = {
  games: NflArchetypeGameRow[]; gameId: string | null;
  plays: NflArchetypePlayRow[]; participants: NflArchetypeParticipantRow[];
};

// Colour carries MEANING, not decoration: scoring green, giveaway red, negative
// amber, neutral grey. Every chip also shows its text, so colour is never the
// only channel.
const PLAY_TONE: Record<string, string> = {
  EARLY_DOWN_EXPLOSIVE: "good", EARLY_DOWN_SUCCESS: "good", LATE_DOWN_CONVERSION: "good",
  EARLY_DOWN_FAILURE: "warn", LATE_DOWN_FAILURE: "warn",
  SACK: "warn", TURNOVER_PLAY: "bad", PENALTY: "warn",
  SPECIAL_TEAMS: "mute", KNEEL: "mute", SPIKE: "mute", NON_PLAY: "mute", EARLY_DOWN_MODEST: "",
  TWO_POINT: "",
};
// The outcome axis, which is what a rate should actually be read from -- the
// archetype label is outranked on late downs by SACK/TURNOVER/PENALTY, all of
// which are third-down attempts, so a conversion rate off the label alone runs
// 4.6 points high. Until now this column reached Postgres and stopped there.
const OUTCOME_TONE: Record<string, string> = {
  CONVERSION: "good", EXPLOSIVE: "good", SUCCESS: "good",
  FAILURE: "warn", MODEST: "",
};
const DRIVE_TONE: Record<string, string> = {
  TOUCHDOWN: "good", FIELD_GOAL: "", MISSED_FG: "warn", STALLED: "warn",
  THREE_AND_OUT: "warn", TURNOVER_GIVEAWAY: "bad", TURNOVER_ON_DOWNS: "bad",
  SCORE_AGAINST: "bad", CLOCK_EXPIRED: "mute", KNEEL_DOWN: "mute",
};

const label = (v: string) => v.replaceAll("_", " ");
const signed = (v: number | null, d = 0) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(d)}`;
// The ball-handler, in the order the play actually ran through them.
const handler = (r: NflArchetypePlayRow) =>
  r.receiver ? `${r.passer ?? "?"} → ${r.receiver}` : r.passer ?? r.rusher ?? null;
const num = (v: number | null, digits = 0) => (v == null ? "—" : v.toFixed(digits));

export default function PbpArchetypeClient({ games, gameId, plays, participants }: Props) {
  const [team, setTeam] = useState("all");
  const [playFilter, setPlayFilter] = useState("all");
  const [open, setOpen] = useState<number | null>(null);

  // Attribution is long-form -- one row per player per role -- so it is
  // indexed by play once rather than scanned per render.
  const credits = useMemo(() => {
    const byPlay = new Map<number, NflArchetypeParticipantRow[]>();
    for (const row of participants) {
      const list = byPlay.get(row.playId);
      if (list) list.push(row); else byPlay.set(row.playId, [row]);
    }
    return byPlay;
  }, [participants]);
  const game = games.find(g => g.gameId === gameId) ?? null;

  const playTypes = useMemo(
    () => [...new Set(plays.map(x => x.playArchetype))].sort(),
    [plays],
  );
  const rows = useMemo(
    () => plays.filter(x =>
      (team === "all" || x.posteam === team) &&
      (playFilter === "all" || x.playArchetype === playFilter)),
    [plays, team, playFilter],
  );

  return <div className={`${s.terminal} ${n.focus} ${p.wrap}`}>
    <section className={p.head}>
      <div>
        <h1 className={p.title}>{game ? `${game.awayTeam} @ ${game.homeTeam}` : "PBP ARCHETYPE"}</h1>
        <p className={p.meta}>
          {game
            ? `${game.season}${game.week ? ` · WK ${game.week}` : ""} · ${game.plays} plays · ${game.playVersion} + ${game.driveVersion}`
            : "Select a labelled game."}
        </p>
      </div>
      <p className={p.note}>
        Descriptive labels only. No prediction, no betting claim: these archetypes have not
        been tested against a closing line, and none of them is evidence of an edge.
      </p>
    </section>

    <section className={p.controls}>
      <label>GAME
        <select value={gameId ?? ""} onChange={e => { window.location.search = `?game=${encodeURIComponent(e.target.value)}`; }}>
          {!gameId && <option value="">Select…</option>}
          {games.map(g => <option key={g.gameId} value={g.gameId}>
            {g.season} {g.week ? `WK${g.week}` : ""} · {g.awayTeam} @ {g.homeTeam}
          </option>)}
        </select>
      </label>
      <label>TEAM
        <select value={team} onChange={e => setTeam(e.target.value)}>
          <option value="all">Both</option>
          {game && [game.awayTeam, game.homeTeam].map(t => <option key={t} value={t}>{t}</option>)}
        </select>
      </label>
      <label>PLAY ARCHETYPE
        <select value={playFilter} onChange={e => setPlayFilter(e.target.value)}>
          <option value="all">All</option>
          {playTypes.map(t => <option key={t} value={t}>{label(t)}</option>)}
        </select>
      </label>
      <span className={p.count}>{rows.length} of {plays.length} plays</span>
    </section>

    <div className={p.scroll}>
      <table className={p.table}>
        <thead><tr>
          {["Q", "Clock", "Off", "Dr", "Dn", "Dist", "Yd", "Type", "Gain",
            "Pers", "Box", "WHO", "PLAY ARCHETYPE", "OUTCOME", "Pre-snap", "DRIVE ARCHETYPE", "Drive flags",
            "Drive QB", "EPA", "Description"]
            .map(h => <th key={h}>{h}</th>)}
        </tr></thead>
        <tbody>
          {rows.map((r, i) => {
            // A rule between drives makes the drive structure readable without
            // a second grouping column.
            const newDrive = i > 0 && rows[i - 1].drive !== r.drive;
            const isOpen = open === r.playId;
            const credit = credits.get(r.playId) ?? [];
            return <Fragment key={r.playId}>
            <tr
              className={`${newDrive ? p.driveBreak : ""} ${isOpen ? p.openRow : ""}`}
              onClick={() => setOpen(isOpen ? null : r.playId)}
              tabIndex={0}
              role="button"
              aria-expanded={isOpen}
              onKeyDown={e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setOpen(isOpen ? null : r.playId); } }}
            >
              <td>{r.quarter ?? "—"}</td>
              <td className={p.mono}>{r.clock ?? "—"}</td>
              <td><strong>{r.posteam}</strong></td>
              <td>{r.drive ?? "—"}</td>
              <td>{r.down ?? "—"}</td>
              {/* Distance is meaningless without a down: a kickoff carries
                  ydstogo 0, and printing "0" invites reading it as 0 to go. */}
              <td>{r.down == null || r.ydstogo == null ? "—" : `${r.ydstogo}${r.distanceBucket && r.distanceBucket !== "unknown" ? ` ${r.distanceBucket[0].toUpperCase()}` : ""}`}</td>
              <td>{r.yardline100 ?? "—"}</td>
              <td>{r.playType ?? "—"}</td>
              <td data-gain={r.yardsGained == null ? "" : r.yardsGained > 0 ? "pos" : r.yardsGained < 0 ? "neg" : ""}>
                {num(r.yardsGained)}{r.explosive ? " ⚡" : ""}
              </td>
              {/* Personnel and box: what down-and-distance cannot say. Blank
                  where nflverse publishes no participation for the season. */}
              <td className={p.mono}>{r.personnelGrouping ?? "—"}</td>
              <td className={p.mono}>{r.defendersInBox == null ? "—" : r.defendersInBox.toFixed(0)}</td>
              {/* Attribution was absent entirely until the participants
                  layer landed; the full credit list for both sides is in the
                  expansion, this is just the ball. */}
              <td className={p.who}>{handler(r) ?? <span className={p.none}>—</span>}</td>
              <td><span className={p.chip} data-tone={PLAY_TONE[r.playArchetype] ?? ""}>{label(r.playArchetype)}</span>
                {r.hadSack && r.playArchetype !== "SACK"
                  ? <span className={p.flag} data-tone="bad" title="Strip sack: this play was both a sack and a lost fumble. TURNOVER_PLAY outranks SACK, so without this flag the sack would vanish from the count.">+SACK</span>
                  : null}</td>
              {/* The outcome axis. Read rates from HERE, not from the
                  archetype: the label is outranked on late downs by SACK,
                  TURNOVER and PENALTY -- all of which are third-down attempts
                  -- so a conversion rate off the label runs 4.6 points high.
                  Kicks show their own result instead; a kick has no down. */}
              <td>{r.outcome
                ? <span className={p.chip} data-tone={OUTCOME_TONE[r.outcome] ?? ""}>{label(r.outcome)}</span>
                : r.stOutcome
                  ? <span className={p.flag} title="Special-teams result">{label(r.stOutcome)}</span>
                  : <span className={p.none}>—</span>}</td>
              <td className={p.flags}>
                {r.goalLine ? <span className={p.flag} data-tone="good" title="Snap from inside the opponent's 5, run or pass">G-LINE</span> : null}
                {r.pressure ? <span className={p.flag} data-tone="bad" title="Quarterback was pressured">PRESS</span> : null}
                {r.blitz ? <span className={p.flag} data-tone="warn" title="Five or more pass rushers">BLITZ</span> : null}
                {r.formation && r.formation !== "SHOTGUN" ? <span className={p.flag}>{r.formation.replace("_", " ")}</span> : null}
                {r.coverageType ? <span className={p.flag}>{r.coverageType.replace("COVER_", "C")}</span> : null}
                {r.penaltyFirstDown ? <span className={p.flag} data-tone="warn" title="Penalty moved the chains">PEN 1ST</span> : null}
                {/* The quarterback was hit but not sacked: the defence won
                    the rep AND the offence survived the play. Both true, and
                    no aggregation on the other side produces it. */}
                {r.qbHit && !r.hadSack ? <span className={p.flag} data-tone="warn" title="Quarterback hit, no sack — the defence won the rep and the offence survived">QB HIT</span> : null}
                {r.injuryOnPlay ? <span className={p.flag} data-tone="bad" title="A player was injured on this snap">INJ</span> : null}
                {/* A flag erased real football here. The penalty is the
                    outcome; this says what was taken off the board. */}
                {r.wipedEvent ? <span className={p.flag} data-tone="bad" title={`Erased by the penalty: ${label(r.wipedEvent)}${r.wipedYards ? `, ${signed(r.wipedYards)} yards` : ""}${r.wipedTouchdown ? ", TOUCHDOWN" : ""}`}>WIPED {r.wipedEvent.slice(0, 3).toUpperCase()}</span> : null}
                {!r.goalLine && !r.pressure && !r.blitz && !r.coverageType && !r.penaltyFirstDown && !r.wipedEvent && !(r.qbHit && !r.hadSack) && !r.injuryOnPlay && (!r.formation || r.formation === "SHOTGUN") ? <span className={p.none}>—</span> : null}
              </td>
              <td>{r.driveArchetype
                ? <span className={p.chip} data-tone={DRIVE_TONE[r.driveArchetype] ?? ""}>{label(r.driveArchetype)}</span>
                : <span className={p.none} title="This play sits on a possession the drive labeller does not recognise as a drive — a conversion attempt after a return touchdown. Left blank rather than guessed.">no drive</span>}</td>
              {/* Independent flags, shown together because they ARE together:
                  a drive can be sacked AND penalised AND stopped short, and
                  none of the three outranks the others. */}
              <td className={p.flags}>
                {r.driveHadSack ? <span className={p.flag} data-tone="bad" title="A sack occurred on this drive">SACK</span> : null}
                {r.driveHadPenalty ? <span className={p.flag} data-tone="warn" title="A penalty wiped out a play on this drive">PEN</span> : null}
                {r.driveFailedShort ? <span className={p.flag} data-tone="warn" title="Final snap was 3rd/4th down with 2 or fewer to go">SHORT</span> : null}
                {r.driveTurnoverType ? <span className={p.flag} data-tone="bad">{r.driveTurnoverType === "interception" ? "INT" : "FUM"}</span> : null}
                {!r.driveHadSack && !r.driveHadPenalty && !r.driveFailedShort && !r.driveTurnoverType ? <span className={p.none}>—</span> : null}
              </td>
              <td>{r.driveQb ?? "—"}{r.driveQbIsStarter === false ? <span className={p.backup} title="Not this team's starting quarterback">◦</span> : null}</td>
              <td data-gain={r.epa == null ? "" : r.epa > 0 ? "pos" : r.epa < 0 ? "neg" : ""}>{num(r.epa, 2)}</td>
              <td className={p.desc} title={r.description ?? ""}>{r.description ?? "—"}</td>
            </tr>
            {/* THE POINT OF THE EXPANSION: one snap carries several true
                descriptions at once, and a single table row can only show
                the scannable few. Everything else the frame knows about this
                play lives here rather than in a 40-column table nobody can
                read. */}
            {isOpen && <tr className={p.detailRow}>
              <td colSpan={20}>
                <div className={p.detail}>
                  {(r.airYards != null || r.runGap) && <div className={p.panel}>
                    <h4>How it was earned</h4>
                    <dl>
                      {r.airYards != null && <><dt>Air yards</dt><dd>{num(r.airYards)}</dd></>}
                      {r.yardsAfterCatch != null && <><dt>After catch</dt><dd>
                        {num(r.yardsAfterCatch)}
                        {r.xyacMeanYardage != null && <span className={p.vs}> vs {num(r.xyacMeanYardage, 1)} expected</span>}
                      </dd></>}
                      {r.passLength && <><dt>Throw</dt><dd>{r.passLength} {r.passLocation}</dd></>}
                      {r.runGap && <><dt>Run</dt><dd>{r.runLocation} {r.runGap}</dd></>}
                      {r.scramble && <><dt>Scramble</dt><dd>a called pass the quarterback ran</dd></>}
                    </dl>
                  </div>}

                  <div className={p.panel}>
                    <h4>Credited</h4>
                    {credit.length === 0 && <p className={p.empty}>
                      No player credited. Timeouts, quarter ends and kneels carry
                      none; a scrimmage snap showing none was labelled before the
                      attribution layer and is waiting on a relabel.
                    </p>}
                    <div className={p.credits}>
                      {["offense", "defense", "kicking", "returning"].map(side => {
                        const rows = credit.filter(c => c.side === side);
                        if (!rows.length) return null;
                        return <div key={side} className={p.side} data-side={side}>
                          <span className={p.sideName}>{side}</span>
                          {rows.map(c => <span key={`${c.role}-${c.playerName}`} className={p.credit}>
                            <em>{label(c.role)}</em> {c.playerName}
                          </span>)}
                        </div>;
                      })}
                    </div>
                  </div>

                  {r.wipedEvent && <div className={p.panel} data-tone="bad">
                    <h4>Erased by the penalty</h4>
                    <dl>
                      <dt>Event</dt><dd>{label(r.wipedEvent)}{r.wipedTouchdown ? " — TOUCHDOWN" : ""}</dd>
                      {r.wipedYards != null && <><dt>Yardage</dt><dd>{signed(r.wipedYards)}, which did not count</dd></>}
                      {r.wipedDefender && <><dt>Credit taken from</dt><dd>{r.wipedDefender}</dd></>}
                    </dl>
                  </div>}

                  <div className={p.panel}>
                    <h4>Situation</h4>
                    <dl>
                      {r.series != null && <><dt>Series</dt><dd>#{r.series} · {r.seriesResult ?? "—"} {r.seriesSuccess ? "✓" : ""}</dd></>}
                      {r.goalToGo && <><dt>Goal to go</dt><dd>yes</dd></>}
                      {r.penaltyType && <><dt>Penalty</dt><dd>{[r.penaltyType, r.penaltyTeam, r.penaltySide, r.penaltyYards != null ? `${num(r.penaltyYards)} yds` : null].filter(Boolean).join(" · ")}</dd></>}
                      {r.passOe != null && <><dt>Pass over expected</dt><dd>{signed(r.passOe, 1)}%</dd></>}
                      {r.cpoe != null && <><dt>CPOE</dt><dd>{signed(r.cpoe, 1)}</dd></>}
                      {r.manZone && <><dt>Coverage</dt><dd>{label(r.manZone)}{r.coverageType ? ` · ${label(r.coverageType)}` : ""}</dd></>}
                      {r.passRushers != null && <><dt>Rushers</dt><dd>{num(r.passRushers)} vs {num(r.defendersInBox)} in box</dd></>}
                    </dl>
                  </div>

                  <div className={p.panel}>
                    <h4>Drive</h4>
                    <dl>
                      <dt>Result</dt><dd>{r.driveResult ?? "—"}</dd>
                      <dt>Field</dt><dd>{label(r.driveStartBucket ?? "?")} → {label(r.driveEndBucket ?? "?")}</dd>
                      {r.driveTimeOfPossession && <><dt>Time</dt><dd>{r.driveTimeOfPossession}</dd></>}
                      {r.driveYardsPenalized != null && r.driveYardsPenalized > 0 && <><dt>Given back</dt><dd>{num(r.driveYardsPenalized)} yards to flags</dd></>}
                      {r.driveInjuries != null && r.driveInjuries > 0 && <><dt>Injuries</dt><dd>{r.driveInjuries} on this drive</dd></>}
                    </dl>
                  </div>
                </div>
              </td>
            </tr>}
            </Fragment>;
          })}
        </tbody>
      </table>
      {!rows.length && <div className={n.empty}>
        {plays.length ? "No plays match this filter." : "No labelled plays for this game."}
      </div>}
    </div>
  </div>;
}
