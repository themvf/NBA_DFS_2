import { movementDefinitions,valueDefinitions,mlbDefinitions,otherDefinitions,type SignalDefinition } from "@/lib/signal-definitions";
import s from "./definition.module.css";
function Definitions({rows}:{rows:SignalDefinition[]}) {return <dl className={s.list}>{rows.map(row=><div key={row.key} id={row.key}><dt>{row.name}<code>{row.key}</code></dt><dd>{row.definition}</dd></div>)}</dl>;}
export default function DefinitionPage(){return <article className={s.page}>
 <header><p>SPORTS / TRACKING</p><h1>Definition</h1><p>A reference for the signals and result labels in the line-movement records.</p></header>
 <p>Sports → Tracking includes 31 game-line signal types. A signal describes an observed market condition; it is not automatically a recommended bet. Thresholds can differ by sport and detector version. <strong>pp</strong> means percentage points of implied probability.</p>
 <nav className={s.sections} aria-label="Definition sections"><a href="#movement">Movement</a><a href="#value">Price differences</a><a href="#mlb">MLB signals</a><a href="#other">Other signals</a><a href="#results">Results and lifecycle</a></nav>
 <h2 id="movement">Core movement signals</h2><Definitions rows={movementDefinitions}/>
 <h2 id="value">Price differences and value signals</h2><p>These compare sources; they do not necessarily indicate that a line has moved.</p><Definitions rows={valueDefinitions}/>
 <h2 id="mlb">Additional MLB signals</h2><p>MLB separates changes in the actual line from changes in its price. These detectors require at least three matching sportsbooks. The threshold is 0.5 runs for line movement or 1.5pp for price movement.</p><Definitions rows={mlbDefinitions}/>
 <h2 id="other">Separate signals outside Tracking</h2><p>The game-line record does not include the following prop and historical signals.</p><Definitions rows={otherDefinitions}/>
 <h2 id="results">Results and lifecycle</h2><dl className={s.list}>
 <div><dt>Win / Loss</dt><dd>The frozen selection won or lost under its market’s settlement rules. Win rate is wins ÷ (wins + losses).</dd></div>
 <div><dt>Push / Draw / Void</dt><dd>A push is an exact line match. Draws are explicitly recorded draws. Voids include no-action results and historical voids without a reason; those are not guessed to be pushes or draws.</dd></div>
 <div><dt>Pending / Unavailable</dt><dd>Pending means no outcome is recorded yet. Unavailable means the stored result does not fit a supported result category. Neither enters win rate.</dd></div>
 <div><dt>Units / ROI</dt><dd>Tracking assumes one unit at the frozen entry price. Missing prices are excluded. ROI uses only priced, resolved records.</dd></div>
 <div><dt>Held / Strengthened / Weakened / Faded / Reversed</dt><dd>These describe what happened after a signal: the initial move stayed, increased, shrank, returned to baseline, or moved past baseline in the opposite direction. They are lifecycle updates, not additional independent bets or wins.</dd></div>
 </dl>
 <p>Tracking counts original prospective signals, excluding repeated movement updates and retrospective backtests. Detectors with no recorded signals do not yet appear in its records table. The supported signal list is maintained explicitly; future detector additions require a tracking update.</p>
 </article>;}
