"""Render the CFB pattern-watch ledger, frozen selections and live board as one HTML page.

The page is for readers (a Substack post links to it). Every number on it comes
from the same files the study grades itself with, so the page cannot say
anything the ledger does not:

    artifacts/cfb_pattern_watch/ledger.jsonl      graded rows (model.cfb_pattern_watch)
    artifacts/cfb_pattern_watch/selections.jsonl  frozen pre-kickoff selections (model.cfb_pattern_board)
    live board                                     model.cfb_pattern_board.build_board (DB read)

Usage:
    python -m model.cfb_pattern_publish            # writes artifacts/cfb_pattern_watch/report.html
"""

from __future__ import annotations

import argparse
import html
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model import cfb_pattern_board as board
from model import cfb_pattern_watch as pw

# Prose blocks for the reader page. Edit them here; there is no override file.
COPY = {
    "dek": "Six betting patterns from the first five weeks of college football, written down before the games they're tested on and graded in public.",
    "intro": [
        "Through the first five weeks and change of the 2026 college football season, favorites won 81.1% of their games. Their closing prices said they should have won 75.3%. Meanwhile the spread market was dead even, which is what a healthy market looks like. So which is it? Were favorites underpriced, or did a few weeks of lopsided scores make a coin flip look like a trend?",
        "Here's the problem with noticing things in betting data: you can always find something. Slice enough games enough ways and a pattern falls out every time, and most of them are ghosts. The only honest fix is boring. Write the rule down before the games happen, grade every one of them, and never touch the rule once it's set.",
        "So that's what this is. Six patterns, locked before most of week 6 kicked off, graded at the closing line every Saturday through the end of the regular season. Winners stay on the page. So do the losers.",
    ],
    "how": [
        ("The rules are frozen.", "Patterns 1 through 5 were locked on October 9, and pattern 6 the next morning. Changing one would make it a new experiment, so I don't."),
        ("Every qualifying game counts.", "One unit on every game that fits a pattern. I don't get to choose which ones you see."),
        ("Grading uses the closing line.", "That's the final consensus price before kickoff, roughly what you could have gotten late in the week. Moneyline favorites cost what they cost, usually -250 to -400. Spreads and totals are scored at -110."),
        ("One verdict, in December.", "Each pattern needs 40 to 80 graded games. The verdict is computed once, after the regular season and not before December 7."),
    ],
    "patterns_intro": "Here's each rule, how it has done since it was locked, and how far it is from having enough games to mean anything.",
    "upcoming_intro": "When a game fits a pattern, its line gets frozen the first time it shows up fresh, and that frozen number is what it's graded against. The current line sits beside it so you can see what moved. If a line goes more than an hour stale close to kickoff, it doesn't count as a pick. \"Watch\" means the market jumped in the last two hours and I don't know why. There's no reliable injury feed for college football, so a sudden move is often the only news you get.",
    "fine_print": [
        "These aren't picks I'm telling you to bet. This is a public test of whether six patterns were ever real.",
        "The discovery column shows how each pattern did on the games that gave me the idea. It's there so you can see where the idea came from, and it proves nothing. Patterns found by looking backward almost always cool off, and for spreads and totals the long-run rate is about 50%.",
        "Grading uses the verified closing consensus line, which isn't the best number of the week. The best retail price at the moment a pick was frozen is shown for comparison, but it isn't what gets graded.",
        "The moneyline favorites in patterns 1 and 5 are pricey. Two upsets can wipe out six or seven wins. The spread and total patterns risk one unit to win 0.91.",
        "A pattern that never reaches its minimum number of games gets called what it is: not enough games to tell.",
    ],
    "signoff": "That's the sheet for now. See you after Saturday.",
    "footer": "Data: my own ledger of college football closing lines from retail and offshore sportsbooks, graded against official final scores. Prices and lines are records of what was posted, not offers. I update this weekly and after every Saturday slate. Nothing here is betting advice.",
}

# How each pattern reads in a sentence.
SHORT = {
    "T1": "Group of Five favorites of 7 to 13.5 on the moneyline",
    "T2": "FBS favorites against FCS teams on the spread",
    "T3": "underdogs getting 3 to 6.5",
    "T4": "overs when the favorite lays 14 to 20.5",
    "T5": "Saturday-evening Group of Five favorites on the moneyline",
    "T6": "favorites laying 7 to 13.5",
}


def _day(d: str) -> str:
    dt = datetime.fromisoformat(d)
    return dt.strftime("%A, %B ") + str(dt.day)


def tldr_lines(summ: list[dict], by_date: dict[str, list[dict]]) -> list[str]:
    """Plain-language summary computed from the ledger, so the copy cannot drift from the numbers."""
    n = sum(x["n"] for x in summ)
    if not n:
        return ["Nothing is graded yet. The first results land after the next slate."]
    w = sum(x["w"] for x in summ); l = sum(x["l"] for x in summ)
    u = sum(x["units"] for x in summ)
    if abs(u) < 3:
        mood = "Basically flat, which is exactly what noise looks like this early."
    elif u > 0:
        mood = "That sounds nice and proves nothing yet."
    else:
        mood = "Some of these patterns may already be cooling off."
    out = [f"The six patterns are {w}-{l} on {n} graded picks, {u:+.2f} units. {mood}"]
    played = [x for x in summ if x["n"]]
    best = max(played, key=lambda x: x["units"])
    worst = min(played, key=lambda x: x["units"])
    if best["units"] > 0:
        out.append(f"{best['code']}, {SHORT[best['code']]}, is carrying the sheet at {best['w']}-{best['l']} and {best['units']:+.2f} units.")
    if worst["units"] < 0 and worst is not best:
        out.append(f"{worst['code']}, {SHORT[worst['code']]}, is the drag at {worst['w']}-{worst['l']} and {worst['units']:+.2f} units.")
    at_floor = [x for x in summ if x["n"] >= x["floor"]]
    if at_floor:
        out.append(f"{len(at_floor)} of six patterns have reached their minimum sample. The verdict still waits until December 7, and I only get one look.")
    else:
        near = max(summ, key=lambda x: x["n"] / x["floor"])
        out.append(f"No pattern is close to enough games to mean anything. The nearest, {near['code']}, has {near['n']} of the {near['floor']} it needs. The verdict waits until December 7, and I only get one look.")
    last = max(by_date)
    rs = by_date[last]
    dw = sum(r["result"] == "won" for r in rs); dl = sum(r["result"] == "lost" for r in rs)
    out.append(f"Most recent slate, {_day(last)}: {dw}-{dl}, {sum(r['pnl_units'] for r in rs):+.2f} units.")
    return out


def edge_sentence(base_rates: list[dict]) -> str:
    weeks = [r for r in base_rates if r.get("excess_pp") is not None]
    if not weeks:
        return ""
    last = weeks[-1]
    ex = last["excess_pp"]
    if ex < 0 and all(r["excess_pp"] >= 0 for r in weeks[:-1]):
        tail = "the first week this season it went the other way."
    elif ex < 0:
        tail = "below what their prices implied."
    else:
        tail = "above what their prices implied."
    return f"Week {last['week']} came in at {ex:+.1f} points, {tail}"

DEFAULT_OUT = Path("artifacts") / "cfb_pattern_watch" / "report.html"

TRIGGER_LABEL = {
    "T1_g5_mid_fav_ml": ("T1", "Group of Five favorite, 7 to 13.5, moneyline", "favorite moneyline"),
    "T2_fcs_fav_spread": ("T2", "FBS favorite over an FCS team", "favorite spread"),
    "T3_small_fav_dog_spread": ("T3", "Favorite of 3 to 6.5", "underdog spread"),
    "T4_big_fav_over": ("T4", "Favorite of 14 to 20.5", "over"),
    "T5_sat_evening_g5_fav_ml": ("T5", "Saturday evening, Group of Five involved", "favorite moneyline"),
    "T6_mid_fav_spread": ("T6", "Favorite of 7 to 13.5", "favorite spread"),
}
# Frozen discovery sample, copied from docs/cfb-early-season-patterns-study.md. It cannot confirm anything.
DISCOVERY = {
    "T1_g5_mid_fav_ml": ("22-2", "+16.2%", "9"),
    "T2_fcs_fav_spread": ("42-25-1", "+19.4%", "8"),
    "T3_small_fav_dog_spread": ("35-26-3", "+9.1%", "13"),
    "T4_big_fav_over": ("25-15", "+19.3%", "7"),
    "T5_sat_evening_g5_fav_ml": ("53-5", "+14.9%", "5"),
    "T6_mid_fav_spread": ("43-23-2", "+23.7%", "13"),
}


def _et(iso: str | datetime) -> str:
    ts = datetime.fromisoformat(iso) if isinstance(iso, str) else iso
    e = ts - timedelta(hours=4)
    hour = e.hour % 12 or 12
    return f"{e:%a %b} {e.day}, {hour}:{e:%M %p} ET"


def _am(d: float) -> str:
    return f"{-100 / (d - 1):+.0f}" if d < 2 else f"+{(d - 1) * 100:.0f}"


def esc(x) -> str:
    return html.escape(str(x))


def trigger_summary(rows: list[dict]) -> list[dict]:
    out = []
    for trig, (code, desc, bet) in TRIGGER_LABEL.items():
        tr = [r for r in rows if r["trigger"] == trig]
        w = sum(r["result"] == "won" for r in tr); l = sum(r["result"] == "lost" for r in tr); p = len(tr) - w - l
        units = sum(r["pnl_units"] for r in tr)
        floor = pw.TRIGGERS[trig][1]
        out.append({"code": code, "desc": desc, "bet": bet, "n": len(tr), "w": w, "l": l, "p": p, "units": units,
                    "roi": (units / len(tr)) if tr else None, "floor": floor, "disc": DISCOVERY[trig]})
    return out


def render(rows: list[dict], selections: list[dict], cards: list[dict], now: datetime,
           base_rates: list[dict] | None = None, fvc: dict | None = None, ev: dict | None = None) -> str:
    from model import cfb_pattern_measures as measures

    base_rates = base_rates or []
    fvc = fvc or {"summary": {"n": 0}, "items": []}
    ev = ev or {"runs": 0, "events": 0, "by_status": {}, "last_run": None}
    gc = measures.game_counts(rows)
    summ = trigger_summary(rows)
    total_units = sum(r["pnl_units"] for r in rows)
    graded_ids = {(r["trigger"], r["matchup_id"]) for r in rows}
    by_date: dict[str, list[dict]] = {}
    for r in rows:
        by_date.setdefault(r["game_date"], []).append(r)
    pending = [s for s in selections if (s["trigger"], s["matchup_id"]) not in graded_ids]
    upcoming = [c for c in cards if c["status"] in ("current", "final", "watch", "dropped", "stale")]

    def pill(status):
        cls = {"won": "won", "lost": "lost", "push": "push", "current": "cur", "final": "cur", "watch": "watch",
               "dropped": "drop", "stale": "stale", "started": "stale", "pending": "pend"}.get(status, "stale")
        return f'<span class="pill {cls}">{esc(status)}</span>'

    parts = []
    parts.append(f"""<title>Pattern Watch Ledger</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700&family=Source+Sans+3:wght@400;600&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>
/* layout: a ledger. one column of running text ~66ch, full-width tables in their own scroll boxes. */
:root{{--bg:#f3f4f2;--panel:#ffffff;--ink:#14191f;--muted:#5b6670;--rule:#d7dbd6;--accent:#0f6e6a;--accent-ink:#ffffff;
--won:#1f7a3a;--won-bg:#e3f2e7;--lost:#a62c2c;--lost-bg:#f8e4e4;--push:#8a6d12;--push-bg:#f6edd0;--watch:#b4560d;--watch-bg:#fbe8d6;--stale:#6b7480;--stale-bg:#e8ebee;
--display:"Barlow Condensed","Arial Narrow",sans-serif;--body:"Source Sans 3","Segoe UI",system-ui,sans-serif;--mono:"IBM Plex Mono",Consolas,monospace}}
@media (prefers-color-scheme:dark){{:root:not([data-theme="light"]){{--bg:#13171b;--panel:#1b2127;--ink:#e8ecef;--muted:#99a4ad;--rule:#2d353d;--accent:#3fb3ad;--accent-ink:#0d1a19;
--won:#6fcf8a;--won-bg:#173322;--lost:#f08a8a;--lost-bg:#3a1c1c;--push:#e0bd5a;--push-bg:#33290f;--watch:#f0a35a;--watch-bg:#3a2612;--stale:#9aa3ac;--stale-bg:#262d34;color-scheme:dark}}}}
:root[data-theme="dark"]{{--bg:#13171b;--panel:#1b2127;--ink:#e8ecef;--muted:#99a4ad;--rule:#2d353d;--accent:#3fb3ad;--accent-ink:#0d1a19;
--won:#6fcf8a;--won-bg:#173322;--lost:#f08a8a;--lost-bg:#3a1c1c;--push:#e0bd5a;--push-bg:#33290f;--watch:#f0a35a;--watch-bg:#3a2612;--stale:#9aa3ac;--stale-bg:#262d34;color-scheme:dark}}
body{{background:var(--bg);color:var(--ink);font-family:var(--body);font-size:16px;line-height:1.5}}
.wrap{{max-width:1100px;margin:0 auto;padding-block:28px 56px;padding-inline:16px}}
header h1{{font-family:var(--display);font-weight:700;font-size:clamp(34px,6vw,56px);line-height:1;margin:0;text-wrap:balance;letter-spacing:.01em}}
header .sub{{color:var(--muted);margin:10px 0 0;max-width:66ch}}
.eyebrow{{font-family:var(--mono);font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:var(--muted)}}
h2{{font-family:var(--display);font-weight:600;font-size:28px;margin:44px 0 10px;text-wrap:balance}}
p,li{{max-width:66ch}}
.strip{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:22px}}
.tile{{background:var(--panel);border:1px solid var(--rule);padding:14px 16px}}
.tile .k{{font-family:var(--mono);font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em}}
.tile .v{{font-family:var(--display);font-size:34px;font-weight:700;line-height:1.1;margin-top:4px;font-variant-numeric:tabular-nums}}
.tile .v small{{font-family:var(--body);font-size:14px;font-weight:400;color:var(--muted)}}
.scroll{{overflow-x:auto;border:1px solid var(--rule);background:var(--panel)}}
table{{border-collapse:collapse;width:100%;font-size:15px}}
th{{text-align:left;font-family:var(--mono);font-size:12px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);padding:10px 12px;border-bottom:1px solid var(--rule);white-space:nowrap}}
td{{padding:10px 12px;border-bottom:1px solid var(--rule);vertical-align:top}}
tr:last-child td{{border-bottom:0}}
td.num,th.num{{text-align:right;font-family:var(--mono);font-variant-numeric:tabular-nums;white-space:nowrap}}
.pos{{color:var(--won)}}.neg{{color:var(--lost)}}
.pill{{display:inline-block;font-family:var(--mono);font-size:11px;letter-spacing:.06em;text-transform:uppercase;padding:2px 8px;border-radius:999px;white-space:nowrap}}
.pill.won{{background:var(--won-bg);color:var(--won)}}.pill.lost{{background:var(--lost-bg);color:var(--lost)}}.pill.push{{background:var(--push-bg);color:var(--push)}}
.pill.cur{{background:var(--accent);color:var(--accent-ink)}}.pill.watch{{background:var(--watch-bg);color:var(--watch)}}.pill.drop,.pill.stale,.pill.pend{{background:var(--stale-bg);color:var(--stale)}}
.note{{border-left:3px solid var(--accent);padding:10px 14px;background:var(--panel);margin:18px 0}}
.tldr{{background:var(--panel);border:1px solid var(--rule);border-top:3px solid var(--accent);padding:14px 18px 6px;margin:26px 0 8px}}
.tldr .k{{font-family:var(--mono);font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:var(--muted)}}
.tldr ul{{margin:8px 0 8px;padding-left:20px}}.tldr li{{margin:6px 0}}
.lede p{{font-size:17px}}
.signoff{{font-style:italic;margin-top:28px}}
.muted{{color:var(--muted)}}
.date-h{{font-family:var(--display);font-size:20px;font-weight:600;margin:22px 0 8px}}
footer{{margin-top:48px;color:var(--muted);font-size:14px;border-top:1px solid var(--rule);padding-top:16px}}
@media (prefers-reduced-motion:reduce){{*{{transition:none}}}}
</style>
<div class="wrap">
<header>
<div class="eyebrow">Pre-registered · updated {esc(now.strftime('%B ') + str(now.day) + now.strftime(', %H:%M UTC'))}</div>
<h1>College Football Pattern Watch</h1>
<p class="sub">{esc(COPY["dek"])}</p>
</header>
""")
    parts.append('<div class="tldr"><div class="k">TL;DR</div><ul>' + "".join("<li>" + esc(t) + "</li>" for t in tldr_lines(summ, by_date)) + "</ul></div>")
    parts.append('<div class="lede">' + "".join("<p>" + esc(t) + "</p>" for t in COPY["intro"]) + "</div>")
    parts.append("<p>Here's how it works, in plain terms:</p><ul>" + "".join(f"<li><strong>{esc(b)}</strong> {esc(t)}</li>" for b, t in COPY["how"]) + "</ul>")
    graded = [r for r in rows if r["result"] in ("won", "lost")]
    wins = sum(r["result"] == "won" for r in graded)
    parts.append(f"""<div class="strip">
<div class="tile"><div class="k">graded picks</div><div class="v">{len(rows)}<small> from {gc['games']} games</small></div></div>
<div class="tile"><div class="k">record</div><div class="v">{wins}-{len(graded) - wins}<small> {len(rows) - len(graded)} push</small></div></div>
<div class="tile"><div class="k">units, flat 1 per pick</div><div class="v {'pos' if total_units >= 0 else 'neg'}">{total_units:+.2f}</div></div>
<div class="tile"><div class="k">open selections</div><div class="v">{len(pending)}</div></div>
<div class="tile"><div class="k">verdict</div><div class="v">none<small> not before Dec 7</small></div></div>
</div>
""")
    # per-trigger table
    parts.append("<h2>The six patterns</h2><p>" + esc(COPY["patterns_intro"]) + "</p>")
    parts.append("""<div class="scroll"><table>
<tr><th>#</th><th>pattern</th><th>bet</th><th class="num">graded</th><th class="num">games</th><th class="num">record</th><th class="num">units</th><th class="num">roi</th><th class="num">floor</th><th class="num">discovery (frozen, cannot confirm)</th></tr>""")
    trig_by_code = {v[0]: k for k, v in TRIGGER_LABEL.items()}
    for s in summ:
        roi = "—" if s["roi"] is None else f"{s['roi']:+.1%}"
        cls = "" if s["roi"] is None else ("pos" if s["roi"] >= 0 else "neg")
        games = gc["per_trigger_games"].get(trig_by_code[s["code"]], 0)
        parts.append(f"<tr><td class=\"num\">{s['code']}</td><td>{esc(s['desc'])}</td><td>{esc(s['bet'])}</td><td class=\"num\">{s['n']}</td><td class=\"num\">{games}</td><td class=\"num\">{s['w']}-{s['l']}{('-' + str(s['p'])) if s['p'] else ''}</td><td class=\"num {cls}\">{s['units']:+.2f}</td><td class=\"num {cls}\">{roi}</td><td class=\"num\">{s['n']}/{s['floor']}</td><td class=\"num muted\">{s['disc'][0]} · {s['disc'][1]} · {s['disc'][2]} dates</td></tr>")
    parts.append("</table></div>")
    parts.append(f"<p class=\"muted\">The {gc['rows']} graded picks come from {gc['games']} different games. {gc['games_with_multiple_triggers']} of those games fed more than one pattern, so the patterns overlap. Don't add their records together as if they were separate bets.</p>")
    # base rate: the effect three of the patterns rest on
    if base_rates:
        parts.append("""<h2>Is the favorite edge still there?</h2>
<p class="muted">Patterns 1, 5 and 6 all exist because favorites kept winning more often than their prices implied early in the season. This table checks whether that's still happening, across every FBS game with a closing line rather than just the ones a pattern picked. It doesn't grade anything. It tells me whether the reason behind those three patterns is still around.</p>
""" + (('<p>' + esc(edge_sentence(base_rates)) + '</p>') if edge_sentence(base_rates) else '') + """
<div class="scroll"><table>
<tr><th class="num">week</th><th class="num">games</th><th class="num">favorites won</th><th class="num">price implied</th><th class="num">difference</th><th class="num">favorites covered</th><th class="num">overs</th><th class="num">decided by 7 or less</th></tr>""")
        for r in base_rates:
            pct = lambda v: "—" if v is None else f"{v:.1%}"
            ex = r["excess_pp"]
            cls = "" if ex is None else ("pos" if ex >= 0 else "neg")
            exs = "—" if ex is None else f"{ex:+.1f} pts"
            parts.append(f"<tr><td class=\"num\">{r['week']}</td><td class=\"num\">{r['games']}</td><td class=\"num\">{pct(r['fav_su'])}</td><td class=\"num\">{pct(r['implied'])}</td><td class=\"num {cls}\">{exs}</td><td class=\"num\">{pct(r['fav_ats'])}</td><td class=\"num\">{pct(r['over'])}</td><td class=\"num\">{pct(r['within_7'])}</td></tr>")
        parts.append("</table></div><p class=\"muted\">Weeks follow CollegeFootballData's numbering. The difference column is how often favorites actually won, minus how often their closing moneylines said they would once the sportsbook's cut is removed.</p>")
    # upcoming board
    parts.append("""<h2>On deck</h2>
<p class="muted">""" + esc(COPY["upcoming_intro"]) + "</p>")
    if upcoming:
        parts.append("""<div class="scroll"><table>
<tr><th>kick</th><th>game</th><th>#</th><th>frozen selection</th><th>best retail at freeze</th><th>line now</th><th>moved</th><th>status</th></tr>""")
        for c in sorted(upcoming, key=lambda c: (c["commence_time"], c["trigger"])):
            fz = c["frozen"]; cur = c["current"]
            code = TRIGGER_LABEL[c["trigger"]][0]
            sel = esc(fz["selection"]) if fz else '<span class="muted">line unavailable</span>'
            br = fz["best_retail"] if fz else None
            brs = f"{esc(br['book'])} {esc(br['line']) if br['line'] is not None else ''} ({br['price']:+d})" if br else "—"
            d = c["drift"]
            moved = "—" if not d else f"spread {d['spread']:+g}, total {d['total'] if d['total'] is None else format(d['total'], '+g')}"
            note = "; ".join(c["late_reasons"]) if c["late_reasons"] else ""
            note_html = (' <span class="muted">' + esc(note) + '</span>') if note else ''
            parts.append(f"<tr><td>{esc(_et(c['commence_time']))}</td><td>{esc(c['away'])} @ {esc(c['home'])}</td><td class=\"num\">{code}</td><td>{sel}</td><td>{brs}</td><td class=\"num\">{cur['home_spread']:+g} / {cur['vegas_total']}</td><td class=\"num\">{esc(moved)}</td><td>{pill(c['status'])}{note_html}</td></tr>")
        parts.append("</table></div>")
    else:
        parts.append('<p class="muted">Nothing on the board in the next 36 hours. Check back closer to Saturday.</p>')
    by_status = ev.get("by_status", {})
    parts.append(f"<p class=\"muted\">So far the board has run {ev['runs']} times. It refused {by_status.get('stale', 0)} stale lines, flagged {by_status.get('watch', 0)} late moves, and logged {by_status.get('dropped', 0)} picks that stopped fitting before kickoff.</p>")
    # published picks vs the close
    fs = fvc["summary"]
    parts.append("<h2>Did the published line hold up?</h2>")
    if fs["n"]:
        lead = fs["median_lead_minutes"]
        lead_txt = "unknown" if lead is None else (f"{lead / 60:.1f} hours" if lead >= 90 else f"{lead:.0f} minutes")
        pts = fs["mean_points_clv"]
        pts_txt = "no spread or total picks yet" if pts is None else f"{pts:+.2f} points on average ({fs['points_better']} better, {fs['points_worse']} worse)"
        parts.append(f"""<p class="muted">Each pick goes out at the line it was frozen at, but the results above are graded at the close. This table compares the two. A positive number means the frozen line was better for the pick than the closing one. So far picks have been frozen a median of {esc(lead_txt)} before kickoff, and picks frozen that late shouldn't move much.</p>
<div class="strip">
<div class="tile"><div class="k">published picks graded</div><div class="v">{fs['n']}</div></div>
<div class="tile"><div class="k">result changed by the move</div><div class="v">{fs['result_changed']}</div></div>
<div class="tile"><div class="k">line vs close</div><div class="v">{'—' if pts is None else f'{pts:+.2f}'}<small> pts</small></div></div>
<div class="tile"><div class="k">units at frozen line</div><div class="v {'pos' if fs['freeze_units'] >= 0 else 'neg'}">{fs['freeze_units']:+.2f}</div></div>
<div class="tile"><div class="k">units at best retail</div><div class="v {'pos' if fs['best_retail_units'] >= 0 else 'neg'}">{fs['best_retail_units']:+.2f}</div></div>
</div>
<p class="muted">Line vs close: {esc(pts_txt)}.</p>
<div class="scroll"><table>
<tr><th>game</th><th>#</th><th>published pick</th><th class="num">frozen</th><th class="num">close</th><th class="num">points vs close</th><th>graded at close</th><th>at frozen line</th><th class="num">best retail</th></tr>""")
        for i in fvc["items"]:
            code = TRIGGER_LABEL[i["trigger"]][0]
            is_total = i["trigger"] == "T4_big_fav_over"
            fz = i["freeze_total"] if is_total else i["freeze_spread"]
            cl = i["close_total"] if is_total else i["close_spread"]
            fmt = (lambda v: "—" if v is None else f"{v:g}") if is_total else (lambda v: "—" if v is None else f"{v:+g}")
            pc = "—" if i["points_clv"] is None else f"{i['points_clv']:+g}"
            pcls = "" if not i["points_clv"] else ("pos" if i["points_clv"] > 0 else "neg")
            brp = "—" if i["best_retail_pnl"] is None else f"{i['best_retail_pnl']:+.2f}"
            parts.append(f"<tr><td>{esc(i['away'])} @ {esc(i['home'])}</td><td class=\"num\">{code}</td><td>{esc(i['selection'])}</td><td class=\"num\">{fmt(fz)}</td><td class=\"num\">{fmt(cl)}</td><td class=\"num {pcls}\">{pc}</td><td>{pill(i['close_result'])}</td><td>{pill(i['freeze_result'])}</td><td class=\"num\">{esc(brp)}</td></tr>")
        parts.append("</table></div>")
    else:
        parts.append('<p class="muted">No published pick has been graded yet.</p>')
    # results by date
    parts.append("<h2>The receipts</h2>")
    for d in sorted(by_date, reverse=True):
        rs = sorted(by_date[d], key=lambda r: (r["commence_time"], r["trigger"]))
        du = sum(r["pnl_units"] for r in rs); dw = sum(r["result"] == "won" for r in rs); dl = sum(r["result"] == "lost" for r in rs)
        parts.append(f"<div class=\"date-h\">{esc(datetime.fromisoformat(d).strftime('%A, %B ') + str(datetime.fromisoformat(d).day))} <span class=\"muted\" style=\"font-family:var(--body);font-size:15px;font-weight:400\">{dw}-{dl}, {du:+.2f} units</span></div>")
        parts.append("""<div class="scroll"><table>
<tr><th>game</th><th>#</th><th>pick</th><th class="num">price</th><th class="num">final</th><th>result</th><th class="num">units</th></tr>""")
        for r in rs:
            code = TRIGGER_LABEL[r["trigger"]][0]
            price = _am(r["price"]) if r["price"] != 1.9091 else "−110"
            cls = "pos" if r["pnl_units"] > 0 else ("neg" if r["pnl_units"] < 0 else "")
            parts.append(f"<tr><td>{esc(r['away'])} @ {esc(r['home'])}</td><td class=\"num\">{code}</td><td>{esc(r['selection'])}</td><td class=\"num\">{esc(price)}</td><td class=\"num\">{r['away_score']}-{r['home_score']}</td><td>{pill(r['result'])}</td><td class=\"num {cls}\">{r['pnl_units']:+.2f}</td></tr>")
        parts.append("</table></div>")
    bullets = "".join("<li>" + esc(t) + "</li>" for t in COPY["fine_print"])
    parts.append(f"""<h2>The fine print</h2>
<ul>
{bullets}</ul>
<p class="signoff">{esc(COPY["signoff"])}</p>
<footer>{esc(COPY["footer"])}</footer>
</div>""")
    return "".join(parts)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--no-store", action="store_true", help="write the HTML file only; do not save it to cfb_pattern_reports")
    args = ap.parse_args(argv)
    now = datetime.now(timezone.utc)
    cfg = load_config()
    db = DatabaseManager(cfg.database_url)  # applies db/schema.py if it changed (the pattern tables)
    from model import cfb_pattern_measures as measures
    from model import cfb_pattern_store as store

    rows = store.read_ledger(db)
    frozen = store.read_selections(db)
    selections = list(frozen.values())
    games = [dict(r) for r in db.execute(board._GAMES_SQL, (pw.SEASON, now - timedelta(hours=1), now + timedelta(hours=board.LOOKAHEAD_HOURS)))]
    hist: dict[int, list[dict]] = {}
    if games:
        for r in db.execute(board._HIST_SQL, ([int(g["matchup_id"]) for g in games],)):
            hist.setdefault(int(r["matchup_id"]), []).append(dict(r))
    cards, _ = board.build_board(games, hist, dict(frozen), now)  # read-only: new rows discarded
    rates = measures.weekly_base_rates(measures.load_base_rate_games(db))
    fvc = measures.freeze_vs_close(rows, selections)
    ev = measures.event_summary(store.read_events(db), store.read_runs(db, "board"))
    html_text = render(rows, selections, cards, now, rates, fvc, ev)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html_text, encoding="utf-8")
    stored = False
    if not args.no_store:
        stored = store.save_report(db, html_text, {
            "study_version": pw.STUDY_VERSION, "graded_rows": len(rows), "units": round(sum(r["pnl_units"] for r in rows), 4),
            "selections": len(selections), "board_cards": len(cards), "published_vs_close": fvc["summary"]["n"],
            "board_events": ev["events"], "base_rate_weeks": len(rates)})
    print(f"wrote {out} ({out.stat().st_size} bytes): {len(rows)} graded rows, {len(selections)} selections, "
          f"{len(cards)} board cards, {len(rates)} base-rate weeks, {fvc['summary']['n']} published picks vs close, "
          f"{ev['events']} board events; {'saved new page version' if stored else 'page unchanged, not stored again' if not args.no_store else 'not stored'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
