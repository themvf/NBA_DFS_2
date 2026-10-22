"""NFL DFS expert-coverage collection (docs/nfl-expert-coverage-study.md):
robots handling, discovery parsing, article parsing, timing, grounding,
slate resolution and identity matching. Offline; no network, no database."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from db import nfl_expert_coverage_schema as schema
from ingest import nfl_expert_coverage as C
from model import nfl_expert_extraction as X

UTC = timezone.utc

# --------------------------------------------------------------------------- robots

# Abridged from the live files of 2026-10-21 (dknetwork has two `*` groups;
# rotowire blocks named scrapers and leaves `*` almost open; lineups uses wildcards).
DK_ROBOTS = """User-agent: *
Disallow: /search-results

User-agent: CCBot
Disallow: /

Sitemap: https://dknetwork.draftkings.com/sitemap_index.xml
User-agent: *
Disallow: /wp-admin/
Allow: /wp-admin/admin-ajax.php
"""
ROTOWIRE_ROBOTS = """LLM-policy: https://www.rotowire.com/llms.txt

User-agent: Fetch
Disallow: /

User-agent: doc
Disallow: /

User-agent: *
Disallow:
Disallow: /users/login.php?
Disallow: /account/
"""
LINEUPS_ROBOTS = """User-agent: *
Disallow: /*/feed
Disallow: /*?s=
Allow: /wp-admin/admin-ajax.php
Disallow: /wp-admin/

User-agent: SemrushBot
Disallow: /
"""


def test_two_star_groups_are_merged() -> None:
    assert not C.robots_allows(DK_ROBOTS, "/search-results?q=x")
    assert not C.robots_allows(DK_ROBOTS, "/wp-admin/options.php")
    assert C.robots_allows(DK_ROBOTS, "/wp-admin/admin-ajax.php"), "longer Allow wins"
    assert C.robots_allows(DK_ROBOTS, "/2026/10/19/commanders-vs-49ers-nfl-dfs-value-picks/")


def test_named_bans_do_not_capture_us_by_substring() -> None:
    # `User-agent: doc` must not match our token by substring.
    assert C.robots_allows(ROTOWIRE_ROBOTS, "/football/article/nfl-dfs-week-6-142291")
    assert not C.robots_allows(ROTOWIRE_ROBOTS, "/account/settings")
    assert not C.robots_allows(ROTOWIRE_ROBOTS, "/users/login.php?next=/")


def test_a_group_naming_our_token_overrides_star() -> None:
    text = "User-agent: *\nDisallow: /\n\nUser-agent: NBADFSv2-ExpertCoverage\nAllow: /articles/\nDisallow: /\n"
    assert C.robots_allows(text, "/articles/x")
    assert not C.robots_allows(text, "/other")
    assert not C.robots_allows("User-agent: *\nDisallow: /\n", "/articles/x")


def test_wildcards_and_end_anchor() -> None:
    assert not C.robots_allows(LINEUPS_ROBOTS, "/nfl/feed")
    assert not C.robots_allows(LINEUPS_ROBOTS, "/page?s=cooks")
    assert C.robots_allows(LINEUPS_ROBOTS, "/nfl/dfs-picks")
    assert not C.robots_allows("User-agent: *\nDisallow: /*.php$\n", "/a/b.php")
    assert C.robots_allows("User-agent: *\nDisallow: /*.php$\n", "/a/b.php?x=1")


def test_missing_and_unreachable_robots_files() -> None:
    assert C.classify_robots_response(404, "<html>404</html>", "/") == ("no_robots_file", None)
    assert C.classify_robots_response(503, "", "/")[0] == "unreachable"
    assert C.classify_robots_response(None, None, "/")[0] == "unreachable"
    assert C.classify_robots_response(200, "User-agent: *\nDisallow: /\n", "/")[0] == "disallowed"
    assert C.classify_robots_response(200, DK_ROBOTS, "/")[0] == "allowed"
    d = C.RobotsDecision("x", "u", datetime.now(UTC), 503, None, "unreachable")
    assert not d.allows("https://example.com/a"), "a 5xx robots.txt means full disallow"
    d = C.RobotsDecision("x", "u", datetime.now(UTC), 404, None, "no_robots_file")
    assert d.allows("https://example.com/a")


def test_polite_session_spaces_requests() -> None:
    slept = []
    sess = C.PoliteSession(interval=1.0, sleep=slept.append)
    sess._last = __import__("time").monotonic()
    sess._wait()
    assert slept and 0.9 <= slept[0] <= 1.0


# --------------------------------------------------------------------------- discovery

NEWS_SITEMAP = """<?xml version="1.0"?><urlset xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">
<url><loc>https://dknetwork.draftkings.com/2026/10/19/commanders-vs-49ers-nfl-dfs-value-picks/</loc>
<news:news><news:publication_date>2026-10-19T17:29:12+00:00</news:publication_date>
<news:title><![CDATA[Commanders vs. 49ers NFL DFS Value Picks for Monday Night Football]]></news:title></news:news></url>
<url><loc>https://dknetwork.draftkings.com/2026/10/20/fantasy-basketball-value-picks/</loc><lastmod>2026-10-20T15:28:43+00:00</lastmod></url>
</urlset>"""
RSS = """<rss><channel><item><guid><![CDATA[https://www.dailyambush.com/2026-10/mnf-wsh-sf-2026-w6-showdown/]]></guid>
<link><![CDATA[https://www.dailyambush.com/2026-10/mnf-wsh-sf-2026-w6-showdown/]]></link>
<title>Monday Night Showdown: Commanders at 49ers</title>
<pubDate><![CDATA[Mon, 19 Oct 2026 14:04:58 +0000]]></pubDate></item></channel></rss>"""


def test_sitemap_and_rss_parsing() -> None:
    cands = C.parse_sitemap(NEWS_SITEMAP, "news")
    assert [c.url.rsplit("/", 2)[-2] for c in cands] == ["commanders-vs-49ers-nfl-dfs-value-picks",
                                                          "fantasy-basketball-value-picks"]
    assert cands[0].title.startswith("Commanders vs. 49ers")
    assert cands[0].listed_at == datetime(2026, 10, 19, 17, 29, 12, tzinfo=UTC)
    assert cands[1].listed_at == datetime(2026, 10, 20, 15, 28, 43, tzinfo=UTC), "lastmod when no news date"
    rss = C.parse_rss(RSS, "rss")
    assert rss[0].url.endswith("mnf-wsh-sf-2026-w6-showdown/")
    assert rss[0].listed_at == datetime(2026, 10, 19, 14, 4, 58, tzinfo=UTC)


@pytest.mark.parametrize("url,title,extra,implied,expected", [
    ("https://dknetwork.draftkings.com/2026/10/19/commanders-vs-49ers-nfl-dfs-value-picks/", None, None, False, True),
    ("https://dknetwork.draftkings.com/2026/10/19/draftkings-nfl-millionaire-winning-lineup-breakdown-for-week-6/",
     None, None, False, False),
    ("https://dknetwork.draftkings.com/2026/10/18/draftkings-pick6-picks-best-nfl-plays-for-week-6/", None, None,
     False, False),
    ("https://dknetwork.draftkings.com/2026/10/18/fantasy-football-week-6-start-em-sit-em-cheat-sheet/", None, None,
     False, False),
    ("https://www.rotoballer.com/college-football-cfb-dfs-week-7-saturday-draftkings-lineup-picks/1", None, None,
     False, False),
    ("https://www.footballguys.com/article/2026-FanDuel-Top10-06", None, None, True, False),
    ("https://www.footballguys.com/article/2026-draftkings-gpp-domination-week-06", None, None, True, True),
    ("https://www.footballguys.com/article/2026-high-leverage-opportunities-running-backs-week07", None, None, True,
     False),
    ("https://www.dailyambush.com/2026-10/nfl-ambush-26-week6/", "The AMBUSH - Week 6",
     C.SOURCES["dailyambush"].extra_include, False, True),
    ("https://www.dailyambush.com/sim-desk/2026-week-6/was-sf/", "Sim Desk: White runs for two",
     C.SOURCES["dailyambush"].extra_include, False, False),
])
def test_nfl_dfs_filter(url, title, extra, implied, expected) -> None:
    assert C.is_nfl_dfs(url, title, extra, implied) is expected


def test_footballguys_week_from_slug() -> None:
    assert C.footballguys_week("https://www.footballguys.com/article/2026-draftkings-sunday-showdown-week-06") == 6
    assert C.footballguys_week("https://www.footballguys.com/article/2026-dfs-first-look-week07") == 7
    assert C.footballguys_week("https://www.footballguys.com/article/2026-gut-check-no-691") is None


def test_no_article_url_is_hard_coded() -> None:
    import inspect
    src = inspect.getsource(C)
    for host in ("dknetwork.draftkings.com/20", "rotowire.com/football/article", "/article/2026-"):
        assert host not in src.replace("article/{year}-", ""), host


# --------------------------------------------------------------------------- article parsing

DK_HTML = """<html><head><meta property="og:title" content="Commanders vs. 49ers NFL DFS Value Picks">
<meta property="article:published_time" content="2026-10-19T17:29:12+00:00">
<script type="application/ld+json">{"@context":"https://schema.org","@graph":[{"@type":"WebPage","datePublished":"2026-10-19T17:00:00Z"},
{"@type":"NewsArticle","datePublished":"2026-10-19T17:29:12+00:00","dateModified":"2026-10-19T17:29:13+00:00"}]}</script>
</head><body><nav>Menu Sportsbook Casino</nav><div class="entry-content"><p>WR Brandin Cooks, San Francisco 49ers ($4,000)</p>
<p>Cooks is the value play I like most on this Showdown slate, and he should be popular.</p>""" + (
    "<p>" + "Filler analysis sentence about the slate. " * 30 + "</p>") + """</div><footer>Legal</footer></body></html>"""
FBG_HTML = """<html><head><title>DraftKings Sunday Showdown: Week 6</title></head><body>
<div class="col"><span>Published 10/18/2026</span></div><div class="article-content"><p>""" + (
    "Ekeler is the cheapest path to the stud build. " * 30) + """</p></div></body></html>"""
PAYWALL_HTML = """<html><head><script type="application/ld+json">{"@type":"Article","datePublished":"2026-10-18T13:22:11+00:00",
"isAccessibleForFree":"False"}</script></head><body><article>teaser</article></body></html>"""


def test_parse_article_prefers_the_article_jsonld_date_and_content_container() -> None:
    a = C.parse_article(DK_HTML, C.SOURCES["dknetwork"])
    assert a.published_at == datetime(2026, 10, 19, 17, 29, 12, tzinfo=UTC)
    assert (a.published_at_method, a.published_precision) == ("jsonld_datePublished", "second")
    assert a.modified_at == datetime(2026, 10, 19, 17, 29, 13, tzinfo=UTC)
    assert a.title == "Commanders vs. 49ers NFL DFS Value Picks"
    assert "Brandin Cooks" in a.text and "Sportsbook Casino" not in a.text and "Legal" not in a.text


def test_printed_date_has_day_precision() -> None:
    a = C.parse_article(FBG_HTML, C.SOURCES["footballguys"])
    assert (a.published_at_method, a.published_precision, a.published_at_raw) == (
        "printed_date_mmddyyyy", "day", "10/18/2026")
    assert a.published_at == datetime(2026, 10, 18, 4, 0, tzinfo=UTC), "local midnight Eastern (EDT)"


def test_paywall_flag_from_jsonld() -> None:
    a = C.parse_article(PAYWALL_HTML, C.SOURCES["dailyambush"])
    assert a.accessible_for_free is False


def test_pass_labels() -> None:
    k = datetime(2026, 10, 25, 17, 0, tzinfo=UTC)
    assert C.pass_label_for(k - timedelta(minutes=83), [k]) == "T-90m"
    assert C.pass_label_for(k - timedelta(hours=6, minutes=-7), [k]) == "T-6h"
    assert C.pass_label_for(k - timedelta(hours=47, minutes=53), [k]) == "T-48h"
    assert C.pass_label_for(k - timedelta(hours=12), [k]) == "adhoc"
    assert C.pass_label_for(k + timedelta(minutes=5), [k]) == "adhoc"


# --------------------------------------------------------------------------- timing

def test_timing_classes_bind_on_capture_time() -> None:
    kick = datetime(2026, 10, 20, 0, 15, tzinfo=UTC)
    assert X.timing_class(kick - timedelta(minutes=21), kick) == "eligible"
    assert X.timing_class(kick - timedelta(minutes=20), kick) == "eligible", "T-20 itself is inside"
    assert X.timing_class(kick - timedelta(minutes=19), kick) == "late"
    assert X.timing_class(kick, kick) == "late"
    assert X.timing_class(kick + timedelta(seconds=1), kick) == "retrospective"


def test_slate_reference_uses_noon_for_a_day_precision_date() -> None:
    day = {"published_at": datetime(2026, 10, 18, 4, 0, tzinfo=UTC), "published_precision": "day",
           "captured_at": datetime(2026, 10, 21, 10, 0, tzinfo=UTC)}
    assert X.slate_reference(day) == datetime(2026, 10, 18, 16, 0, tzinfo=UTC)
    none = {"published_at": None, "published_precision": "none", "captured_at": day["captured_at"]}
    assert X.slate_reference(none) == day["captured_at"]


# --------------------------------------------------------------------------- grounding

TEXT = ("WR Brandin Cooks, San Francisco 49ers ($4,000) Cooks is the value play I like most — "
        "projected ownership 19.3% in flex. Captain Kittle   at 12% captain rostership. "
        "Sample lineup: CPT George Kittle, FLEX Brandin Cooks, FLEX Austin Ekeler.")


def test_quotes_must_be_substrings_after_whitespace_and_quote_normalization() -> None:
    assert X.is_grounded("Cooks is the value play I like most - projected", TEXT), "em dash vs hyphen"
    assert X.is_grounded("Captain Kittle at 12% captain", TEXT), "collapsed whitespace"
    assert not X.is_grounded("Cooks is the best value play", TEXT)
    assert not X.is_grounded("Cooks", TEXT), "too short to ground anything"


def test_validate_counts_every_discard() -> None:
    response = {"is_nfl_dfs": True, "players": [
        {"name": "Brandin Cooks", "team": "49ers", "role": "value", "slot": "FLEX", "ownership_pct": 19.3,
         "ownership_definition": "flex", "quote": "projected ownership 19.3% in flex"},
        {"name": "George Kittle", "team": "SF", "role": "captain", "ownership_pct": 25,
         "quote": "Captain Kittle   at 12% captain rostership"},
        {"name": "Jayden Daniels", "team": "WAS", "role": "core", "quote": "Daniels is the top play on the slate"},
        {"name": "Austin Ekeler", "team": "WAS", "role": "sleeper", "quote": "FLEX Austin Ekeler"},
        {"name": "Brandin Cooks", "team": "SF", "role": "value", "slot": "flex",
         "quote": "Cooks is the value play I like most"},
    ], "lineups": [
        {"label": "GPP", "players": [{"name": "George Kittle", "slot": "CPT"}, {"name": "Brandin Cooks", "slot": "FLEX"},
                                     {"name": "Austin Ekeler", "slot": "FLEX"}]},
        {"label": "made up", "players": [{"name": "George Kittle", "slot": "CPT"}, {"name": "Deebo Samuel"}]},
    ]}
    r = X.validate(response, TEXT)
    assert [(m["name"], m["role"], m["team"], m["ownership_pct"]) for m in r.mentions] == [
        ("Brandin Cooks", "value", "SF", 19.3), ("George Kittle", "captain", "SF", None)]
    assert r.discard_reasons == {"ungrounded_ownership": 1, "ungrounded_quote": 1, "bad_role": 1, "duplicate": 1,
                                 "ungrounded_lineup": 1}
    assert len(r.lineups) == 1 and r.lineups[0]["players"][0] == {"name": "George Kittle", "slot": "CPT", "team": None}
    assert (r.mentions_raw, r.lineups_raw) == (5, 2)


# --------------------------------------------------------------------------- slates

def _g(away, home, iso, week=6):
    return X.Game(2026, week, away, home, datetime.fromisoformat(iso))


SCHEDULE = [
    _g("SEA", "DEN", "2026-10-16T00:15:00+00:00"), _g("HOU", "JAX", "2026-10-18T13:30:00+00:00"),
    _g("CAR", "PHI", "2026-10-18T17:00:00+00:00"), _g("BAL", "CLE", "2026-10-18T17:00:00+00:00"),
    _g("ARI", "LAR", "2026-10-18T20:05:00+00:00"), _g("DAL", "GB", "2026-10-19T00:20:00+00:00"),
    _g("WAS", "SF", "2026-10-20T00:15:00+00:00"),
    _g("SF", "ATL", "2026-10-25T17:00:00+00:00", 7), _g("GB", "DET", "2026-10-25T20:25:00+00:00", 7),
]


def test_showdown_slate_key_matches_the_scorecard_convention() -> None:
    sl = X.resolve_slates([{"format": "showdown", "games": [{"away": "Commanders", "home": "49ers"}]}], SCHEDULE,
                          datetime(2026, 10, 19, 17, 29, tzinfo=UTC))
    assert [s.key for s in sl] == ["showdown|WAS@SF|2026-10-20T00:15:00+00:00"]
    sl = X.resolve_slates([{"format": "showdown", "games": ["49ers vs Commanders"]}], SCHEDULE,
                          datetime(2026, 10, 19, 17, 29, tzinfo=UTC))
    assert [s.key for s in sl] == ["showdown|WAS@SF|2026-10-20T00:15:00+00:00"], "order-free pair, string form"


def test_classic_is_the_sunday_main_slate_without_london_or_primetime() -> None:
    sl = X.resolve_slates([{"format": "classic", "games": []}], SCHEDULE, datetime(2026, 10, 17, 3, 15, tzinfo=UTC))
    assert len(sl) == 1
    assert sl[0].key == "classic|ARI@LAR+BAL@CLE+CAR@PHI|2026-10-18T17:00:00+00:00"
    assert sl[0].resolution == "classic_main_next_sunday"
    nxt = X.resolve_slates([{"format": "classic", "games": [{"away": "SF", "home": "ATL"}]}], SCHEDULE,
                           datetime(2026, 10, 21, 12, 0, tzinfo=UTC))
    assert nxt[0].week == 7 and nxt[0].first_kickoff == datetime(2026, 10, 25, 17, 0, tzinfo=UTC)


def test_team_codes() -> None:
    assert X.team_code("WSH") == "WAS"
    assert X.team_code("San Francisco 49ers") == "SF"
    assert X.team_code("Niners") == "SF"
    assert X.team_code("LA Rams") == "LAR"
    assert X.team_code("Los Angeles Chargers") == "LAC"
    assert X.team_code("Mars") is None


# --------------------------------------------------------------------------- identity

POOL = [
    {"dk_pid": 1, "name": "Brandin Cooks", "normalized_name": "brandincooks", "team": "SF", "position": "WR",
     "salary": 4000},
    {"dk_pid": 2, "name": "Austin Ekeler", "normalized_name": "austinekeler", "team": "WAS", "position": "RB",
     "salary": 3600},
    {"dk_pid": 3, "name": "Kenneth Walker III", "normalized_name": "kennethwalker", "team": "SF", "position": "RB",
     "salary": 5000},
    {"dk_pid": 4, "name": "49ers", "normalized_name": "49ers", "team": "SF", "position": "DST", "salary": 4200},
    {"dk_pid": 5, "name": "Dyami Brown", "normalized_name": "dyamibrown", "team": "WAS", "position": "WR",
     "salary": 1000},
    {"dk_pid": 6, "name": "Noah Brown", "normalized_name": "noahbrown", "team": "WAS", "position": "WR",
     "salary": 800},
]
TEAMS = {"SF", "WAS"}


@pytest.mark.parametrize("name,team,expected", [
    ("Brandin Cooks", "SF", ("matched", "name_team", 1)),
    ("Brandin Cooks", None, ("matched", "name", 1)),
    ("Kenneth Walker", "SF", ("matched", "name_team", 3)),        # suffix dropped by normalize_name
    ("Austin Ekeler", "SF", ("matched", "name_other_team", 2)),   # wrong team from the writer
    ("Cooks", "SF", ("matched", "surname_team", 1)),
    ("Brown", "WAS", ("ambiguous", "surname", None)),
    ("49ers D/ST", "SF", ("matched", "team_dst", 4)),
    ("Ja'Marr Chase", "SF", ("unmatched", None, None)),
])
def test_identity_matching(name, team, expected) -> None:
    status, method, row = X.match_player(name, team, POOL, TEAMS)
    assert (status, method, row["dk_pid"] if row else None) == expected


def test_no_pool_is_its_own_status() -> None:
    assert X.match_player("Brandin Cooks", "SF", [], TEAMS) == ("no_pool", None, None)


# --------------------------------------------------------------------------- schema

def test_every_table_is_append_only() -> None:
    ddl = "\n".join(schema.STATEMENTS)
    for table in schema.APPEND_ONLY_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {table}" in ddl
        assert f"CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE" in ddl
    assert "UNIQUE (url, content_sha256)" in ddl, "an unchanged re-fetch can never append a duplicate revision"


def test_schema_is_registered_in_migrations() -> None:
    from db.schema import MIGRATIONS
    assert all(s in MIGRATIONS for s in schema.STATEMENTS)
