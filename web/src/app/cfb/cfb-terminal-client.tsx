"use client";

import {
  Activity,
  BellRing,
  BookOpen,
  CloudSun,
  Newspaper,
  Radio,
  Search,
  ShieldAlert,
  TrendingDown,
  TrendingUp,
  Zap,
} from "lucide-react";
import { useMemo, useState } from "react";
import styles from "./cfb-terminal.module.css";

type MarketKey = "spread" | "total" | "moneyline";
type AlertTone = "critical" | "market" | "news" | "weather";

type HistoryPoint = {
  time: string;
  Pinnacle: number;
  DraftKings: number;
  Circa: number;
};

type BookQuote = {
  book: string;
  line: string;
  price: string;
  relativeLimit: number;
  stale?: boolean;
};

type Catalyst = {
  time: string;
  type: string;
  title: string;
  detail: string;
  tone: AlertTone;
};

type Market = {
  label: string;
  current: string;
  open: string;
  move: string;
  axisLabel: string;
  history: HistoryPoint[];
  catalysts: Array<{ at: string; label: string }>;
  books: BookQuote[];
};

type Game = {
  id: string;
  away: string;
  home: string;
  kickoff: string;
  venue: string;
  network: string;
  headlineLine: string;
  headlineMove: number;
  markets: Record<MarketKey, Market>;
  pulse: Catalyst[];
  related: Array<{ label: string; value: string; note: string }>;
};

type PaperPosition = {
  id: string;
  game: string;
  market: string;
  book: string;
  entry: string;
  current: string;
  state: string;
};

const TIMES = ["OPEN", "THU 12P", "THU 8P", "FRI 10A", "FRI 4P", "SAT 8A", "SAT 11A", "NOW"];

function history(pinnacle: number[], draftKings: number[], circa: number[]): HistoryPoint[] {
  return TIMES.map((time, index) => ({
    time,
    Pinnacle: pinnacle[index],
    DraftKings: draftKings[index],
    Circa: circa[index],
  }));
}

const GAMES: Game[] = [
  {
    id: "osu-ore",
    away: "OHIO STATE",
    home: "OREGON",
    kickoff: "7:30 PM ET",
    venue: "Autzen Stadium",
    network: "NBC",
    headlineLine: "ORE -4.5",
    headlineMove: 2,
    markets: {
      spread: {
        label: "Full-game spread",
        current: "ORE -4.5",
        open: "ORE -2.5",
        move: "2.0 pts toward Oregon",
        axisLabel: "Oregon spread",
        history: history(
          [-2.5, -2.5, -3, -3.5, -4, -4.5, -4.5, -4.5],
          [-2.5, -2.5, -2.5, -3.5, -3.5, -4, -4.5, -4.5],
          [-2.5, -2.5, -3, -3.5, -4, -4, -4.5, -5],
        ),
        catalysts: [{ at: "FRI 10A", label: "QB upgraded" }, { at: "SAT 8A", label: "wind 18 mph" }],
        books: [
          { book: "Pinnacle", line: "ORE -4.5", price: "-108", relativeLimit: 92 },
          { book: "Circa", line: "ORE -5.0", price: "-105", relativeLimit: 100 },
          { book: "DraftKings", line: "ORE -4.5", price: "-112", relativeLimit: 58 },
          { book: "FanDuel", line: "ORE -4.0", price: "-115", relativeLimit: 50, stale: true },
          { book: "BetMGM", line: "ORE -4.5", price: "-110", relativeLimit: 42 },
        ],
      },
      total: {
        label: "Full-game total",
        current: "52.0",
        open: "54.0",
        move: "2.0 pts toward under",
        axisLabel: "Game total",
        history: history(
          [54, 54, 53.5, 53.5, 53, 52.5, 52, 52],
          [54, 54, 54, 53.5, 53.5, 52.5, 52.5, 52],
          [54, 54, 53.5, 53, 53, 52.5, 52, 51.5],
        ),
        catalysts: [{ at: "FRI 10A", label: "CB doubtful" }, { at: "SAT 8A", label: "wind update" }],
        books: [
          { book: "Pinnacle", line: "52.0", price: "U -109", relativeLimit: 92 },
          { book: "Circa", line: "51.5", price: "O -106", relativeLimit: 100 },
          { book: "DraftKings", line: "52.0", price: "U -112", relativeLimit: 58 },
          { book: "FanDuel", line: "52.5", price: "U -110", relativeLimit: 50, stale: true },
          { book: "BetMGM", line: "52.0", price: "U -108", relativeLimit: 42 },
        ],
      },
      moneyline: {
        label: "Oregon moneyline",
        current: "ORE -185",
        open: "ORE -142",
        move: "6.6pp implied probability",
        axisLabel: "Vig-free Oregon win probability",
        history: history(
          [0.584, 0.588, 0.602, 0.619, 0.632, 0.646, 0.648, 0.649],
          [0.581, 0.584, 0.589, 0.613, 0.621, 0.639, 0.645, 0.647],
          [0.586, 0.59, 0.605, 0.622, 0.638, 0.65, 0.654, 0.657],
        ),
        catalysts: [{ at: "FRI 10A", label: "QB upgraded" }, { at: "SAT 8A", label: "limits raised" }],
        books: [
          { book: "Pinnacle", line: "ORE ML", price: "-185", relativeLimit: 92 },
          { book: "Circa", line: "ORE ML", price: "-190", relativeLimit: 100 },
          { book: "DraftKings", line: "ORE ML", price: "-188", relativeLimit: 58 },
          { book: "FanDuel", line: "ORE ML", price: "-176", relativeLimit: 50, stale: true },
          { book: "BetMGM", line: "ORE ML", price: "-185", relativeLimit: 42 },
        ],
      },
    },
    pulse: [
      { time: "12:03", type: "PRICE GAP", title: "FanDuel trails the sharp spread", detail: "A half-point remains available against the current reference consensus.", tone: "critical" },
      { time: "11:47", type: "TOTAL STEAM", title: "Four books move under", detail: "The median total fell 1.5 points within six minutes.", tone: "market" },
      { time: "11:42", type: "WEATHER", title: "Wind forecast rises", detail: "Sustained wind is now 18 mph with gusts to 27.", tone: "weather" },
      { time: "10:18", type: "KEY CROSS", title: "Oregon crosses -3", detail: "Pinnacle moved first after confirmed quarterback availability.", tone: "market" },
      { time: "09:56", type: "NEWS", title: "Ohio State corner doubtful", detail: "Local beat report changed the starter's game designation.", tone: "news" },
    ],
    related: [
      { label: "1H spread", value: "ORE -2.5", note: "Moved before full game" },
      { label: "Ohio St team total", value: "U 24.5", note: "Strongest price pressure" },
      { label: "Weather", value: "18 mph", note: "Gusts to 27" },
    ],
  },
  {
    id: "uga-tex",
    away: "GEORGIA",
    home: "TEXAS",
    kickoff: "3:30 PM ET",
    venue: "DKR–Texas Memorial",
    network: "ABC",
    headlineLine: "TEX +1.0",
    headlineMove: -1.5,
    markets: {
      spread: {
        label: "Full-game spread",
        current: "TEX +1.0",
        open: "TEX -0.5",
        move: "1.5 pts toward Georgia",
        axisLabel: "Texas spread",
        history: history([-0.5, -0.5, 0, 0.5, 0.5, 1, 1, 1], [-0.5, -0.5, -0.5, 0, 0.5, 0.5, 1, 1], [-0.5, 0, 0, 0.5, 1, 1, 1, 1]),
        catalysts: [{ at: "FRI 10A", label: "LT ruled out" }, { at: "SAT 8A", label: "buyback" }],
        books: [
          { book: "Pinnacle", line: "TEX +1.0", price: "-106", relativeLimit: 94 },
          { book: "Circa", line: "TEX +1.0", price: "-110", relativeLimit: 100 },
          { book: "DraftKings", line: "TEX +1.5", price: "-115", relativeLimit: 61, stale: true },
          { book: "FanDuel", line: "TEX +1.0", price: "-108", relativeLimit: 54 },
          { book: "BetMGM", line: "TEX +1.0", price: "-110", relativeLimit: 45 },
        ],
      },
      total: {
        label: "Full-game total", current: "48.5", open: "49.5", move: "1.0 pt toward under", axisLabel: "Game total",
        history: history([49.5, 49.5, 49, 49, 48.5, 48.5, 48.5, 48.5], [49.5, 49.5, 49.5, 49, 49, 48.5, 48.5, 48.5], [49.5, 49, 49, 48.5, 48.5, 48.5, 48, 48.5]),
        catalysts: [{ at: "FRI 10A", label: "OL news" }, { at: "SAT 8A", label: "under buy" }],
        books: [
          { book: "Pinnacle", line: "48.5", price: "U -108", relativeLimit: 94 }, { book: "Circa", line: "48.0", price: "O -105", relativeLimit: 100 }, { book: "DraftKings", line: "48.5", price: "U -112", relativeLimit: 61 }, { book: "FanDuel", line: "49.0", price: "U -110", relativeLimit: 54, stale: true }, { book: "BetMGM", line: "48.5", price: "U -110", relativeLimit: 45 },
        ],
      },
      moneyline: {
        label: "Texas moneyline", current: "TEX +102", open: "TEX -106", move: "4.0pp implied probability", axisLabel: "Vig-free Texas win probability",
        history: history([0.51, 0.508, 0.5, 0.492, 0.484, 0.476, 0.474, 0.47], [0.512, 0.51, 0.505, 0.497, 0.488, 0.48, 0.478, 0.472], [0.508, 0.505, 0.498, 0.49, 0.48, 0.472, 0.468, 0.466]),
        catalysts: [{ at: "FRI 10A", label: "LT ruled out" }, { at: "SAT 8A", label: "limits raised" }],
        books: [
          { book: "Pinnacle", line: "TEX ML", price: "+102", relativeLimit: 94 }, { book: "Circa", line: "TEX ML", price: "+105", relativeLimit: 100 }, { book: "DraftKings", line: "TEX ML", price: "+100", relativeLimit: 61 }, { book: "FanDuel", line: "TEX ML", price: "+108", relativeLimit: 54, stale: true }, { book: "BetMGM", line: "TEX ML", price: "+102", relativeLimit: 45 },
        ],
      },
    },
    pulse: [
      { time: "11:31", type: "REVERSAL", title: "Texas buyback appears", detail: "Circa returned from +1.5 to +1 after limits increased.", tone: "market" },
      { time: "10:22", type: "INJURY", title: "Texas left tackle ruled out", detail: "The market moved through pick'em nine minutes later.", tone: "critical" },
      { time: "09:48", type: "PRICE GAP", title: "DraftKings hangs Texas +1.5", detail: "The extra half-point remains isolated from sharp books.", tone: "market" },
      { time: "08:40", type: "NEWS", title: "Georgia receiver expected active", detail: "Pregame warmup participation was confirmed.", tone: "news" },
    ],
    related: [
      { label: "1H spread", value: "UGA -0.5", note: "Sharp books aligned" },
      { label: "Texas team total", value: "U 23.5", note: "Juice building" },
      { label: "Key state", value: "Through 0", note: "No longer pick'em" },
    ],
  },
  {
    id: "bama-lsu",
    away: "ALABAMA",
    home: "LSU",
    kickoff: "8:00 PM ET",
    venue: "Tiger Stadium",
    network: "ESPN",
    headlineLine: "LSU +2.5",
    headlineMove: 1,
    markets: {
      spread: {
        label: "Full-game spread", current: "LSU +2.5", open: "LSU +3.5", move: "1.0 pt toward LSU", axisLabel: "LSU spread",
        history: history([3.5, 3.5, 3.5, 3, 3, 2.5, 2.5, 2.5], [3.5, 3.5, 3.5, 3.5, 3, 3, 2.5, 2.5], [3.5, 3.5, 3, 3, 3, 2.5, 2.5, 2]),
        catalysts: [{ at: "FRI 10A", label: "WR active" }, { at: "SAT 8A", label: "sharp buy" }],
        books: [
          { book: "Pinnacle", line: "LSU +2.5", price: "-108", relativeLimit: 91 }, { book: "Circa", line: "LSU +2.0", price: "-105", relativeLimit: 100 }, { book: "DraftKings", line: "LSU +2.5", price: "-110", relativeLimit: 56 }, { book: "FanDuel", line: "LSU +3.0", price: "-120", relativeLimit: 51, stale: true }, { book: "BetMGM", line: "LSU +2.5", price: "-112", relativeLimit: 44 },
        ],
      },
      total: {
        label: "Full-game total", current: "61.5", open: "60.5", move: "1.0 pt toward over", axisLabel: "Game total",
        history: history([60.5, 60.5, 60.5, 61, 61, 61.5, 61.5, 61.5], [60.5, 60.5, 60.5, 60.5, 61, 61, 61.5, 61.5], [60.5, 60.5, 61, 61, 61, 61.5, 61.5, 62]),
        catalysts: [{ at: "FRI 10A", label: "WR active" }, { at: "SAT 8A", label: "over steam" }],
        books: [
          { book: "Pinnacle", line: "61.5", price: "O -106", relativeLimit: 91 }, { book: "Circa", line: "62.0", price: "U -110", relativeLimit: 100 }, { book: "DraftKings", line: "61.5", price: "O -110", relativeLimit: 56 }, { book: "FanDuel", line: "61.0", price: "O -112", relativeLimit: 51, stale: true }, { book: "BetMGM", line: "61.5", price: "O -108", relativeLimit: 44 },
        ],
      },
      moneyline: {
        label: "LSU moneyline", current: "LSU +118", open: "LSU +145", move: "4.5pp implied probability", axisLabel: "Vig-free LSU win probability",
        history: history([0.408, 0.41, 0.414, 0.425, 0.435, 0.447, 0.451, 0.453], [0.405, 0.408, 0.41, 0.419, 0.43, 0.44, 0.449, 0.451], [0.41, 0.412, 0.42, 0.43, 0.44, 0.45, 0.454, 0.458]),
        catalysts: [{ at: "FRI 10A", label: "WR active" }, { at: "SAT 8A", label: "limits raised" }],
        books: [
          { book: "Pinnacle", line: "LSU ML", price: "+118", relativeLimit: 91 }, { book: "Circa", line: "LSU ML", price: "+115", relativeLimit: 100 }, { book: "DraftKings", line: "LSU ML", price: "+120", relativeLimit: 56 }, { book: "FanDuel", line: "LSU ML", price: "+125", relativeLimit: 51, stale: true }, { book: "BetMGM", line: "LSU ML", price: "+118", relativeLimit: 44 },
        ],
      },
    },
    pulse: [
      { time: "12:11", type: "KEY NUMBER", title: "LSU reaches +2.5", detail: "The market traded off +3 after limits rose.", tone: "critical" },
      { time: "11:55", type: "STEAM", title: "LSU and over move together", detail: "Three reference books adjusted both markets.", tone: "market" },
      { time: "10:07", type: "ROSTER", title: "LSU receiver confirmed active", detail: "The designation changed after the final walkthrough.", tone: "news" },
      { time: "09:20", type: "WEATHER", title: "Dry forecast holds", detail: "No meaningful wind or precipitation risk is expected.", tone: "weather" },
    ],
    related: [
      { label: "1H total", value: "O 31.0", note: "Two-book steam" },
      { label: "LSU team total", value: "O 29.5", note: "Price firming" },
      { label: "Key state", value: "Off +3", note: "Crossed at 11:54" },
    ],
  },
  {
    id: "usc-mich",
    away: "USC",
    home: "MICHIGAN",
    kickoff: "12:00 PM ET",
    venue: "Michigan Stadium",
    network: "FOX",
    headlineLine: "MICH -3.0",
    headlineMove: 0,
    markets: {
      spread: {
        label: "Full-game spread", current: "MICH -3.0", open: "MICH -3.0", move: "Two-way trade at key number", axisLabel: "Michigan spread",
        history: history([-3, -3, -3, -3, -3, -3, -3, -3], [-3, -3, -2.5, -3, -3, -3, -3, -3], [-3, -3, -3, -3.5, -3, -3, -3, -3]),
        catalysts: [{ at: "FRI 10A", label: "rain risk" }, { at: "SAT 8A", label: "two-way trade" }],
        books: [
          { book: "Pinnacle", line: "MICH -3.0", price: "+102", relativeLimit: 93 }, { book: "Circa", line: "MICH -3.0", price: "-105", relativeLimit: 100 }, { book: "DraftKings", line: "MICH -3.0", price: "-108", relativeLimit: 60 }, { book: "FanDuel", line: "MICH -2.5", price: "-120", relativeLimit: 53 }, { book: "BetMGM", line: "MICH -3.0", price: "+100", relativeLimit: 46 },
        ],
      },
      total: {
        label: "Full-game total", current: "45.0", open: "46.5", move: "1.5 pts toward under", axisLabel: "Game total",
        history: history([46.5, 46.5, 46, 46, 45.5, 45, 45, 45], [46.5, 46.5, 46.5, 46, 46, 45.5, 45, 45], [46.5, 46, 46, 45.5, 45.5, 45, 44.5, 45]),
        catalysts: [{ at: "FRI 10A", label: "rain risk" }, { at: "SAT 8A", label: "under buy" }],
        books: [
          { book: "Pinnacle", line: "45.0", price: "U -106", relativeLimit: 93 }, { book: "Circa", line: "44.5", price: "O -110", relativeLimit: 100 }, { book: "DraftKings", line: "45.0", price: "U -110", relativeLimit: 60 }, { book: "FanDuel", line: "45.5", price: "U -115", relativeLimit: 53, stale: true }, { book: "BetMGM", line: "45.0", price: "U -108", relativeLimit: 46 },
        ],
      },
      moneyline: {
        label: "Michigan moneyline", current: "MICH -152", open: "MICH -150", move: "0.3pp implied probability", axisLabel: "Vig-free Michigan win probability",
        history: history([0.595, 0.596, 0.594, 0.598, 0.596, 0.597, 0.596, 0.598], [0.593, 0.594, 0.592, 0.596, 0.595, 0.596, 0.597, 0.597], [0.597, 0.596, 0.598, 0.6, 0.597, 0.598, 0.599, 0.6]),
        catalysts: [{ at: "FRI 10A", label: "weather" }, { at: "SAT 8A", label: "balanced" }],
        books: [
          { book: "Pinnacle", line: "MICH ML", price: "-152", relativeLimit: 93 }, { book: "Circa", line: "MICH ML", price: "-155", relativeLimit: 100 }, { book: "DraftKings", line: "MICH ML", price: "-154", relativeLimit: 60 }, { book: "FanDuel", line: "MICH ML", price: "-148", relativeLimit: 53 }, { book: "BetMGM", line: "MICH ML", price: "-152", relativeLimit: 46 },
        ],
      },
    },
    pulse: [
      { time: "11:32", type: "BALANCED", title: "Spread pinned at -3", detail: "Books are adjusting price rather than leaving the key number.", tone: "market" },
      { time: "10:58", type: "TOTAL STEAM", title: "Under reaches 45", detail: "Circa briefly tested 44.5 before buyback.", tone: "critical" },
      { time: "09:35", type: "WEATHER", title: "Rain probability increases", detail: "The latest forecast now carries a 65% game-window probability.", tone: "weather" },
      { time: "08:15", type: "NEWS", title: "Michigan backfield intact", detail: "Both listed running backs completed warmups.", tone: "news" },
    ],
    related: [
      { label: "Spread price", value: "+102", note: "Market protects -3" },
      { label: "1H total", value: "U 22.0", note: "Earlier move" },
      { label: "Weather", value: "65% rain", note: "Wind remains low" },
    ],
  },
];

const MARKET_LABELS: Record<MarketKey, string> = {
  spread: "SPREAD",
  total: "TOTAL",
  moneyline: "MONEYLINE",
};

const SERIES_COLORS = {
  Pinnacle: "#f6a800",
  DraftKings: "#59b6ff",
  Circa: "#c58cff",
};

function displayTick(value: number, market: MarketKey): string {
  if (market === "moneyline") return `${(value * 100).toFixed(0)}%`;
  return value > 0 ? `+${value.toFixed(1)}` : value.toFixed(1);
}

function fmtEt(value: string): string {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(new Date(value));
}

function pulseIcon(tone: AlertTone) {
  if (tone === "weather") return CloudSun;
  if (tone === "news") return Newspaper;
  if (tone === "critical") return ShieldAlert;
  return Zap;
}

function MarketChart({ market, marketKey }: { market: Market; marketKey: MarketKey }) {
  const width = 760;
  const height = 300;
  const frame = { left: 62, right: 738, top: 26, bottom: 250 };
  const values = market.history.flatMap((point) => [point.Pinnacle, point.DraftKings, point.Circa]);
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const padding = Math.max((rawMax - rawMin) * 0.25, marketKey === "moneyline" ? 0.008 : 0.4);
  const min = rawMin - padding;
  const max = rawMax + padding;
  const x = (index: number) => frame.left + (index / Math.max(market.history.length - 1, 1)) * (frame.right - frame.left);
  const y = (value: number) => frame.top + ((max - value) / Math.max(max - min, 0.001)) * (frame.bottom - frame.top);
  const points = (series: keyof typeof SERIES_COLORS) => market.history.map((point, index) => `${x(index).toFixed(1)},${y(point[series]).toFixed(1)}`).join(" ");
  const ticks = Array.from({ length: 5 }, (_, index) => max - (index / 4) * (max - min));

  return (
    <svg className={styles.marketChart} viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${market.axisLabel} movement across Pinnacle, DraftKings, and Circa`}>
      <title>{`${market.axisLabel} movement`}</title>
      {ticks.map((tick) => {
        const tickY = y(tick);
        return <g key={tick}><line x1={frame.left} x2={frame.right} y1={tickY} y2={tickY} className={styles.chartGrid} /><text x={frame.left - 9} y={tickY + 4} textAnchor="end">{displayTick(tick, marketKey)}</text></g>;
      })}
      <line x1={frame.left} x2={frame.right} y1={frame.bottom} y2={frame.bottom} className={styles.chartAxis} />
      <line x1={frame.left} x2={frame.left} y1={frame.top} y2={frame.bottom} className={styles.chartAxis} />
      {market.history.map((point, index) => (
        <text key={point.time} x={x(index)} y={frame.bottom + 24} textAnchor={index === 0 ? "start" : index === market.history.length - 1 ? "end" : "middle"} className={styles.chartTime}>{point.time}</text>
      ))}
      {market.catalysts.map((catalyst, index) => {
        const pointIndex = Math.max(market.history.findIndex((point) => point.time === catalyst.at), 0);
        const catalystX = x(pointIndex);
        return <g key={`${catalyst.at}-${catalyst.label}`}><line x1={catalystX} x2={catalystX} y1={frame.top} y2={frame.bottom} className={styles.catalystLine} /><text x={catalystX + 5} y={frame.top + 13 + index * 14} className={styles.catalystLabel}>{catalyst.label}</text></g>;
      })}
      {(Object.keys(SERIES_COLORS) as Array<keyof typeof SERIES_COLORS>).map((series) => (
        <polyline key={series} points={points(series)} fill="none" stroke={SERIES_COLORS[series]} strokeWidth={series === "Pinnacle" ? 2.6 : 1.9} vectorEffect="non-scaling-stroke" />
      ))}
      {(Object.keys(SERIES_COLORS) as Array<keyof typeof SERIES_COLORS>).map((series, index) => (
        <g key={`legend-${series}`} transform={`translate(${frame.left + index * 125}, 291)`}><line x1="0" x2="18" y1="-4" y2="-4" stroke={SERIES_COLORS[series]} strokeWidth={series === "Pinnacle" ? 2.6 : 1.9} /><text x="24" y="0" className={styles.legendLabel}>{series}</text></g>
      ))}
    </svg>
  );
}

export default function CfbTerminalClient({ evaluatedAt }: { evaluatedAt: string }) {
  const [gameId, setGameId] = useState(GAMES[0].id);
  const [marketKey, setMarketKey] = useState<MarketKey>("spread");
  const [query, setQuery] = useState("");
  const [selectedBook, setSelectedBook] = useState("FanDuel");
  const [positions, setPositions] = useState<PaperPosition[]>([]);
  const [lockMessage, setLockMessage] = useState<string | null>(null);

  const game = GAMES.find((item) => item.id === gameId) ?? GAMES[0];
  const market = game.markets[marketKey];
  const quote = market.books.find((item) => item.book === selectedBook) ?? market.books[0];
  const filteredGames = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) return GAMES;
    return GAMES.filter((item) => `${item.away} ${item.home} ${item.network}`.toLowerCase().includes(normalized));
  }, [query]);

  function chooseGame(id: string) {
    setGameId(id);
    setSelectedBook("FanDuel");
    setLockMessage(null);
  }

  function chooseMarket(next: MarketKey) {
    setMarketKey(next);
    setSelectedBook("FanDuel");
    setLockMessage(null);
  }

  function addPaperPosition() {
    const next = {
      game: `${game.away} @ ${game.home}`,
      market: MARKET_LABELS[marketKey],
      book: quote.book,
      entry: `${quote.line} ${quote.price}`,
      current: market.current,
      state: quote.stale ? "Price gap captured" : "At market",
    };
    setPositions((current) => [{ ...next, id: `${game.id}-${marketKey}-${quote.book}-${current.length + 1}` }, ...current]);
    setLockMessage(`Paper locked ${quote.line} ${quote.price} at ${quote.book}`);
  }

  return (
    <div className={styles.terminal}>
      <header className={styles.topbar}>
        <div className={styles.brand}>CFB LINE TERMINAL</div>
        <label className={styles.command}>
          <Search aria-hidden="true" />
          <span className={styles.srOnly}>Search market watch</span>
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="SEARCH TEAM, GAME, MARKET OR NEWS" />
        </label>
        <div className={styles.marketOpen}><Radio aria-hidden="true" /> MARKET OPEN</div>
        <div className={styles.shadowMode}>SHADOW MARKET · SAMPLE DATA</div>
      </header>

      <div className={styles.shell}>
        <aside className={styles.watchPane} aria-label="CFB market watch">
          <div className={styles.sectionTitle}><span>MARKET WATCH</span><span>SATURDAY</span></div>
          <div className={styles.watchHeader}><span>GAME</span><span>LINE</span><span>MOVE</span></div>
          <div className={styles.watchList}>
            {filteredGames.map((item) => {
              const active = item.id === game.id;
              const Trend = item.headlineMove > 0 ? TrendingUp : item.headlineMove < 0 ? TrendingDown : Activity;
              return (
                <button key={item.id} type="button" className={styles.watchRow} data-active={active} onClick={() => chooseGame(item.id)}>
                  <span className={styles.watchGame}><strong>{item.away} @ {item.home}</strong><small>{item.kickoff} · {item.network} · 5 books</small></span>
                  <span className={styles.watchLine}>{item.headlineLine}</span>
                  <span className={item.headlineMove > 0 ? styles.positive : item.headlineMove < 0 ? styles.negative : styles.neutral}><Trend aria-hidden="true" /> {Math.abs(item.headlineMove).toFixed(1)}</span>
                </button>
              );
            })}
            {filteredGames.length === 0 ? <div className={styles.empty}>No games match that search.</div> : null}
          </div>
        </aside>

        <main className={styles.instrumentPane}>
          <section className={styles.instrumentHeader}>
            <div className={styles.instrumentTop}>
              <div>
                <div className={styles.instrumentTitle}>{game.away} @ {game.home}</div>
                <div className={styles.instrumentMeta}>{game.venue} · {game.kickoff} · {game.network} · {market.label}</div>
              </div>
              <div className={styles.primaryQuote}>
                <strong>{market.current}</strong>
                <span>OPEN {market.open} · {market.move.toUpperCase()}</span>
              </div>
            </div>
            <div className={styles.marketTabs} aria-label="Market type">
              {(Object.keys(MARKET_LABELS) as MarketKey[]).map((key) => (
                <button key={key} type="button" data-active={marketKey === key} onClick={() => chooseMarket(key)}>{MARKET_LABELS[key]}</button>
              ))}
            </div>
          </section>

          <section className={styles.chartSection}>
            <div className={styles.chartLabelRow}>
              <span>{market.axisLabel}</span>
              <span>{fmtEt(evaluatedAt)} · pregame snapshots only</span>
            </div>
            <div className={styles.chartWrap}>
              <MarketChart market={market} marketKey={marketKey} />
            </div>
          </section>

          <section className={styles.lowerGrid}>
            <div className={styles.ladderPane}>
              <div className={styles.sectionTitle}><span>BOOK LADDER</span><span>EXECUTABLE PRICES</span></div>
              <div className={styles.bookHeader}><span>BOOK</span><span>REL. LIMIT</span><span>LINE</span><span>PRICE</span></div>
              {market.books.map((item) => (
                <button key={item.book} type="button" className={styles.bookRow} data-selected={selectedBook === item.book} onClick={() => { setSelectedBook(item.book); setLockMessage(null); }}>
                  <span>{item.book}{item.stale ? <em>GAP</em> : null}</span>
                  <span className={styles.limitTrack}><span style={{ width: `${item.relativeLimit}%` }} /></span>
                  <span>{item.line}</span>
                  <span>{item.price}</span>
                </button>
              ))}
              <div className={styles.paperAction}>
                <button type="button" onClick={addPaperPosition}><BookOpen aria-hidden="true" /> PAPER LOCK {quote.book.toUpperCase()} {quote.line} {quote.price}</button>
                <div aria-live="polite">{lockMessage ?? "Track entry-to-close CLV without placing a wager."}</div>
              </div>
            </div>

            <div className={styles.catalystPane}>
              <div className={styles.sectionTitle}><span>CATALYST TIMELINE</span><span>LINE-ALIGNED</span></div>
              {game.pulse.slice(0, 5).map((item) => (
                <div key={`${item.time}-${item.type}`} className={styles.catalystRow}>
                  <span>{item.time}</span><strong>{item.type}</strong><p>{item.title}</p>
                </div>
              ))}
            </div>
          </section>

          <section className={styles.blotter}>
            <div className={styles.sectionTitle}><span>PAPER TRADE BLOTTER</span><span>{positions.length} OPEN</span></div>
            {positions.length === 0 ? (
              <div className={styles.blotterEmpty}>Select an executable quote and paper lock it to begin tracking entry-to-close movement.</div>
            ) : (
              <div className={styles.blotterTableWrap}>
                <table>
                  <thead><tr><th>Game</th><th>Market</th><th>Book</th><th>Entry</th><th>Current mark</th><th>State</th></tr></thead>
                  <tbody>{positions.map((position) => <tr key={position.id}><td>{position.game}</td><td>{position.market}</td><td>{position.book}</td><td>{position.entry}</td><td>{position.current}</td><td>{position.state}</td></tr>)}</tbody>
                </table>
              </div>
            )}
          </section>
        </main>

        <aside className={styles.pulsePane} aria-label="CFB market pulse">
          <div className={styles.sectionTitle}><span>MARKET PULSE</span><span>PRIORITY</span></div>
          {game.pulse.map((item) => {
            const Icon = pulseIcon(item.tone);
            return (
              <article key={`${item.time}-${item.type}`} className={styles.pulseRow} data-tone={item.tone}>
                <div><span>{item.time}</span><strong><Icon aria-hidden="true" /> {item.type}</strong></div>
                <h3>{item.title}</h3>
                <p>{item.detail}</p>
              </article>
            );
          })}
          <div className={styles.sectionTitle}><span>CROSS-MARKET</span><span>RELATED</span></div>
          {game.related.map((item) => (
            <div key={item.label} className={styles.relatedRow}>
              <span>{item.label}</span><strong>{item.value}</strong><small>{item.note}</small>
            </div>
          ))}
          <div className={styles.disclosure}>
            <BellRing aria-hidden="true" />
            <div><strong>Research terminal</strong><p>Sample quotes demonstrate the product experience. No CFB model edge or live execution is represented.</p></div>
          </div>
        </aside>
      </div>

      <footer className={styles.ticker}>
        <span><strong>TOP MOVE</strong> GEORGIA/TEXAS THROUGH 0</span>
        <span><strong>STEAM</strong> OHIO STATE/OREGON UNDER</span>
        <span><strong>STALE PRICE</strong> FANDUEL ORE -4.0</span>
        <span><strong>KEY CROSS</strong> LSU OFF +3</span>
        <span><strong>NEXT KICK</strong> USC @ MICHIGAN 12:00 ET</span>
      </footer>
    </div>
  );
}
