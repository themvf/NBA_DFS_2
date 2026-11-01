import { parseCsvLine } from "@/app/dfs/csv";

export type CbbMode = "classic" | "showdown";
export interface CbbPlayer {
  id: string; name: string; team: string; opponent: string; game: string;
  tipoff: string; positions: string[]; salary: number; dkAverage: number | null;
  status: string; captainId?: string; captainSalary?: number;
}
export interface CbbSlate { mode: CbbMode; players: CbbPlayer[]; firstTipoff: string }

export function csvRows(text: string): Record<string, string>[] {
  const lines = text.replace(/^\uFEFF/, "").split(/\r?\n/).filter(line => line.trim());
  if (lines.length < 2) throw new Error("The CSV needs a header and at least one data row.");
  const headers = parseCsvLine(lines[0]).map(s => s.trim());
  if (new Set(headers).size !== headers.length) throw new Error("Duplicate CSV column names.");
  return lines.slice(1).map((line, i) => {
    const cells = parseCsvLine(line).map(s => s.trim());
    if (cells.length !== headers.length) throw new Error(`CSV row ${i + 2} has the wrong number of cells.`);
    return Object.fromEntries(headers.map((h, n) => [h, cells[n]]));
  });
}

export function numberCell(value: string | undefined, label: string, min = 0, max = Infinity): number {
  const n = Number(value);
  if (!value?.trim() || !Number.isFinite(n) || n < min || n > max) throw new Error(`${label} must be a number from ${min} to ${max}.`);
  return n;
}

/** DK Game Info is Eastern wall time. Resolve the zone offset using Intl, including DST. */
function tipoff(info: string): string {
  const m = info.match(/^(\S+@\S+)\s+(\d{2})\/(\d{2})\/(\d{4})\s+(\d{1,2}):(\d{2})(AM|PM)\s+ET$/i);
  if (!m) throw new Error(`Unrecognized Game Info: ${info}. Expected a scheduled Eastern tipoff.`);
  const [, , mm, dd, yyyy, hh, minute, meridiem] = m;
  if (+hh < 1 || +hh > 12 || +minute > 59 || +mm < 1 || +mm > 12 || +dd < 1 || +dd > 31) throw new Error("Invalid tipoff date/time.");
  const h = +hh % 12 + (meridiem.toUpperCase() === "PM" ? 12 : 0);
  const guess = Date.UTC(+yyyy, +mm - 1, +dd, h, +minute);
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }).formatToParts(new Date(guess));
  const get = (type: string) => Number(parts.find(p => p.type === type)?.value);
  const ny = Date.UTC(get("year"), get("month") - 1, get("day"), get("hour"), get("minute"));
  const result = new Date(guess + guess - ny);
  const check = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", month: "2-digit", day: "2-digit", year: "numeric" }).format(result);
  if (check !== `${mm}/${dd}/${yyyy}`) throw new Error("Invalid tipoff date.");
  return result.toISOString();
}

export function parseCbbSalaries(text: string, mode: CbbMode): CbbSlate {
  const rows = csvRows(text);
  const required = ["ID", "Name", "Position", "Roster Position", "Salary", "Game Info", "TeamAbbrev"];
  if (required.some(key => !(key in rows[0]))) throw new Error(`DraftKings salaries require: ${required.join(", ")}.`);
  const seen = new Set<string>();
  const all: CbbPlayer[] = rows.map(row => {
    if (!/^\d+$/.test(row.ID) || seen.has(row.ID)) throw new Error(`Invalid or repeated DraftKings ID: ${row.ID}.`);
    seen.add(row.ID);
    const game = row["Game Info"].split(/\s/)[0].toUpperCase();
    const teams = game.split("@");
    const team = row.TeamAbbrev.toUpperCase();
    if (teams.length !== 2 || teams[0] === teams[1] || !teams.includes(team) || !row.Name) throw new Error(`Invalid team/game for ${row.Name}.`);
    const positions = row["Roster Position"].toUpperCase().split("/");
    const allowed = mode === "classic" ? ["G", "F", "UTIL"] : ["CPT", "UTIL", "FLEX"];
    if (!positions.length || positions.some(p => !allowed.includes(p)) || (mode === "classic" && !positions.some(p => p === "G" || p === "F"))) throw new Error(`Wrong contest format or unsupported eligibility for ${row.Name}.`);
    const salary = numberCell(row.Salary, "Salary", 1, 50000);
    if (!Number.isInteger(salary)) throw new Error("Salary must be a whole dollar amount.");
    return { id: row.ID, name: row.Name, team, opponent: teams.find(t => t !== team)!, game,
      tipoff: tipoff(row["Game Info"]), positions, salary,
      dkAverage: row.AvgPointsPerGame?.trim() ? numberCell(row.AvgPointsPerGame, "DK average", -100) : null,
      status: (row["Injury Status"] ?? row.Status ?? "").toUpperCase() } satisfies CbbPlayer;
  });
  const games = new Set(all.map(p => `${p.game}|${p.tipoff}`));
  if ((mode === "classic" && games.size < 2) || (mode === "showdown" && games.size !== 1)) throw new Error(mode === "classic" ? "Classic requires at least two games." : "Showdown requires exactly one game.");
  const identity = (p: CbbPlayer) => `${p.name.toLowerCase()}|${p.team}|${p.game}`;
  const players = mode === "classic" ? all : all.filter(p => !p.positions.includes("CPT"));
  if (new Set(players.map(identity)).size !== players.length) throw new Error("Duplicate player identities; resolve the salary file before continuing.");
  if (mode === "showdown") {
    const captains = all.filter(p => p.positions.length === 1 && p.positions[0] === "CPT");
    if (captains.length !== players.length || new Set(captains.map(identity)).size !== captains.length) throw new Error("Showdown needs one Captain and one utility row per player.");
    for (const p of players) {
      const captain = captains.find(c => identity(c) === identity(p));
      if (!captain || captain.salary !== p.salary * 1.5) throw new Error(`Missing or invalid Captain row for ${p.name}.`);
      if (captain.status && p.status && captain.status !== p.status) throw new Error(`Conflicting Captain/utility status for ${p.name}.`);
      p.status = p.status || captain.status;
      p.captainId = captain.id; p.captainSalary = captain.salary;
    }
  }
  if (players.length < (mode === "classic" ? 8 : 6)) throw new Error("Not enough players in the salary file.");
  return { mode, players, firstTipoff: all.map(p => p.tipoff).sort()[0] };
}
