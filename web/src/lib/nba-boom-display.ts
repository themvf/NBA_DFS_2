import type { BoomBoard, BoomPlayer } from "./nba-boom-lineups";
export function boomIdentity(eventId: string, name: string): string {
  return `${eventId}:${name.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]/g, "")}`;
}
/** Same event and unambiguous name only; absent or ambiguous stays unavailable. */
export function boomLookup(board: BoomBoard | null): Map<string, BoomPlayer | null> {
  const result = new Map<string, BoomPlayer | null>();
  for (const p of board?.players ?? []) {
    const key = boomIdentity(p.id.slice(0, p.id.indexOf(":")), p.name);
    result.set(key, result.has(key) ? null : p);
  }
  return result;
}
