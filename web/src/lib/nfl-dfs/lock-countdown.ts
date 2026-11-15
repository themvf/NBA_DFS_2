/** NFL teams post inactives 90 minutes before kickoff. */
export const INACTIVES_LEAD_MS = 90 * 60_000;

function span(ms: number): string {
  const minutes = Math.max(0, Math.round(ms / 60_000));
  const h = Math.floor(minutes / 60);
  const m = minutes % 60;
  return h ? `${h}h ${String(m).padStart(2, "0")}m` : `${m}m`;
}

function clock(at: number): string {
  return new Date(at).toLocaleTimeString("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" });
}

export type LockCountdown = {
  /** "locked" once the first game has started. */
  state: "open" | "closing" | "locked";
  text: string;
};

/**
 * Time left to the slate's first kickoff and where the first game's inactives
 * stand. `closing` is the last 20 minutes, when a rebuild may not fit.
 */
export function lockCountdown(firstKickoff: string | null | undefined, now: number): LockCountdown | null {
  const kickoff = Date.parse(firstKickoff ?? "");
  if (!Number.isFinite(kickoff)) return null;
  const left = kickoff - now;
  if (left <= 0) return { state: "locked", text: "Games have started." };
  const inactivesAt = kickoff - INACTIVES_LEAD_MS;
  const inactives = now >= inactivesAt
    ? `first game's inactives posted about ${clock(inactivesAt)} ET`
    : `first game's inactives about ${clock(inactivesAt)} ET (in ${span(inactivesAt - now)})`;
  return { state: left <= 20 * 60_000 ? "closing" : "open", text: `Locks in ${span(left)} · ${inactives}` };
}

/** True when the tab is on a Vercel address other than the production one (a per-deployment address never updates). */
export function isFrozenDeploymentHost(host: string, productionHost: string | null | undefined): boolean {
  if (!productionHost || !host.endsWith(".vercel.app")) return false;
  const h = host.toLowerCase();
  if (h === productionHost.toLowerCase()) return false;
  // The project and branch aliases follow the newest deployment; only a hashed per-deployment name is frozen.
  return /^[a-z0-9]+-[a-z0-9]{9}-/.test(h) && !/-git-/.test(h);
}
