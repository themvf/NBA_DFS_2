/**
 * When to poll DraftKings' live NFL player pool.
 *
 * Vercel calls `/api/cron/nfl-dk-pool` every 30 minutes as a reliable clock.
 * This decides whether a given half-hour is worth a poll. It exists because
 * GitHub's own `schedule:` trigger for the same workflow skipped every one of
 * its first four Thursday slots on 2026-09-24 -- the documented sub-hourly
 * unreliability the MLB odds bridge was built to route around.
 *
 * Weighted by game proximity, in UTC, because that is where availability news
 * lands and because each poll costs a GitHub Actions run:
 *   every half-hour  Thu 16:00-Fri 01:59, Sun 12:00-Mon 01:59,
 *                    Mon 16:00-Tue 01:59   (the Thursday, Sunday and Monday
 *                                           game windows, through kickoff)
 *   every 6 hours    Fri, Sat              (news lands, nothing is imminent)
 *   twice a day      Tue, Wed              (a new week's pools appear)
 * Late windows belong to the NEXT UTC day: an 8:15pm ET kickoff is 00:15 UTC.
 */
export function dkPoolDispatchDue(now: Date): boolean {
  const day = now.getUTCDay(); // 0 = Sunday
  const hour = now.getUTCHours();
  const firstHalf = now.getUTCMinutes() < 30;

  const gameWindow =
    (day === 4 && hour >= 16) || (day === 5 && hour <= 1) ||   // Thursday night
    (day === 0 && hour >= 12) || (day === 1 && hour <= 1) ||   // Sunday
    (day === 1 && hour >= 16) || (day === 2 && hour <= 1);     // Monday night
  if (gameWindow) return true;

  if ((day === 5 || day === 6) && hour % 6 === 0) return firstHalf;
  if ((day === 2 || day === 3) && (hour === 12 || hour === 22)) return firstHalf;
  return false;
}
