/**
 * Accounts whose starter and injury posts are searched directly and marked
 * "trusted" in the X news panel. Every handle was checked against the X API on
 * 2026-09-26 (name, follower count, bio). Rejected that day: Matt_Zenitz (no
 * such user), ChrisHummer, DianaRussini, Jordan_Schultz (0-2 followers, not the
 * reporters), Stephania_ESPN (no such user). Recheck before adding a handle:
 * reporters change handles and employers.
 */
export const CFB_TRUSTED_ACCOUNTS = [
  "PeteThamel",       // ESPN; broke Gutierrez starting, 2026-09-25
  "Brett_McMurphy",   // On3; same
  "BruceFeldmanCFB",  // The Athletic / FOX
  "RossDellenger",    // Yahoo Sports
  "UnderdogCFB",      // news feed
  "freeplays",        // injury feed; had Woodson doubtful at 11 AM, 2026-09-25
];

export const NFL_TRUSTED_ACCOUNTS = [
  "AdamSchefter", "RapSheet", "TomPelissero", "MikeGarafolo", "JayGlazer", "JFowlerESPN", // insiders
  "UnderdogNFL", "Rotoworld_FB", "FieldYates",  // news feeds
  "ProFootballDoc",   // former team doctor; injury analysis
  "freeplays",
];

/** X handles: letters, digits, underscore, 1-15 characters. */
export const isXHandle = (h: string) => /^[A-Za-z0-9_]{1,15}$/.test(h);
