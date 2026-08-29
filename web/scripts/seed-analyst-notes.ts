// One-time seed for ff_player_notes: the original 100 hand-written analyst notes
// that shipped hardcoded in PR #152, before the /fantasy-football/notes admin
// page existed.
//
// The database is now the single source of truth for player notes. This file is
// the migration input, not app code -- it lives in scripts/ so it never reaches
// the client bundle, and it is safe to delete once the seed has run and you are
// happy with what is in the table.
//
// Idempotent: re-running updates the same rows rather than duplicating, because
// ff_player_notes is UNIQUE(player_id). It will NOT clobber a note you have
// since edited unless you pass --force.
//
//   npm run seed:analyst-notes           # insert only where no note exists
//   npm run seed:analyst-notes -- --force  # also overwrite edited notes
//
// Matching is by normalized name + season, deliberately not by team: several
// notes describe 2026 moves the roster feed may not agree with.

import { sql } from "drizzle-orm";

import { db } from "../src/db";
import { ensureFantasyFootballTables } from "../src/db/ensure-schema";
import { normalizeAnalystName, type AnalystVerdict } from "../src/lib/fantasy-football/analyst-notes";

const SEASON = 2026;

type SeedNote = {
  listRank: number;
  name: string;
  position: string;
  team: string;
  adp: number;
  verdict: AnalystVerdict;
  verdictLabel: string;
  note: string;
};

const SEED_NOTES: SeedNote[] = [
  { listRank: 1, name: "Jahmyr Gibbs", position: "RB", team: "DET", adp: 1, verdict: "fair", verdictLabel: "Fair", note: "He combines elite receiving usage, explosive rushing and touchdown access in an offense built to create RB production. There's no bargain at 1.01, but he has perhaps the cleanest combination of floor and overall RB1 ceiling." },
  { listRank: 2, name: "Bijan Robinson", position: "RB", team: "ATL", adp: 2, verdict: "target", verdictLabel: "Target", note: "Robinson gives you true three-down volume with less age/workload concern than most elite backs. I would be perfectly comfortable taking him first overall and actually prefer his risk profile slightly to Gibbs." },
  { listRank: 3, name: "Puka Nacua", position: "WR", team: "LAR", adp: 3, verdict: "caution", verdictLabel: "Caution", note: "His 129-catch, 1,715-yard 2025 season gives him legitimate overall WR1 upside whenever Matthew Stafford is healthy. The only reason not to simply stamp him a top-three lock is the unresolved NFL review of his off-field situation, though no suspension has been announced." },
  { listRank: 4, name: "Ja'Marr Chase", position: "WR", team: "CIN", adp: 4, verdict: "target", verdictLabel: "Target", note: "Chase remains attached to Joe Burrow and is still arguably the safest bet in football for 150-plus targets and double-digit touchdowns. FantasyPros currently has him No. 1 overall, so getting him fourth is excellent value." },
  { listRank: 5, name: "Jaxon Smith-Njigba", position: "WR", team: "SEA", adp: 5, verdict: "fair", verdictLabel: "Fair", note: "JSN has graduated from breakout candidate to true target-dominating fantasy superstar. The price assumes another elite season, but his age, route efficiency and role make that a reasonable bet." },
  { listRank: 6, name: "Amon-Ra St. Brown", position: "WR", team: "DET", adp: 6, verdict: "fair", verdictLabel: "Fair", note: "St. Brown's underneath/intermediate dominance gives him one of the strongest weekly PPR floors in fantasy. His ceiling may be slightly lower than Chase or Nacua, but there are very few ways this pick fails absent injury." },
  { listRank: 7, name: "Christian McCaffrey", position: "RB", team: "SF", adp: 7, verdict: "caution", verdictLabel: "Caution", note: "McCaffrey led fantasy scoring again in 2025, but he did it while accumulating a career-high 413 touches entering his age-30 season. The ceiling remains league-winning, yet workload history and declining rushing efficiency make him substantially riskier than the younger first-round backs." },
  { listRank: 8, name: "Jonathan Taylor", position: "RB", team: "IND", adp: 8, verdict: "fair", verdictLabel: "Fair", note: "Taylor remains one of the few backs with a realistic pathway to 300 touches and 15 touchdowns. He doesn't have Gibbs/Achane receiving upside, but enormous rushing volume makes him a very defensible first-round anchor." },
  { listRank: 9, name: "De'Von Achane", position: "RB", team: "MIA", adp: 9, verdict: "caution", verdictLabel: "Caution", note: "Achane can break fantasy scoring on relatively modest touch totals because his receiving work and explosive-play rate are exceptional. You're nevertheless paying a first-round price for a smaller back in an offense with meaningful uncertainty, so I prefer Taylor and the elite WRs around him." },
  { listRank: 10, name: "CeeDee Lamb", position: "WR", team: "DAL", adp: 10, verdict: "target", verdictLabel: "Target", note: "Lamb still has a realistic 160-target ceiling with Dak Prescott and plays in an offense that should continue feeding him. He's Mike Clay's WR5 and No. 9 overall, making the late first round a strong entry point." },
  { listRank: 11, name: "Drake London", position: "WR", team: "ATL", adp: 11, verdict: "fair", verdictLabel: "Fair", note: "London's five missed games obscured a 17-game pace of roughly 96 catches, 1,300 yards and 10 touchdowns last season. Quarterback uncertainty adds some volatility, but the target concentration makes him a legitimate WR1." },
  { listRank: 12, name: "Justin Jefferson", position: "WR", team: "MIN", adp: 12, verdict: "target", verdictLabel: "Target", note: "His 2025 fantasy season was disappointing, but the underlying player has not suddenly stopped being Justin Jefferson. Kyler Murray/J.J. McCarthy uncertainty is real, yet I'd gladly bet on elite talent at the first/second-round turn rather than draft as though last year's QB problems are permanent." },
  { listRank: 13, name: "James Cook III", position: "RB", team: "BUF", adp: 13, verdict: "fair", verdictLabel: "Fair", note: "Cook has become a centerpiece rather than merely one piece of Buffalo's offense. Touchdown regression is possible, but his efficiency and improved scoring usage justify an early-second-round price." },
  { listRank: 14, name: "Chase Brown", position: "RB", team: "CIN", adp: 14, verdict: "caution", verdictLabel: "Caution", note: "Brown's passing-game involvement creates a terrific PPR floor in one of the league's highest-scoring offenses. I'm less enthusiastic about paying RB7-type prices because his profile is more volume-dependent than the backs above him." },
  { listRank: 15, name: "Rashee Rice", position: "WR", team: "KC", adp: 15, verdict: "fade", verdictLabel: "Fade at this price", note: "Rice averaged 18.5 PPR points in his eight games last season and is capable of being Kansas City's dominant short-area target. But recent legal trouble, missed offseason work, knee rehabilitation and uncertainty surrounding Patrick Mahomes make a mid-second-round investment unusually fragile." },
  { listRank: 16, name: "Derrick Henry", position: "RB", team: "BAL", adp: 16, verdict: "fair", verdictLabel: "Fair", note: "Age is increasingly uncomfortable, but Baltimore remains almost the perfect environment for Henry's touchdown-heavy skill set. I'm willing to accept declining receiving value when 15-plus rushing touchdowns remains a realistic outcome." },
  { listRank: 17, name: "A.J. Brown", position: "WR", team: "NE", adp: 17, verdict: "caution", verdictLabel: "Caution", note: "Brown still has elite efficiency and now plays with rising star Drake Maye, so the upside argument is easy to make. The question is whether New England can manufacture enough passing volume to justify drafting him ahead of several more established target monopolies." },
  { listRank: 18, name: "Chris Olave", position: "WR", team: "NO", adp: 18, verdict: "caution", verdictLabel: "Caution", note: "Tyler Shough gives Olave an intriguing chance to translate his separation ability into a monster target season. The recurring availability concerns are significant enough that I'd prefer him closer to the late second or early third round." },
  { listRank: 19, name: "Ashton Jeanty", position: "RB", team: "LV", adp: 19, verdict: "caution", verdictLabel: "Caution", note: "The Raiders have upgraded his surrounding environment, and his talent still gives him a strong chance to jump from last year's RB11 finish into the elite tier. He's currently dealing with an ankle issue, although Las Vegas is counting on him for the opener, so I'd want the medical picture cleared before paying full freight." },
  { listRank: 20, name: "Saquon Barkley", position: "RB", team: "PHI", adp: 20, verdict: "target", verdictLabel: "Target", note: "Barkley's age and accumulated workload deserve attention, but falling to the back half of Round 2 compensates you for those risks. Philadelphia's line and scoring environment still give him a much easier path to an RB1 finish than ADP 20 suggests." },
  { listRank: 21, name: "George Pickens", position: "WR", team: "DAL", adp: 21, verdict: "caution", verdictLabel: "Caution", note: "Pickens demonstrated that playing opposite Lamb does not eliminate his big-play or touchdown ceiling. At WR11 pricing, however, you're now paying for the breakout rather than profiting from it." },
  { listRank: 22, name: "Nico Collins", position: "WR", team: "HOU", adp: 22, verdict: "target", verdictLabel: "Target", note: "Collins remains Houston's clear alpha receiver and one of the NFL's better downfield/intermediate weapons. If C.J. Stroud rebounds behind an improved offensive line, Collins has top-five WR upside at a third-round-ish price." },
  { listRank: 23, name: "Kenneth Walker III", position: "RB", team: "KC", adp: 23, verdict: "target", verdictLabel: "Target", note: "Kansas City is a tremendous landing spot for a back with Walker's rushing talent, particularly if the Chiefs remain more balanced while Mahomes works back. Mike Clay has him RB11 and No. 18 overall, so ADP 23 offers legitimate upside." },
  { listRank: 24, name: "Omarion Hampton", position: "RB", team: "LAC", adp: 24, verdict: "target", verdictLabel: "Target", note: "Hampton flashed RB1 ability before a fractured ankle interrupted his rookie season, and Mike McDaniel's arrival creates even more receiving/explosive-play potential. Multiple ESPN analysts selected him as a 2026 breakout, and I'm comfortable betting on that outcome." },
  { listRank: 25, name: "Garrett Wilson", position: "WR", team: "NYJ", adp: 25, verdict: "fair", verdictLabel: "Fair", note: "Wilson's talent and target volume continue to provide a strong floor even if the Jets offense isn't elite. He's exactly the type of WR1/2 bridge player I'm happy to take near the 2/3 turn." },
  { listRank: 26, name: "Zay Flowers", position: "WR", team: "BAL", adp: 26, verdict: "caution", verdictLabel: "Caution", note: "Flowers has the route-running and after-catch skill to produce an excellent PPR season. The concern is simply price: Baltimore can score heavily without creating the passing volume normally associated with a top-12 fantasy receiver." },
  { listRank: 27, name: "Malik Nabers", position: "WR", team: "NYG", adp: 27, verdict: "caution", verdictLabel: "Caution", note: "Healthy Nabers possesses legitimate top-five WR ability and should remain Jaxson Dart's primary target. The ACL recovery makes Round 3 materially safer than his old Round 1 price, but ESPN's analysts still view his health as enough of a concern to prefer safer alternatives." },
  { listRank: 28, name: "Jeremiyah Love", position: "RB", team: "ARI", adp: 28, verdict: "target", verdictLabel: "Target with injury tolerance", note: "Love went third overall in the NFL draft, and Mike Clay notes the previous nine top-12 drafted RBs all finished top 12 at the position as rookies. His preseason high-ankle injury creates early-season uncertainty, but that is precisely why a player Clay ranks 13th overall has fallen to 28th in ADP." },
  { listRank: 29, name: "Trey McBride", position: "TE", team: "ARI", adp: 29, verdict: "target", verdictLabel: "Target", note: "McBride provides WR-level reception volume at fantasy's thinnest premium position. Clay has him No. 20 overall, and I think the positional advantage makes late Round 2/early Round 3 entirely reasonable." },
  { listRank: 30, name: "DeVonta Smith", position: "WR", team: "PHI", adp: 30, verdict: "fair", verdictLabel: "Fair", note: "The post-A.J. Brown version of Philadelphia creates an opportunity for Smith to function as a true No. 1 receiver. His efficiency and route-running are trustworthy enough that I like the floor, although Philadelphia can remain run-heavy." },
  { listRank: 31, name: "Tetairoa McMillan", position: "WR", team: "CAR", adp: 31, verdict: "fair", verdictLabel: "Fair", note: "McMillan already established himself as Carolina's foundational receiver and offers both target volume and red-zone size. The next leap largely depends on Bryce Young and the offense taking another step forward." },
  { listRank: 32, name: "Josh Jacobs", position: "RB", team: "GB", adp: 32, verdict: "fade", verdictLabel: "High-risk / discount only", note: "On football alone, this is a reasonable RB2 price, but declining efficiency and more than 2,100 career touches were already concerns. Jacobs was charged August 27 with battery and criminal damage to property; Green Bay expects him to remain on the roster, but his Week 1 availability and potential NFL discipline are unresolved." },
  { listRank: 33, name: "Kyren Williams", position: "RB", team: "LAR", adp: 33, verdict: "fair", verdictLabel: "Fair", note: "Sean McVay has repeatedly shown a willingness to give Williams high-value touches, especially near the goal line. Blake Corum limits the truly massive workload ceiling, but Williams remains a solid RB2." },
  { listRank: 34, name: "Breece Hall", position: "RB", team: "NYJ", adp: 34, verdict: "target", verdictLabel: "Target", note: "Hall's price has finally fallen far enough that you're no longer drafting him as though the Jets offense is guaranteed to cooperate. Clay ranks him RB14 and 23rd overall, making him one of my favorite post-hype bets in this range." },
  { listRank: 35, name: "Josh Allen", position: "QB", team: "BUF", adp: 35, verdict: "fair", verdictLabel: "Fair", note: "Allen remains the safest combination of passing touchdowns, rushing touchdowns and week-winning QB ceiling. Round 3 is expensive in a one-QB format, but he's the one quarterback for whom I don't consider it an obvious overpay." },
  { listRank: 36, name: "Emeka Egbuka", position: "WR", team: "TB", adp: 36, verdict: "target", verdictLabel: "Target", note: "Mike Evans' departure clears the path for Egbuka to become Tampa Bay's primary target after a 68-938-6 rookie season. ESPN's Matt Bowen sees legitimate top-10 WR potential, which makes the fourth-round area appealing despite a recent toe issue." },
  { listRank: 37, name: "Brock Bowers", position: "TE", team: "LV", adp: 37, verdict: "target", verdictLabel: "Strong target", note: "Bowers gives you elite-volume receiving production at TE and is only being pushed down because of the unusually deep early skill-position pool. Clay ranks him 21st overall, so this is one of the clearest positional-value gaps in the top 50." },
  { listRank: 38, name: "Travis Etienne Jr.", position: "RB", team: "NO", adp: 38, verdict: "target", verdictLabel: "Target", note: "New Orleans gives Etienne a passing-down role that meshes well with Tyler Shough and immediately raises his PPR floor. Two ESPN analysts selected him as a sleeper, and Clay ranks him essentially at this same overall price." },
  { listRank: 39, name: "Javonte Williams", position: "RB", team: "DAL", adp: 39, verdict: "target", verdictLabel: "Target", note: "Dallas resurrected Williams' value last season, and he now owns a valuable role in a Dak Prescott offense. Clay has him 32nd overall, making the fourth round a nice combination of established workload and touchdown upside." },
  { listRank: 40, name: "Tee Higgins", position: "WR", team: "CIN", adp: 40, verdict: "fair", verdictLabel: "Fair", note: "Higgins remains one of fantasy's highest-upside No. 2 real-life receivers because Cincinnati supports two major passing-game producers. His weekly ceiling is WR1-level, but Chase naturally limits the target floor." },
  { listRank: 41, name: "Ladd McConkey", position: "WR", team: "LAC", adp: 41, verdict: "target", verdictLabel: "Target", note: "McConkey's route efficiency and chemistry with Justin Herbert provide a particularly attractive PPR profile. ESPN lists him among its breakout candidates, and I prefer him to several receivers drafted 10-15 picks earlier." },
  { listRank: 42, name: "Cam Skattebo", position: "RB", team: "NYG", adp: 42, verdict: "target", verdictLabel: "Target", note: "Skattebo offers exactly what I want in the RB dead zone: a plausible workload expansion rather than merely a safe committee role. Mike Clay named him a sleeper and ranks him RB20/40th overall, almost perfectly validating this price." },
  { listRank: 43, name: "Davante Adams", position: "WR", team: "LAR", adp: 43, verdict: "fair", verdictLabel: "Fair", note: "Adams remains an elite touchdown threat in Matthew Stafford's offense even with Nacua dominating overall target share. Age and likely touchdown regression keep me from reaching, but the fourth round properly discounts those issues." },
  { listRank: 44, name: "Jameson Williams", position: "WR", team: "DET", adp: 44, verdict: "caution", verdictLabel: "Caution", note: "Williams can produce 25 fantasy points on relatively low volume because few players match his vertical explosiveness. The problem is you're now drafting that ceiling aggressively despite St. Brown and Gibbs commanding large portions of Detroit's offense." },
  { listRank: 45, name: "Bucky Irving", position: "RB", team: "TB", adp: 45, verdict: "target", verdictLabel: "Target", note: "Irving's receiving ability gives him ways to score even when Tampa isn't controlling game script. If healthy, the fifth-round range is much more attractive than the premium RB1 prices drafters paid previously." },
  { listRank: 46, name: "D'Andre Swift", position: "RB", team: "CHI", adp: 46, verdict: "caution", verdictLabel: "Caution", note: "Swift retains a valuable role in Ben Johnson's offense and can contribute meaningfully as a receiver. Clay has him only 59th overall, and Chicago has enough alternative weapons that I don't love paying a top-50 price." },
  { listRank: 47, name: "Jaylen Waddle", position: "WR", team: "DEN", adp: 47, verdict: "target", verdictLabel: "Target", note: "Denver gives Waddle a fresh environment and a quarterback capable of maximizing his speed. Clay ranks him WR23 and 47th overall exactly, and there remains room for upside if the Broncos consolidate targets around him and Sutton." },
  { listRank: 48, name: "Terry McLaurin", position: "WR", team: "WAS", adp: 48, verdict: "fair", verdictLabel: "Fair", note: "McLaurin's connection with Jayden Daniels provides a strong touchdown and explosive-play foundation. His age keeps the ceiling from being unlimited, but Round 4/5 is appropriate." },
  { listRank: 49, name: "DJ Moore", position: "WR", team: "BUF", adp: 49, verdict: "caution", verdictLabel: "Caution", note: "Josh Allen is an enormous quarterback upgrade, and reuniting with Joe Brady creates an obvious upside story. Buffalo has historically spread targets around, however, and Moore's 2025 production was extremely volatile, so WR25 pricing assumes a cleaner transition than I'm willing to guarantee." },
  { listRank: 50, name: "Quinshon Judkins", position: "RB", team: "CLE", adp: 50, verdict: "target", verdictLabel: "Target", note: "Judkins already showed he can handle NFL volume, and Cleveland has incentive to lean on its running game. Clay ranks him 39th overall, so you're receiving almost a round of theoretical value here." },
  { listRank: 51, name: "Drake Maye", position: "QB", team: "NE", adp: 51, verdict: "fair", verdictLabel: "Fair", note: "Maye's rushing ability and improved supporting cast make him one of the few quarterbacks capable of challenging Allen/Jackson/Daniels. He's Clay's QB4, though I generally prefer taking another RB/WR here if my league starts only one quarterback." },
  { listRank: 52, name: "Bhayshul Tuten", position: "RB", team: "JAX", adp: 52, verdict: "target", verdictLabel: "Target", note: "Etienne's departure gives Tuten a clear path to lead-back work after he flashed elite tackle-breaking and speed as a rookie. ESPN explicitly identified him as a breakout candidate with high-end RB2 potential." },
  { listRank: 53, name: "Rome Odunze", position: "WR", team: "CHI", adp: 53, verdict: "fair", verdictLabel: "Fair", note: "Chicago's passing offense should continue improving with Caleb Williams, and Odunze remains a prototype perimeter target. Luther Burden's emergence prevents me from projecting an enormous target share, but this is a reasonable WR3 price." },
  { listRank: 54, name: "Mike Evans", position: "WR", team: "SF", adp: 54, verdict: "caution", verdictLabel: "Caution", note: "Evans and Brock Purdy are an intriguing touchdown combination, and San Francisco knows how to scheme efficient targets. Age and a completely new offensive ecosystem make the fifth round richer than I'd prefer; Clay has him WR36 and 77th overall." },
  { listRank: 55, name: "Luther Burden III", position: "WR", team: "CHI", adp: 55, verdict: "target", verdictLabel: "Target", note: "Burden produced an outstanding 2.92 yards per route run as a rookie in limited work and now benefits from significant vacated targets. ESPN's bullish case is legitimately enormous - one analyst called out a top-five positional ceiling - although a current groin issue deserves monitoring." },
  { listRank: 56, name: "Colston Loveland", position: "TE", team: "CHI", adp: 56, verdict: "target", verdictLabel: "Target", note: "Loveland has already developed into one of football's premium receiving tight ends and is Clay's TE3. If you miss McBride/Bowers, I like attacking this tier rather than waiting until the position becomes touchdown-dependent." },
  { listRank: 57, name: "Lamar Jackson", position: "QB", team: "BAL", adp: 57, verdict: "fair", verdictLabel: "Fair", note: "Jackson's combination of rushing production and improved passing efficiency still provides overall QB1 upside. The only objection is opportunity cost - Round 5 contains several RB/WR breakout candidates I like." },
  { listRank: 58, name: "Joe Burrow", position: "QB", team: "CIN", adp: 58, verdict: "caution", verdictLabel: "Caution", note: "Burrow can lead the NFL in passing touchdowns with Chase and Higgins healthy. Because he offers less rushing than the elite dual-threat quarterbacks, I prefer him after Maye/Jackson/Daniels rather than drafting strictly on name value." },
  { listRank: 59, name: "David Montgomery", position: "RB", team: "HOU", adp: 59, verdict: "fair", verdictLabel: "Fair", note: "Houston acquired Montgomery to stabilize a running game that C.J. Stroud badly needs, so meaningful early-down and goal-line work should be there. His receiving ceiling is modest, but he profiles as a dependable RB2/3." },
  { listRank: 60, name: "Christian Watson", position: "WR", team: "GB", adp: 60, verdict: "caution", verdictLabel: "Caution", note: "Watson possesses league-winning touchdown and big-play ability when healthy. Green Bay still spreads targets and his durability record remains uncomfortable enough that I'd rather take him closer to Clay's No. 76 overall ranking." },
  { listRank: 61, name: "Courtland Sutton", position: "WR", team: "DEN", adp: 61, verdict: "fair", verdictLabel: "Fair", note: "Sutton remains a trusted red-zone receiver and should continue benefiting from Bo Nix's development. Waddle reduces the odds of huge target volume, but Sutton's touchdown equity keeps him useful." },
  { listRank: 62, name: "Jaylen Warren", position: "RB", team: "PIT", adp: 62, verdict: "fair", verdictLabel: "Fair", note: "Warren's receiving work and efficiency give him a nice PPR floor even if Pittsburgh maintains a committee. The upside is somewhat capped by Rico Dowdle, so this is about the right spot." },
  { listRank: 63, name: "Rhamondre Stevenson", position: "RB", team: "NE", adp: 63, verdict: "fair", verdictLabel: "Fair", note: "Stevenson remains capable of handling early downs, pass protection and receiving work. TreVeyon Henderson ensures this isn't a bell-cow situation, but the improving Patriots offense raises the value of both backs." },
  { listRank: 64, name: "TreVeyon Henderson", position: "RB", team: "NE", adp: 64, verdict: "caution", verdictLabel: "Caution", note: "Henderson has the explosive skill set to win weeks if his role expands. Clay ranks him RB30 and 97th overall, though, suggesting you're paying almost entirely for the breakout rather than receiving it for free." },
  { listRank: 65, name: "Parker Washington", position: "WR", team: "JAX", adp: 65, verdict: "caution", verdictLabel: "Caution", note: "Washington has earned a larger role and can provide both manufactured touches and downfield production. I like the player, but WR33-type pricing leaves less upside than I'd normally want from a fifth/sixth-round receiver." },
  { listRank: 66, name: "Marvin Harrison Jr.", position: "WR", team: "ARI", adp: 66, verdict: "target", verdictLabel: "Target", note: "Harrison's fantasy stock has fallen dramatically relative to the expectations attached to him early in his career. At WR30-ish cost, you're finally paying for his floor while retaining the possibility that elite prospect talent eventually translates into a major breakout." },
  { listRank: 67, name: "DK Metcalf", position: "WR", team: "PIT", adp: 67, verdict: "fair", verdictLabel: "Fair", note: "Metcalf remains a rare size/speed player and gives Pittsburgh a vertical/red-zone threat. The target competition and quarterback environment keep him closer to WR3 than automatic WR2 territory." },
  { listRank: 68, name: "Tyler Warren", position: "TE", team: "IND", adp: 68, verdict: "target", verdictLabel: "Target", note: "Warren's usage has quickly made him one of the higher-floor tight ends in PPR leagues. Clay has him TE4 and 50th overall, creating almost a 20-pick value gap versus market ADP." },
  { listRank: 69, name: "Dak Prescott", position: "QB", team: "DAL", adp: 69, verdict: "caution", verdictLabel: "Caution", note: "Lamb and Pickens give Prescott one of the better receiving duos in football, and Dallas should throw enough to support big passing weeks. He lacks the rushing floor of the elite fantasy QBs, so I prefer waiting rather than taking him this early." },
  { listRank: 70, name: "Tony Pollard", position: "RB", team: "TEN", adp: 70, verdict: "caution", verdictLabel: "Caution", note: "Pollard still has enough receiving ability and workload access to provide useful RB2 weeks. Clay ranks him only RB31/98 overall, making this price dependent on Tennessee giving him substantially more high-value work than projections currently expect." },
  { listRank: 71, name: "Alec Pierce", position: "WR", team: "IND", adp: 71, verdict: "fair", verdictLabel: "Fair", note: "Pierce's downfield role creates significant weekly upside and his game has expanded beyond simply being a deep-ball specialist. He recently returned to practice after an injury absence, so his health appears to be trending in the right direction." },
  { listRank: 72, name: "Jayden Daniels", position: "QB", team: "WAS", adp: 72, verdict: "target", verdictLabel: "Strong target", note: "Daniels is Clay's QB2 and No. 55 overall because rushing production gives him a ceiling few quarterbacks can match. If he's available in Round 6 after Allen, Maye, Jackson and Burrow are already being selected, this is one of my favorite QB values." },
  { listRank: 73, name: "Brian Thomas Jr.", position: "WR", team: "JAX", adp: 73, verdict: "target", verdictLabel: "Target", note: "Thomas' combination of size and explosive ability remains considerably more interesting than his post-hype ADP suggests. At this price, you aren't requiring him to become Jacksonville's target monster - you simply need the efficiency to rebound." },
  { listRank: 74, name: "Jadarian Price", position: "RB", team: "SEA", adp: 74, verdict: "target", verdictLabel: "Strong target", note: "Seattle drafted Price after moving on from Kenneth Walker, and the rookie enters a championship-caliber offense with relatively modest competition. ESPN's Eric Karabell specifically calls him a major bargain compared with former Notre Dame teammate Jeremiyah Love." },
  { listRank: 75, name: "Rico Dowdle", position: "RB", team: "PIT", adp: 75, verdict: "caution", verdictLabel: "Caution", note: "Dowdle should have a meaningful role in Pittsburgh's physical offense and can deliver useful rushing volume. Warren's presence makes this a committee, and Clay ranks Dowdle 95th overall, so ADP 75 feels somewhat aggressive." },
  { listRank: 76, name: "Michael Pittman Jr.", position: "WR", team: "PIT", adp: 76, verdict: "target", verdictLabel: "Target", note: "Pittman's route-running and possession profile give him an easier path to steady PPR production than many receivers in this range. Mike Clay named him a sleeper and ranks him 66th overall, so the market is offering a modest discount." },
  { listRank: 77, name: "Michael Wilson", position: "WR", team: "ARI", adp: 77, verdict: "fair", verdictLabel: "Fair", note: "Wilson has quietly developed into an effective complement to Harrison and offers useful touchdown upside. Target competition limits the ceiling, but he is appropriately priced as a flex/bench receiver." },
  { listRank: 78, name: "Kyle Pitts", position: "TE", team: "ATL", adp: 78, verdict: "target", verdictLabel: "Target", note: "Pitts remains frustrating, but the price no longer requires the mythical monster breakout season everyone spent years paying for. Clay still has him TE5, which makes Round 7 a much more appealing gamble." },
  { listRank: 79, name: "Matthew Stafford", position: "QB", team: "LAR", adp: 79, verdict: "fair", verdictLabel: "Fair", note: "Stafford's Nacua-Adams pairing provides tremendous passing upside whenever the veteran quarterback is healthy. Lack of rushing limits his fantasy ceiling, but he can deliver elite passing stretches for managers who wait at QB." },
  { listRank: 80, name: "Harold Fannin Jr.", position: "TE", team: "CLE", adp: 80, verdict: "fair", verdictLabel: "Fair", note: "Fannin has already established himself as an important receiving weapon and is ranked TE6 by Clay. This is a reasonable price, although Loveland/Warren offer cleaner offensive environments if they are available earlier." },
  { listRank: 81, name: "Jalen Hurts", position: "QB", team: "PHI", adp: 81, verdict: "target", verdictLabel: "Target", note: "The receiving corps changed, but Philadelphia's goal-line rushing usage still gives Hurts a scoring mechanism most quarterbacks simply don't have. Clay ranks him QB5 and 58th overall, so falling into the seventh round is excellent value." },
  { listRank: 82, name: "Chris Godwin Jr.", position: "WR", team: "TB", adp: 82, verdict: "caution", verdictLabel: "Caution", note: "Godwin can still command targets and provide high-end PPR production when fully healthy. Egbuka's ascension changes the target hierarchy, however, so I don't want to pay for the old Tampa Bay role." },
  { listRank: 83, name: "Carnell Tate", position: "WR", team: "TEN", adp: 83, verdict: "target", verdictLabel: "Strong target", note: "Tate has one of the clearest rookie pathways to becoming his team's No. 1 receiver and ESPN identified him as a sleeper. Clay already ranks him WR28 and 54th overall, nearly 30 spots ahead of market ADP." },
  { listRank: 84, name: "Chuba Hubbard", position: "RB", team: "CAR", adp: 84, verdict: "caution", verdictLabel: "Caution", note: "Hubbard can still provide useful volume, particularly early in the season. Jonathon Brooks gives Carolina a higher-upside alternative, and Clay has Hubbard only 100th overall." },
  { listRank: 85, name: "Jakobi Meyers", position: "WR", team: "JAX", adp: 86, verdict: "fair", verdictLabel: "Fair", note: "Meyers is rarely exciting but consistently earns targets and catches, which matters greatly in full PPR. Jacksonville's crowded receiving group caps his ceiling, but he is an excellent roster stabilizer." },
  { listRank: 86, name: "Wan'Dale Robinson", position: "WR", team: "TEN", adp: 87, verdict: "target", verdictLabel: "Target", note: "Robinson's short-area role makes him particularly useful in full PPR and should pair naturally with Tennessee's developing passing game. Two ESPN analysts independently named him a sleeper, which reinforces the late-round value case." },
  { listRank: 87, name: "J.K. Dobbins", position: "RB", team: "DEN", adp: 88, verdict: "caution", verdictLabel: "Caution", note: "Dobbins has repeatedly demonstrated that he can still produce when healthy and Denver can create efficient rushing opportunities. His durability plus a crowded Broncos backfield makes him more attractive around RB34/100 overall than at ADP 88." },
  { listRank: 88, name: "Brock Purdy", position: "QB", team: "SF", adp: 89, verdict: "target", verdictLabel: "Target", note: "Purdy's efficiency plus the Shanahan infrastructure gives him a very strong weekly floor despite limited rushing production. ESPN's Eric Karabell specifically names him a sleeper, making him one of my preferred wait-on-QB selections." },
  { listRank: 89, name: "Josh Downs", position: "WR", team: "IND", adp: 91, verdict: "fair", verdictLabel: "Fair", note: "Downs is an excellent underneath separator and can pile up receptions when Indianapolis' passing game is functioning efficiently. Current injury concerns deserve monitoring, but a ninth-round-type price leaves room for that uncertainty." },
  { listRank: 90, name: "Caleb Williams", position: "QB", team: "CHI", adp: 92, verdict: "target", verdictLabel: "Target", note: "Williams has a deep collection of weapons and another year in Ben Johnson's system, giving him genuine breakout potential. Because he is being drafted after several lower-rushing veterans, he's a much more attractive upside bet than his QB13 ranking implies." },
  { listRank: 91, name: "Trevor Lawrence", position: "QB", team: "JAX", adp: 93, verdict: "target", verdictLabel: "Target", note: "Lawrence's improved supporting cast and rushing contribution create a credible route back to QB1 production. Clay has him QB8 and 82nd overall, so he fits the late-QB strategy extremely well." },
  { listRank: 92, name: "Kenneth Gainwell", position: "RB", team: "TB", adp: 94, verdict: "fair", verdictLabel: "Fair", note: "Gainwell offers receiving value and can become highly useful if Tampa Bay's backfield suffers an injury or reshuffling. Clay ranks him RB32/99 overall, essentially matching market price." },
  { listRank: 93, name: "Stefon Diggs", position: "WR", team: "WAS", adp: 95, verdict: "caution", verdictLabel: "Caution", note: "Diggs' route-running intelligence and Jayden Daniels' accuracy give him some interesting spike-week potential. Age and McLaurin's entrenched role leave him more dependent on efficiency than the Diggs of old, and Clay ranks him WR48/114 overall." },
  { listRank: 94, name: "Jonathon Brooks", position: "RB", team: "CAR", adp: 96, verdict: "target", verdictLabel: "Target", note: "Brooks is exactly the type of mid-round back worth drafting because his role could grow substantially as the season progresses. Multiple ESPN analysts named him a sleeper, and the market price is almost identical to Clay's No. 96 overall rank." },
  { listRank: 95, name: "RJ Harvey", position: "RB", team: "DEN", adp: 97, verdict: "caution", verdictLabel: "Caution", note: "Harvey has explosive ability and could emerge from Denver's backfield if opportunities break correctly. The trouble is that Clay ranks him only RB41/122 overall, so drafters are already pricing in a meaningful role change." },
  { listRank: 96, name: "Jayden Reed", position: "WR", team: "GB", adp: 98, verdict: "target", verdictLabel: "Target", note: "Reed remains one of Green Bay's most dynamic offensive players even if his weekly usage can be maddening. At essentially WR45 pricing, I prefer betting on his talent and manufactured-touch ability rather than chasing lower-upside veterans." },
  { listRank: 97, name: "Jordan Addison", position: "WR", team: "MIN", adp: 99, verdict: "target", verdictLabel: "Target", note: "Addison gets discounted because Jefferson dominates attention and Minnesota's quarterback picture isn't pristine. He's still a polished touchdown-producing receiver, and Clay has him nine spots ahead of this overall ADP." },
  { listRank: 98, name: "Quentin Johnston", position: "WR", team: "LAC", adp: 100, verdict: "caution", verdictLabel: "Caution", note: "Johnston has developed into a legitimate scoring and vertical threat rather than the early-career disappointment many remember. McConkey's ascension keeps Johnston's reception floor low, so I prefer him slightly later than market." },
  { listRank: 99, name: "Tucker Kraft", position: "TE", team: "GB", adp: 102, verdict: "fair", verdictLabel: "Fair", note: "Kraft offers touchdown upside in an efficient Packers offense and has enough after-catch ability to produce big games without enormous target totals. Clay ranks him TE9 and 102nd overall - the market has this one almost exactly right." },
  { listRank: 100, name: "Jared Goff", position: "QB", team: "DET", adp: 103, verdict: "target", verdictLabel: "Target", note: "Goff lacks rushing production, but Detroit gives him St. Brown, Gibbs, Jameson Williams and one of football's most fantasy-friendly offensive environments. At QB18 pricing, you're barely paying anything for a quarterback with a realistic path to another top-10 passing season." },
];

async function main(): Promise<void> {
  const force = process.argv.includes("--force");
  await ensureFantasyFootballTables();

  // Resolve against the CURRENT board, not all of ff_players. Duplicate player
  // rows genuinely exist (a stale orphan with no sleeper/gsis id alongside the
  // live row -- Puka Nacua and Trevor Lawrence both have one, and CLAUDE.md
  // already records the Lawrence case). Matching the whole table silently
  // attached those notes to the dead row, where no surface would ever render
  // them. Only players on the latest ranking set are eligible.
  const players = await db.execute(sql`
    SELECT p.id, p.normalized_name, p.position, p.canonical_name
    FROM ff_players p
    JOIN ff_player_rankings r ON r.player_id = p.id
    JOIN ff_ranking_sets rs ON rs.id = r.ranking_set_id
    WHERE p.season = ${SEASON}
      AND rs.id = (
        SELECT rs2.id FROM ff_ranking_sets rs2
        WHERE COALESCE(rs2.scoring_profile->>'preset','PPR') = 'PPR'
        ORDER BY rs2.created_at DESC LIMIT 1
      )
  `);
  const byName = new Map<string, Array<{ id: number; position: string; name: string }>>();
  for (const row of players.rows as Array<Record<string, unknown>>) {
    const key = String(row.normalized_name);
    const entry = { id: Number(row.id), position: String(row.position), name: String(row.canonical_name) };
    const bucket = byName.get(key);
    if (bucket) bucket.push(entry);
    else byName.set(key, [entry]);
  }

  let inserted = 0;
  let updated = 0;
  let skipped = 0;
  const unmatched: string[] = [];

  for (const seed of SEED_NOTES) {
    const bucket = byName.get(normalizeAnalystName(seed.name)) ?? [];
    // Position disambiguates a shared name; with one candidate, take it -- the
    // note's position and the roster feed's can legitimately differ.
    const matches = bucket.length === 1 ? bucket : bucket.filter((row) => row.position === seed.position);
    if (matches.length !== 1) {
      // Refuse to guess. Silently taking the first is exactly how the notes for
      // Puka Nacua and Trevor Lawrence landed on rows nothing renders.
      unmatched.push(
        matches.length === 0
          ? `#${seed.listRank} ${seed.name} (${seed.position}) -- no board player`
          : `#${seed.listRank} ${seed.name} (${seed.position}) -- ambiguous, ${matches.length} board players`,
      );
      continue;
    }
    const player = matches[0];

    const result = await db.execute(sql`
      INSERT INTO ff_player_notes (
        player_id, season, normalized_name, position, verdict, verdict_label,
        note, list_rank, source_team, source_adp, author
      ) VALUES (
        ${player.id}, ${SEASON}, ${normalizeAnalystName(seed.name)}, ${seed.position},
        ${seed.verdict}, ${seed.verdictLabel}, ${seed.note}, ${seed.listRank},
        ${seed.team}, ${seed.adp}, ${"seed:pr-152"}
      )
      ON CONFLICT (player_id) DO UPDATE SET
        verdict = CASE WHEN ${force} THEN EXCLUDED.verdict ELSE ff_player_notes.verdict END,
        verdict_label = CASE WHEN ${force} THEN EXCLUDED.verdict_label ELSE ff_player_notes.verdict_label END,
        note = CASE WHEN ${force} THEN EXCLUDED.note ELSE ff_player_notes.note END,
        updated_at = CASE WHEN ${force} THEN NOW() ELSE ff_player_notes.updated_at END
      RETURNING (xmax = 0) AS was_insert, updated_at
    `);
    const row = (result.rows as Array<Record<string, unknown>>)[0];
    if (row?.was_insert === true || row?.was_insert === "t") inserted += 1;
    else if (force) updated += 1;
    else skipped += 1;
  }

  console.log(`seed:analyst-notes -> ${inserted} inserted, ${updated} overwritten, ${skipped} left alone (already present)`);
  if (unmatched.length) {
    console.log(`${unmatched.length} note(s) matched no ${SEASON} player and were skipped:`);
    for (const label of unmatched) console.log(`  - ${label}`);
  }
}

main().then(() => process.exit(0)).catch((error) => {
  console.error(error);
  process.exit(1);
});
