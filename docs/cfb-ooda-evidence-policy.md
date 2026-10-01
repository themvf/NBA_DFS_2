# CFB evidence loop and spread favorite research policy

## Observe

The terminal shows exact bookmaker quotes, their update times, and the quality of
the frozen closing capture. A quote is an observation, not proof that a wager
was accepted. A final score can settle an outcome even when no qualifying close
exists; in that case price and line CLV remain unavailable.

## Orient

Prospective returns use the current head of `cfb_economic_resolutions`. Reports
show the number of alert rows, unique games, settled and pending outcomes,
excluded records, and the denominator for each CLV average. Multiple alerts on
one game are separate observations but not independent games.

Historical first-quote comparisons are retrospective: the side of an alert
triggered later was not known at the first quote. They must be labeled as such
and compare only picks with first, trigger, and close quotes for the same book
and market. The first quote means the first *observed* eligible quote; it is
not necessarily the sportsbook's true opener.

## Decide

The frozen CFB moneyline v4 study and its confirmation windows remain unchanged.
Its pilot cannot qualify a consumer for betting decisions. The opening spread
favorite question is a separate candidate and must not inherit moneyline study
results or the exploratory 2026 Weeks 1–4 return.

Before activating a prospective spread study, freeze these rules in a versioned
study artifact:

1. Eligible game: scheduled CFB full-game matchup with a valid pre-kickoff
   DraftKings spread quote after the study activation time.
2. Entry: the first observed DraftKings spread quote with both side prices, a
   nonzero line, an update time no more than five minutes before capture, and a
   valid American price on the favored side.
3. Selection: the side with the negative spread in that quote. Enroll at most
   one paper decision per game; freeze the quote ID, side, line, price, and time.
4. Grading: final score against the frozen entry line and price. Count a push
   with zero profit and one returned stake; keep missing or stale closing
   captures separate from final-score settlement.
5. Evaluation: group uncertainty by game and game date, publish weekly results
   and a one-percent adverse-price sensitivity, and require a later confirmation
   window before any decision clearance. Never fold pre-activation games into
   that confirmation window.

The rule above is a research specification. The terminal's paper-position
control remains manual and does not enroll this proposed study or place bets.

## Act

While evidence is collecting, the site permits paper observations from fresh
quotes and shows data gaps for review. It does not present these signals as
betting recommendations. Any future change to decision permission requires a
separate, versioned qualification and an explicit product gate.
