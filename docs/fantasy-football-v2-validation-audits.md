# Fantasy Football V2 validation audits

**Version:** `ff-v2-validation-audit-v1`
**Status:** shadow validation gate; no Team Opportunity fit

`model/ff_v2_audits.py` is the fail-closed V2-006 contract. It freezes the
champion-versus-challenger protocol and audits prediction-time features before
any roster-aware model may fit or score a held-out season.

## Frozen comparison protocol

The protocol pins:

- champion `ff-independent-v1.14` and combined artifact digest
  `d9dbdb129aeec4e79e6421ca32bc71c06f128b4b822ac2917bca7f202989d6ac`;
- rolling-origin harness `ff-v2-backtest-v1` and seed `20260828`;
- the complete V2-005 metric policy, required model labels, deterministic
  2,000-draw paired bootstrap, and bootstrap seed `20260829`;
- exact shared artifact identity as the comparison population; and
- V2-021 as the verdict gate, followed by explicit post-evidence user
  authorization before activation.

The frozen protocol digest is
`56462cd23e414065a3fbc5157b2e67ad9e1ffda777d6d7fa474a95fb1963907b`.
The audit rejects a fitted model or calibration version, so this contract is
demonstrably frozen while Team Opportunity is still unfitted.

## Row-level feature contract

Every evaluated identity must provide a stable seed token and a metadata record
for every feature. Each record declares its value, availability timestamp,
source dataset and season, feature group, eligibility, and missingness reason.
The emitted row audit preserves those fields plus a value digest.

Validation fails on:

- feature availability after the fold's preseason cutoff;
- future-season, current-roster, current-depth-chart, or live-transaction data;
- ADP, ECR, rankings, consensus, or market data used as a
  `football_performance` feature;
- duplicate evaluated identities;
- seeds not deterministically derived from the frozen root seed;
- missing values without an explicit ineligible state and reason; and
- a changed protocol, self-digest, replay result, or stored backtest field.

Market artifacts remain permitted as separately labeled comparison baselines
under V2-005. They are prohibited only from football-performance features.

## Representative historical artifact

[`../artifacts/ff_v2_validation_audit_2020_2025.json`](../artifacts/ff_v2_validation_audit_2020_2025.json)
audits all five scorable historical folds, 2021–2025. It contains 5 evaluated
rows and 15 feature records: 10 eligible and 5 explicitly missing play-caller
features. Its digest is
`a94acd8c45cef2985531c9f6e6b335119aeba9bfd4ce8b029562790c957115f8`.

```powershell
python -m model.ff_v2_audits `
  --backtest-artifact artifacts/ff_v2_backtest_harness_2020_2025.json `
  --champion-artifact artifacts/ff_champion_baseline_v1.14.json `
  --artifact artifacts/ff_v2_validation_audit_2020_2025.json

python -m model.ff_v2_audits `
  --backtest-artifact artifacts/ff_v2_backtest_harness_2020_2025.json `
  --champion-artifact artifacts/ff_champion_baseline_v1.14.json `
  --artifact artifacts/ff_v2_validation_audit_2020_2025.json `
  --verify
```

The stronger `model.ff_v2_backtest --verify` path also compares every
deterministic artifact field, every persisted run field, and every field on all
six persisted split records. A matching row count or top-level digest alone is
not sufficient.
