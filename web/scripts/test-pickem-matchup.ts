import assert from "node:assert/strict";
import { matchupResidual, threeWayLoss, type ResidualInput } from "../src/lib/nfl/pickem-matchup";
const input: ResidualInput = { gameId: "2026_03_TB_MIN", decisionCutoff: "2026-09-27T15:00:00Z", kickoff: "2026-09-27T17:00:00Z",
  baseline: { homeConditional: .6, tie: .01, marketCapturedAt: "2026-09-27T14:30:00Z", source: "market" },
  model: { artifactId: "fixture-fit-v1", definitionId: "fixture-combined-v1", version: "fixture-v1", consumerId: "nfl-pickem",
    useCase: "game-win", cohort: "regular-season", coefficients: { pressure: .2 }, intercept: 0,
    trainedThrough: "2026-09-20T00:00:00Z", trainingManifest: { testFixture: true } },
  features: [{ definitionId: "pressure", value: 0, snapshotId: "frozen-1", availableAt: "2026-09-27T12:00:00Z" }] };
const zero = matchupResidual(input);
assert.equal(zero.status,"shadow"); assert.equal(zero.candidate?.homeConditional,.6);
assert.equal(zero.candidate?.home,zero.baseline?.home); assert.equal(zero.candidate?.tie,.01);
const shift = matchupResidual({...input,features:[{...input.features[0],value:1}]});
assert.ok(shift.candidate!.home>zero.candidate!.home); assert.equal(shift.candidate!.tie,zero.candidate!.tie);
assert.ok(Math.abs(shift.candidate!.home+shift.candidate!.away+shift.candidate!.tie-1)<1e-12);
assert.equal(matchupResidual({...input,model:null}).candidate,null);
const unavailable = matchupResidual({...input,features:[{...input.features[0],availableAt:"2026-09-27T16:00:00Z"}]});
assert.equal(unavailable.status,"fallback"); assert.equal(unavailable.candidate?.home, unavailable.baseline?.home);
assert.equal(matchupResidual({...input,baseline:{...input.baseline,tie:null}}).candidate,null);
assert.equal(matchupResidual({...input,baseline:{...input.baseline,marketCapturedAt:"2026-09-26T01:00:00Z"}}).candidate,null);
assert.equal(matchupResidual({...input,model:{...input.model!,trainedThrough:"2026-09-27T16:00:00Z"}}).candidate,null);
assert.equal(matchupResidual({...input,features:[...input.features,...input.features]}).status,"fallback");
const loss = threeWayLoss({home:.5,away:.4,tie:.1},"tie");
assert.ok(Math.abs(loss.logLoss+Math.log(.1))<1e-12); assert.ok(Math.abs(loss.brier-1.22)<1e-12);
assert.ok(Number.isFinite(threeWayLoss({home:1,away:0,tie:0},"tie").logLoss));
console.log("Pick'em residual tests passed: exact baseline, shared ties, fitted coefficients, chronology, missing inputs, three-way losses.");
