import assert from "node:assert/strict";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { PfrEvidencePanel } from "../src/app/nfl/pickem/pfr-evidence";
import { pfrGame, pfrTeam } from "../src/lib/nfl/pickem-pfr";
assert.equal(pfrTeam("LA"), "LAR");
assert.equal(pfrTeam("WAS"), "WSH");
const missing = pfrGame("g", 1, null, null);
assert.equal(missing.missing.length, 4);
assert.equal(missing.players.length, 0);
const game = pfrGame("g", 1, "2026-09-27T00:00:00Z", {
  stats_schema: "nflverse_pfr_fields_percentage_points", coverage: { passing_advanced: {status: "available"} },
  rows: [{section: "passing_advanced",player_name:"QB",pfr_player_id:"QB00",team:"TB",
    stats:{times_pressured_pct:12.5,times_sacked:0,passing_drops:null,times_hit:"invalid"}}],
});
assert.equal(game.missing.length, 3);
assert.equal(game.players[0].stats.times_pressured_pct, 12.5);
assert.equal(game.players[0].stats.times_sacked, 0);
assert.equal(game.players[0].stats.passing_drops, null);
assert.equal(pfrGame("g", 1, null, {stats_schema:"unknown",rows:[]}).missing.length, 4);
const markup = renderToStaticMarkup(React.createElement(PfrEvidencePanel, {evidence:[{team:"TB",games:[game]}]}));
assert.ok(markup.includes("Pressure, protection"));
assert.ok(markup.includes("12.5"));
assert.ok(markup.includes("Missing: rushing, receiving, defense"));
assert.ok(renderToStaticMarkup(React.createElement(PfrEvidencePanel, {})).includes("unavailable"));
console.log("PFR evidence tests passed: aliases, coverage, null/zero, percentage units, unsupported schemas.");
const frozen = pfrGame("g", 1, "2026-09-27T00:00:00Z", {
  stats_schema: "nflverse_pfr_fields_percentage_points", source_provider: "nflverse_pfr", source_url: "https://example.org/original.csv",
  identity_manifest: { version: "pfr-identity-v1", digest: "frozen", mappings: [{ pfr_player_id: "QB00", gsis_id: "00-1", status: "resolved" }] },
  source_files: [{url: "https://example.org/original.csv",sha256:"file-hash"}],
  rows: [{section:"passing_advanced",player_name:"QB",pfr_player_id:"QB00",team:"LA",gsis_id:"00-1",identity_status:"resolved",stats:{}}],
}, {snapshotId:12,recordedAt:"2026-09-27T00:01:00Z",parserVersion:"parser-v1",sourceSha256:"payload-hash"});
assert.equal(frozen.manifest.snapshotId,12); assert.equal(frozen.manifest.sourceSha256,"payload-hash");
assert.equal(frozen.sourceUrl,"https://example.org/original.csv"); assert.equal(frozen.players[0].gsisId,"00-1");
assert.equal(frozen.players[0].sourceTeam,"LA"); assert.equal(frozen.players[0].team,"LAR");
assert.equal(game.manifest.identityResolution,"unresolved"); assert.equal(game.players[0].gsisId,null);
assert.deepEqual(JSON.parse(JSON.stringify(frozen)),frozen);
