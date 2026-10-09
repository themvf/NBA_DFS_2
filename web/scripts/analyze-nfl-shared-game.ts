import { readFileSync, writeFileSync } from 'node:fs';
import { gunzipSync } from 'node:zlib';
import { analyzePartialGame, analyzeCompleteDfs } from '../src/lib/nfl-dfs/shared-game-model';

const [mode, input, output] = process.argv.slice(2);
if (!['partial', 'complete'].includes(mode) || !input || !output) throw new Error('Usage: analyze-nfl-shared-game.ts partial|complete INPUT OUTPUT');
const bytes = readFileSync(input);
const payload = JSON.parse((input.endsWith('.gz') ? gunzipSync(bytes) : bytes).toString('utf8'));
const result = mode === 'partial' ? { ...analyzePartialGame(payload.shared_draws ?? payload),
  sourceSha256: payload.source_sha256 ?? null, workloadDiagnostics: payload.diagnostics ?? null,
  historyCoverage: payload.history_coverage ?? null } : { ...analyzeCompleteDfs(payload.slate, payload.evaluation ?? payload.bank, payload.lineups ?? []),
  generatorManifest: payload.manifest ?? null, sourceCoverage: payload.coverage ?? null,
  generatorLimits: payload.limitations ?? [], retrospective: payload.retrospective ?? null };
writeFileSync(output, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
console.log(JSON.stringify({ output, scope: mode, players: result.players.length }));
