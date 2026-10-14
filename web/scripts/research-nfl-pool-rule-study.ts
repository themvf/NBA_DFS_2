/** Offline worker; database capture/persistence belongs to the Python command. */
import { readFileSync, writeFileSync } from 'node:fs';
import { buildPair } from './nfl-pool-rule-study-core';
const [source, target] = process.argv.slice(2);
if (!source || !target) throw new Error('usage: research-nfl-pool-rule-study.ts INPUT.json OUTPUT.json');
const pair = buildPair(JSON.parse(readFileSync(source, 'utf8')));
writeFileSync(target, JSON.stringify(pair));
console.log(JSON.stringify(pair.arms.map(a => ({ arm: a.arm, status: a.status,
  requested: a.requestedLineups, built: a.lineups.length, error: a.failureReason }))));
