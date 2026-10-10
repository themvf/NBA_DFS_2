/** Replay the frozen research candidate set. No database or entry writes. */
import { readFileSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { compareJointPortfolioIfAvailable } from '../src/lib/nfl-dfs/joint-portfolio';
import type { NflDkSlate } from '../src/lib/nfl-dfs/dk-salary-csv';
import type { NflScenarioBank } from '../src/lib/nfl-dfs/scenarios';
import type { NflGeneratedLineup, NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';
const [bankPath,portfolioPath,outputPath]=process.argv.slice(2);
if(!bankPath||!portfolioPath||!outputPath)throw new Error('Supply bank, frozen portfolio report and a new output path.');
const bankBytes=readFileSync(bankPath),portfolioBytes=readFileSync(portfolioPath);
const banks=JSON.parse(bankBytes.toString()) as {slate:NflDkSlate;selection:NflScenarioBank;evaluation:NflScenarioBank;audit:{uploadId:string}};
const prior=JSON.parse(portfolioBytes.toString()) as {uploadId:string;sourceDigest:string;records:{mode:string;settings:NflOptimizerSettings;
  baselineGenerated:NflGeneratedLineup[];shadowComparison:{selectedGenerated:NflGeneratedLineup[]}}[]};
const digest=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
if(prior.uploadId!==banks.audit.uploadId||prior.sourceDigest!==digest(bankBytes))throw new Error('Portfolio and joint bank snapshots differ.');
const records=prior.records.map(record=>({mode:record.mode,result:compareJointPortfolioIfAvailable({slate:banks.slate,
  settings:record.settings,selection:banks.selection,evaluation:banks.evaluation,
  baseline:{lineups:record.baselineGenerated,warnings:[],sourceCoverage:{requested:0,direct:0,fallback:0,excluded:0}},
  candidates:record.shadowComparison.selectedGenerated??[]})}));
writeFileSync(outputPath,JSON.stringify({version:'nfl-joint-constrained-shadow-v1',bankDigest:digest(bankBytes),
  portfolioDigest:digest(portfolioBytes),productionChanged:false,exportAuthorized:false,records},null,2),{flag:'wx'});
console.log(JSON.stringify({records:records.map(r=>({mode:r.mode,status:r.result.status,reason:r.result.reason})),productionChanged:false}));
