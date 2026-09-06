import fs from 'node:fs';
import {createHash} from 'node:crypto';
import { replayWorkload } from '../src/lib/nfl-dfs/workload-scenario';
import type { PlayerContext } from '../src/lib/nfl-dfs/player-context';
const data=JSON.parse(fs.readFileSync('src/data/nfl-player-context-2025.json','utf8')) as PlayerContext;
const digest=(path:string)=>createHash('sha256').update(fs.readFileSync(path)).digest('hex');
const result={...replayWorkload(data),sourceDigest:digest('src/data/nfl-player-context-2025.json'),recipeDigest:digest('src/lib/nfl-dfs/workload-scenario.ts')};
fs.writeFileSync('src/data/nfl-wr-workload-replay.json',JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify(result,null,2));
