import { capturePool, captureDuePools, archiveOptimizerPool } from '../src/lib/nfl-dfs/pool-capture-service';
async function main() {
  const [mode,id]=process.argv.slice(2);
  const result=mode==='--archive-optimizer'&&id?await archiveOptimizerPool(id)
    :mode==='--upload'&&id?await capturePool(id):mode==='--due'?await captureDuePools():null;
  if(!result)throw new Error('Use --upload UUID, --archive-optimizer UUID, or --due');
  console.log(JSON.stringify(result,null,2));
  if('errors' in result && result.errors.length)process.exitCode=1;
}
main().catch(e=>{console.error(e instanceof Error?e.message:'Capture failed');process.exitCode=1;});
