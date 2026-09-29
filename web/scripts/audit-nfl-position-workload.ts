import {getCalibratedSnapshots} from '../src/db/nfl-dfs-calibrated';
// Usage: audit-nfl-position-workload.ts [season] [week] [asOf ISO]. Defaults to the current week-1 audit as of now.
const [season,week,asOf]=[Number(process.argv[2]??2026),Number(process.argv[3]??1),new Date(process.argv[4]??Date.now())];
async function main(){const rows=await getCalibratedSnapshots(season,week,asOf);const counts:Record<string,{snapshots:number;candidates:number}>={};for(const r of rows){const p=r.payload as {position:string;candidate:unknown};const c=counts[p.position]??={snapshots:0,candidates:0};c.snapshots++;if(p.candidate)c.candidates++;}console.log(JSON.stringify(counts,null,2));}
main().catch(()=>{console.error('Position snapshot audit failed.');process.exitCode=1;});
