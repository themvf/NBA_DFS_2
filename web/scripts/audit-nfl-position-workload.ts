import {getCalibratedSnapshots} from '../src/db/nfl-dfs-calibrated';
async function main(){const rows=await getCalibratedSnapshots(2026,1);const counts:Record<string,{snapshots:number;candidates:number}>={};for(const r of rows){const p=r.payload as {position:string;candidate:unknown};const c=counts[p.position]??={snapshots:0,candidates:0};c.snapshots++;if(p.candidate)c.candidates++;}console.log(JSON.stringify(counts,null,2));}
main().catch(()=>{console.error('Position snapshot audit failed.');process.exitCode=1;});
