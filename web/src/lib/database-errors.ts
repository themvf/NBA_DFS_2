/** Drizzle wraps provider errors in `cause`; missing optional research tables are pending states. */
export function isMissingRelation(error:unknown):boolean {
 let current:unknown=error;const seen=new Set<unknown>();
 for(let depth=0;depth<6&&current!=null&&!seen.has(current);depth++){
  seen.add(current);
  if(typeof current==='object'){
   const value=current as {code?:unknown;message?:unknown;cause?:unknown};
   if(value.code==='42P01'||typeof value.message==='string'&&/relation .* does not exist/i.test(value.message))return true;
   current=value.cause;
  }else{if(typeof current==='string'&&/relation .* does not exist/i.test(current))return true;break}
 }
 return false;
}
