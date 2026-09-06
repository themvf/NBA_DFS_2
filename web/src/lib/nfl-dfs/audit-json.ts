/** Stable across JSONB key reordering; omit undefined exactly as JSON transport does. */
export function canonicalAuditJson(value:unknown):string {
  const sort=(v:unknown):unknown=>Array.isArray(v)?v.map(sort):v!==null&&typeof v==='object'?Object.fromEntries(Object.entries(v).sort(([a],[b])=>a<b?-1:a>b?1:0).map(([k,x])=>[k,sort(x)])):v;
  return JSON.stringify(sort(JSON.parse(JSON.stringify(value))));
}
