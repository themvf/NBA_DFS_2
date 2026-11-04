"use client";
import {useEffect,useState} from 'react';
export function EvidenceAge({capturedAt}:{capturedAt:string|null}){
 const [now,setNow]=useState<number|null>(null);
 useEffect(()=>{const update=()=>setNow(Date.now());update();const id=setInterval(update,60000);return()=>clearInterval(id)},[]);
 if(!capturedAt||now===null)return null;
 const stamp=Date.parse(capturedAt);if(!Number.isFinite(stamp))return <span> · capture age unavailable</span>;
 const minutes=Math.floor((now-stamp)/60000);
 if(minutes<0)return <span> · capture clock mismatch</span>;
 return <span> · archived quote age {minutes<60?`${minutes} min`:`${Math.floor(minutes/60)} hr ${minutes%60} min`}</span>;
}
