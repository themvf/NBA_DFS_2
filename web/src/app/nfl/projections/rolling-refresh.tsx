"use client";

import { useEffect, useTransition } from "react";
import { useRouter } from "next/navigation";

export default function RollingRefresh() {
  const router = useRouter();
  const [pending, startTransition] = useTransition();
  useEffect(() => {
    const timer = window.setInterval(() => startTransition(() => router.refresh()), 5 * 60 * 1000);
    return () => window.clearInterval(timer);
  }, [router]);
  return <button type="button" onClick={() => startTransition(() => router.refresh())}
    disabled={pending} className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm font-medium hover:bg-slate-50 disabled:opacity-50">
    {pending ? "Refreshing…" : "Refresh now"}
  </button>;
}
