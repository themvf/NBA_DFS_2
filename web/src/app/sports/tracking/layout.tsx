import Link from "next/link";
import type { ReactNode } from "react";
export default function TrackingLayout({children}:{children:ReactNode}) {
  return <><nav aria-label="Tracking pages" className="mb-4 flex flex-wrap gap-4 text-sm"><Link href="/sports">Sports</Link><Link href="/sports/tracking">Tracking</Link><Link href="/sports/tracking/definition">Definition</Link></nav>{children}</>;
}
