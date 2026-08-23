"use client";

/**
 * Fires the browser's own print dialog.
 *
 * Deliberately a button the user presses rather than an auto-print on load:
 * the print pages are also readable on screen (the scoring switcher and the
 * back link only make sense there), and a page that throws up a print dialog
 * the moment it opens is hostile if you only meant to look at it.
 *
 * Client component because window.print() needs a real click handler; the rest
 * of the print shell stays a server component.
 */
export default function PrintButton() {
  return <button
    type="button"
    onClick={() => window.print()}
    className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-bold text-white hover:bg-slate-700"
  >
    Print
  </button>;
}
