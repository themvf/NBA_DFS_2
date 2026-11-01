"use server";

import { getCbbPropGame, getCbbPropSchedule } from "@/lib/cbb-props/provider";

export async function loadCbbPropSchedule() {
  try { return { ok: true as const, data: await getCbbPropSchedule() }; }
  catch (error) { return { ok: false as const, error: error instanceof Error ? error.message : "The CBB schedule is unavailable." }; }
}
export async function loadCbbPropGame(id: string) {
  try { return { ok: true as const, data: await getCbbPropGame(id) }; }
  catch (error) { return { ok: false as const, error: error instanceof Error ? error.message : "CBB player props are unavailable." }; }
}
