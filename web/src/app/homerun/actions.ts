"use server";

import { redirect } from "next/navigation";

import { loadMlbSlateFromDraftGroupId } from "@/app/dfs/actions";
import { getMlbHomerunBoard, type MlbHomerunBoardView } from "@/db/queries";

function cleanPositiveInt(value: FormDataEntryValue | null): number | null {
  if (typeof value !== "string" || value.trim() === "") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

function cleanDate(value: FormDataEntryValue | null): string | null {
  if (typeof value !== "string") return null;
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : null;
}

function cleanView(value: FormDataEntryValue | null): MlbHomerunBoardView {
  return value === "edge" || value === "leverage" || value === "longshots" ? value : "likely";
}

function addLoadError(params: URLSearchParams, message: string): void {
  params.set("loadError", message.slice(0, 300));
}

export async function loadHomerunBoardAction(formData: FormData): Promise<void> {
  const dkId = cleanPositiveInt(formData.get("dkId"));
  const date = cleanDate(formData.get("date"));
  const view = cleanView(formData.get("view"));

  const params = new URLSearchParams({ sport: "mlb", view });
  if (dkId != null) params.set("dkId", String(dkId));
  if (date) params.set("date", date);

  if (dkId == null) {
    redirect(`/homerun?${params.toString()}`);
  }

  const board = await getMlbHomerunBoard({ date, dkId, view });
  if (board.dkDraftGroupId == null) {
    addLoadError(params, board.dkIdError ?? `DraftKings ID ${dkId} could not be resolved to a Home Runs draft group.`);
    redirect(`/homerun?${params.toString()}`);
  }

  const loadResult = await loadMlbSlateFromDraftGroupId(board.dkDraftGroupId, undefined, "homerun", undefined, "gpp");
  if (!loadResult.ok) {
    addLoadError(params, loadResult.message);
  }

  redirect(`/homerun?${params.toString()}`);
}
