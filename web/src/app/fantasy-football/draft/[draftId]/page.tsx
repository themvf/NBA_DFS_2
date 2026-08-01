export const dynamic = "force-dynamic";

import { notFound } from "next/navigation";
import { getFantasyDraftState } from "@/db/queries-fantasy-football";
import DraftRoomClient from "./draft-room-client";

export default async function FantasyDraftRoomPage({ params }: { params: Promise<{ draftId: string }> }) {
  const { draftId } = await params;
  const state = await getFantasyDraftState(draftId);
  if (!state) notFound();
  return <DraftRoomClient initialState={state} />;
}
