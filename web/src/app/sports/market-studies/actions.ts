"use server";

import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { CFB_PATTERN_SESSION, createPatternWatchSession } from "@/lib/cfb-pattern-watch-access";
import { DAILY_STUDIES_PATH, studyDate } from "@/lib/daily-market-studies";
import { easternDateString } from "@/lib/eastern-date";

export async function unlockPatternWatch(form: FormData) {
  const date = studyDate(form.get("date"), easternDateString());
  const key = form.get("key");
  const session = typeof key === "string" ? createPatternWatchSession(key, Date.now()) : null;
  const query = new URLSearchParams({ date });
  if (session) {
    (await cookies()).set(CFB_PATTERN_SESSION, session.value, {
      expires: new Date(session.expiresAt),
      httpOnly: true,
      sameSite: "lax",
      secure: process.env.NODE_ENV === "production",
      path: "/",
    });
  } else {
    query.set("access", "denied");
  }
  redirect(DAILY_STUDIES_PATH + "?" + query.toString());
}
