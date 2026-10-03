import type { Metadata } from "next";
import Link from "next/link";
import { getCfbAnalyticsTeams } from "@/db/cfb-analytics";
import { AnalyticsShell } from "../_components";
import styles from "../analytics.module.css";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "CFB Team Profiles" };

export default async function CfbAnalyticsTeamsPage() {
  const teams = await getCfbAnalyticsTeams();
  return <AnalyticsShell eyebrow="CFB · TEAM DIRECTORY" title="Team profiles" description="Direct links to independent team pages. Profiles use point-in-time scores and roster context and remain available outside the Line Terminal.">
    <section className={styles.section}><div className={styles.sectionHead}><h2>FBS teams</h2><p>{teams.length} teams</p></div><div className={styles.teamList}>{teams.map((team) => <Link key={team.id} href={`/cfb/analytics/teams/${team.id}`}>{team.name}<small>{team.conference ?? "Conference unavailable"}</small></Link>)}</div></section>
  </AnalyticsShell>;
}
