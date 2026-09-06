import type { Metadata } from "next";
import ScenarioLab from "./scenario-lab";

export const metadata: Metadata = { title: "NFL DFS · Scenario Lab", description: "Explore complete-lineup score distributions and compare NFL DFS scenario policies." };

export default function Page() { return <ScenarioLab />; }
