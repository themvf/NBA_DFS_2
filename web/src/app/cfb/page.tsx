import type { Metadata } from "next";
import CfbTerminalClient from "./cfb-terminal-client";

export const metadata: Metadata = {
  title: "CFB Line Terminal",
  description: "College football line movement, market catalysts, news, and paper-trade tracking.",
};

export default function CfbPage() {
  return <CfbTerminalClient evaluatedAt={new Date().toISOString()} />;
}
