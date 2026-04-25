import type { NextConfig } from "next";
import { withWorkflow } from "workflow/next";

const nextConfig: NextConfig = {
  // @workflow/core uses require(targetWorld) — a dynamic require that Turbopack
  // can't statically analyse. Marking workflow packages as server externals
  // keeps them as Node.js require() calls at runtime rather than bundled.
  serverExternalPackages: [
    "@workflow/core",
    "@workflow/world",
    "@workflow/world-local",
    "@workflow/world-vercel",
    "workflow",
  ],
  images: {
    remotePatterns: [
      { protocol: "https", hostname: "cdn.nba.com" },
    ],
  },
};

export default withWorkflow(nextConfig);
