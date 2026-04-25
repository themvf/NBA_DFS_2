import type { NextConfig } from "next";
import { withWorkflow } from "workflow/next";

const nextConfig: NextConfig = {
  // Packages that use require() inside conditional/function bodies, which
  // Turbopack can't statically resolve. Marking them as server externals
  // delegates loading to Node.js's native require() at runtime.
  //
  // @workflow/core: require(targetWorld) — dynamic variable require
  // javascript-lp-solver: require("fs") / require("child_process") inside
  //   conditional blocks in solver.js — not resolvable at Turbopack compile time
  serverExternalPackages: [
    "@workflow/core",
    "@workflow/world",
    "@workflow/world-local",
    "@workflow/world-vercel",
    "workflow",
    "javascript-lp-solver",
  ],
  images: {
    remotePatterns: [
      { protocol: "https", hostname: "cdn.nba.com" },
    ],
  },
};

export default withWorkflow(nextConfig);
