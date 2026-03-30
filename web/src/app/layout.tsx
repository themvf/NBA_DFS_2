import type { Metadata } from "next";
import { Suspense } from "react";
import { SportNav } from "@/components/sport-nav";
import "./globals.css";

export const metadata: Metadata = {
  title: "DFS Optimizer",
  description: "DraftKings DFS optimizer and analytics — NBA, MLB",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="antialiased">
        {/* Suspense required: SportNav uses useSearchParams() */}
        <Suspense fallback={
          <header className="sticky top-0 z-50 h-14 border-b bg-background/95" />
        }>
          <SportNav />
        </Suspense>
        <main className="mx-auto max-w-7xl px-4 py-6">{children}</main>
      </body>
    </html>
  );
}
