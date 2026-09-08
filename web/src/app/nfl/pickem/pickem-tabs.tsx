import Link from "next/link";

/**
 * Sub-navigation shared by the pick'em board and its method page.
 *
 * Kept in one component so the two routes cannot drift into looking like
 * unrelated pages — the method page only earns its place if it is obviously
 * the explanation OF the board, not a separate document about the same topic.
 */
export default function PickemTabs({ active }: { active: "board" | "method" }) {
  const tab = (href: string, label: string, key: "board" | "method") => (
    <Link
      href={href}
      className={`rounded-t border-b-2 px-3 py-1.5 text-sm transition-colors ${
        active === key
          ? "border-foreground font-semibold text-foreground"
          : "border-transparent text-muted-foreground hover:text-foreground"
      }`}
    >
      {label}
    </Link>
  );
  return (
    <div className="flex items-center gap-1 border-b">
      {tab("/nfl/pickem", "Board", "board")}
      {tab("/nfl/pickem/method", "Method & archetypes", "method")}
    </div>
  );
}
