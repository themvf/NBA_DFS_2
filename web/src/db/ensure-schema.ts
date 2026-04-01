import { sql } from "drizzle-orm";

import { db } from ".";

let ensureDkPlayerPropColumnsPromise: Promise<void> | null = null;

const DK_PLAYER_PROP_COLUMN_DDLS = [
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_pts_price INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_pts_book TEXT`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_reb_price INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_reb_book TEXT`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_ast_price INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_ast_book TEXT`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_blk REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_blk_price INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_blk_book TEXT`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_stl REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_stl_price INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_stl_book TEXT`,
];

export async function ensureDkPlayerPropColumns(): Promise<void> {
  if (!ensureDkPlayerPropColumnsPromise) {
    ensureDkPlayerPropColumnsPromise = (async () => {
      for (const ddl of DK_PLAYER_PROP_COLUMN_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureDkPlayerPropColumnsPromise = null;
      throw error;
    });
  }
  await ensureDkPlayerPropColumnsPromise;
}
