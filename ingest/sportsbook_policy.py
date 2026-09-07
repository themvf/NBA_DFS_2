"""User-selected sportsbook universe for MLB, Tennis, CFB and NFL."""
BOOKMAKER_KEYS = ("pinnacle", "fanduel", "fanatics", "draftkings", "williamhill_us", "betmgm")
BOOKMAKERS = ",".join(BOOKMAKER_KEYS)

def requested_bookmakers(requested: str | None = None) -> str:
    if not requested:
        return BOOKMAKERS
    keys = {"williamhill_us" if key.strip() == "caesars" else key.strip() for key in requested.split(",")}
    selected = [key for key in BOOKMAKER_KEYS if key in keys]
    if not selected:
        raise ValueError("No configured sportsbooks in requested bookmaker subset")
    return ",".join(selected)

def selected_books(books: dict | None) -> dict:
    source = dict(books or {})
    if "caesars" in source and "williamhill_us" not in source:
        source["williamhill_us"] = source["caesars"]
    return {key: source[key] for key in BOOKMAKER_KEYS if key in source}


def selected_event(event: dict) -> dict:
    return {**event, "bookmakers": [book for book in event.get("bookmakers", []) if book.get("key") in BOOKMAKER_KEYS]}
