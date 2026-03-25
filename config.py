"""Configuration for NBA DFS v2."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)


@dataclass
class NbaApiConfig:
    season: str = "2025-26"          # nba_api season string format
    rolling_games: int = 10          # rolling window for player stat averages
    timeout_seconds: int = 30
    # stats.nba.com requires a browser-like User-Agent; nba_api sets this automatically


@dataclass
class OddsApiConfig:
    api_key: str = ""
    base_url: str = "https://api.the-odds-api.com/v4"
    sport_key: str = "basketball_nba"
    timeout_seconds: int = 20
    max_retries: int = 5
    retry_backoff_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> OddsApiConfig:
        return cls(api_key=os.getenv("ODDS_API_KEY", ""))


@dataclass
class AppConfig:
    nba_api: NbaApiConfig = field(default_factory=NbaApiConfig)
    odds_api: OddsApiConfig = field(default_factory=OddsApiConfig)
    database_url: Optional[str] = None

    @classmethod
    def from_env(cls) -> AppConfig:
        return cls(
            odds_api=OddsApiConfig.from_env(),
            database_url=os.getenv("DATABASE_URL"),
        )


def load_config() -> AppConfig:
    """Load configuration from environment variables."""
    return AppConfig.from_env()
