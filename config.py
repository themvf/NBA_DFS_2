"""Configuration for NBA DFS v2 + MLB expansion."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional
import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)


def nba_season_for(day: date | str | None = None) -> str:
    """nba_api season string for a date. The season turns over in August, so
    2026-10-24 is "2026-27" and 2026-04-10 is "2025-26". Defaults to today.

    Hard-coding the string silently fed a new season last season's final ten
    games (2026-27 slates projected from April 2026 tank-rotation minutes).
    """
    if day is None:
        day = date.today()
    elif isinstance(day, str):
        day = date.fromisoformat(day[:10])
    start = day.year if day.month >= 8 else day.year - 1
    return f"{start}-{(start + 1) % 100:02d}"


@dataclass
class NbaApiConfig:
    season: str = field(default_factory=nba_season_for)   # nba_api season string, from today
    rolling_games: int = 10          # rolling window for player stat averages
    timeout_seconds: int = 30
    # stats.nba.com requires a browser-like User-Agent; nba_api sets this automatically


@dataclass
class MlbApiConfig:
    season: str = field(default_factory=lambda: str(date.today().year))  # MLB season year
    rolling_games: int = 15            # rolling window for batter/pitcher stat averages
    base_url: str = "https://statsapi.mlb.com/api/v1"
    timeout_seconds: int = 20


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
class DeepSeekApiConfig:
    api_key: str = ""
    base_url: str = "https://api.deepseek.com/chat/completions"
    model: str = "deepseek-chat"
    timeout_seconds: int = 60
    max_retries: int = 5
    retry_backoff_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> DeepSeekApiConfig:
        return cls(api_key=os.getenv("DEEPSEEK_API_KEY", ""))


@dataclass
class AppConfig:
    nba_api: NbaApiConfig = field(default_factory=NbaApiConfig)
    mlb_api: MlbApiConfig = field(default_factory=MlbApiConfig)
    odds_api: OddsApiConfig = field(default_factory=OddsApiConfig)
    deepseek_api: DeepSeekApiConfig = field(default_factory=DeepSeekApiConfig)
    database_url: Optional[str] = None

    @classmethod
    def from_env(cls) -> AppConfig:
        return cls(
            odds_api=OddsApiConfig.from_env(),
            deepseek_api=DeepSeekApiConfig.from_env(),
            database_url=os.getenv("DATABASE_URL"),
        )


def load_config() -> AppConfig:
    """Load configuration from environment variables."""
    return AppConfig.from_env()
