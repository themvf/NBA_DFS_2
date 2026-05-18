"""
DeepSeek MCP Server.

Exposes tools for:
  - generate_code: write code for a task
  - review_code: critique existing code
  - explain_game_outcome: explain why a game finished the way it did from
    structured context such as box score, Vegas lines, and team environment
"""

import os
from textwrap import dedent
import httpx
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Server setup
# ---------------------------------------------------------------------------

mcp = FastMCP("deepseek")

DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"


def _get_api_key() -> str:
    key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not key:
        raise RuntimeError(
            "DEEPSEEK_API_KEY environment variable is not set. "
            "Set it in PowerShell with: $env:DEEPSEEK_API_KEY = 'your-key'"
        )
    return key


def _call_deepseek(system_prompt: str, user_message: str) -> str:
    """Send a chat completion request to DeepSeek and return the reply text."""
    headers = {
        "Authorization": f"Bearer {_get_api_key()}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0.2,
    }
    with httpx.Client(timeout=60) as client:
        response = client.post(DEEPSEEK_API_URL, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]


def explain_game_outcome_message(
    context: str,
    sport: str = "nba",
    audience: str = "dfs analyst",
) -> str:
    """
    Explain why a game had a particular outcome from structured context.

    Args:
        context: JSON or plain-text game context including score, lines,
            team environment, and box-score details.
        sport: Sport name used to tailor terminology.
        audience: Who the explanation is for.

    Returns:
        A structured game outcome explanation.
    """
    system = dedent(
        f"""
        You are an expert {sport.upper()} game analyst helping a {audience}.
        Your job is to explain why a game finished the way it did using the
        supplied game context only.

        Hard rules:
        - Treat the supplied JSON as the source of truth.
        - Do not invert favorite and underdog. Read favorite/underdog strictly
          from the Vegas fields in the input.
        - If moneylines, implied probabilities, or spread direction conflict
          with your intuition, trust the input and state the market context from
          the input only.
        - If a fact is missing, say it is not provided instead of inferring it.
        - Do not claim foul trouble, garbage time, injuries, coaching changes,
          lineup changes, or rotation changes unless the input explicitly
          contains evidence for them.
        - Do not treat plus/minus alone as proof of game script.
        - Do not treat bench scoring alone as proof of bench dominance,
          garbage time, or starter failure; explain only what the box score
          directly supports.
        - If a player shooting line is incomplete or missing, do not invent
          attempts or makes. Use only the values present in the input.
        - Treat implied team points and win probabilities as different fields.
          Do not describe implied points as probabilities.
        - Do not describe the game as "playoff-style", "tight rotation",
          "garbage time", "blowout", "clutch-heavy", or similar game-script
          labels unless the input explicitly contains evidence for that label.
        - Do not infer minute patterns beyond what the raw minutes directly show.
          Heavy minutes alone do not prove rotation tightening.
        - If the input includes an evidenceFlags object, treat it as the
          controlling gate for claims about competitiveness, garbage time,
          and heavy-minute workload. If the relevant flag is false or missing,
          do not make that claim.
        - If the input includes deterministic summary fields such as
          shootingGapSummary, starterVsBenchProduction, largestTeamEdge,
          minuteSummary, playByPlaySummary, recentForm, or seriesContext,
          prefer those over re-deriving the same conclusions from raw box
          score fields.

        Prioritize:
        1. The actual score and how it differed from expectation.
        2. Which team-level box score factors drove the result.
        3. Which player-level performances or absences changed the game.
        4. Whether pace, shooting variance, rebounding, turnovers, free throws,
           bench scoring, matchup context, or recent-form deviation mattered most.
        5. Any DFS-relevant takeaway such as star underperformance, role-player
           spike, blowout/minutes effect, or overtime distortion.

        Keep the analysis grounded. Do not invent injuries, rotations, or
        narrative details that are not in the input.

        In "Market / Expectation Context":
        - Explicitly identify which team was favored pregame.
        - Prefer the input's precomputed fields such as favorite, underdog,
          favoriteWinProb, favoriteWon, favoriteCovered, and gameWentOver over
          your own derived market math.
        - Use homeMl, awayMl, homeWinProb, and homeSpread only as supporting
          fields when needed.
        - If favoriteWon is true, do not call it an upset.
        - If favoriteWon is false, say it was an upset and quantify it.

        Style rules:
        - Prefer measured language: "suggests", "supports", or "the box score
          indicates" when the evidence is indirect.
        - Separate observed facts from interpretation.
        - If an interpretation is plausible but unproven from the input, say so.
        - Avoid cinematic or narrative framing. Stay analytical.
        - Prefer explicit references to evidenceFlags or minuteSummary when
          discussing workload or game-flow context.
        - Prefer explicit references to playByPlaySummary when discussing runs,
          lead changes, quarter swings, clutch state, or decisive stretches.
        - Prefer explicit references to recentForm and seriesContext when
          explaining what was different from recent baseline or prior matchups.

        Format:
        1. Outcome Summary
        2. Why The Result Happened
        3. Key Player Drivers
        4. Market / Expectation Context
        5. Compared With Recent Games
        6. DFS Takeaways
        """
    ).strip()
    user_message = f"Explain this {sport.upper()} game outcome from the structured context below:\n\n{context}"
    return _call_deepseek(system, user_message)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def generate_code(task: str, language: str = "python") -> str:
    """
    Generate code for a given task using DeepSeek.

    Args:
        task: A plain-language description of what the code should do.
        language: The programming language to use (default: python).

    Returns:
        The generated code with a brief explanation.
    """
    system = (
        f"You are an expert {language} developer. "
        "Write clean, well-commented, production-quality code. "
        "Include a short explanation of your approach before the code block."
    )
    return _call_deepseek(system, task)


@mcp.tool()
def review_code(code: str, language: str = "python", focus: str = "general") -> str:
    """
    Review code for bugs, style issues, and improvements using DeepSeek.

    Args:
        code: The source code to review.
        language: The programming language of the code (default: python).
        focus: Review focus — one of: general, security, performance, style.

    Returns:
        A structured review with findings and suggested improvements.
    """
    focus_instructions = {
        "general":     "Cover correctness, readability, edge cases, and best practices.",
        "security":    "Focus on security vulnerabilities, input validation, and unsafe patterns.",
        "performance": "Focus on algorithmic complexity, bottlenecks, and optimization opportunities.",
        "style":       "Focus on naming conventions, code structure, and language idioms.",
    }
    guidance = focus_instructions.get(focus, focus_instructions["general"])

    system = (
        f"You are a senior {language} code reviewer. "
        f"{guidance} "
        "Structure your review as: 1) Summary, 2) Issues Found, 3) Suggested Improvements. "
        "Be specific and reference line numbers or code snippets where relevant."
    )
    user_message = f"Please review this {language} code:\n\n```{language}\n{code}\n```"
    return _call_deepseek(system, user_message)


@mcp.tool()
def explain_game_outcome(
    context: str,
    sport: str = "nba",
    audience: str = "dfs analyst",
) -> str:
    """
    Explain why a game had a specific result from structured game context.

    Args:
        context: JSON or plain-text context including box score, final score,
            Vegas lines, and team/player summaries.
        sport: Sport name, typically nba or mlb.
        audience: Intended reader, such as dfs analyst or bettor.

    Returns:
        A structured explanation of the game outcome and its key drivers.
    """
    return explain_game_outcome_message(context=context, sport=sport, audience=audience)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="stdio")
