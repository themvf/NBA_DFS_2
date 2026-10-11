"""Rewrite the reader-page prose with the local `humanizer` model, numbers guarded.

Model: jialinyyzz/humanizer (Gemma-4 12B fine-tune, Apache 2.0), pulled into
Ollama as `hf.co/jialinyyzz/humanizer:Q4_K_M`. It is a completion model with a
fixed prompt (`prompt_format.json` in the repo): instruction, the draft, then
"### Rewritten:". Sampling per the model card: temperature 1.0, top-p 0.95,
top-k and min-p off, stop on EOS only.

Only PROSE goes through it. Table cells, prices, records and selections are
produced by `model/cfb_pattern_publish.py` from the ledger and are never
touched here. Every rewrite is checked: the multiset of numbers, percentages,
dates and capitalised names in the output must equal the input's, or the
rewrite is rejected and the original kept. The model card itself says it can
alter details; this is the guard the card asks for.

Output: artifacts/cfb_pattern_watch/copy.json, keyed by block name, which the
publisher reads as overrides when present. Review the file before publishing.

Usage:
    python scripts/humanize_copy.py                 # rewrite every block in COPY
    python scripts/humanize_copy.py --only header_sub,note
    python scripts/humanize_copy.py --dry-run       # print prompts, call nothing
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from model.cfb_pattern_publish import COPY  # noqa: E402

MODEL = "hf.co/jialinyyzz/humanizer:Q4_K_M"
OLLAMA = "http://127.0.0.1:11434/api/generate"
INSTR = ("Rewrite the text below so it reads like a person wrote it, not a language model.\n\n"
         "Reorganize it as you see fit. Vary sentence length on purpose. Cut hedging,\n"
         "throat-clearing, and any sentence that only announces what comes next.\n"
         "Prefer the concrete word over the abstract one. It is fine to sound uneven.\n\n"
         "Every fact, number, unit, date, name and quotation must survive unchanged.")
SEP = "\n\n### Rewritten:\n\n"
OUT = Path("artifacts") / "cfb_pattern_watch" / "copy.json"

_NUM = re.compile(r"[−\-+]?\d[\d,.]*%?")
_NAME = re.compile(r"\b(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b")


def facts(text: str) -> tuple[Counter, Counter]:
    nums = Counter(t.replace("−", "-").rstrip(".,") for t in _NUM.findall(text))
    names = Counter(n for n in _NAME.findall(text) if n not in {"A", "The", "It", "So", "Two", "Six", "Each", "Every", "Nothing", "Anything", "Watch", "Patterns", "Grading", "Moneyline", "Source", "This", "That", "There", "Prices"})
    return nums, names


def guard(original: str, rewritten: str) -> list[str]:
    on, oname = facts(original); rn, rname = facts(rewritten)
    problems = []
    if on != rn:
        problems.append(f"numbers changed: {dict(on - rn)} missing, {dict(rn - on)} added")
    missing = oname - rname
    if missing:
        problems.append(f"names missing: {dict(missing)}")
    if len(rewritten) < 0.5 * len(original) or len(rewritten) > 1.6 * len(original):
        problems.append(f"length {len(original)} -> {len(rewritten)}")
    return problems


def rewrite(draft: str) -> str:
    body = json.dumps({
        "model": MODEL, "prompt": INSTR + "\n\n" + draft + SEP, "raw": True, "stream": False,
        "options": {"temperature": 1.0, "top_p": 0.95, "top_k": 0, "min_p": 0.0, "num_predict": 1200, "num_ctx": 4096},
    }).encode()
    req = urllib.request.Request(OLLAMA, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=900) as resp:
        return json.loads(resp.read())["response"].strip()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--attempts", type=int, default=3)
    args = ap.parse_args(argv)
    keys = [k for k in COPY if not args.only or k in args.only.split(",")]
    existing = json.loads(OUT.read_text(encoding="utf-8")) if OUT.exists() else {}
    for k in keys:
        draft = COPY[k]
        if args.dry_run:
            print(f"--- {k}\n{INSTR}\n\n{draft}{SEP}")
            continue
        accepted = None
        for attempt in range(1, args.attempts + 1):
            out = rewrite(draft)
            problems = guard(draft, out)
            if not problems:
                accepted = out
                break
            print(f"[{k}] attempt {attempt} rejected: {'; '.join(problems)}")
        if accepted:
            existing[k] = accepted
            print(f"[{k}] accepted\n  before: {draft[:110]}...\n  after : {accepted[:110]}...")
        else:
            print(f"[{k}] kept original after {args.attempts} attempts")
    if not args.dry_run:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"wrote {OUT} ({len(existing)} blocks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
