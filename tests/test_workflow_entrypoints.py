"""Python entrypoints that workflows run must convert failure into an exit code.

Two patterns silently turn a failing `python -m X` step green:

1. `main()` returns an int (a status) but the `__main__` block calls it bare,
   so the return value is dropped and the process exits 0.
2. The `__main__` block catches an exception and logs it instead of re-raising
   or exiting non-zero (`ingest.soccer_schedule` did this for its prediction
   writer until 2026-09-29).

The check reads every module a workflow references (`python -m X` or the inline
`from X import main`), parses it, and inspects only the entrypoint block and
`main()`'s return statements -- function bodies are out of scope here.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"
MODULE_REFS = re.compile(r"python -m ([\w.]+)|from ([\w.]+) import main;")


def workflow_modules(root: Path = WORKFLOWS) -> set[str]:
    modules = set()
    for wf in sorted(root.glob("*.yml")):
        for match in MODULE_REFS.finditer(wf.read_text(encoding="utf-8")):
            modules.add(match.group(1) or match.group(2))
    return modules


def _main_block(tree: ast.Module) -> ast.If | None:
    for node in tree.body:
        if isinstance(node, ast.If):
            test = node.test
            if isinstance(test, ast.Compare) and isinstance(test.left, ast.Name) and test.left.id == "__name__":
                return node
    return None


def _returns_value(func: ast.FunctionDef) -> bool:
    """True when main() can return a status: `-> int`, or a top-level `return <expr>`."""
    if isinstance(func.returns, ast.Name) and func.returns.id == "int":
        return True
    stack: list[ast.AST] = list(func.body)
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
            continue  # nested definitions return to their own callers
        if isinstance(node, ast.Return) and node.value is not None:
            if not (isinstance(node.value, ast.Constant) and node.value.value is None):
                return True
        stack.extend(ast.iter_child_nodes(node))
    return False


def _bare_main_call(block: ast.If) -> bool:
    for stmt in block.body:
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
            func = stmt.value.func
            if isinstance(func, ast.Name) and func.id == "main":
                return True
    return False


def _handler_exits(handler: ast.ExceptHandler) -> bool:
    """A handler is fine when it re-raises or calls something that exits.

    `raise`, `sys.exit(...)`, `SystemExit(...)`, and a delegated helper such as
    `exit_on_account_error(exc, ...)` (which raises SystemExit itself) all count.
    """
    for node in ast.walk(handler):
        if isinstance(node, ast.Raise):
            return True
        if isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else ""
            if "exit" in name.lower():
                return True
    return False


def _swallowing_handlers(block: ast.If) -> list[str]:
    found = []
    for node in ast.walk(block):
        if not isinstance(node, ast.Try):
            continue
        for handler in node.handlers:
            if _handler_exits(handler):
                continue
            found.append(ast.unparse(handler).splitlines()[0][:80])
    return found


def entrypoint_defects(root: Path = ROOT, modules: set[str] | None = None) -> list[str]:
    bad = []
    for module in sorted(modules if modules is not None else workflow_modules(root / ".github" / "workflows")):
        path = root / Path(*module.split("."))
        path = path.with_suffix(".py")
        if not path.exists():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        block = _main_block(tree)
        if block is None:
            continue
        mains = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main"]
        if mains and _returns_value(mains[0]) and _bare_main_call(block):
            bad.append(f"{module}: main() returns a status but __main__ calls it bare; use raise SystemExit(main())")
        for handler in _swallowing_handlers(block):
            bad.append(f"{module}: __main__ swallows an exception without re-raising or exiting: {handler}")
    return bad


def test_workflow_entrypoints_turn_failure_into_an_exit_code() -> None:
    assert entrypoint_defects() == []


def test_the_check_catches_the_bugs_and_allows_the_fixes(tmp_path: Path) -> None:
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "dropped.py").write_text(
        "def main() -> int:\n    return 1\n\nif __name__ == '__main__':\n    main()\n", encoding="utf-8")
    (pkg / "swallowed.py").write_text(
        "def main():\n    pass\n\nif __name__ == '__main__':\n    try:\n        main()\n"
        "    except Exception as exc:\n        print(exc)\n", encoding="utf-8")
    (pkg / "nested.py").write_text(
        "def main():\n    def helper():\n        return 1\n    helper()\n\nif __name__ == '__main__':\n    main()\n",
        encoding="utf-8")
    (pkg / "ok.py").write_text(
        "def main() -> int:\n    return 0\n\nif __name__ == '__main__':\n    try:\n        raise SystemExit(main())\n"
        "    except KeyboardInterrupt:\n        raise\n", encoding="utf-8")
    (pkg / "delegated.py").write_text(
        "def main():\n    pass\n\nif __name__ == '__main__':\n    try:\n        main()\n"
        "    except ValueError as exc:\n        exit_on_account_error(exc, job='x')\n", encoding="utf-8")
    modules = {"pkg.dropped", "pkg.swallowed", "pkg.nested", "pkg.ok", "pkg.delegated"}
    assert entrypoint_defects(tmp_path, modules) == [
        "pkg.dropped: main() returns a status but __main__ calls it bare; use raise SystemExit(main())",
        "pkg.swallowed: __main__ swallows an exception without re-raising or exiting: except Exception as exc:",
    ]
