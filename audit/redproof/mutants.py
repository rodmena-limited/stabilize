from __future__ import annotations

import ast
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

FLIP_CMP: dict[type[ast.cmpop], type[ast.cmpop]] = {
    ast.Eq: ast.NotEq,
    ast.NotEq: ast.Eq,
    ast.Lt: ast.GtE,
    ast.GtE: ast.Lt,
    ast.Gt: ast.LtE,
    ast.LtE: ast.Gt,
    ast.Is: ast.IsNot,
    ast.IsNot: ast.Is,
    ast.In: ast.NotIn,
    ast.NotIn: ast.In,
}
FLIP_BIN: dict[type[ast.operator], type[ast.operator]] = {
    ast.Add: ast.Sub,
    ast.Sub: ast.Add,
    ast.Mult: ast.FloorDiv,
    ast.FloorDiv: ast.Mult,
    ast.Div: ast.Mult,
}


@dataclass(frozen=True)
class Mutant:
    path: str
    line: int
    col: int
    end_line: int
    end_col: int
    operator: str
    replacement: str
    qualname: str = ""
    digest: str = ""
    ordinal: int = 0

    @property
    def key(self) -> str:
        return f"{self.path}:{self.line}:{self.col}:{self.operator}"

    @property
    def stable_key(self) -> str:
        """A key that survives line shifts: the enclosing definition, the operator, the node's shape and its rank."""
        return f"{self.path}:{self.qualname}:{self.operator}:{self.digest}:{self.ordinal}"


TENANT_PREDICATE = re.compile(r"(?:\b\w+\.)?stabilize_tenant = (%\(tenant\)s|\?|:tenant)")
TAUTOLOGY = {"%(tenant)s": "%(tenant)s::text IS NOT NULL", "?": "? IS NOT NULL", ":tenant": ":tenant IS NOT NULL"}


def _drop_nth_predicate(text: str, n: int) -> str:
    count = -1

    def swap(match: re.Match[str]) -> str:
        nonlocal count
        count += 1
        return TAUTOLOGY[match.group(1)] if count == n else match.group(0)

    return TENANT_PREDICATE.sub(swap, text)


def _tenant_predicate_mutants(node: ast.AST) -> list[tuple[str, ast.AST, str]]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        source_text = node.value
        render = repr
    elif isinstance(node, ast.JoinedStr):
        source_text = ast.unparse(node)
        render = str
    else:
        return []
    hits = len(TENANT_PREDICATE.findall(source_text))
    return [("tenant-pred-drop", node, render(_drop_nth_predicate(source_text, i))) for i in range(hits)]


def _qualnames(tree: ast.Module) -> dict[int, str]:
    names: dict[int, str] = {}

    def visit(node: ast.AST, stack: list[str]) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                inner = [*stack, child.name]
                names[id(child)] = ".".join(inner)
                visit(child, inner)
            else:
                names[id(child)] = ".".join(stack)
                visit(child, stack)

    visit(tree, [])
    return names


def _segment_lines(source_lines: list[str], node: ast.AST) -> tuple[int, int, int, int]:
    return node.lineno, node.col_offset, node.end_lineno or node.lineno, node.end_col_offset or 0


def _expr(node: ast.expr) -> str:
    return "(" + ast.unparse(node) + ")"


def _inert_ranges(tree: ast.Module) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING":
            ranges.append((node.lineno, node.end_lineno or node.lineno))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                if isinstance(body[0].value.value, str):
                    ranges.append((body[0].lineno, body[0].end_lineno or body[0].lineno))
    return ranges


def _annotation_ids(tree: ast.Module) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        anns: list[ast.AST | None] = []
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            anns.append(node.returns)
            args = node.args
            for a in args.posonlyargs + args.args + args.kwonlyargs:
                anns.append(a.annotation)
            anns.extend(
                [
                    args.vararg.annotation if args.vararg else None,
                    args.kwarg.annotation if args.kwarg else None,
                ]
            )
        elif isinstance(node, ast.AnnAssign):
            anns.append(node.annotation)
        for ann in anns:
            if ann is not None:
                ids.update(id(n) for n in ast.walk(ann))
    return ids


def _quiet_ids(tree: ast.Module) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr) or _is_logging_call(node):
            ids.update(id(n) for n in ast.walk(node) if n is not node or isinstance(node, ast.JoinedStr))
        if isinstance(node, ast.Raise) and node.exc is not None:
            ids.update(id(n) for n in ast.walk(node.exc) if isinstance(n, ast.Constant))
    return ids


def _is_logging_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return False
    target = node.func.value
    return isinstance(target, ast.Name) and target.id in {"logger", "log", "logging", "_logger"}


def _candidates(node: ast.AST) -> list[tuple[str, ast.AST, str]]:
    out: list[tuple[str, ast.AST, str]] = _tenant_predicate_mutants(node)
    if isinstance(node, ast.Compare) and len(node.ops) == 1 and type(node.ops[0]) in FLIP_CMP:
        flipped = ast.Compare(
            left=node.left, ops=[FLIP_CMP[type(node.ops[0])]()], comparators=node.comparators
        )
        out.append(("cmp-flip", node, _expr(flipped)))
    elif isinstance(node, ast.BoolOp):
        other = ast.Or() if isinstance(node.op, ast.And) else ast.And()
        out.append(("bool-flip", node, _expr(ast.BoolOp(op=other, values=node.values))))
    elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        out.append(("not-drop", node, _expr(node.operand)))
    elif isinstance(node, ast.BinOp) and type(node.op) in FLIP_BIN:
        out.append(
            (
                "arith-flip",
                node,
                _expr(ast.BinOp(left=node.left, op=FLIP_BIN[type(node.op)](), right=node.right)),
            )
        )
    elif isinstance(node, ast.Constant) and isinstance(node.value, bool):
        out.append(("bool-const", node, repr(not node.value)))
    elif isinstance(node, ast.Constant) and type(node.value) is int:
        out.append(("int-const", node, repr(node.value + 1)))
    elif isinstance(node, ast.Constant) and type(node.value) is str and len(node.value) <= 200:
        out.append(("str-const", node, repr(node.value + "_mut" if node.value else "mut")))
    elif isinstance(node, ast.ClassDef) and node.bases:
        for base in node.bases:
            others = [b for b in node.bases if b is not base]
            out.append(("base-drop", base, ast.unparse(others[0]) if others else "object"))
    elif isinstance(node, (ast.If, ast.While)):
        out.append(("cond-negate", node.test, "(not " + _expr(node.test) + ")"))
    elif isinstance(node, ast.Return) and node.value is not None:
        if not (isinstance(node.value, ast.Constant) and node.value.value is None):
            out.append(("return-none", node, "return None"))
    elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and not _is_logging_call(node.value):
        out.append(("call-drop", node, "pass"))
    elif isinstance(node, (ast.Raise, ast.AugAssign)):
        out.append(("stmt-drop", node, "pass"))
    elif (
        isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Attribute)
    ):
        out.append(("attr-assign-drop", node, "pass"))
    return out


def mutants_for_file(path: Path, rel: str) -> list[Mutant]:
    source = path.read_text()
    tree = ast.parse(source)
    inert = _inert_ranges(tree)
    annotation = _annotation_ids(tree)
    quiet = _quiet_ids(tree)
    lines = source.splitlines(keepends=True)
    qualnames = _qualnames(tree)
    joined_parts = {
        id(part)
        for joined in ast.walk(tree)
        if isinstance(joined, ast.JoinedStr)
        for part in joined.values
        if isinstance(part, ast.Constant)
    }
    seen: dict[tuple[str, str, str], int] = {}
    result: list[Mutant] = []
    for node in ast.walk(tree):
        if id(node) in annotation or not hasattr(node, "lineno"):
            continue
        if any(a <= node.lineno <= b for a, b in inert):
            continue
        for op, target, replacement in _candidates(node):
            if op == "str-const" and id(node) in quiet:
                continue
            if op == "tenant-pred-drop" and id(node) in joined_parts:
                continue
            line, col, end_line, end_col = _segment_lines(lines, target)
            if end_line != line and replacement != "pass":
                replacement = replacement.replace("\n", " ")
            qualname = qualnames.get(id(node), "")
            shape = ast.dump(target, include_attributes=False) + "|" + op + "|" + replacement
            digest = hashlib.sha1(shape.encode()).hexdigest()[:10]
            ordinal = seen.get((qualname, op, digest), 0)
            seen[(qualname, op, digest)] = ordinal + 1
            result.append(Mutant(rel, line, col, end_line, end_col, op, replacement, qualname, digest, ordinal))
    return result


def apply(source: str, m: Mutant) -> str:
    lines = source.splitlines(keepends=True)
    head = "".join(lines[: m.line - 1]) + lines[m.line - 1][: m.col]
    tail = lines[m.end_line - 1][m.end_col :] + "".join(lines[m.end_line :])
    padding = "\n" * (m.end_line - m.line)
    mutated = head + m.replacement + padding + tail
    ast.parse(mutated)
    return mutated


def all_mutants(src_root: Path) -> list[Mutant]:
    found: list[Mutant] = []
    for path in sorted(src_root.rglob("*.py")):
        rel = path.relative_to(src_root.parent.parent).as_posix()
        try:
            found.extend(mutants_for_file(path, rel))
        except SyntaxError:
            continue
    return found
