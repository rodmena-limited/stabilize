"""
Safe expression evaluator for workflow control-flow conditions.

Used by OR-split (WCP-6) to evaluate per-downstream conditions,
and by other patterns that need runtime condition evaluation.

Only supports a safe subset of operations - no arbitrary code execution.
Expressions can reference context values using dot notation or bracket notation.
"""

from __future__ import annotations

import ast
import operator
from typing import Any

# Safe operators for expression evaluation
_SAFE_OPERATORS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Is: operator.is_,
    ast.IsNot: operator.is_not,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
}

_SAFE_BOOL_OPS = {
    ast.And: all,
    ast.Or: any,
}

_SAFE_UNARY_OPS = {
    ast.Not: operator.not_,
    ast.USub: operator.neg,
}


class ExpressionError(Exception):
    """Raised when an expression cannot be evaluated."""


def evaluate_expression(expression: str, context: dict[str, Any]) -> Any:
    """Evaluate a simple expression against a context dictionary.

    Supports:
    - Boolean literals: true, false, True, False
    - String/number/None literals
    - Context lookups: context_key, context["key"]
    - Comparisons: ==, !=, <, <=, >, >=, in, not in
    - Boolean operators: and, or, not
    - Attribute access for nested dicts: a.b.c

    Does NOT support:
    - Function calls
    - Imports
    - Assignments
    - Arbitrary code

    Args:
        expression: The expression string to evaluate
        context: Dictionary of values available to the expression

    Returns:
        The result of evaluating the expression

    Raises:
        ExpressionError: If the expression is invalid or uses unsupported features
    """
    if not expression or not expression.strip():
        raise ExpressionError("Empty expression")

    expr = expression.strip()

    # Fast path for simple boolean literals
    if expr.lower() in ("true", "1"):
        return True
    if expr.lower() in ("false", "0"):
        return False

    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise ExpressionError(f"Invalid expression syntax: {e}") from e

    return _eval_node(tree.body, context)


def _eval_node(node: ast.AST, context: dict[str, Any]) -> Any:
    """Recursively evaluate an AST node."""
    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Name):
        if node.id in ("True", "true"):
            return True
        if node.id in ("False", "false"):
            return False
        if node.id in ("None", "none", "null"):
            return None
        if node.id in context:
            return context[node.id]
        return None  # Missing context keys evaluate to None

    if isinstance(node, ast.Attribute):
        value = _eval_node(node.value, context)
        if isinstance(value, dict):
            return value.get(node.attr)
        return None

    if isinstance(node, ast.Subscript):
        value = _eval_node(node.value, context)
        if isinstance(node.slice, ast.Constant):
            key = node.slice.value
        else:
            key = _eval_node(node.slice, context)
        if isinstance(value, dict):
            return value.get(key)
        if isinstance(value, (list, tuple)) and isinstance(key, int):
            try:
                return value[key]
            except IndexError:
                return None
        return None

    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, context)
        for op, comparator in zip(node.ops, node.comparators):
            right = _eval_node(comparator, context)
            op_func = _SAFE_OPERATORS.get(type(op))
            if op_func is None:
                raise ExpressionError(f"Unsupported comparison operator: {type(op).__name__}")
            if not op_func(left, right):
                return False
            left = right
        return True

    if isinstance(node, ast.BoolOp):
        values = [_eval_node(v, context) for v in node.values]
        func = _SAFE_BOOL_OPS.get(type(node.op))
        if func is None:
            raise ExpressionError(f"Unsupported boolean operator: {type(node.op).__name__}")
        return func(values)

    if isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, context)
        unary_func = _SAFE_UNARY_OPS.get(type(node.op))
        if unary_func is None:
            raise ExpressionError(f"Unsupported unary operator: {type(node.op).__name__}")
        return unary_func(operand)

    if isinstance(node, ast.IfExp):
        test = _eval_node(node.test, context)
        if test:
            return _eval_node(node.body, context)
        return _eval_node(node.orelse, context)

    if isinstance(node, ast.List):
        return [_eval_node(elt, context) for elt in node.elts]

    if isinstance(node, ast.Tuple):
        return tuple(_eval_node(elt, context) for elt in node.elts)

    raise ExpressionError(f"Unsupported expression node: {type(node).__name__}")
