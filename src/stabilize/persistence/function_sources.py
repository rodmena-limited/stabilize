"""The bodies of the PostgreSQL functions the engine relies on, as the migrations and mg-tenancy install them."""

from __future__ import annotations

from typing import Any

FENCE_NAME = "stabilize_tenant_fence"
OWNER_NAME = "stabilize_is_table_owner"
REQUIRED_SEARCH_PATH = "search_path=pg_catalog, pg_temp"

OWNER_BODY = " SELECT pg_has_role(current_user, c.relowner, 'MEMBER') FROM pg_catalog.pg_class c WHERE c.oid = rel "


def fence_body() -> str:
    from stabilize.cli.tenancy_cli import FENCE_FUNCTION

    return FENCE_FUNCTION.split("$fence$")[1]


def canonical(body: str) -> str:
    return "\n".join(line.rstrip() for line in body.replace("\r\n", "\n").strip().splitlines())


def function_problems(found: dict[str, dict[str, Any]], tenancy: bool) -> list[str]:
    """What is wrong with the installed functions: *found* maps name -> {body, security_definer, config}."""
    problems: list[str] = []
    expected = {OWNER_NAME: (OWNER_BODY, "the tenant_key migration (stabilize mg-up)")}
    if tenancy:
        expected[FENCE_NAME] = (fence_body(), "stabilize mg-tenancy disable then enable on an empty schema")
    for name, (body, remedy) in expected.items():
        installed = found.get(name)
        if installed is None:
            if tenancy:
                problems.append(f"function {name} is missing; recreate it from {remedy}")
            continue
        if canonical(str(installed["body"])) != canonical(body):
            problems.append(f"function {name} has a body stabilize did not install; recreate it from {remedy}")
        if installed["security_definer"]:
            problems.append(f"function {name} is SECURITY DEFINER; stabilize installs it SECURITY INVOKER ({remedy})")
        config = installed["config"] or []
        if not any(str(item).replace(" ", "") == REQUIRED_SEARCH_PATH.replace(" ", "") for item in config):
            problems.append(f"function {name} lacks SET {REQUIRED_SEARCH_PATH}; recreate it from {remedy}")
    return problems
