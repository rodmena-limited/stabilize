# 42 — stabilize mg-up finds no migrations when run from a source checkout

Ticket: issuedb #42 · status open · priority low

EARS SPEC:
- When mg-up is invoked from a source checkout, the CLI shall locate the migration files and apply them.
- If migrations cannot be located, then the CLI shall name both locations it searched rather than reporting only that none were found.

MEASURED: running the RLS probe from a checkout produced

    No migrations found in package
    psycopg.errors.UndefinedTable: relation "pipeline_executions" does not exist

CAUSE: cli/migrations.py:10 get_migrations() resolves through importlib -- files("stabilize.migrations"). The .sql files live at the REPOSITORY ROOT in migrations/ and are force-included into the wheel at build time by pyproject.toml:113-115 ([tool.hatch.build.targets.wheel.force-include] "migrations" = "stabilize/migrations"). So src/stabilize/migrations/ contains only __init__.py, and an editable or source-tree invocation finds nothing.

CONSEQUENCE: mg-up works from an installed wheel and silently does nothing useful from a checkout. Anyone developing against the repo, or any probe in audit/evaluations/ that calls mg_up, hits it. audit/evaluations/probe_multitenant_rls.py works around it locally by reading whichever location is populated; that workaround should be removed once this is fixed.

SEVERITY: developer-facing rather than customer-facing -- no released artifact is affected, because the wheel carries the files. It is filed because the failure message actively misleads: 'No migrations found in package' reads as 'this version ships no migrations' rather than 'you are running from a checkout'.

SOLUTION DOMAINS: importlib.resources for the installed case; a repository-root fallback for the checkout case. The pattern already written in probe_multitenant_rls.py::_apply_migrations is the candidate to lift.

ALTERNATIVES:
- get_migrations() falls back to <repo root>/migrations when the package directory has no .sql [CHOSEN: one function, no build change, and the installed path is untouched] vs making the source tree carry the files and the build stop force-including [REJECTED: duplicates the files or moves them, and every existing checkout's paths change] vs documenting 'install before running mg-up' [REJECTED: the misleading message stays, and probes that call mg_up still break].
- The message must name BOTH searched locations either way, so a future failure says where it looked.
