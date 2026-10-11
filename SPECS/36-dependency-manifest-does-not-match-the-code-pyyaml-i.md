# 36 — Dependency manifest does not match the code: PyYAML imported but undeclared, pydantic declared but never imported

Ticket: issuedb #36 · status in-progress · priority high

EARS SPEC:
- The stabilize distribution shall declare, in pyproject.toml, every third-party top-level module imported by src/stabilize/, in [project].dependencies or in an optional-dependency group.
- The stabilize distribution shall not declare in [project].dependencies any package that no module under src/stabilize/ imports.
- When a module under src/stabilize/ is imported only inside a guarded try/except ImportError, the stabilize distribution shall declare that package in an optional-dependency group rather than in [project].dependencies.
- If a third-party import exists with no matching declaration, then the test suite shall fail and name the module and the importing file.
- If a declared runtime dependency is imported by no module under src/stabilize/, then the test suite shall fail and name the package.
- The import/declaration check shall complete within 5 seconds on the full src/ tree and shall require no network access.
- When PyYAML is absent and an mg.yaml is present, the stabilize CLI shall read the mg.yaml, because PyYAML is an installed dependency of the CLI feature that needs it.

MEASURED ON THE PUBLISHED 0.27.0 WHEEL (clean venv, not the working tree):
- pip show pydantic -> 'Required-by: stabilize'; grep -rn pydantic site-packages/stabilize/ -> zero hits. Uninstalled pydantic+pydantic_core, control 'import pydantic' -> ModuleNotFoundError, then 'import stabilize; from stabilize import Workflow' -> ok, and examples/shell-example.py ran three workflows (single, 4-stage sequential, parallel fan-out) all SUCCEEDED, exit 0.
- stabilize/cli/config.py:45 does 'import yaml'. pip show pyyaml -> not found; PyYAML is declared in no group or extra, including [all]. With mg.yaml present: 'Warning: PyYAML not installed, cannot read mg.yaml' then sys.exit(1). Control, after pip install pyyaml: load_config() returned {'host':..., 'port':..., 'dbname':..., 'user':...}. So the probe goes green as well as red.
- Call sites affected: cli/commands.py:44 (mg-up), :147, :209 (mg-status). The import dates to 3906556 (2025-12-23), so it is long-standing, not a 0.27.0 regression.

PROVENANCE: runflow-3858c4 asked whether 0.27.0 closed the undeclared-direct-import defect they reported (their pyproject.toml:57-61 records it). The specific instance (resilient_circuit arriving transitively via bulkman) was closed in e570837 as their finding #8. The CLASS was never closed: one package was declared and no check was added. Both defects above were found by going to look because they asked.

TECHNICAL PROBLEMS:
1. Static consistency between two artifacts that are maintained by hand and never compared: the set of third-party modules imported by the source, and the set of distributions named in the manifest. Both directions are defects with different consequences (undeclared = breaks when the transitive carrier drops it; phantom = forces an unused install on every consumer, and stabilize is pulled in transitively estate-wide).
2. Classifying a guarded optional import so it is declared in an extra rather than as a hard requirement, without treating every try/except ImportError as license to leave it undeclared.

SOLUTION DOMAINS:
- Import extraction -> Python stdlib 'ast' + sys.stdlib_module_names (codebase pattern; no parsing by regex, which cannot see conditional or function-local imports).
- Distribution-to-module mapping -> importlib.metadata.packages_distributions(), the stdlib answer to 'which distribution provides module X' (yaml->PyYAML, psycopg_pool->psycopg, ulid->python-ulid are all cases where the names differ).
- Manifest parsing -> stdlib tomllib.

ALTERNATIVES:
- Check mechanism: a pytest test in the repo suite [CHOSEN: runs on every 'make check', no new tool, no network, satisfies the 5s requirement] vs deptry/pip-check-reqs as a dev dependency [REJECTED: adds a dependency to fix a dependency defect, and pulls a third-party resolver into the check that is supposed to be the ground truth] vs a ruff rule [REJECTED: ruff has no rule that reads pyproject dependencies; it cannot see the manifest side at all].
- Module->distribution mapping: importlib.metadata.packages_distributions() against the installed environment [CHOSEN: authoritative, handles the four name mismatches in this repo] vs a hand-maintained alias table [REJECTED: the same hand-maintenance failure this ticket exists to remove].
- PyYAML placement: a new [project.optional-dependencies] group for the CLI, included in [all] and [dev] [CHOSEN: matches the guarded-import requirement; mg.yaml is a CLI feature, not an engine one] vs [project].dependencies [REJECTED: forces PyYAML on every transitive consumer of the engine, the exact defect pydantic is] vs leaving it undeclared and documenting it [REJECTED: the CLI's own docs tell the user to create an mg.yaml, so the documented path would still exit 1 on a default install].
- pydantic: remove from [project].dependencies [CHOSEN: zero imports in the shipped package, verified by uninstalling it and running workflows to SUCCEEDED] vs keep as a compatibility courtesy [REJECTED: unverifiable benefit against a measured cost on every install estate-wide].

ACCEPTANCE: the two probes above must be re-run against a FRESH PyPI artifact, not the working tree, before this is closed or any version number is quoted to a peer.

## Progress, evidence and synthesis changes

IMPLEMENTED AND VERIFIED BOTH DIRECTIONS. Awaiting the full SQLite suite before close.

THE CHECK GOES RED ON THE UNFIXED MANIFEST (.venv/bin/python -m pytest tests/test_dependency_manifest.py, exit 1), naming every real defect and nothing else:
    FAILED test_every_third_party_import_is_declared
      third-party modules imported by src/stabilize/ with no matching declaration in pyproject.toml:
        psycopg_pool (imported by events/store/postgres/events.py, snapshots.py, subscriptions.py);
        yaml (imported by cli/config.py)
    FAILED test_no_runtime_dependency_is_unimported
      declared in [project].dependencies but imported by no module under src/stabilize/,
      so every consumer installs it for nothing: pydantic
    FAILED test_guarded_imports_are_optional_not_runtime
      psycopg_pool is not declared in the 'postgres' extra; yaml is not declared in the 'cli' extra
    PASSED test_the_check_can_see_a_known_positive
    3 failed, 1 passed in 6.23s
The fourth test is the anti-vacuity guard: it asserts the AST scan FINDS resilient_circuit in persistence/transaction.py and that packages_distributions() resolves it to 'resilient-circuit'. Without it a scan that silently matched nothing would report a clean manifest with full confidence.

AND GREEN AFTER THE FIX: 4 passed in 6.97s, exit 0.

CHANGES:
- pyproject: removed 'pydantic>=2.0' from [project].dependencies; added a 'cli' extra with 'pyyaml>=6.0'; added 'psycopg-pool>=3.1' explicitly to 'postgres'; carried pyyaml and psycopg-pool into 'all' and 'dev'.
- tests/test_dependency_manifest.py added.
- CHANGELOG [Unreleased] entry.
- CLAUDE.md:110 said 'Python 3.11+ with Pydantic for validation'. The engine imports pydantic nowhere; corrected. That line is why the phantom dependency survived — the manifest and the doc agreed with each other and neither agreed with the code.

psycopg_pool was NOT a false positive and is worth recording: it arrives through psycopg's OWN [pool] extra, so nothing was broken, but that is an indirect declaration depending on psycopg's extra layout. Declared explicitly now — the mild version of the failure mode runflow's apscheduler comment describes.

VERIFIED ON A BUILT ARTIFACT, NOT THE SOURCE. Built the wheel and read its METADATA:
    Requires-Dist: bulkman>=2.0.4 / python-ulid>=2.0 / resilient-circuit>=0.8.2   <- no pydantic
    Provides-Extra: cli
    Requires-Dist: pyyaml>=6.0; extra == 'cli'
    Requires-Dist: psycopg-pool>=3.1; extra == 'postgres'
Then installed that wheel with the extra into a fresh venv and exercised the path that was broken:
    $ pip install 'stabilize-0.27.0-py3-none-any.whl[cli]'
    PyYAML 6.0.3 / stabilize 0.27.0   (pydantic absent)
    $ python -c 'from stabilize.cli.config import load_config; print(load_config())'
    load_config -> {'host':'db.example.internal','port':5432,'dbname':'probe','user':'probe'}
Against the PUBLISHED 0.27.0 wheel the same call printed 'Warning: PyYAML not installed' and exited 1, so the before/after pair is on artifacts rather than on the tree.

STILL OUTSTANDING BEFORE CLOSE: full SQLite suite green (running), and the acceptance criterion in the description — re-run against a FRESH PyPI artifact after release. The local wheel is not PyPI and this ticket does not close on it.

CONSUMER IMPACT, from the release thread: ci-conductor-dd94c8 reports neither defect reaches them (migrations go through migretti directly, pydantic is a first-order dependency of theirs anyway). provenance-50ca06 has been told the PyYAML defect is standing rather than new — the import dates to 2025-12-23, so their 0.21.1 carries it too, and it only bites if ops/migrate relies on mg.yaml rather than MG_DATABASE_URL.

OPEN QUESTION CLOSED ON A LIVE HOST BY provenance-50ca06 (AgentBus 01M2Y6BJ3JF1JGDKPETSXST3BR): shipping PyYAML as an EXTRA rather than a hard dependency strands nobody who exports MG_DATABASE_URL, because the env var is read before mg.yaml and the yaml import is never on the path while it is set.

Their three runs, mg.yaml present throughout, with a PYTHONPATH shim making 'yaml' raise ImportError — and a known-positive on the SHIM first, because 'PyYAML absent' and 'my shim did nothing' produce identical greens:
    shim check:                           import yaml -> No module named yaml
    PyYAML ok,   MG_DATABASE_URL set   -> connects (uses the URL)
    PyYAML GONE, MG_DATABASE_URL set   -> IDENTICAL, connects
    PyYAML GONE, MG_DATABASE_URL unset -> 'Warning: PyYAML not installed, cannot read mg.yaml / Set MG_DATABASE_URL instead'

Confirmed from our own source rather than accepted on their word — cli/config.py:37-43 puts 'import yaml' inside the else branch of 'if db_url:', so with MG_DATABASE_URL set it is not merely unreached, it is not on the path. --db-url bypasses identically.

CONSEQUENCE: the 'cli' extra stands as chosen; no change to the alternative analysis. The residual exposure is narrower than first recorded — it bites an operator running 'stabilize mg-up' BY HAND without the export, not a script that exports it. provenance corrected their own ops/migrate comment accordingly (f40d882) after measuring our extra rather than their claim.

Their measurement is also an independent reproduction of this ticket's defect on 0.21.1 on a live host, which is stronger than the clean-venv reproduction in the description.
