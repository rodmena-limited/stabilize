from __future__ import annotations

from pathlib import Path


def shard_suites(repo: Path, spec: str) -> list[str]:
    """The test files of shard *spec* ("i/N"), balanced by file size and stable between runs."""
    index, _, total = spec.partition("/")
    position, count = int(index), int(total)
    if not 1 <= position <= count:
        raise SystemExit(f"REDPROOF_SHARD={spec!r}: expected i/N with 1 <= i <= N")
    files = sorted(
        str(path.relative_to(repo))
        for root in ("tests", "golden_standard_tests")
        for path in (repo / root).rglob("test_*.py")
    )
    shards: list[list[str]] = [[] for _ in range(count)]
    loads = [0] * count
    for name in sorted(files, key=lambda f: (-(repo / f).stat().st_size, f)):
        lightest = loads.index(min(loads))
        shards[lightest].append(name)
        loads[lightest] += (repo / name).stat().st_size
    return sorted(shards[position - 1])
