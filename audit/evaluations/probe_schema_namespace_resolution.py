"""Schema verification must assert the NAMESPACE, not the rendered name.

`to_regclass(name)::text` omits the schema exactly when the schema is on the
search_path -- the healthy case. So asserting it equals 'stabilize.events' fails
when everything is correct, and the natural repair (compare against the
unqualified 'events' it actually returns) then accepts a shadow copy in any
other schema, because the rendering is unqualified there too. A loud false alarm
becomes a silent false approval by the most obvious debugging step available.

Found by ci-conductor-dd94c8, who proposed the qualified-name assertion, tested
it against their live database, and reported that their own fix would have been
repaired into the defect it was meant to close (stabilize #43).

Both directions, because a check that refuses everything is as broken as one
that accepts everything:

  A  a shadow table in another schema is REFUSED when schema= is configured
  B  CONTROL: the same DSN with no shadow CONSTRUCTS, with no false alarm

    python audit/evaluations/probe_schema_namespace_resolution.py
"""

import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).parent))
from _backends import fresh_database  # noqa: E402
from stabilize.events.store.postgres.store import EventStoreSchemaError, PostgresEventStore

with fresh_database("shadow") as admin:

    # real tables in `stabilize`, plus a SHADOW `events` in public
    PostgresEventStore(admin, create_tables=True)
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS stabilize")
        for t in ("events", "snapshots", "event_subscriptions"):
            conn.execute(f"ALTER TABLE {t} SET SCHEMA stabilize")
        conn.execute("CREATE TABLE public.events (LIKE stabilize.events INCLUDING ALL)")

    # search_path reaches public FIRST, so the bare name resolves to the shadow
    dsn = admin + "?options=" + "-c%20search_path%3Dpublic%2Cstabilize"
    print("=== configured schema=stabilize, but public.events shadows it ===")
    try:
        PostgresEventStore(dsn, schema="stabilize")
        print("  CONSTRUCTED — the shadow satisfied the check")
        sys.exit(1)
    except EventStoreSchemaError as e:
        print(f"  RAISED: {str(e)[:130]}")
        if "resolves to the wrong namespace" not in str(e):
            print("  [FAIL] refused, but not by the namespace check (the shadow is a full column copy)")
            sys.exit(1)
        print("  [PASS] refused by the namespace check")

    print()
    print("=== CONTROL: same DSN, no shadow -> must construct ===")
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute("DROP TABLE public.events")
    try:
        PostgresEventStore(dsn, schema="stabilize")
        print("  CONSTRUCTED correctly once the shadow is gone")
    except EventStoreSchemaError as e:
        print(f"  FALSE ALARM — refused a healthy schema: {str(e)[:110]}")
        sys.exit(1)
print()
print("VERDICT: PASS — resolution is asserted by namespace, not by rendered name")
