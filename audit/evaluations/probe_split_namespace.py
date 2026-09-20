"""The namespace protection must not depend on a configuration we advise against.

`schema=` deliberately stands down when the DSN carries its own `options`
segment, so a caller following our own recommendation sets no schema at all.
A namespace assertion gated on `schema=` would therefore protect only the shape
we told people NOT to use.

Without a configured schema there is nothing to compare each table against --
but they must still all resolve to ONE namespace. A shadow copy of a single
table ahead of the real one on the search_path splits them, and the engine would
read and write a mixture.

Composition found by ci-conductor-dd94c8, who asked whether the assertion fires
at all for the configuration I had just told them to keep. It did not.

  A  CONTROL: recommended config, no shadow -> constructs, reports the namespace
  B  a shadow of ONE table -> refused, naming which table split off

NOT CLOSED, AND DELIBERATELY SO: a shadow of ALL the tables resolves
consistently, agrees with itself and passes -- measured, resolved_schema()
returns 'public' while the real tables sit in 'stabilize'. Without a configured
expectation there is nothing to compare against, so internal consistency is the
most this check can assert. resolved_schema() is the ONLY cover for that case
and is therefore the mechanism, not a nicety. A reader who sees the fixes and
no statement of this may reasonably conclude the engine has it covered.

    python audit/evaluations/probe_split_namespace.py
"""

import sys
import psycopg
from testcontainers.postgres import PostgresContainer
from stabilize.events.store.postgres.store import EventStoreSchemaError, PostgresEventStore

with PostgresContainer("postgres:16") as c:
    h, p = c.get_container_host_ip(), c.get_exposed_port(5432)
    admin = f"postgresql://{c.username}:{c.password}@{h}:{p}/{c.dbname}"
    PostgresEventStore(admin, create_tables=True)
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS stabilize")
        for t in ("events", "snapshots", "event_subscriptions"):
            conn.execute(f"ALTER TABLE {t} SET SCHEMA stabilize")
    dsn = admin + "?options=" + "-c%20search_path%3Dpublic%2Cstabilize"

    print("=== A. CONTROL: no schema= configured, no shadow -> must construct ===")
    print("    this is the configuration we RECOMMEND (options in the DSN)")
    try:
        st = PostgresEventStore(dsn)
        print(f"  CONSTRUCTED; resolved_schema = {st.resolved_schema()!r}")
    except EventStoreSchemaError as e:
        print(f"  FALSE ALARM: {str(e)[:110]}"); sys.exit(1)

    print()
    print("=== B. a shadow of ONE table, still no schema= configured ===")
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute("CREATE TABLE public.events (id int)")
    try:
        PostgresEventStore(dsn)
        print("  CONSTRUCTED — the split went undetected"); sys.exit(1)
    except EventStoreSchemaError as e:
        print(f"  RAISED: {str(e)[:150]}")
print()
print("VERDICT: PASS — the recommended configuration is protected too")
