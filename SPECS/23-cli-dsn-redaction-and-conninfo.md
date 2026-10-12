EARS SPEC:
- When parse_db_url() cannot parse a database URL, the CLI shall print the URL with the userinfo password replaced by *** and shall never emit the password in clear text.
- When a database URL carries no password, the CLI shall omit the password connection parameter entirely so libpq falls back to PGPASSWORD/.pgpass, rather than passing an explicit empty password.
- The CLI shall construct libpq connection parameters using a quoting-correct mechanism, so that no value containing whitespace, quotes or '=' can alter, inject or drop any other connection parameter.
- Where the monitor command prints or reports a database URL, it shall print the redacted form.

ROOT CAUSE (verified locally, not taken on report):
Both halves of the reported 'defect 2' are ONE bug. commands.py mg_up:49 and mg_status:221 build a libpq conninfo by f-string:
    f"... user={user} password={config.get('password','')} dbname={config['dbname']}"
parse_db_url() substitutes '' for a missing password (config.py:80), producing the literal text 'password= dbname=provenance'. libpq skips whitespace after '=' before reading a value, so it consumes 'dbname=provenance' AS THE PASSWORD.

Proven with psycopg 3.3.4 conninfo_to_dict, with a known-positive control:
  control 'password=secret dbname=provenance' -> {'password':'secret','dbname':'provenance'}
  bug     'password= dbname=provenance'       -> {'password':'dbname=provenance'}  dbname ABSENT

Consequences, all three reported symptoms from one cause:
 1. dbname is absent -> libpq defaults the database to the USERNAME -> 'database "zzz_probe_user"'. This is the user-as-database defect. It explains why an exported PGDATABASE masks it (libpq fills the missing dbname) and why a permissive pg_hba masks it (auth fails before the database is checked).
 2. password is explicitly set (to garbage) -> PGPASSWORD is overridden. This is the PGPASSWORD-override defect.
 3. The same f-string is a CONNINFO INJECTION vector: a password containing "dbname=evil" redirects the connection to another database. Verified that make_conninfo quotes this correctly and the f-string does not.

DEFECT 1 (separate, same file): config.py:73 prints the raw DSN including the password on a parse failure. commands.py:188 prints the full db_url and commands.py:192 prints db_url[:50], which also contains the password.

ALTERNATIVES CONSIDERED:
 (a) quote the f-string values by hand -- rejected, re-implements libpq quoting.
 (b) psycopg.conninfo.make_conninfo() -- correct quoting, but still builds a string.
 (c) pass a parameter DICT to psycopg.connect(**params) and omit absent keys -- chosen. No string is built, so no quoting or injection surface exists at all, and omission is expressible (which '' is not).

TEST REQUIREMENT: tests must assert on VALUES via conninfo_to_dict / captured connect kwargs, not on the shape of the string. A test asserting only that a conninfo string was produced passes with the bug present. Each redaction test must be paired with a known-positive proving the assertion can see the password when it IS leaked.

## Audit addendum (two-pass adversarial review, 2026-09-19)

Additional EARS requirements, all implemented in 0.22.1:

- When HTTPTask validates a URL, the guard shall normalise IPv4-mapped, 6to4 and Teredo IPv6 addresses to the IPv4 address they denote before any range check, so ::ffff:127.0.0.1 is treated as loopback.
- When HTTPTask cannot resolve a hostname, the guard shall fail closed rather than allow the URL.
- The HTTPTask guard shall permit only the http and https schemes.
- Where HTTPTask records a request URL in stage outputs or logs, it shall record the userinfo-redacted form, so a credential is not amplified beyond the stage that declared it.
- When a database URL is echoed after a parse failure, redaction shall cover a DSN typed without its host (scheme://user:secret), treating a colon whose right-hand side is not a port number as a password.

Residual, accepted and recorded rather than fixed:
- stage.context retains the author's own URL verbatim, because it is the stage definition required to execute or replay the stage. Credentials belong in headers or the `secrets` list.
- Ticket #3 (DNS-rebinding TOCTOU) remains open: the guard resolves a hostname and urllib then resolves it again independently.
