# Spec: mg-up — support a target schema so tables stop landing in `public`

**Ticket:** issuedb #2 · **Date:** 2026-07-30 · **Origin:** agent-mail report from
SponsorSignal (id `01KYK144AM5T97F82B0Y7760CY`, thread `thr-f7dc3dcf6790451abd9c`)

## EARS requirements

- Where a schema is configured (db URL `?schema=`, mg.yaml `schema:` key, or `MG_SCHEMA` env), the stabilize `mg-up` command shall create the schema if missing and apply all migrations with `search_path` set to that schema, so the migration tracking table and all stabilize tables land in it.
- If no schema is configured, then `mg-up` shall behave exactly as before (tables land in the connection default schema), preserving backward compatibility.
- If the configured schema name is not a valid PostgreSQL identifier, then `mg-up` shall refuse with a clear error before touching the database.
- The stabilize docs shall state how to point the runtime store at the same schema (DSN `options=-csearch_path`) and how to move an existing public-schema install (`ALTER TABLE ... SET SCHEMA`).
- When `mg-up` runs against a database where migrations were already applied in another schema, the schema-qualified migration tracking shall not re-apply or corrupt existing migrations.
