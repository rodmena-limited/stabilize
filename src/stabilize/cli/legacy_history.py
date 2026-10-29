"""The migration history kept by stabilize's own runner before 0.33.2, and its takeover plan."""

from __future__ import annotations

from dataclasses import dataclass, field

from stabilize.cli.migrations import migration_id, migretti_checksum

LEGACY_TABLE = "stabilize_migrations"

KNOWN_LEGACY_CHECKSUMS: dict[str, frozenset[str]] = {
    "01KDQ4N9QPJ6Q4MCV3V9GHWPV4_initial_schema.sql": frozenset({"5d2edd47dd5f2f141305169815a3c36c"}),
    "01KDRK3TXW4R2GERC1WBCQYJGG_rag_embeddings.sql": frozenset({"0c05b1306b39c39841bf4a82d229d7f5"}),
    "01KE39HEAN221S7AWNS4A0H750_add_performance_indexes.sql": frozenset(
        {"4f5c0456e333bbece1f355960e4e7755", "e2bd7c7f2d403a6ff58a85fbc920ceab"}
    ),
    "01KE4YJT2W8A6NBX5R7CQMJK84_add_dlq_table.sql": frozenset({"c11e0a45e9f580cbf7192092db881726"}),
    "01KE5ZQT3X9B7PCS8R8DRMLN85_add_processed_messages.sql": frozenset({"58cb345d5806b80480064efa5c76e9f2"}),
    "01KE60R04X0C8QDTS9SEESMN96_add_task_version.sql": frozenset({"eb3c044f50b817a3e779303509cae949"}),
    "01KFA6JDAS811CB5RKPD1FBAST_add_stage_version.sql": frozenset({"b5edbcb4f300bb98ce4354f533b83938"}),
    "01KGFG6RM7NRCJ2PPJ7FZAM0Y9_widen_message_id_column.sql": frozenset({"1e2102d0b51b130b2ee5e6b4edc9ecac"}),
    "01KGHWCP1M2QSDE3TN4GUBLV8A_add_workflow_control_flow_patterns.sql": frozenset(
        {"12ffb38f9f8a95aff1265ccb4e33950c"}
    ),
    "01KWHFBW2B5VZJENEA3D8Z2YPW_add_stage_claims.sql": frozenset({"76d71bf48d1a9f143eac17b0cdef8aef"}),
    "01KYTB6S57X765P3BJYY58H79Z_add_queue_task_id_index.sql": frozenset({"45a9b7343ca368d18c4997a1dcf99af2"}),
    "01M482Z7ZP5AXJ8PJCGB5XZTCZ_widen_queue_ids_to_bigint.sql": frozenset({"8a2da87dd48395d6747082a152d5fedc"}),
    "01M483DVR3DS980FHS8N754CWH_queue_bigint_ids_and_timestamptz.sql": frozenset(
        {"1d1ceafde27bfe3418ea762fdf004ed2"}
    ),
    "01M4PAJCF82KCKYT5X4GMZH2XG_workflow_queue_table_and_stage_index.sql": frozenset(
        {"fe9b451a184b47de24b2dc5e8c944c73"}
    ),
    "01M4Q8VTM40WEC4HR2X31Q07A5_workflow_config_version.sql": frozenset({"026396d2774c6daf17f212dd761d8205"}),
    "01M4SVVY5XE3S18Y0CPQ8TATDP_purge_and_lookup_indexes.sql": frozenset({"fc62b3bc67649383f3126cae4b62b3ff"}),
    "01M64SFQG36NHV5KQ3SHH5Z832_event_store_tables.sql": frozenset({"768990e96c776f72bd4162dddcd76245"}),
    "01M64SFQGDVM97KQS2FFAJ72R5_task_leases.sql": frozenset({"63f15c08b91b60648281736c27fc9216"}),
}

RETIRED_MIGRATIONS = frozenset(
    {
        "01KDRK3TXW4R2GERC1WBCQYJGG_rag_embeddings.sql",
        "01M482Z7ZP5AXJ8PJCGB5XZTCZ_widen_queue_ids_to_bigint.sql",
    }
)

PLACEHOLDER_SQL = "-- migrate: up\nSELECT 1;\n-- migrate: down\nSELECT 1;\n"
PLACEHOLDER_CHECKSUM = migretti_checksum(PLACEHOLDER_SQL)


class TakeoverRefused(RuntimeError):
    """The recorded history cannot be handed to migretti without a decision by the operator."""


@dataclass(frozen=True)
class MigrettiState:
    tracking: bool
    applied: dict[str, str]
    failed: tuple[str, ...]


@dataclass
class TakeoverPlan:
    placeholders: list[str] = field(default_factory=list)
    fixes: list[str] = field(default_factory=list)
    retired: list[str] = field(default_factory=list)
    adopted: list[str] = field(default_factory=list)

    @property
    def needed(self) -> bool:
        return bool(self.placeholders or self.fixes)


def _check_legacy_rows(legacy: dict[str, str], shipped: dict[str, str]) -> None:
    unknown: list[str] = []
    for name, checksum in sorted(legacy.items()):
        known = KNOWN_LEGACY_CHECKSUMS.get(name)
        if known is None and name in shipped:
            raise TakeoverRefused(
                f"{LEGACY_TABLE} names {name}, which this stabilize ships but has no recorded history for; "
                "the checksum table in stabilize.cli.legacy_history is out of date"
            )
        if known is None:
            unknown.append(f"{name} (never shipped by stabilize)")
        elif checksum not in known:
            unknown.append(f"{name} (checksum {checksum} matches no committed version of the file)")
    if unknown:
        raise TakeoverRefused(
            f"{LEGACY_TABLE} holds rows stabilize cannot vouch for; nothing was changed: " + "; ".join(unknown)
        )


def check_history(shipped: dict[str, str], state: MigrettiState) -> None:
    """Refuse a migretti history that holds failed migrations or migrations this stabilize does not ship."""
    if state.failed:
        raise TakeoverRefused(
            "migretti's history records failed migrations: "
            + ", ".join(state.failed)
            + "; repair them with `mg fix <id> --applied` or `mg fix <id> --remove` first"
        )

    shipped_ids = {migration_id(name): name for name in shipped}
    foreign = sorted(set(state.applied) - set(shipped_ids))
    if foreign:
        raise TakeoverRefused(
            "migretti's history in this schema holds migrations this stabilize does not ship: "
            + ", ".join(foreign)
            + ". Either another project keeps its own migretti migrations in the same schema (give "
            "stabilize a schema of its own with ?schema=), or the database was migrated by a newer "
            "stabilize"
        )


def plan_takeover(legacy: dict[str, str], shipped: dict[str, str], state: MigrettiState) -> TakeoverPlan:
    """Decide what must be recorded in migretti so that it knows what the old runner applied.

    *legacy* maps filename to the MD5 the old runner recorded, *shipped* maps
    filename to the text of each migration this stabilize ships, and *state* is
    migretti's history in the target schema.
    """
    check_history(shipped, state)
    shipped_ids = {migration_id(name): name for name in shipped}
    _check_legacy_rows(legacy, shipped)
    plan = TakeoverPlan(retired=sorted(name for name in legacy if name in RETIRED_MIGRATIONS))

    legacy_ids = {migration_id(name) for name in legacy if name in shipped}
    for mig_id, name in sorted(shipped_ids.items()):
        recorded = state.applied.get(mig_id)
        current = migretti_checksum(shipped[name])
        if recorded == PLACEHOLDER_CHECKSUM and mig_id not in legacy_ids:
            raise TakeoverRefused(
                f"migretti records a takeover placeholder for {name}, but {LEGACY_TABLE} has no record "
                "of the old runner applying it"
            )
        if mig_id not in legacy_ids:
            continue
        plan.adopted.append(mig_id)
        if recorded is None:
            plan.placeholders.append(mig_id)
            plan.fixes.append(mig_id)
        elif recorded == PLACEHOLDER_CHECKSUM:
            plan.fixes.append(mig_id)
        elif recorded != current:
            raise TakeoverRefused(
                f"migretti records {name} with checksum {recorded}, which is neither the shipped file's "
                f"({current}) nor the takeover placeholder's; nothing was changed"
            )
    return plan
