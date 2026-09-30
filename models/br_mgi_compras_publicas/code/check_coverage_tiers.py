"""Verify that the metadata registration cannot break the BD Pro paywall.

    uv run python -m models.br_mgi_compras_publicas.code.check_coverage_tiers

Run it as a module, not as a path: the sibling import is absolute, per
AGENTS.md, so `models` has to resolve through the editable install rather than
through the script's own directory.

No backend is touched: `register_metadata`'s MCP calls are stubbed and the
recorded calls are asserted against. The four properties checked are the four
ways the previous, position-based code damaged production:

1. The free Coverage is resolved by `is_closed`, not by list position. On prod
   `ata_registro_preco_item` lists the PRO coverage first, so position-based
   reuse overwrote the BD Pro window with the free range.
2. A pipeline-owned range is never restated, and for the paid tier is never
   written at all -- not even when absent. The flow recomputes it every run,
   day-granular and rolling; a month-granular literal would coarsen the
   free/pro boundary and move it forward, releasing paywalled data.
3. `prune` never deletes a pro Coverage. It is a legitimate second coverage,
   and without it the next flow run dies at `assert_coverage_topology`.
4. A part_bdpro table missing its pro Coverage gets one; an all-free table
   never does, because `assert_coverage_topology` fails an AllFree table that
   has one.
"""

from __future__ import annotations

from typing import Any

from models.br_mgi_compras_publicas.code import register_metadata as rm


def tier(coverage_id: str, range_ids: list[str]) -> dict[str, Any]:
    return {
        "id": coverage_id,
        "range_ids": list(range_ids),
        "extra_coverage_ids": [],
        "extra_range_ids": [],
    }


class Recorder:
    """Stands in for `rm.fn`, recording every call instead of issuing it."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, name: str):
        def call(**kwargs: Any) -> dict[str, Any]:
            self.calls.append((name, kwargs))
            return {"id": kwargs.get("id") or f"new-{name}"}

        return call

    def of(self, name: str) -> list[dict[str, Any]]:
        return [kwargs for called, kwargs in self.calls if called == name]


def plan(table: str, tiers: dict[bool, dict[str, Any]]) -> rm.CoveragePlan:
    """The real decision function -- not a reimplementation of it.

    `register_metadata.coverage_plan` is pure precisely so this script can
    exercise the same code the registration runs, rather than a copy that could
    silently drift from it.
    """
    return rm.coverage_plan(table, tiers)


def main() -> int:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"  {'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    # 1. prod shape, pro coverage listed FIRST (ata_registro_preco_item).
    print("1. free coverage resolved by tier, not position")
    got = plan(
        "ata_registro_preco_item",
        {
            True: tier("cov-pro", ["rng-pro"]),
            False: tier("cov-free", ["rng-free"]),
        },
    )
    check("reuses the FREE coverage id", got.free_coverage_id == "cov-free")
    check("reuses the FREE range id", got.free_range_id == "rng-free")
    check("does not create a second pro coverage", not got.create_pro_coverage)

    # 2. a pipeline-owned range is left alone.
    print("2. pipeline-owned ranges are not restated")
    for table in ("contratacao", "orgao"):
        tiers = {False: tier("cov-free", ["rng-free"])}
        if isinstance(rm.COVERAGE.get(table), rm.PartBdpro):
            tiers[True] = tier("cov-pro", ["rng-pro"])
        check(
            f"{table}: range not written", not plan(table, tiers).write_range
        )
    print("   ... and the PAID tier is not seeded even when absent")
    # Seeding contratacao from the month literal would declare the paid window
    # free until the first materialisation; the flow creates both ranges.
    absent_paid = plan(
        "contratacao", {False: tier("cov-free", []), True: tier("cov-pro", [])}
    )
    check(
        "contratacao with no range: not written", not absent_paid.write_range
    )
    print("   ... while a missing all-free range still is")
    seeded = plan("orgao", {False: tier("cov-free", [])})
    check("orgao with no range: seeded", seeded.write_range)
    check("seeded as a create, not an update", seeded.free_range_id is None)

    # 3. a static (legado) table's range IS written, so corrections land.
    print("3. static tables keep their declared range")
    got = plan("pregao_item_oferta", {False: tier("cov-free", ["rng-free"])})
    check("range rewritten", got.write_range)
    check("reuses the range id", got.free_range_id == "rng-free")

    # 4. the pro coverage is created when absent, and only for the pro tier.
    print("4. pro coverage created only where the tier requires it")
    check(
        "part_bdpro: pro created when missing",
        plan(
            "contratacao", {False: tier("cov-free", ["r"])}
        ).create_pro_coverage,
    )
    check(
        "part_bdpro: not created when present",
        not plan(
            "contratacao",
            {False: tier("cov-free", ["r"]), True: tier("cov-pro", [])},
        ).create_pro_coverage,
    )
    check(
        "all_free: no pro ever created",
        not plan(
            "pregao_item_oferta", {False: tier("cov-free", ["r"])}
        ).create_pro_coverage,
    )

    # 5. a brand-new table creates both records from scratch.
    print("5. an unregistered table creates its coverage")
    got = plan("contratacao", {})
    check("free coverage created", got.free_coverage_id is None)
    check("no range written for the paid tier", not got.write_range)
    check("pro coverage created", got.create_pro_coverage)
    free_new = plan("orgao", {})
    check("all-free: free coverage created", free_new.free_coverage_id is None)
    check("all-free: range created too", free_new.write_range)

    # 6. prune keeps one coverage PER TIER and one range per coverage.
    print("6. prune deduplicates within a tier, never across")
    recorder = Recorder()
    original_fn, original_server = rm.fn, rm.server
    rm.fn = recorder  # pyrefly: ignore [bad-assignment]

    class Stub:
        delete_record = staticmethod(lambda **_: None)

    rm.server = Stub()  # pyrefly: ignore [bad-assignment]
    try:
        rm.prune(
            {"observation_levels": [], "updates": []},
            {
                False: {
                    "id": "cov-free",
                    "range_ids": ["rng-free", "rng-free-dup"],
                    "extra_coverage_ids": ["cov-free-dup"],
                    "extra_range_ids": ["rng-orphan"],
                },
                True: tier("cov-pro", ["rng-pro"]),
            },
            "test",
        )
    finally:
        rm.fn = original_fn  # pyrefly: ignore [bad-assignment]
        rm.server = original_server  # pyrefly: ignore [bad-assignment]
    deleted = {c["record_id"] for c in recorder.of("delete_record")}
    check("duplicate free coverage deleted", "cov-free-dup" in deleted)
    check("pro coverage NOT deleted", "cov-pro" not in deleted)
    check("pro range NOT deleted", "rng-pro" not in deleted)
    check("kept free coverage NOT deleted", "cov-free" not in deleted)
    check("kept free range NOT deleted", "rng-free" not in deleted)
    check("duplicate free range deleted", "rng-free-dup" in deleted)
    check("orphan range deleted", "rng-orphan" in deleted)

    print()
    if failures:
        print(f"{len(failures)} check(s) FAILED:")
        for label in failures:
            print("  -", label)
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
