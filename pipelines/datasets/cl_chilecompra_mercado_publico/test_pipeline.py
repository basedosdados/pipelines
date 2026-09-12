"""Exercise the stale-month selection and geography lookup without touching the network."""

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pandas as pd
import pytest

from pipelines.datasets.cl_chilecompra_mercado_publico import utils
from pipelines.datasets.cl_chilecompra_mercado_publico.tasks import (
    select_stale_months_task,
)


def entry(days_ago, kind="orden_compra", year=2026, month=1):
    when = datetime.now(UTC) - timedelta(days=days_ago)
    return {
        "kind": kind,
        "year": year,
        "month": month,
        "last_modified": format_datetime(when),
        "etag": "x",
        "bytes": 1,
    }


def run(manifest, lookback=10, force_all=False):
    return select_stale_months_task.fn(manifest, lookback, force_all)


def test_recent_month_is_stale():
    assert len(run([entry(1)])) == 1


def test_old_month_is_skipped():
    assert run([entry(400)]) == []


def test_boundary_is_inclusive_side_only():
    assert len(run([entry(9)])) == 1
    assert run([entry(11)]) == []


def test_force_all_returns_everything():
    assert len(run([entry(400), entry(999)], force_all=True)) == 2


def test_unparseable_header_is_reingested_not_dropped():
    bad = entry(1)
    bad["last_modified"] = "not a date"
    assert len(run([bad])) == 1, (
        "a month with an unreadable header must not be skipped"
    )


def test_missing_header_is_reingested_not_dropped():
    bad = entry(1)
    del bad["last_modified"]
    assert len(run([bad])) == 1


def test_publisher_window_shape_is_selected():
    """The real shape: a trailing run of recent months plus a long stale tail."""
    manifest = [entry(1, month=m) for m in range(1, 16)] + [
        entry(400, year=2015, month=m) for m in range(1, 13)
    ]
    assert len(run(manifest)) == 15


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Región del Maule", "07"),
        ("Región de la Araucanía", "09"),
        ("Región Aysén del General Carlos Iba", "11"),  # truncated at 35 chars
        (
            "Región del Libertador General Bernardo O\u00b4Higgins",
            "06",
        ),  # acute accent
        ("Región Metropolitana de Santiago.", "13"),  # trailing period
        ("no such region", None),
    ],
)
def test_region_lookup(raw, expected):
    got = utils.resolve_geography(pd.Series([raw]), "region").iloc[0]
    assert (None if pd.isna(got) else got) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Santiago Centro", "13101"),
        ("Llay-Llay", "05703"),
        ("Til Til", "13303"),
        ("Alto Bio Bio", "08314"),
        ("La Calera", "05502"),
        ("Temuco", "09101"),
        ("Arica 1", None),
    ],
)
def test_comuna_lookup(raw, expected):
    got = utils.resolve_geography(pd.Series([raw]), "comuna").iloc[0]
    assert (None if pd.isna(got) else got) == expected


def test_duplicate_source_month_is_excluded_from_the_manifest():
    """lic-da/2014-4 holds March 2014 data, so it must never be ingested.

    Before partition files were named for their source month it silently overwrote
    March; after, it would silently duplicate March.
    """
    manifest = [
        entry(1, kind="licitacion", year=2014, month=4),
        entry(1, kind="licitacion", year=2014, month=3),
    ]
    kept = {
        (e["kind"], e["year"], e["month"])
        for e in run(manifest, force_all=True)
    }
    assert ("licitacion", 2014, 4) not in kept
    assert ("licitacion", 2014, 3) in kept


def test_duplicate_source_month_excluded_even_when_stale():
    manifest = [entry(400, kind="licitacion", year=2014, month=4)]
    assert run(manifest, force_all=True) == []


def test_partition_files_are_named_for_their_source_month(tmp_path):
    """Two source months contributing to one partition must not overwrite each other."""
    arch = utils.read_architecture("licitacion_oferta")
    cols = list(arch["name"])

    def frame(codigo):
        row = {c: "x" for c in cols}
        row.update(
            ano="2026",
            mes="06",
            codigo_licitacion=codigo,
            codigo_item="1",
            codigo_proveedor="1",
            nombre_oferta="o",
        )
        return pd.DataFrame([row], columns=cols)

    utils.write_partitioned(
        frame("A"), "licitacion_oferta", tmp_path, "2026-06"
    )
    utils.write_partitioned(
        frame("B"), "licitacion_oferta", tmp_path, "2026-03"
    )

    part = tmp_path / "licitacion_oferta" / "ano=2026" / "mes=06"
    names = sorted(p.name for p in part.glob("*.parquet"))
    assert names == ["data_2026-03.parquet", "data_2026-06.parquet"], names


def test_rewriting_one_source_month_is_idempotent(tmp_path):
    arch = utils.read_architecture("licitacion_oferta")
    cols = list(arch["name"])
    row = {c: "x" for c in cols}
    row.update(
        ano="2026",
        mes="06",
        codigo_licitacion="A",
        codigo_item="1",
        codigo_proveedor="1",
        nombre_oferta="o",
    )
    df = pd.DataFrame([row], columns=cols)
    for _ in range(3):
        utils.write_partitioned(df, "licitacion_oferta", tmp_path, "2026-06")
    part = tmp_path / "licitacion_oferta" / "ano=2026" / "mes=06"
    assert len(list(part.glob("*.parquet"))) == 1


def test_newest_month_per_kind_picks_the_latest_of_each_kind():
    """force_run's fallback must give the run something real to do, per kind."""
    from pipelines.datasets.cl_chilecompra_mercado_publico.flows import (
        newest_month_per_kind,
    )

    manifest = [
        {"kind": "orden_compra", "year": 2026, "month": 7},
        {"kind": "orden_compra", "year": 2026, "month": 8},
        {"kind": "orden_compra", "year": 2025, "month": 12},
        {"kind": "licitacion", "year": 2026, "month": 8},
        {"kind": "licitacion", "year": 2026, "month": 3},
    ]
    assert newest_month_per_kind(manifest) == [
        {"kind": "licitacion", "year": 2026, "month": 8},
        {"kind": "orden_compra", "year": 2026, "month": 8},
    ]


def test_newest_month_per_kind_compares_year_before_month():
    """A December of an older year must not beat a January of a newer one."""
    from pipelines.datasets.cl_chilecompra_mercado_publico.flows import (
        newest_month_per_kind,
    )

    manifest = [
        {"kind": "orden_compra", "year": 2025, "month": 12},
        {"kind": "orden_compra", "year": 2026, "month": 1},
    ]
    assert newest_month_per_kind(manifest) == [
        {"kind": "orden_compra", "year": 2026, "month": 1}
    ]


def test_newest_month_per_kind_is_empty_for_an_empty_manifest():
    from pipelines.datasets.cl_chilecompra_mercado_publico.flows import (
        newest_month_per_kind,
    )

    assert newest_month_per_kind([]) == []
