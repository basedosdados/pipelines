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
            "Región del Libertador General Bernardo O´Higgins",
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
