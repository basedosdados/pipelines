"""Compare what we harvested against the totals the source reports for itself.

This is the only check that catches silent truncation, and no dbt test
substitutes for it. `licitacao_item_pregao` passed every dbt test -- uniqueness,
not-null, relationships -- while missing 21% of its rows, because a boundary
error dropped one day in five uniformly. Nothing about the data it *did* have
looked wrong.

Compares against the **consolidated parquet**, not the dbt model: several
endpoints serve the same row many times (endpoint 6 about 3.3x), and the source's
own totals count those repeats, so the model is legitimately smaller.

    uv run python models/br_mgi_compras_publicas/code/reconcile.py [table ...]
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
import requests

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.br_mgi_compras_publicas.constants import (  # noqa: E402
    constants,
)

BASE = constants.BASE_URL.value
DATA = Path.home() / "Downloads" / "br_mgi_compras_publicas_data"

#: table -> (path, param names, kind). "year_range" sends a full calendar year
#: through the endpoint's own date parameters; "year_param" sends the year as a
#: single integer; "modalidade" has no date filter at all.
SOURCES: dict[str, tuple] = {
    "contratacao": (
        "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
        ("dataPublicacaoPncpInicial", "dataPublicacaoPncpFinal"),
        "year_range_half_open",
        range(2021, 2027),
    ),
    "licitacao": (
        "/modulo-legado/1_consultarLicitacao",
        ("data_publicacao_inicial", "data_publicacao_final"),
        "year_range_closed",
        range(1997, 2026),
    ),
    "licitacao_pregao": (
        "/modulo-legado/3_consultarPregoes",
        ("dt_data_edital_inicial", "dt_data_edital_final"),
        "year_range_closed",
        range(1997, 2026),
    ),
    "licitacao_item_pregao": (
        "/modulo-legado/4_consultarItensPregoes",
        ("dt_hom_inicial", "dt_hom_final"),
        "year_range_half_open",
        range(2000, 2026),
    ),
    "compra_sem_licitacao": (
        "/modulo-legado/5_consultarComprasSemLicitacao",
        ("dt_ano_aviso",),
        "year_param",
        range(1997, 2026),
    ),
    "compra_sem_licitacao_item": (
        "/modulo-legado/6_consultarCompraItensSemLicitacao",
        ("dt_ano_aviso_licitacao",),
        "year_param",
        range(1997, 2025),
    ),
    "licitacao_item": (
        "/modulo-legado/2_consultarItemLicitacao",
        ("modalidade",),
        "modalidade",
        constants.MODALIDADES_LEGADO_TIER_A.value,
    ),
}


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {"accept": "application/json", "user-agent": "basedosdados/reconcile"}
    )
    return s


def source_total(session: requests.Session, table: str) -> int | None:
    path, params, kind, values = SOURCES[table]
    total = 0
    for v in values:
        query: dict = {"pagina": 1, "tamanhoPagina": 10}
        if kind == "year_param" or kind == "modalidade":
            query[params[0]] = v
        else:
            end = (
                f"{v + 1}-01-01"
                if kind.endswith("half_open")
                else f"{v}-12-31"
            )
            query[params[0]], query[params[1]] = f"{v}-01-01", end
        for attempt in range(4):
            try:
                r = session.get(f"{BASE}{path}", params=query, timeout=240)
                if r.status_code == 200:
                    total += int((r.json() or {}).get("totalRegistros") or 0)
                    break
                if r.status_code == 429:
                    time.sleep(8)
                    continue
            except requests.RequestException:
                pass
            time.sleep(4)
        else:
            print(f"    ! {table}: {v} unreadable, total is a lower bound")
            return None
        time.sleep(0.3)
    return total


def local_rows(table: str) -> int:
    root = DATA / "output" / table
    if not root.is_dir():
        chunks = DATA / "_chunks" / table
        if not chunks.is_dir():
            return -1
        return sum(
            pq.read_metadata(f).num_rows for f in chunks.glob("*.parquet")
        )
    return sum(pq.read_metadata(f).num_rows for f in root.rglob("*.parquet"))


def main() -> int:
    names = sys.argv[1:] or list(SOURCES)
    session = _session()
    bad = 0
    for table in names:
        if table not in SOURCES:
            print(f"{table}: no source-total query defined, skipped")
            continue
        got = local_rows(table)
        want = source_total(session, table)
        if want is None or got < 0:
            print(f"{table}: could not compare")
            continue
        delta = got - want
        pct = 100 * delta / want if want else 0.0
        flag = "OK " if abs(pct) < 0.1 else ">>>"
        bad += abs(pct) >= 0.1
        print(
            f"{flag} {table:<28} harvested {got:>12,}  source {want:>12,}  "
            f"{delta:+,} ({pct:+.2f}%)"
        )
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
