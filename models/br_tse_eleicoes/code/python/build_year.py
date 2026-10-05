"""
Incremental build of a single election year, for adding a new year's
partitions without rebuilding 1994+.

Point TSE_DATA_DIR at a directory holding only that year's raw input (see
download.py). OUTPUT_PYTHON then contains only that year's intermediates, so
the all-years normalization and aggregation steps below emit only that year:
every builder treats a missing year as empty.

The candidate-level join that adds ``titulo_eleitoral_candidato`` to results
uses only this year's candidates, which is all it needs — results join on
(ano, tipo_eleicao, sigla_uf, id_municipio_tse, cargo, numero).

Usage:
    TSE_DATA_DIR=~/Library/Caches/br_tse_eleicoes_data \
        python -m models.br_tse_eleicoes.code.python.build_year 2026 [step ...]
"""

import sys
import time

import pandas as pd

from models.br_tse_eleicoes.code.python import (
    aggregation,
    normalization_partition,
)
from models.br_tse_eleicoes.code.python.config import (
    OUTPUT_PYTHON,
    STREAM_SECAO_ROOT,
)
from models.br_tse_eleicoes.code.python.sub import (
    candidates,
    parties,
    results_mun_zone,
    streaming_secao,
    vacancies,
    voter_profile_mun_zone,
    voter_profile_polling_place,
    voting_details_mun_zone,
    voting_details_section,
)


def _save(df: pd.DataFrame, name: str) -> None:
    out = OUTPUT_PYTHON / f"{name}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"    {name}: {len(df):,} rows")


def phase1(ano: int) -> dict:
    return {
        "candidates": lambda: _save(
            candidates.build_candidatos(ano), f"candidatos_{ano}"
        ),
        "parties": lambda: _save(
            parties.build_partidos(ano), f"partidos_{ano}"
        ),
        "vacancies": lambda: _save(vacancies.build_vagas(ano), f"vagas_{ano}"),
        "voting_details_section": lambda: _save(
            voting_details_section.build_detalhes_secao(ano),
            f"detalhes_votacao_secao_{ano}",
        ),
        "voting_details_mun_zone": lambda: _save(
            voting_details_mun_zone.build_detalhes_mun_zona(ano),
            f"detalhes_votacao_municipio_zona_{ano}",
        ),
        "voter_profile_mun_zone": lambda: _save(
            voter_profile_mun_zone.build_perfil_mun_zona(ano),
            f"perfil_eleitorado_municipio_zona_{ano}",
        ),
        "voter_profile_polling_place": lambda: _save(
            voter_profile_polling_place.build_perfil_local_votacao(ano),
            "perfil_eleitorado_local_votacao",
        ),
        "results_mun_zone": lambda: (
            _save(
                results_mun_zone._build_candidato(ano),
                f"resultados_candidato_municipio_zona_{ano}",
            ),
            _save(
                results_mun_zone._build_partido(ano),
                f"resultados_partido_municipio_zona_{ano}",
            ),
        ),
        "results_section": lambda: streaming_secao.stream_resultados_secao(
            ano, STREAM_SECAO_ROOT
        ),
        "voter_profile_section": lambda: streaming_secao.stream_perfil_secao(
            ano, STREAM_SECAO_ROOT
        ),
    }


def main(ano: int, steps: list[str]) -> None:
    builders = phase1(ano)
    later = {
        "normalize": normalization_partition.build_all,
        "aggregate": aggregation.build_all,
    }
    for name in steps or [*builders, *later]:
        print(f"\n=== {name} ({ano})", flush=True)
        t0 = time.time()
        (builders.get(name) or later[name])()
        print(f"    {time.time() - t0:.0f}s", flush=True)


if __name__ == "__main__":
    main(int(sys.argv[1]), sys.argv[2:])
