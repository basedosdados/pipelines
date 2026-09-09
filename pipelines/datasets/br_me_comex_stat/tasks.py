"""
Tasks for br_me_comex_stat.
"""

from collections.abc import Callable
from datetime import date
from pathlib import Path

from pipelines.crawler.me_comex_stat.constants import (
    constants as comex_constants,
)
from pipelines.crawler.me_comex_stat.tasks import (
    clean_br_me_comex_stat,
    download_br_me_comex_stat,
    parse_last_date,
)
from pipelines.datasets.br_me_comex_stat.constants import TABLE_SPECS
from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth
from pipelines.utils.stage_dispatch import CheckResult, DownloadResult

# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas (município/NCM x exportação/importação) — ver constants.py
#
# `check_for_update` é a MESMA função pras 4 tabelas — a fonte de check é
# uma página de metadados única (`DOWNLOAD_LINK`), compartilhada, não por
# tabela (o flow antigo já comentava isso: "A fonte é uma só para as
# quatro tabelas"). Diferente de `br_ibge_ipca`, aqui o check é leve de
# verdade: `parse_last_date` só faz scrape de HTML de uma página de
# metadados, não baixa o dado bruto (~poucos KB de HTML) — não precisa da
# lógica de "baixar duas vezes" usada lá.
#
# `download_data` precisa de fábrica por tabela (`table_name`/`table_type`
# variam). `clean_br_me_comex_stat` particiona por `ano/mes` (tabelas NCM)
# ou `ano/mes/sigla_uf` (tabelas de município, várias UFs por arquivo
# baixado) — `_discover_partition_folders` descobre as pastas-folha
# realmente escritas em disco, sem hardcoded as UFs presentes no arquivo.
# ──────────────────────────────────────────────────────────────────────────────


def br_me_comex_stat_check_for_update() -> CheckResult:
    last_date = parse_last_date(link=comex_constants.DOWNLOAD_LINK.value)
    # pyrefly: ignore [missing-attribute]
    year, month = last_date.split("-")
    return CheckResult(reference_date=date(int(year), int(month), 1))


def _discover_partition_folders(base_path: str) -> list[str] | None:
    """Encontra as pastas-folha (`ano=.../mes=...[/sigla_uf=...]`) escritas
    por `to_partitions` dentro de `base_path`, relativas a ele — genérico
    pros dois esquemas de partição deste dataset (2 ou 3 níveis), sem
    precisar saber de antemão quais UFs vieram no arquivo baixado."""
    base = Path(base_path)
    if not base.exists():
        return None
    leaves = [
        str(p.relative_to(base))
        for p in base.rglob("*")
        if p.is_dir() and not any(c.is_dir() for c in p.iterdir())
    ]
    return sorted(leaves) or None


def make_download_data(table_id: str) -> Callable[[dict], DownloadResult]:
    spec = TABLE_SPECS[table_id]
    table_name = spec["table_name"]
    table_type = spec["table_type"]

    def download_data(download_params: dict) -> DownloadResult:
        ref = date.fromisoformat(download_params["reference_date"])
        year_download = f"{ref.year}-{ref.month:02d}"

        download_br_me_comex_stat(
            table_name=table_name, year_download=year_download
        )
        filepath = clean_br_me_comex_stat(
            path=comex_constants.PATH.value,
            table_type=table_type,
            table_name=table_name,
        )

        # `clean_br_me_comex_stat` está tipado (errado) como -> pd.DataFrame,
        # mas devolve `str` de verdade (mesma pendência do código original,
        # já marcada lá com `# pyrefly: ignore [bad-return]`).
        return DownloadResult(
            coverage=PartBdpro(
                date_column=YearMonth(year="ano", month="mes"),
                date_format=DateFormat.YEAR_MONTH,
            ).model_dump(),
            # pyrefly: ignore [bad-argument-type]
            data_path=filepath,
            bq_project="basedosdados",
            # pyrefly: ignore [bad-argument-type]
            partition_folders=_discover_partition_folders(filepath),
        )

    return download_data
