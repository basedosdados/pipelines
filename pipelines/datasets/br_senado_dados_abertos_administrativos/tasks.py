"""Tasks de br_senado_dados_abertos_administrativos."""

import datetime as dt

from prefect import task

from pipelines.datasets.br_senado_dados_abertos_administrativos.utils import (
    clean_all,
)


@task
def extract_and_clean(
    output_dir: str, sub_resources: bool, years: list[int] | None = None
) -> dict:
    """Extrai e limpa as tabelas da janela e grava cada uma em parquet.

    Args:
        output_dir: diretório de saída.
        sub_resources: True inclui as tabelas filhas das contratações.
        years: anos consultados nas séries; None para o histórico completo.

    Returns:
        Um dicionário com `counts` (linhas gravadas por tabela),
        `extracted_at` (data da extração, em `aaaa-mm-dd`) e `tables` (as
        tabelas gravadas).
    """
    today = dt.date.today()
    extracted_at = today.isoformat()
    counts = clean_all(
        output=output_dir,
        years=years,
        extracted_at=extracted_at,
        today=today,
        sub_resources=sub_resources,
    )
    return {
        "counts": counts,
        "extracted_at": extracted_at,
        "tables": list(counts),
    }
