"""Tasks de br_senado_dados_abertos_administrativos.

`make_pipeline` monta o par check_update/extract_and_load de cada tabela.
"""

import os
import tempfile
from collections.abc import Callable
from datetime import date

from prefect import task

from pipelines.datasets.br_senado_dados_abertos_administrativos.constants import (
    TABLES_CONSTANTS,
    constants,
)
from pipelines.datasets.br_senado_dados_abertos_administrativos.utils import (
    clean_ata_acionamento,
    clean_contratacao,
    clean_contratacao_garantia,
    clean_contratacao_item,
    clean_contratacao_orgao_gestor,
    clean_contratacao_pagamento,
    clean_contrato_aditivo,
    clean_despesa_ceaps,
    clean_diretor_coordenador,
    clean_empresa,
    clean_estagiario,
    clean_licitacao,
    clean_menor_aprendiz,
    clean_pensionista,
    clean_previsao_aposentadoria,
    clean_quadro_pessoal,
    clean_senador,
    clean_senador_aposentado_pensionista,
    clean_senador_auxilio_moradia,
    clean_senador_escritorio_apoio,
    clean_senador_gabinete,
    clean_servidor,
    clean_servidor_aposentado,
    clean_servidor_ativo,
    clean_servidor_cedido,
    clean_servidor_exonerado,
    clean_servidor_hora_extra,
    clean_servidor_remuneracao,
    clean_supridos,
    clean_terceirizado,
    fetch_contratacoes,
    get_senador_reference,
    list_months,
    write_table,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateOnly,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


@task
def extract_table(table_id: str) -> list[dict]:
    """Extrai e limpa uma tabela na janela de rotina.

    Args:
        table_id: nome da tabela.

    Returns:
        As linhas da tabela. As tabelas de `data_extracao` vêm com o retrato
        de hoje; as séries, só nos anos da janela.

    Raises:
        ValueError: se a tabela não for do conjunto ou for estática.
    """
    today = date.today()
    extracted_at = today.isoformat()
    years = list(
        range(today.year - constants.REFRESH_PRIOR_YEARS.value, today.year + 1)
    )
    match table_id:
        case "senador":
            identificacoes, crosswalk = get_senador_reference()
            return clean_senador(identificacoes, crosswalk)
        case "senador_gabinete":
            crosswalk = get_senador_reference()[1]
            return clean_senador_gabinete(extracted_at, crosswalk)
        case "senador_escritorio_apoio":
            crosswalk = get_senador_reference()[1]
            return clean_senador_escritorio_apoio(extracted_at, crosswalk)
        case "senador_auxilio_moradia":
            crosswalk = get_senador_reference()[1]
            return clean_senador_auxilio_moradia(extracted_at, crosswalk)
        case "senador_aposentado_pensionista":
            return clean_senador_aposentado_pensionista(extracted_at)
        case "despesa_ceaps":
            return clean_despesa_ceaps(years)
        case "servidor":
            return clean_servidor(extracted_at)
        case "servidor_ativo":
            return clean_servidor_ativo(extracted_at)
        case "servidor_aposentado":
            return clean_servidor_aposentado(extracted_at)
        case "servidor_exonerado":
            return clean_servidor_exonerado(extracted_at)
        case "servidor_cedido":
            return clean_servidor_cedido(extracted_at)
        case "pensionista":
            return clean_pensionista(extracted_at)
        case "servidor_remuneracao":
            return clean_servidor_remuneracao(list_months(years, today))
        case "servidor_hora_extra":
            return clean_servidor_hora_extra(list_months(years, today))[0]
        case "servidor_hora_extra_dia":
            return clean_servidor_hora_extra(list_months(years, today))[1]
        case "contratacao":
            return clean_contratacao(fetch_contratacoes(), extracted_at)
        case "contratacao_orgao_gestor":
            return clean_contratacao_orgao_gestor(
                fetch_contratacoes(), extracted_at
            )
        case "contratacao_item":
            return clean_contratacao_item(fetch_contratacoes(), extracted_at)
        case "contratacao_garantia":
            return clean_contratacao_garantia(
                fetch_contratacoes(), extracted_at
            )
        case "contratacao_pagamento":
            return clean_contratacao_pagamento(
                fetch_contratacoes(), extracted_at
            )[0]
        case "contratacao_documento_fiscal":
            return clean_contratacao_pagamento(
                fetch_contratacoes(), extracted_at
            )[1]
        case "contratacao_pagamento_empenho":
            return clean_contratacao_pagamento(
                fetch_contratacoes(), extracted_at
            )[2]
        case "contrato_aditivo":
            return clean_contrato_aditivo(fetch_contratacoes(), extracted_at)
        case "ata_acionamento":
            return clean_ata_acionamento(fetch_contratacoes(), extracted_at)
        case "licitacao":
            return clean_licitacao(extracted_at)[0]
        case "licitacao_detalhamento":
            return clean_licitacao(extracted_at)[1]
        case "empresa":
            return clean_empresa(extracted_at)
        case "terceirizado":
            return clean_terceirizado(extracted_at)
        case "menor_aprendiz":
            return clean_menor_aprendiz(extracted_at)
        case "estagiario":
            return clean_estagiario(extracted_at)
        case (
            "suprido_ato_concessao"
            | "suprido_empenho"
            | "suprido_transacao"
            | "suprido_transacao_objeto"
            | "suprido_movimentacao"
            | "suprido_movimentacao_subtipo"
        ):
            return clean_supridos(years)[table_id]
        case "quadro_pessoal":
            return clean_quadro_pessoal(extracted_at)
        case "diretor_coordenador":
            return clean_diretor_coordenador(extracted_at)
        case "previsao_aposentadoria":
            return clean_previsao_aposentadoria(extracted_at)
    raise ValueError(f"tabela fora da extração de rotina: {table_id}")


@task
def get_max_date(table_id: str, rows: list[dict]) -> date:
    """Lê a maior data de uma série, na coluna de data da cobertura.

    Args:
        table_id: nome da série.
        rows: as linhas da série, extraídas por `extract_table`.

    Returns:
        A maior data; numa série por mês, o primeiro dia do maior mês. Devolve
        `constants.NO_NEW_DATA_DATE` se a série vier sem data.

    Raises:
        ValueError: se a tabela não tiver coluna de data, ou se ela não for
            por dia nem por mês.
    """
    coverage = TABLES_CONSTANTS[table_id].COVERAGE.value
    if not isinstance(coverage, AllFree | PartBdpro):
        raise ValueError(f"tabela sem coluna de data: {table_id}")
    date_column = coverage.date_column
    if isinstance(date_column, DateOnly):
        dates = [row[date_column.col] for row in rows if row[date_column.col]]
        if not dates:
            return constants.NO_NEW_DATA_DATE.value
        return date.fromisoformat(max(dates))
    if isinstance(date_column, YearMonth):
        months = [
            (row[date_column.year], row[date_column.month])
            for row in rows
            if row[date_column.year] and row[date_column.month]
        ]
        if not months:
            return constants.NO_NEW_DATA_DATE.value
        year, month = max(months)
        return date(year, month, 1)
    raise ValueError(f"coluna de data por ano: {table_id}")


@task
def save_table(table_id: str, rows: list[dict]) -> str:
    """Grava uma tabela em parquet num diretório temporário.

    Args:
        table_id: nome da tabela.
        rows: as linhas da tabela, extraídas por `extract_table`.

    Returns:
        O diretório da tabela, no formato esperado por `upload_to_gcs`.

    Raises:
        ValueError: se a tabela vier sem linhas.
    """
    if not rows:
        raise ValueError(f"a fonte devolveu a tabela {table_id} sem linhas")
    work_dir = tempfile.mkdtemp(
        prefix=f"{constants.DATASET_ID.value}_{table_id}_"
    )
    return write_table(os.path.join(work_dir, "output"), table_id, rows)


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """Monta o check_update de uma tabela.

    Args:
        table_id: nome da tabela.

    Returns:
        Função que lê a data de referência da fonte. `senador` não tem data:
        usa a data de hoje, comparada com o `Table.Update`. As tabelas de
        `data_extracao` usam a data de hoje, comparada com a cobertura. As
        séries usam a maior data da janela, comparada com a cobertura; uma
        série só com `ano` usa a maior data da série irmã, em
        `constants.REFERENCE_DATE_TABLES`.
    """

    def get_latest_update() -> SourceInspection:
        """Lê a data de referência da fonte."""
        if table_id in constants.UNPARTITIONED.value:
            return SourceInspection(
                reference_date=date.today(), compare_against="table_update"
            )
        if table_id in constants.SNAPSHOTS.value:
            return SourceInspection(reference_date=date.today())
        date_table_id = constants.REFERENCE_DATE_TABLES.value.get(
            table_id, table_id
        )
        rows = extract_table(table_id=date_table_id)
        return SourceInspection(
            reference_date=get_max_date(table_id=date_table_id, rows=rows)
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Monta o extract_and_load de uma tabela.

    Args:
        table_id: nome da tabela.

    Returns:
        Função que extrai a tabela e devolve o parquet e a cobertura.

    Raises:
        ValueError: se a tabela não tiver cobertura.
    """
    coverage = TABLES_CONSTANTS[table_id].COVERAGE.value
    if coverage is None:
        raise ValueError(f"tabela sem cobertura: {table_id}")
    coverage_dump = coverage.model_dump()

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Extrai a tabela e devolve o parquet e a cobertura."""
        rows = extract_table(table_id=table_id)
        return ExtractAndLoad(
            coverage=coverage_dump,
            data_path=save_table(table_id=table_id, rows=rows),
            dump_mode="overwrite",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    constants.DATASET_ID.value,
    make_get_latest_update,
    make_extract_load_data,
)
