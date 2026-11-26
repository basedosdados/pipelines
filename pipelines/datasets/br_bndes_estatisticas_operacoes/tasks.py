"""
Tasks de br_bndes_estatisticas_operacoes.

As `@task` embrulham as funções de `utils.py`, onde fica a lógica. Abaixo delas
ficam as duas etapas que o conjunto escreve, uma casca por tabela, porque o
pipeline chama cada etapa sem argumentos ou só com `download_params`.
"""

from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.br_bndes_estatisticas_operacoes import utils
from pipelines.datasets.br_bndes_estatisticas_operacoes.constants import (
    EXIM_APROVACAO_DOLAR_TABLE_ID,
    EXIM_APROVACAO_TABLE_ID,
    EXIM_CONSULTA_DOLAR_TABLE_ID,
    EXIM_CONSULTA_TABLE_ID,
    EXIM_SETOR_CNAE_APROVACAO_DOLAR_TABLE_ID,
    EXIM_SETOR_CNAE_CONSULTA_DOLAR_TABLE_ID,
    EXIM_SUBSETOR_CNAE_APROVACAO_DOLAR_TABLE_ID,
    EXIM_SUBSETOR_CNAE_CONSULTA_DOLAR_TABLE_ID,
    PORTE_APROVACAO_TABLE_ID,
    PORTE_CONSULTA_TABLE_ID,
    PRODUTO_DIRETA_APROVACAO_DOLAR_TABLE_ID,
    PRODUTO_DIRETA_APROVACAO_TABLE_ID,
    PRODUTO_DIRETA_CONSULTA_DOLAR_TABLE_ID,
    PRODUTO_DIRETA_CONSULTA_TABLE_ID,
    PRODUTO_INDIRETA_APROVACAO_DOLAR_TABLE_ID,
    PRODUTO_INDIRETA_APROVACAO_TABLE_ID,
    PRODUTO_INDIRETA_CONSULTA_DOLAR_TABLE_ID,
    PRODUTO_INDIRETA_CONSULTA_TABLE_ID,
    SETOR_BNDES_APROVACAO_TABLE_ID,
    SETOR_BNDES_CONSULTA_TABLE_ID,
    SETOR_CNAE_APROVACAO_TABLE_ID,
    SETOR_CNAE_CONSULTA_TABLE_ID,
    SUBSETOR_BNDES_APROVACAO_TABLE_ID,
    SUBSETOR_BNDES_CONSULTA_TABLE_ID,
    SUBSETOR_CNAE_APROVACAO_TABLE_ID,
    SUBSETOR_CNAE_CONSULTA_TABLE_ID,
    UF_APROVACAO_TABLE_ID,
    UF_CONSULTA_TABLE_ID,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task(retries=3, retry_delay_seconds=60)
def get_source_last_modified(table_id: str) -> date:
    """Lê o dia da última substituição do CSV da tabela no CKAN.

    Args:
        table_id: Slug da tabela.

    Returns:
        O dia do `last_modified` do recurso.
    """
    return utils.get_source_last_modified(table_id=table_id)


@task(retries=3, retry_delay_seconds=60)
def download_table(table_id: str) -> Path:
    """Baixa o CSV da tabela e confere o MD5.

    Args:
        table_id: Slug da tabela.

    Returns:
        O caminho do CSV baixado.
    """
    return utils.download_table(table_id=table_id)


@task
def clean_table(table_id: str, csv_path: Path) -> Path:
    """Passa o CSV para o formato longo e particiona por ano.

    Args:
        table_id: Slug da tabela.
        csv_path: CSV baixado por `download_table`.

    Returns:
        O diretório particionado em formato Hive.
    """
    return utils.clean_table(table_id=table_id, csv_path=csv_path)


def get_latest_update(table_id: str) -> SourceInspection:
    """Etapa check_update: quando a fonte substituiu o arquivo pela última vez.

    O poll compara o `last_modified` do recurso com a última atualização da
    tabela na BD (`Table.Update`), e não com a faixa de datas: assim uma
    correção sem mês novo também dispara a carga.

    Args:
        table_id: Slug da tabela.

    Returns:
        A data que o poll compara com o `Table.Update` da tabela.
    """
    return SourceInspection(
        reference_date=get_source_last_modified(table_id=table_id),
        compare_against="table_update",
    )


def extract_load_data(table_id: str, download_params: dict) -> ExtractAndLoad:
    """Etapa extract_and_load: baixa e limpa o histórico inteiro.

    A fonte republica o arquivo inteiro a cada atualização, então a tabela é
    reescrita (`overwrite`) e `download_params` não é usado.

    Args:
        table_id: Slug da tabela.
        download_params: O que o check_update mandou.

    Returns:
        A pasta particionada e o modo de escrita.
    """
    csv_path = download_table(table_id=table_id)
    output_dir = clean_table(table_id=table_id, csv_path=csv_path)
    return ExtractAndLoad(data_path=str(output_dir), dump_mode="overwrite")


def produto_direta_aprovacao_get_latest_update() -> SourceInspection:
    """Etapa check_update da produto_direta_aprovacao."""
    return get_latest_update(table_id=PRODUTO_DIRETA_APROVACAO_TABLE_ID)


def produto_direta_aprovacao_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da produto_direta_aprovacao."""
    return extract_load_data(
        table_id=PRODUTO_DIRETA_APROVACAO_TABLE_ID,
        download_params=download_params,
    )


def produto_indireta_aprovacao_get_latest_update() -> SourceInspection:
    """Etapa check_update da produto_indireta_aprovacao."""
    return get_latest_update(table_id=PRODUTO_INDIRETA_APROVACAO_TABLE_ID)


def produto_indireta_aprovacao_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da produto_indireta_aprovacao."""
    return extract_load_data(
        table_id=PRODUTO_INDIRETA_APROVACAO_TABLE_ID,
        download_params=download_params,
    )


def porte_aprovacao_get_latest_update() -> SourceInspection:
    """Etapa check_update da porte_aprovacao."""
    return get_latest_update(table_id=PORTE_APROVACAO_TABLE_ID)


def porte_aprovacao_extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Etapa extract_and_load da porte_aprovacao."""
    return extract_load_data(
        table_id=PORTE_APROVACAO_TABLE_ID, download_params=download_params
    )


def uf_aprovacao_get_latest_update() -> SourceInspection:
    """Etapa check_update da uf_aprovacao."""
    return get_latest_update(table_id=UF_APROVACAO_TABLE_ID)


def uf_aprovacao_extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Etapa extract_and_load da uf_aprovacao."""
    return extract_load_data(
        table_id=UF_APROVACAO_TABLE_ID, download_params=download_params
    )


def setor_cnae_aprovacao_get_latest_update() -> SourceInspection:
    """Etapa check_update da setor_cnae_aprovacao."""
    return get_latest_update(table_id=SETOR_CNAE_APROVACAO_TABLE_ID)


def setor_cnae_aprovacao_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da setor_cnae_aprovacao."""
    return extract_load_data(
        table_id=SETOR_CNAE_APROVACAO_TABLE_ID, download_params=download_params
    )


def subsetor_cnae_aprovacao_get_latest_update() -> SourceInspection:
    """Etapa check_update da subsetor_cnae_aprovacao."""
    return get_latest_update(table_id=SUBSETOR_CNAE_APROVACAO_TABLE_ID)


def subsetor_cnae_aprovacao_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da subsetor_cnae_aprovacao."""
    return extract_load_data(
        table_id=SUBSETOR_CNAE_APROVACAO_TABLE_ID,
        download_params=download_params,
    )


def setor_bndes_aprovacao_get_latest_update() -> SourceInspection:
    """Etapa check_update da setor_bndes_aprovacao."""
    return get_latest_update(table_id=SETOR_BNDES_APROVACAO_TABLE_ID)


def setor_bndes_aprovacao_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da setor_bndes_aprovacao."""
    return extract_load_data(
        table_id=SETOR_BNDES_APROVACAO_TABLE_ID,
        download_params=download_params,
    )


def subsetor_bndes_aprovacao_get_latest_update() -> SourceInspection:
    """Etapa check_update da subsetor_bndes_aprovacao."""
    return get_latest_update(table_id=SUBSETOR_BNDES_APROVACAO_TABLE_ID)


def subsetor_bndes_aprovacao_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da subsetor_bndes_aprovacao."""
    return extract_load_data(
        table_id=SUBSETOR_BNDES_APROVACAO_TABLE_ID,
        download_params=download_params,
    )


def exim_aprovacao_get_latest_update() -> SourceInspection:
    """Etapa check_update da exim_aprovacao."""
    return get_latest_update(table_id=EXIM_APROVACAO_TABLE_ID)


def exim_aprovacao_extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Etapa extract_and_load da exim_aprovacao."""
    return extract_load_data(
        table_id=EXIM_APROVACAO_TABLE_ID, download_params=download_params
    )


def produto_direta_aprovacao_dolar_get_latest_update() -> SourceInspection:
    """Etapa check_update da produto_direta_aprovacao_dolar."""
    return get_latest_update(table_id=PRODUTO_DIRETA_APROVACAO_DOLAR_TABLE_ID)


def produto_direta_aprovacao_dolar_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da produto_direta_aprovacao_dolar."""
    return extract_load_data(
        table_id=PRODUTO_DIRETA_APROVACAO_DOLAR_TABLE_ID,
        download_params=download_params,
    )


def produto_indireta_aprovacao_dolar_get_latest_update() -> SourceInspection:
    """Etapa check_update da produto_indireta_aprovacao_dolar."""
    return get_latest_update(
        table_id=PRODUTO_INDIRETA_APROVACAO_DOLAR_TABLE_ID
    )


def produto_indireta_aprovacao_dolar_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da produto_indireta_aprovacao_dolar."""
    return extract_load_data(
        table_id=PRODUTO_INDIRETA_APROVACAO_DOLAR_TABLE_ID,
        download_params=download_params,
    )


def exim_aprovacao_dolar_get_latest_update() -> SourceInspection:
    """Etapa check_update da exim_aprovacao_dolar."""
    return get_latest_update(table_id=EXIM_APROVACAO_DOLAR_TABLE_ID)


def exim_aprovacao_dolar_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da exim_aprovacao_dolar."""
    return extract_load_data(
        table_id=EXIM_APROVACAO_DOLAR_TABLE_ID, download_params=download_params
    )


def exim_setor_cnae_aprovacao_dolar_get_latest_update() -> SourceInspection:
    """Etapa check_update da exim_setor_cnae_aprovacao_dolar."""
    return get_latest_update(table_id=EXIM_SETOR_CNAE_APROVACAO_DOLAR_TABLE_ID)


def exim_setor_cnae_aprovacao_dolar_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da exim_setor_cnae_aprovacao_dolar."""
    return extract_load_data(
        table_id=EXIM_SETOR_CNAE_APROVACAO_DOLAR_TABLE_ID,
        download_params=download_params,
    )


def exim_subsetor_cnae_aprovacao_dolar_get_latest_update() -> SourceInspection:
    """Etapa check_update da exim_subsetor_cnae_aprovacao_dolar."""
    return get_latest_update(
        table_id=EXIM_SUBSETOR_CNAE_APROVACAO_DOLAR_TABLE_ID
    )


def exim_subsetor_cnae_aprovacao_dolar_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da exim_subsetor_cnae_aprovacao_dolar."""
    return extract_load_data(
        table_id=EXIM_SUBSETOR_CNAE_APROVACAO_DOLAR_TABLE_ID,
        download_params=download_params,
    )


def produto_direta_consulta_get_latest_update() -> SourceInspection:
    """Etapa check_update da produto_direta_consulta."""
    return get_latest_update(table_id=PRODUTO_DIRETA_CONSULTA_TABLE_ID)


def produto_direta_consulta_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da produto_direta_consulta."""
    return extract_load_data(
        table_id=PRODUTO_DIRETA_CONSULTA_TABLE_ID,
        download_params=download_params,
    )


def produto_indireta_consulta_get_latest_update() -> SourceInspection:
    """Etapa check_update da produto_indireta_consulta."""
    return get_latest_update(table_id=PRODUTO_INDIRETA_CONSULTA_TABLE_ID)


def produto_indireta_consulta_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da produto_indireta_consulta."""
    return extract_load_data(
        table_id=PRODUTO_INDIRETA_CONSULTA_TABLE_ID,
        download_params=download_params,
    )


def porte_consulta_get_latest_update() -> SourceInspection:
    """Etapa check_update da porte_consulta."""
    return get_latest_update(table_id=PORTE_CONSULTA_TABLE_ID)


def porte_consulta_extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Etapa extract_and_load da porte_consulta."""
    return extract_load_data(
        table_id=PORTE_CONSULTA_TABLE_ID, download_params=download_params
    )


def uf_consulta_get_latest_update() -> SourceInspection:
    """Etapa check_update da uf_consulta."""
    return get_latest_update(table_id=UF_CONSULTA_TABLE_ID)


def uf_consulta_extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Etapa extract_and_load da uf_consulta."""
    return extract_load_data(
        table_id=UF_CONSULTA_TABLE_ID, download_params=download_params
    )


def setor_cnae_consulta_get_latest_update() -> SourceInspection:
    """Etapa check_update da setor_cnae_consulta."""
    return get_latest_update(table_id=SETOR_CNAE_CONSULTA_TABLE_ID)


def setor_cnae_consulta_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da setor_cnae_consulta."""
    return extract_load_data(
        table_id=SETOR_CNAE_CONSULTA_TABLE_ID, download_params=download_params
    )


def subsetor_cnae_consulta_get_latest_update() -> SourceInspection:
    """Etapa check_update da subsetor_cnae_consulta."""
    return get_latest_update(table_id=SUBSETOR_CNAE_CONSULTA_TABLE_ID)


def subsetor_cnae_consulta_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da subsetor_cnae_consulta."""
    return extract_load_data(
        table_id=SUBSETOR_CNAE_CONSULTA_TABLE_ID,
        download_params=download_params,
    )


def setor_bndes_consulta_get_latest_update() -> SourceInspection:
    """Etapa check_update da setor_bndes_consulta."""
    return get_latest_update(table_id=SETOR_BNDES_CONSULTA_TABLE_ID)


def setor_bndes_consulta_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da setor_bndes_consulta."""
    return extract_load_data(
        table_id=SETOR_BNDES_CONSULTA_TABLE_ID, download_params=download_params
    )


def subsetor_bndes_consulta_get_latest_update() -> SourceInspection:
    """Etapa check_update da subsetor_bndes_consulta."""
    return get_latest_update(table_id=SUBSETOR_BNDES_CONSULTA_TABLE_ID)


def subsetor_bndes_consulta_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da subsetor_bndes_consulta."""
    return extract_load_data(
        table_id=SUBSETOR_BNDES_CONSULTA_TABLE_ID,
        download_params=download_params,
    )


def exim_consulta_get_latest_update() -> SourceInspection:
    """Etapa check_update da exim_consulta."""
    return get_latest_update(table_id=EXIM_CONSULTA_TABLE_ID)


def exim_consulta_extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Etapa extract_and_load da exim_consulta."""
    return extract_load_data(
        table_id=EXIM_CONSULTA_TABLE_ID, download_params=download_params
    )


def produto_direta_consulta_dolar_get_latest_update() -> SourceInspection:
    """Etapa check_update da produto_direta_consulta_dolar."""
    return get_latest_update(table_id=PRODUTO_DIRETA_CONSULTA_DOLAR_TABLE_ID)


def produto_direta_consulta_dolar_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da produto_direta_consulta_dolar."""
    return extract_load_data(
        table_id=PRODUTO_DIRETA_CONSULTA_DOLAR_TABLE_ID,
        download_params=download_params,
    )


def produto_indireta_consulta_dolar_get_latest_update() -> SourceInspection:
    """Etapa check_update da produto_indireta_consulta_dolar."""
    return get_latest_update(table_id=PRODUTO_INDIRETA_CONSULTA_DOLAR_TABLE_ID)


def produto_indireta_consulta_dolar_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da produto_indireta_consulta_dolar."""
    return extract_load_data(
        table_id=PRODUTO_INDIRETA_CONSULTA_DOLAR_TABLE_ID,
        download_params=download_params,
    )


def exim_consulta_dolar_get_latest_update() -> SourceInspection:
    """Etapa check_update da exim_consulta_dolar."""
    return get_latest_update(table_id=EXIM_CONSULTA_DOLAR_TABLE_ID)


def exim_consulta_dolar_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da exim_consulta_dolar."""
    return extract_load_data(
        table_id=EXIM_CONSULTA_DOLAR_TABLE_ID, download_params=download_params
    )


def exim_setor_cnae_consulta_dolar_get_latest_update() -> SourceInspection:
    """Etapa check_update da exim_setor_cnae_consulta_dolar."""
    return get_latest_update(table_id=EXIM_SETOR_CNAE_CONSULTA_DOLAR_TABLE_ID)


def exim_setor_cnae_consulta_dolar_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da exim_setor_cnae_consulta_dolar."""
    return extract_load_data(
        table_id=EXIM_SETOR_CNAE_CONSULTA_DOLAR_TABLE_ID,
        download_params=download_params,
    )


def exim_subsetor_cnae_consulta_dolar_get_latest_update() -> SourceInspection:
    """Etapa check_update da exim_subsetor_cnae_consulta_dolar."""
    return get_latest_update(
        table_id=EXIM_SUBSETOR_CNAE_CONSULTA_DOLAR_TABLE_ID
    )


def exim_subsetor_cnae_consulta_dolar_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da exim_subsetor_cnae_consulta_dolar."""
    return extract_load_data(
        table_id=EXIM_SUBSETOR_CNAE_CONSULTA_DOLAR_TABLE_ID,
        download_params=download_params,
    )
