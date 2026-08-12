"""
Lógica compartilhada para os datasets br_cgu_* — Prefect 3.
"""

from pipelines.crawler.cgu.tasks import (
    dict_for_table,
    get_current_date_and_download_file,
    partition_data,
    read_and_partition_beneficios_cidadao,
    verify_all_url_exists_to_download,
)
from pipelines.utils.metadata.domain import (
    CoverageSpec,
    DateFormat,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

# Formato em que cada tabela do br_cgu_beneficios_cidadao é gravada em disco por
# `read_and_partition_beneficios_cidadao`. Precisa casar com o que o
# `upload_to_gcs` procura: quando a staging já existe, ele chama `dump_header`,
# que estoura FileNotFoundError se não achar arquivo no formato declarado.
_SOURCE_FORMAT_BENEFICIOS_CIDADAO = {
    "novo_bolsa_familia": "parquet",
    "garantia_safra": "parquet",
    "bpc": "csv",
}


def _part_bdpro_year_month(year: str, month: str) -> PartBdpro:
    """Monta a cobertura padrão dos flows CGU: part_bdpro mensal.

    Args:
        year: nome da coluna de ano na tabela (ex. `ano`, `ano_extrato`).
        month: nome da coluna de mês na tabela (ex. `mes`, `mes_extrato`).

    Returns:
        Cobertura part_bdpro em granularidade ano/mês, com o free_lag
        padrão de 6 meses definido em `PartBdpro`.
    """
    return PartBdpro(
        date_column=YearMonth(year=year, month=month),
        date_format=DateFormat.YEAR_MONTH,
    )


def _materialize_and_metadata(
    *,
    filepath: str,
    dataset_id: str,
    table_id: str,
    dbt_alias: bool,
    target: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    coverage: CoverageSpec,
    source_format: str = "csv",
) -> None:
    """Sobe os dados particionados, materializa no dbt e atualiza metadados.

    Sempre roda a metade de dev (upload no bucket `basedosdados-dev` +
    `dbt run/test` no target dev). A metade de prod só roda com
    `materialize_after_dump`, e o registro de materialização só dentro dela.

    O Update da fonte original (`commit_source_update_task`) não é gravado
    aqui — os callers já o gravam mais cedo, logo depois que o poll confirma
    dado novo, antes de baixar/materializar.

    Args:
        filepath: caminho do diretório particionado gerado pelo passo de
            particionamento.
        dataset_id: id do dataset no BigQuery (ex. `br_cgu_servidores_publicos`).
        table_id: id da tabela dentro do dataset.
        dbt_alias: repassado ao `run_dbt`; indica se o modelo usa alias.
        target: target do dbt na etapa de prod. A etapa de dev é sempre
            `dev`, independente deste valor.
        materialize_after_dump: se falso, para depois do dbt em dev e não
            toca em prod nem em metadados.
        update_metadata: se verdadeiro, registra a materialização da
            tabela — sempre em `env="prod"`.
        coverage: cobertura da tabela, usada no registro da materialização.
        source_format: formato em que o passo de particionamento gravou os
            arquivos em disco (`csv` ou `parquet`). Vale para os dois
            uploads; se não casar com o que está no disco, o `dump_header`
            chamado pelo `upload_to_gcs` estoura `FileNotFoundError`.
    """
    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
        source_format=source_format,
    )
    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
        source_format=source_format,
    )
    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=coverage,
            env="prod",
            bq_project="basedosdados",
        )


def _run_cgu_cartao_pagamento(
    dataset_id: str,
    table_id: str,
    relative_month: int,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    """Roda uma tabela do br_cgu_cartao_pagamento de ponta a ponta.

    Baixa o mês seguinte ao último registrado nos metadados, para se a fonte
    não tiver nada novo, e então particiona, sobe e materializa.

    Args:
        dataset_id: id do dataset no BigQuery.
        table_id: id da tabela dentro do dataset.
        relative_month: quantos meses somar à última data registrada nos
            metadados para chegar no período a baixar.
        materialize_after_dump: se verdadeiro, materializa também em prod.
        dbt_alias: repassado ao `run_dbt`.
        update_metadata: se verdadeiro, atualiza cobertura da tabela e o
            Update da fonte original.
        target: target do dbt na etapa de prod.
        force_run: ignora o poll e roda mesmo sem dado novo na fonte.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    data_source_max_date = get_current_date_and_download_file(
        table_id, dataset_id, relative_month
    )

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data:
            return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=data_source_max_date,
        env="prod",
        date_format="%Y-%m",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    filepath = partition_data(table_id=table_id, dataset_id=dataset_id)
    _materialize_and_metadata(
        filepath=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        target=target,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        coverage=_part_bdpro_year_month("ano_extrato", "mes_extrato"),
    )


def _run_cgu_servidores_publicos(
    dataset_id: str,
    table_id: str,
    relative_month: int,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    """Roda uma tabela do br_cgu_servidores_publicos de ponta a ponta.

    Diferente dos outros flows CGU, checa antes se todas as URLs do mês
    existem — a fonte publica os arquivos de servidores em lotes, e baixar
    com parte deles no ar geraria mês incompleto.

    Args:
        dataset_id: id do dataset no BigQuery.
        table_id: id da tabela dentro do dataset.
        relative_month: quantos meses somar à última data registrada nos
            metadados para chegar no período a baixar.
        materialize_after_dump: se verdadeiro, materializa também em prod.
        dbt_alias: repassado ao `run_dbt`.
        update_metadata: se verdadeiro, atualiza cobertura da tabela e o
            Update da fonte original.
        target: target do dbt na etapa de prod.
        force_run: ignora a checagem de URLs e o poll, rodando mesmo sem
            dado novo na fonte.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    url_ok = verify_all_url_exists_to_download(
        dataset_id, table_id, relative_month
    )
    if not url_ok and not force_run:
        print("URLs não disponíveis; encerrando.")
        return

    data_source_max_date = get_current_date_and_download_file(
        table_id, dataset_id, relative_month
    )

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data:
            return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=data_source_max_date,
        env="prod",
        date_format="%Y-%m",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    filepath = partition_data(table_id=table_id, dataset_id=dataset_id)
    _materialize_and_metadata(
        filepath=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        target=target,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        coverage=_part_bdpro_year_month("ano", "mes"),
    )


def _run_cgu_licitacao_contrato(
    dataset_id: str,
    table_id: str,
    relative_month: int,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    """Roda uma tabela do br_cgu_licitacao_contrato de ponta a ponta.

    Args:
        dataset_id: id do dataset no BigQuery.
        table_id: id da tabela dentro do dataset.
        relative_month: quantos meses somar à última data registrada nos
            metadados para chegar no período a baixar.
        materialize_after_dump: se verdadeiro, materializa também em prod.
        dbt_alias: repassado ao `run_dbt`.
        update_metadata: se verdadeiro, atualiza cobertura da tabela e o
            Update da fonte original.
        target: target do dbt na etapa de prod.
        force_run: ignora o poll e roda mesmo sem dado novo na fonte.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    data_source_max_date = get_current_date_and_download_file(
        table_id=table_id, dataset_id=dataset_id, relative_month=relative_month
    )

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data:
            return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=data_source_max_date,
        env="prod",
        date_format="%Y-%m",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    filepath = partition_data(table_id=table_id, dataset_id=dataset_id)
    _materialize_and_metadata(
        filepath=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        target=target,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        coverage=_part_bdpro_year_month("ano", "mes"),
    )


def _run_cgu_beneficios_cidadao(
    dataset_id: str,
    table_id: str,
    relative_month: int,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    """Roda uma tabela do br_cgu_beneficios_cidadao de ponta a ponta.

    Único flow CGU que declara `source_format` por tabela: o
    `novo_bolsa_familia` e a `garantia_safra` são gravados em parquet, o
    `bpc` em csv (ver `_SOURCE_FORMAT_BENEFICIOS_CIDADAO`).

    Args:
        dataset_id: id do dataset no BigQuery.
        table_id: id da tabela dentro do dataset. Precisa estar em
            `_SOURCE_FORMAT_BENEFICIOS_CIDADAO`.
        relative_month: quantos meses somar à última data registrada nos
            metadados para chegar no período a baixar.
        materialize_after_dump: se verdadeiro, materializa também em prod.
        dbt_alias: repassado ao `run_dbt`.
        update_metadata: se verdadeiro, atualiza cobertura da tabela e o
            Update da fonte original.
        target: target do dbt na etapa de prod.
        force_run: ignora o poll e roda mesmo sem dado novo na fonte.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    data_source_max_date = get_current_date_and_download_file(
        table_id=table_id, dataset_id=dataset_id, relative_month=relative_month
    )

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data:
            return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=data_source_max_date,
        env="prod",
        date_format="%Y-%m",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    filepath = read_and_partition_beneficios_cidadao(table_id=table_id)
    _materialize_and_metadata(
        filepath=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        target=target,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        coverage=_part_bdpro_year_month(**dict_for_table(table_id)),
        source_format=_SOURCE_FORMAT_BENEFICIOS_CIDADAO[table_id],
    )
