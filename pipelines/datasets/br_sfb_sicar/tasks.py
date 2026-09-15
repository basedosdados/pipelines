"""Tasks for br_sfb_sicar.

⚠️ Este dataset NÃO segue o padrão simples de `CheckThenDownloadPipeline`
(1 tabela, 1 `DownloadResult`, dispatch pro `mat_test_flow` genérico) —
ver banner em `flows.py` pra detalhes de por quê. Só `check_for_update`
usa a cápsula; o resto do trabalho (download+clean+stage+dbt+test+
metadata das 9 tabelas de tema) é uma função só, portada quase sem
mudanças do flow monolítico antigo.
"""

import shutil
import tempfile
from datetime import date

from dateutil.relativedelta import relativedelta

from pipelines.crawler.sfb_sicar.tasks import (
    clean_uf_theme,
    download_uf_theme,
    get_release_dates_task,
)
from pipelines.crawler.sfb_sicar.utils import max_release_iso
from pipelines.datasets.br_sfb_sicar.constants import (
    DATASET_ID,
    DOWNLOAD_MAX_RETRIES,
    DOWNLOAD_TRIES,
    TABLE_TO_POLYGON,
    THEME_TABLES,
    UF_SIGLAS,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    CoverageSpec,
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
)
from pipelines.utils.metadata.tasks import register_table_materialization_task
from pipelines.utils.stage_dispatch import CheckResult
from pipelines.utils.tasks import run_dbt, upload_to_gcs

# ──────────────────────────────────────────────────────────────────────────────
# check_for_update — genuinamente leve, ver comentário original: "Cheap: one
# page fetch. Do the poll BEFORE downloading gigabytes of zips." Confirmado
# no código: `get_release_dates_task` só lê a página de datas de release do
# SICAR, não baixa nenhum zip. Esse dataset é mesmo "Padrão" (check leve),
# ao contrário de `br_ibge_ipca` (que era check_and_download disfarçado).
# ──────────────────────────────────────────────────────────────────────────────


def check_for_update() -> CheckResult:
    release_iso = get_release_dates_task()
    max_date = max_release_iso(release_iso)
    if max_date is None:
        raise RuntimeError(
            "SICAR release-dates page devolveu vazio — nenhuma UF com data "
            "de release."
        )
    return CheckResult(reference_date=date.fromisoformat(max_date))


# ──────────────────────────────────────────────────────────────────────────────
# download + materialização — NÃO usa DownloadResult/dispatch_mat_test
# genérico. Motivo: são 9 tabelas por execução (não 1), com upload próprio
# por UF/tema (checkpoint resumível em GCS, corrida de ~30h no backfill
# nacional) e testes dbt CRUZADOS entre as 9 tabelas depois que todas
# estão construídas ("Test after every theme is built: cross-table tests
# read sibling models"). O `mat_test_flow` genérico testa 1 tabela por vez
# — não dá pra encaixar sem quebrar essa depedência cruzada. Portado quase
# inalterado do corpo do flow monolítico antigo (`br_sfb_sicar_flow`),
# só sem a parte de poll/commit (que agora mora no check_update, via
# `check_update_and_dispatch` genérico).
# ──────────────────────────────────────────────────────────────────────────────


def coverage_for(
    table_id: str, min_data: date | None, max_data: date | None
) -> CoverageSpec:
    """Coverage spec pra uma tabela dado o span atual da coluna `data`.

    AllFree enquanto o histórico empilhado tiver <= 6 meses de span;
    PartBdpro (free_lag=6 meses) quando ultrapassar. `table_id` não é
    usado hoje (mantido pra facilitar exceção por tabela no futuro).
    """
    date_col = DateOnly(col="data")
    span_over_6mo = (
        min_data is not None
        and max_data is not None
        and max_data > min_data + relativedelta(months=6)
    )
    if span_over_6mo:
        return PartBdpro(
            date_column=date_col,
            date_format=DateFormat.YEAR_MD,
            free_lag=FreeLag(unit="months", value=6),
        )
    return AllFree(date_column=date_col, date_format=DateFormat.YEAR_MD)


def _bq_min_max_data(
    table_id: str, billing_project: str
) -> tuple[date | None, date | None]:
    from google.cloud import bigquery

    client = bigquery.Client(project=billing_project)
    sql = (
        f"SELECT MIN(data) AS mn, MAX(data) AS mx "
        f"FROM `basedosdados.{DATASET_ID}.{table_id}`"
    )
    try:
        row = next(iter(client.query(sql).result()))
    except Exception as exc:
        print(f"min/max(data) query failed for {table_id}: {exc}")
        return None, None
    return row["mn"], row["mx"]


def _staging_uf_done(
    bucket_name: str, table_id: str, snapshot_iso: str, sigla_uf: str
) -> bool:
    import basedosdados as bd

    st = bd.Storage(
        dataset_id=DATASET_ID,
        table_id=table_id,
        bucket_name=bucket_name,
        billing_project_id=bucket_name,
    )
    prefix = (
        f"staging/{DATASET_ID}/{table_id}/"
        f"data={snapshot_iso}/sigla_uf={sigla_uf}/"
    )
    try:
        return (
            next(
                iter(st.bucket.list_blobs(prefix=prefix, max_results=1)), None
            )
            is not None
        )
    except Exception as exc:
        print(f"staging check failed for {table_id}/{sigla_uf}: {exc}")
        return False


def _bq_table_exists(project: str, table_id: str) -> bool:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    sql = (
        f"SELECT COUNT(1) AS c FROM `{project}.{DATASET_ID}.__TABLES__` "
        f"WHERE table_id = '{table_id}'"
    )
    try:
        return next(iter(client.query(sql).result()))["c"] > 0
    except Exception as exc:
        print(f"table-exists check failed for {project}.{table_id}: {exc}")
        return False


def download_and_materialize(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    only_themes: str = "",
    only_ufs: str = "",
    clean_only: bool = False,
    stage_only: bool = False,
) -> None:
    """Baixa+limpa+stagea as 9 tabelas de tema (27 UFs cada) e materializa.

    Idêntico ao corpo do antigo `br_sfb_sicar_flow` depois do poll/commit
    (que agora acontece no `check_update`, genérico, antes de disparar
    este estágio). Busca `release_iso` de novo aqui (barato — mesma
    página de release dates do check) já que os dois estágios rodam em
    pods separados.
    """
    themes = [
        t
        for t in THEME_TABLES
        if not only_themes or t in only_themes.split(",")
    ]
    ufs = [u for u in UF_SIGLAS if not only_ufs or u in only_ufs.split(",")]

    release_iso = get_release_dates_task()

    work_dir = tempfile.mkdtemp(prefix="br_sfb_sicar_")
    input_dir = f"{work_dir}/input"
    output_dir = f"{work_dir}/output"
    dev_bucket = "basedosdados-dev"
    prod_bucket = "basedosdados"
    try:
        skipped: list[str] = []
        built_themes: list[str] = []
        for table in themes:
            polygon = TABLE_TO_POLYGON[table]
            theme_root = f"{output_dir}/{table}"
            fresh_dev = False
            fresh_prod = False
            any_staged = False
            for sigla_uf in ufs:
                snapshot_iso = release_iso.get(sigla_uf)
                if not snapshot_iso:
                    print(f"no release date for {sigla_uf}; skipping")
                    skipped.append(f"{table}/{sigla_uf} (no release date)")
                    continue
                dev_done = _staging_uf_done(
                    dev_bucket, table, snapshot_iso, sigla_uf
                )
                prod_done = not materialize_to_prod or _staging_uf_done(
                    prod_bucket, table, snapshot_iso, sigla_uf
                )
                if dev_done and prod_done:
                    any_staged = True
                    continue
                try:
                    zip_path = download_uf_theme(
                        input_dir=input_dir,
                        sigla_uf=sigla_uf,
                        polygon=polygon,
                        tries=DOWNLOAD_TRIES,
                        max_retries=DOWNLOAD_MAX_RETRIES,
                    )
                except Exception as exc:
                    print(
                        f"download failed for {sigla_uf} {table} after "
                        f"{DOWNLOAD_MAX_RETRIES} retries; SKIPPING this state "
                        f"and continuing: {exc}"
                    )
                    skipped.append(f"{table}/{sigla_uf} (download)")
                    continue
                rows = clean_uf_theme(
                    zip_path=zip_path,
                    output_dir=output_dir,
                    table=table,
                    snapshot_iso=snapshot_iso,
                    sigla_uf=sigla_uf,
                )
                if not rows:
                    continue
                if clean_only:
                    shutil.rmtree(theme_root, ignore_errors=True)
                    any_staged = True
                    continue
                if not dev_done:
                    upload_to_gcs(
                        data_path=theme_root,
                        dataset_id=DATASET_ID,
                        table_id=table,
                        bucket_name=dev_bucket,
                        dump_mode="append",
                        source_format="parquet",
                    )
                    fresh_dev = True
                if materialize_to_prod and not prod_done:
                    upload_to_gcs(
                        data_path=theme_root,
                        dataset_id=DATASET_ID,
                        table_id=table,
                        bucket_name=prod_bucket,
                        dump_mode="append",
                        source_format="parquet",
                    )
                    fresh_prod = True
                shutil.rmtree(theme_root, ignore_errors=True)
                any_staged = True

            if clean_only or not any_staged:
                continue
            built_themes.append(table)
            if stage_only:
                continue
            if fresh_dev or not _bq_table_exists(dev_bucket, table):
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target="dev",
                )
            if materialize_to_prod and (
                fresh_prod or not _bq_table_exists(prod_bucket, table)
            ):
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target="prod",
                )

        if skipped:
            print(
                f"WARNING: {len(skipped)} UF x theme combo(s) skipped "
                f"(source unreachable): {', '.join(skipped)}. A resubmit / "
                f"only_ufs re-run retries them (staged UFs are fast-forwarded)."
            )

        if clean_only:
            print("clean_only: cleaned without staging; returning")
            return

        if not built_themes:
            print("no UF x theme produced output; nothing to build")
            return

        if stage_only:
            print(
                f"stage_only: {len(built_themes)} theme(s) fully staged to GCS "
                f"(dev bucket); skipped dbt run/test and metadata."
            )
            return

        for table in built_themes:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="dev",
            )
            if materialize_to_prod:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="prod",
                )

        if update_metadata and materialize_to_prod:
            for table in built_themes:
                min_data, max_data = _bq_min_max_data(table, "basedosdados")
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage_for(table, min_data, max_data),
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
