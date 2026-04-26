"""
Tasks for br_me_rais
"""

import ftplib
import gc
import re
from pathlib import Path

import basedosdados as bd
import numpy as np
import pandas as pd
from prefect import task

from pipelines.datasets.br_me_rais.constants import constants as rais_constants
from pipelines.datasets.br_me_rais.utils import download_rais_file
from pipelines.utils.metadata.utils import get_api_most_recent_date
from pipelines.utils.utils import constants as utils_constants
from pipelines.utils.utils import log, to_partitions

CHUNKSIZE = 1_000_000


@task
def build_table_paths(
    table_id: str,
    parent_dir: Path = rais_constants.DATASET_DIR.value,
) -> tuple[Path, Path]:
    parent_dir = Path(parent_dir)
    parent_dir.mkdir(parents=True, exist_ok=True)

    table_dir = parent_dir / table_id
    table_dir.mkdir(exist_ok=True)
    input_dir = table_dir / "input"
    output_dir = table_dir / "output"
    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)

    return input_dir, output_dir


@task
def get_source_last_year(
    ftp_host: str = rais_constants.FTP_HOST.value,
) -> int:
    """Connect to MTE FTP and return the most recent available RAIS year."""
    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    ftp.encoding = "latin-1"
    ftp.cwd(rais_constants.REMOTE_DIR.value)
    try:
        items = ftp.nlst()
        years = [
            int(m.group(0))
            for item in items
            if (m := re.search(r"^\d{4}$", item))
        ]
        years.sort(reverse=True)
        last_year = years[0]
        log(f"Source last year: {last_year}")
        return last_year
    finally:
        ftp.quit()


@task
def get_table_last_year(dataset_id: str, table_id: str) -> int:
    """Return the most recent year already materialized in the BD table."""
    backend = bd.Backend(graphql_url=utils_constants.API_URL.value["prod"])
    date_str = get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        backend=backend,
        date_format="%Y",
    )
    last_year = int(str(date_str)[:4])
    log(f"Table last year: {last_year}")
    return last_year


@task
def generate_year_range(
    table_last_year: int, source_last_year: int
) -> list[int]:
    """Return the list of years to fetch: from (table_last_year + 1) to source_last_year."""
    start = max(table_last_year + 1, rais_constants.START_YEAR.value)
    years = list(range(start, source_last_year + 1))
    log(f"Years to process: {years}")
    return years


@task
def crawl_rais_ftp(
    year: int,
    table_id: str,
    input_dir: Path,
    ftp_host: str = rais_constants.FTP_HOST.value,
) -> list[dict]:
    """Download all .7z files for a given year and table from RAIS FTP."""
    year_dir = str(year)

    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    ftp.encoding = "latin-1"
    ftp.cwd(f"{rais_constants.REMOTE_DIR.value}/{year_dir}")

    available = ftp.nlst()

    if table_id == "microdados_estabelecimentos":
        files_to_download = [rais_constants.ESTAB_FILE.value]
    else:
        # Discover vinculos files dynamically — name and count vary by year
        files_to_download = [
            f for f in available if f.startswith("RAIS_VINC_PUB_")
        ]

    log(f"Files to download for {table_id} {year}: {files_to_download}")

    year_input_dir = Path(input_dir) / year_dir
    year_input_dir.mkdir(parents=True, exist_ok=True)

    failed = []
    success_count = 0
    try:
        for filename in files_to_download:
            ok, err = download_rais_file(ftp, filename, year_input_dir)
            if ok:
                success_count += 1
            else:
                failed.append(err)
                log(f"Failed to download {filename} for year {year}")
    finally:
        ftp.quit()

    if success_count == 0:
        raise RuntimeError(
            f"No files downloaded successfully for {table_id} year {year}. Failures: {failed}"
        )

    log(
        f"Downloaded {success_count}/{len(files_to_download)} files for year {year}"
    )
    return failed


@task
def build_partitions(
    table_id: str,
    year: int,
    input_dir: Path,
    output_dir: Path,
) -> Path:
    """Clean and partition RAIS data for one year, writing CSV output."""
    year_input_dir = Path(input_dir) / str(year)
    output_dir = Path(output_dir)

    df_municipio = bd.read_sql(
        "SELECT id_municipio, id_municipio_6, sigla_uf FROM `basedosdados.br_bd_diretorios_brasil.municipio`",
        billing_project_id="basedosdados",
        from_file=True,
    )

    if table_id == "microdados_estabelecimentos":
        _build_estab_partitions(year, year_input_dir, output_dir, df_municipio)
    else:
        _build_vinculos_partitions(
            year, year_input_dir, output_dir, df_municipio
        )

    return output_dir


def _detect_csv_file(directory: Path) -> Path:
    """Return the extracted data file (.comt for 2024+, .txt for earlier years)."""
    for ext in ("*.comt", "*.txt"):
        matches = list(directory.glob(ext))
        if matches:
            return matches[0]
    raise FileNotFoundError(f"No .comt or .txt file found in {directory}")


def _build_estab_partitions(
    year: int,
    input_dir: Path,
    output_dir: Path,
    df_municipio: pd.DataFrame,
) -> None:
    df_uf = bd.read_sql(
        "SELECT id_uf, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id="basedosdados",
        reauth=False,
    )

    csv_file = _detect_csv_file(input_dir)
    log(f"Processing establishments file: {csv_file}")

    rename = rais_constants.ESTAB_RENAME.value
    vars_list = rais_constants.ESTAB_VARS.value

    invalid_codes = [
        "0000",
        "00000",
        "000000",
        "0000000",
        "0000-1",
        "000-1",
        "998",
        "999",
        "9999",
        "9997",
        "00",
        "-1",
    ]
    numeric_cols = [
        "id_municipio",
        "quantidade_vinculos_ativos",
        "quantidade_vinculos_clt",
        "quantidade_vinculos_estatutarios",
        "tamanho",
        "indicador_cei_vinculado",
        "indicador_pat",
        "indicador_simples",
        "indicador_rais_negativa",
        "indicador_atividade_ano",
    ]

    for df in pd.read_csv(
        csv_file, encoding="latin1", sep=",", dtype=str, chunksize=CHUNKSIZE
    ):
        df = df.rename(columns=rename)
        df["ano"] = str(year)

        df["municipio"] = df["municipio"].astype(str)
        df = df.merge(
            df_municipio[["id_municipio", "id_municipio_6"]],
            left_on="municipio",
            right_on="id_municipio_6",
            how="left",
        )
        df = df.drop(columns=["id_municipio_6", "municipio"])

        df["uf"] = df["uf"].astype(str)
        df = df.merge(df_uf, left_on="uf", right_on="id_uf", how="left")
        df = df.drop(columns=["id_uf", "uf"])
        df = df.rename(columns={"sigla": "sigla_uf"})
        df["sigla_uf"] = df["sigla_uf"].replace(np.nan, "IGNORADO")

        for var in vars_list:
            if var not in df.columns:
                df[var] = ""

        df = df[vars_list]

        for col in [
            "bairros_sp",
            "distritos_sp",
            "bairros_fortaleza",
            "bairros_rj",
            "regioes_administrativas_df",
            "cnae_2",
            "cnae_2_subclasse",
            "subsetor_ibge",
            "subatividade_ibge",
        ]:
            df[col] = df[col].replace(invalid_codes, "")

        df["natureza_juridica"] = df["natureza_juridica"].replace(
            ["9990", "9999"], ""
        )
        df["cep"] = df["cep"].replace("0", "")

        for old, new in [
            ("CNPJ", "1"),
            ("Cnpj", "1"),
            ("01", "1"),
            ("1", "1"),
        ]:
            df["tipo"] = df["tipo"].replace(old, new)
        df["tipo"] = df["tipo"].replace(["CAEPF", "Caepf"], "2")
        df["tipo"] = df["tipo"].replace(
            ["CEI", "Cei", "CEI/CNO", "Cei/Cno", "CNO", "Cno", "03", "3"], "3"
        )

        df[numeric_cols] = df[numeric_cols].apply(
            pd.to_numeric, errors="coerce"
        )

        to_partitions(
            data=df,
            partition_columns=["ano", "sigla_uf"],
            savepath=output_dir,
            file_type="csv",
        )

        del df
        gc.collect()


def _build_vinculos_partitions(
    year: int,
    input_dir: Path,
    output_dir: Path,
    df_municipio: pd.DataFrame,
) -> None:
    rename = rais_constants.VINCULOS_RENAME.value
    vars_list = rais_constants.VINCULOS_VARS.value

    invalid_codes_bairros = [
        "0000",
        "00000",
        "000000",
        "0000000",
        "0000-1",
        "000-1",
        "9999",
        "9997",
    ]
    invalid_codes_general = [
        "0000",
        "00000",
        "000000",
        "0000000",
        "0000-1",
        "000-1",
    ]

    monetary_vars = [
        "tempo_emprego",
        "valor_remuneracao_janeiro",
        "valor_remuneracao_fevereiro",
        "valor_remuneracao_marco",
        "valor_remuneracao_abril",
        "valor_remuneracao_maio",
        "valor_remuneracao_junho",
        "valor_remuneracao_julho",
        "valor_remuneracao_agosto",
        "valor_remuneracao_setembro",
        "valor_remuneracao_outubro",
        "valor_remuneracao_novembro",
        "valor_remuneracao_dezembro",
        "valor_remuneracao_dezembro_sm",
        "valor_remuneracao_media",
        "valor_remuneracao_media_sm",
    ]

    # Process each regional file sequentially to manage 2GB+ memory
    regional_files = sorted(input_dir.glob("*.comt")) or sorted(
        input_dir.glob("*.txt")
    )
    if not regional_files:
        raise FileNotFoundError(f"No data files found in {input_dir}")

    for csv_file in regional_files:
        log(f"Processing vinculos file: {csv_file}")

        for df in pd.read_csv(
            csv_file,
            sep=",",
            encoding="latin1",
            low_memory=False,
            chunksize=CHUNKSIZE,
            dtype=str,
        ):
            df = df.rename(columns=rename)
            df["ano"] = str(year)

            df[["municipio", "id_municipio_trabalho"]] = df[
                ["municipio", "id_municipio_trabalho"]
            ].astype(str)

            df = df.merge(
                df_municipio,
                left_on="municipio",
                right_on="id_municipio_6",
                how="left",
            )
            df = df.merge(
                df_municipio,
                left_on="id_municipio_trabalho",
                right_on="id_municipio_6",
                how="left",
            )
            df = df.drop(
                columns=[
                    "id_municipio_trabalho",
                    "municipio",
                    "id_municipio_6_x",
                    "id_municipio_6_y",
                    "sigla_uf_y",
                ]
            )
            df = df.rename(
                columns={
                    "id_municipio_x": "id_municipio",
                    "sigla_uf_x": "sigla_uf",
                    "id_municipio_y": "id_municipio_trabalho",
                }
            )
            df["sigla_uf"] = df["sigla_uf"].replace([np.nan, "NI"], "IGNORADO")

            for var in vars_list:
                if var not in df.columns:
                    df[var] = ""

            df = df[vars_list]

            df = df.apply(
                lambda col: col.map(
                    lambda x: x.strip() if isinstance(x, str) else x
                )
            )

            for col in [
                "bairros_rj",
                "bairros_sp",
                "bairros_fortaleza",
                "distritos_sp",
                "regioes_administrativas_df",
            ]:
                df[col] = df[col].replace(invalid_codes_bairros, "")

            for col in [
                "cbo_1994",
                "cbo_2002",
                "cnae_1",
                "cnae_2",
                "cnae_2_subclasse",
                "ano_chegada_brasil",
            ]:
                df[col] = df[col].replace(invalid_codes_general, "")

            df["mes_admissao"] = df["mes_admissao"].replace("00", "")
            df["mes_desligamento"] = df["mes_desligamento"].replace("00", "")
            df["motivo_desligamento"] = df["motivo_desligamento"].replace(
                "0", ""
            )
            df["causa_desligamento_1"] = df["causa_desligamento_1"].replace(
                "99", ""
            )
            df["raca_cor"] = df["raca_cor"].replace("99", "9")
            df["natureza_juridica"] = df["natureza_juridica"].replace(
                ["9990", "9999"], ""
            )

            df["tipo_estabelecimento"] = df["tipo_estabelecimento"].replace(
                ["CNPJ", "Cnpj", "01", "1"], "1"
            )
            df["tipo_estabelecimento"] = df["tipo_estabelecimento"].replace(
                "CAEPF", "2"
            )
            df["tipo_estabelecimento"] = df["tipo_estabelecimento"].replace(
                ["CEI", "Cei", "CEI/CNO", "Cei/Cno", "CNO", "Cno", "03", "3"],
                "3",
            )

            for var in monetary_vars:
                df[var] = (
                    df[var]
                    .astype(str)
                    .str.replace(",", ".", regex=False)
                    .replace("n/d", np.nan)
                    .astype(float)
                )

            to_partitions(
                data=df,
                partition_columns=["ano", "sigla_uf"],
                savepath=output_dir,
                file_type="csv",
            )

            del df
            gc.collect()
