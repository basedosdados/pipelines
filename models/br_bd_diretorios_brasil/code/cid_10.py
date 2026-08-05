import zipfile
from enum import Enum
from pathlib import Path

import pandas as pd
import requests

from pipelines.utils.tasks import _upload_to_gcs


class constants(Enum):
    """Constants for the cid_10 pipeline."""

    IVIS_FILENAME = "PREVISAO_TABELA_CID10.xlsx"
    IVIS_URL_BASE = "http://plataforma.saude.gov.br/cc-br-fic"

    DATASUS_URL_BASE = "http://www2.datasus.gov.br/cid10/V2008/downloads"
    DATASUS_FILENAME = "CID10CSV.zip"

    CHAPTER_DF_NAME = "CID-10-CAPITULOS"
    CHAPTER_RENAME_MAPPING = {
        "NUMCAP": "capitulo",
        "CATINIC": "categoria_inicio",
        "CATFIM": "categoria_fim",
        "DESCRICAO": "descricao_capitulo",
        "DESCRABREV": "descricao_abreviada_capitulo",
    }

    IVIS_RENAME_MAPPING = {
        "co_categoria_subcategoria": "subcategoria_cp",
        "co_agrupamento": "agrupamento",
        "co_categoria_pai": "categoria",
        "no_categoria_subcategoria": "descricao_categoria_subcategoria",
        "st_cruz": "indicador_cruz",
        "st_asterisco": "indicador_asterisco",
        "co_categ_subcateg_sp": "subcategoria",
        "st_registro_ativo": "indicador_registro_ativo",
        "dt_inclusao": "data_inclusao",
        "dt_atualizacao": "data_atualizacao",
    }

    CAUSA_VIOLENCIA = [
        "W32",
        "W33",
        "W34",
        "X85",
        "X86",
        "X87",
        "X88",
        "X89",
        "X90",
        "X91",
        "X92",
        "X93",
        "X94",
        "X95",
        "X96",
        "X97",
        "X98",
        "X99",
        "Y00",
        "Y01",
        "Y02",
        "Y03",
        "Y04",
        "Y05",
        "Y06",
        "Y07",
        "Y08",
        "Y09",
        "Y10",
        "Y11",
        "Y12",
        "Y13",
        "Y14",
        "Y15",
        "Y16",
        "Y17",
        "Y18",
        "Y19",
        "Y20",
        "Y21",
        "Y22",
        "Y23",
        "Y24",
        "Y25",
        "Y26",
        "Y27",
        "Y28",
        "Y29",
        "Y30",
        "Y31",
        "Y32",
        "Y33",
        "Y34",
        "Y35",
        "Y87",
        "Y89",
    ]
    CAUSA_OVERDOSE = [
        "F10",
        "F11",
        "F12",
        "F14",
        "F16",
        "F19",
        "T40",
        "T41",
        "T42",
        "T43",
        "T44",
        "T45",
        "T46",
        "T47",
        "T48",
        "T49",
        "T50",
        "X42",
        "X43",
        "X44",
        "X45",
        "X46",
        "X47",
        "X48",
        "X49",
        "X60",
        "X61",
        "X62",
        "X63",
        "X64",
        "X65",
        "X66",
        "X67",
        "X68",
        "X69",
        "Y12",
        "Y13",
        "Y14",
        "Y15",
        "Y16",
        "Y49",
        "Y50",
        "Y51",
        "Z64",
        "Z65",
    ]

    FINAL_COLUMNS = [
        "subcategoria",
        "descricao_subcategoria",
        "categoria",
        "descricao_categoria",
        "capitulo",
        "descricao_capitulo",
        "causa_violencia",
        "causa_overdose",
        "indicador_registro_ativo",
    ]


# DATASUS
def download_datasus(input_dir: Path) -> None:
    """Download and unzip the DATASUS CID-10 CSV package into `input_dir`."""
    input_dir.mkdir(parents=True, exist_ok=True)

    response = requests.get(
        f"{constants.DATASUS_URL_BASE.value}/{constants.DATASUS_FILENAME.value}"
    )
    response.raise_for_status()

    zip_path = input_dir / constants.DATASUS_FILENAME.value
    with open(zip_path, "wb") as fp:
        fp.write(response.content)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(input_dir)


# IVIS
def download_ivis(input_dir: Path) -> None:
    """Download the IVIS categoria/subcategoria spreadsheet into `input_dir`."""
    input_dir.mkdir(parents=True, exist_ok=True)

    response = requests.get(
        f"{constants.IVIS_URL_BASE.value}/{constants.IVIS_FILENAME.value}"
    )
    response.raise_for_status()

    xlsx_path = input_dir / constants.IVIS_FILENAME.value

    with open(xlsx_path, "wb") as fp:
        fp.write(response.content)


def _expandir_categorias(row) -> list:
    """Expand a chapter's `categoria_inicio`..`categoria_fim` bounds into every code in between."""
    letra_inicio, numero_inicio = (
        row["categoria_inicio"][0],
        int(row["categoria_inicio"][1:]),
    )
    letra_fim, numero_fim = (
        row["categoria_fim"][0],
        int(row["categoria_fim"][1:]),
    )

    categorias = []
    for letra in row["letras_array"]:
        inicio = numero_inicio if letra == letra_inicio else 0
        fim = numero_fim if letra == letra_fim else 99
        categorias.extend(
            f"{letra}{numero:02d}" for numero in range(inicio, fim + 1)
        )
    return categorias


def fix_invalid_chars(value):
    """Repair the mojibake left by IVIS exporting UTF-8 text through cp1252."""
    if isinstance(value, str):
        try:
            return value.encode("gbk").decode("cp1252")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return value
    return value


def build_chapter_df(input_path: Path) -> pd.DataFrame:
    """Linearize the DATASUS chapters table into one row per `categoria` code."""
    csv_paths = sorted(input_path.glob("*.CSV")) + sorted(
        input_path.glob("*.csv")
    )
    datasus_dfs = {
        csv_path.stem: pd.read_csv(csv_path, sep=";", encoding="latin-1")
        for csv_path in csv_paths
    }
    df_chapter_raw = datasus_dfs[constants.CHAPTER_DF_NAME.value].rename(
        columns=constants.CHAPTER_RENAME_MAPPING.value
    )

    df_chapter_raw["letra_inicio"] = df_chapter_raw["categoria_inicio"].str[0]
    df_chapter_raw["letra_fim"] = df_chapter_raw["categoria_fim"].str[0]
    df_chapter_raw["letras_array"] = df_chapter_raw.apply(
        lambda row: [
            chr(c)
            for c in range(ord(row["letra_inicio"]), ord(row["letra_fim"]) + 1)
        ],
        axis=1,
    )
    df_chapter_raw["categorias_array"] = df_chapter_raw.apply(
        _expandir_categorias, axis=1
    )

    df_chapter_linear = (
        df_chapter_raw.explode("categorias_array")
        .rename(columns={"categorias_array": "categoria"})
        .drop(columns=["letras_array", "letra_inicio", "letra_fim"])
    )
    df_chapter_linear = df_chapter_linear.drop(
        columns=[
            "capitulo",
            "categoria_inicio",
            "categoria_fim",
            "descricao_capitulo",
            "Unnamed: 5",
        ]
    )
    df_chapter_linear[["capitulo", "descricao_capitulo"]] = df_chapter_linear[
        "descricao_abreviada_capitulo"
    ].str.extract(r"([A-Z]+)\.\s*(.+)")
    return df_chapter_linear[["capitulo", "descricao_capitulo", "categoria"]]


def build_categoria_subcategoria(input_path: Path) -> tuple:
    """Split the cleaned IVIS table into a categoria-level and a subcategoria-level frame."""
    df_raw = pd.read_excel(input_path)
    df_raw = df_raw.map(fix_invalid_chars)
    df_raw = df_raw.rename(
        columns={
            "co_categoria_subcategoria": "subcategoria_cp",
            "co_agrupamento": "agrupamento",
            "co_categoria_pai": "categoria",
            "no_categoria_subcategoria": "descricao_categoria_subcategoria",
            "st_cruz": "indicador_cruz",
            "st_asterisco": "indicador_asterisco",
            "co_categ_subcateg_sp": "subcategoria",
            "st_registro_ativo": "indicador_registro_ativo",
            "dt_inclusao": "data_inclusao",
            "dt_atualizacao": "data_atualizacao",
        }
    )
    df_categorias = df_raw[df_raw["subcategoria"] == df_raw["categoria"]][
        ["categoria", "descricao_categoria_subcategoria"]
    ].copy()
    df_subcategorias = df_raw[
        [
            "categoria",
            "subcategoria",
            "descricao_categoria_subcategoria",
            "indicador_cruz",
            "indicador_asterisco",
            "indicador_registro_ativo",
            "data_inclusao",
            "data_atualizacao",
        ]
    ].copy()
    df_categorias = df_categorias.rename(
        columns={"descricao_categoria_subcategoria": "descricao_categoria"}
    )
    df_subcategorias = df_subcategorias.rename(
        columns={"descricao_categoria_subcategoria": "descricao_subcategoria"}
    )
    return df_categorias, df_subcategorias


def build_final(
    df_chapter: pd.DataFrame,
    df_categorias: pd.DataFrame,
    df_subcategorias: pd.DataFrame,
) -> pd.DataFrame:
    """Merge chapter, categoria and subcategoria frames into the final cid_10 table."""
    df_final = df_chapter.merge(
        df_categorias, on="categoria", how="left"
    ).merge(df_subcategorias, on=["categoria"], how="left")
    df_final["indicador_registro_ativo"] = df_final[
        "indicador_registro_ativo"
    ].replace({"N": 0, "S": 1})
    df_final["subcategoria"] = df_final["subcategoria"].fillna(
        df_final["categoria"]
    )
    df_final["descricao_subcategoria"] = df_final[
        "descricao_subcategoria"
    ].fillna(df_final["descricao_categoria"])

    df_final["causa_violencia"] = (
        df_final["categoria"]
        .isin(constants.CAUSA_VIOLENCIA.value)
        .replace({True: 1, False: 0})
    )
    df_final["causa_overdose"] = (
        df_final["categoria"]
        .isin(constants.CAUSA_OVERDOSE.value)
        .replace({True: 1, False: 0})
    )

    df_final = df_final[constants.FINAL_COLUMNS.value]
    return df_final.drop_duplicates(subset=df_final.columns)


if __name__ == "__main__":
    dataset_id = "br_bd_diretorios_brasil"
    table_id = "cid_10"
    root_dir = Path(__file__).parent.parent.parent.parent
    input_dir = root_dir / "tmp" / dataset_id / table_id / "input"
    output_dir = root_dir / "tmp" / dataset_id / table_id / "output"

    input_dir.mkdir(exist_ok=True, parents=True)
    output_dir.mkdir(exist_ok=True, parents=True)

    download_datasus(input_dir)
    download_ivis(input_dir)

    df_chapter = build_chapter_df(input_dir)
    df_categorias, df_subcategorias = build_categoria_subcategoria(
        input_dir / constants.IVIS_FILENAME.value
    )

    df_final = build_final(df_chapter, df_categorias, df_subcategorias)
    print(df_final.columns)
    df_final = df_final.drop_duplicates(subset=df_final.columns)
    df_final.to_csv(output_dir / "data.csv", encoding="latin1", index=False)

    _upload_to_gcs(
        data_path=output_dir / "data.csv",
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
    )
