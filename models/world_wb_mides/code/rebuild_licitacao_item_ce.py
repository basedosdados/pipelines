"""Rebuild one CE exercise of the legacy `licitacao_item` staging partition.

WHY THIS EXISTS
---------------
The legacy staging partition `ano=2019/sigla_uf=CE` was a byte-identical copy of
`ano=2019/sigla_uf=RS` (md5 `A2esJ0D87PUWfJJBNnqDYg==`, 994,327 rows): the
partition cell of `licitacao_item.ipynb` shadowed its own loop variable. The genuine
CE 2019 items were never published. This script rebuilds them from the same TCE-CE
harvest the notebook read, and writes a file that drops into the existing staging
prefix next to the other legacy years.

INPUTS
------
The raw files are the Aug-2023 TCE-CE harvest kept in the legacy Colab Drive
(`ComprasPublicas_Brasil/input/CE/Licitações/<year>.zip`, plus
`input/CE/municipios.csv`). The source API (`api-dados-abertos.tce.ce.gov.br`) times
out from outside Brazil, so the archived harvest is also the only reachable copy.
Download and unzip it OUTSIDE Dropbox -- `~/Downloads` is a Dropbox symlink on the
maintainers' machines -- e.g. `~/Library/Caches/world_wb_mides_data/ce/raw<year>/`.

The tender ids the notebook cross-checks against come from the published
`licitacao` table, which is what `output/licitacao_ce.csv` became.

WHAT IS REPRODUCED, AND WHAT DELIBERATELY IS NOT
------------------------------------------------
The notebook's CE cell does not run as written on this harvest: it reads UTF-8
files as latin-1, merges unpadded `codigo_municipio` ("2") against "002", and
drops a `codigo_uf ` column (trailing space) that does not exist. The transform
below is the notebook's logic with those three mechanical fixes, calibrated on CE
2018 against the published partition (422,809 rows): it rebuilds 422,810 rows, and
every id, value and document matches on 422,808 of them. The published partition
was built from an earlier harvest and an earlier run, so three things differ:

  * `documento` -- the published years carry it as an integer round-trip, leading
    zeros stripped (CNPJs at 13 digits, CPFs at 9-10). This is reproduced, so the
    rebuilt year joins the other CE years and `licitacao_participante`. The
    notebook's `zfill` repair was never applied to any published year.
  * `quantidade` -- the published years truncated it to an integer (1,017 of
    422,809 rows in 2018). This is NOT reproduced: the column is FLOAT64 now, and
    the fraction is in the source.
  * text -- the published years lost every `&` and `Â` (TRÂNSITO -> TRNSITO);
    the source here has them, and they are kept. `nome_vencedor` is 96.9% filled
    in 2019 against 100% in the published 2018, because this harvest's
    `licitantes_*` files are less complete for about 23 municipalities.

Usage:
    .venv/bin/python models/world_wb_mides/code/rebuild_licitacao_item_ce.py \
        --year 2019 --raw-dir ~/Library/Caches/world_wb_mides_data/ce/raw2019 \
        --municipios ~/Library/Caches/world_wb_mides_data/ce/municipios.csv \
        --out-dir ~/Library/Caches/world_wb_mides_data/ce/out [--upload]
"""

from __future__ import annotations

import argparse
import glob
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account

CREDENTIALS = Path.home() / ".basedosdados/credentials/staging.json"
BUCKET = "basedosdados-dev"
STAGING = "staging/world_wb_mides/licitacao_item/ano={year}/sigla_uf=CE/microdados.csv"

# Column order of the staging external table; `ano` and `sigla_uf` come from the
# hive path and are not in the file.
COLUMNS = [
    "id_municipio",
    "orgao",
    "id_unidade_gestora",
    "id_licitacao_bd",
    "id_licitacao",
    "id_dispensa",
    "id_item_bd",
    "id_item",
    "descricao",
    "numero",
    "numero_lote",
    "unidade_medida",
    "quantidade_cotada",
    "valor_unitario_cotacao",
    "quantidade",
    "valor_unitario",
    "valor_total",
    "quantidade_proposta",
    "valor_proposta",
    "valor_vencedor",
    "nome_vencedor",
    "documento",
]


def read_all(pattern: str, year_at: int) -> pd.DataFrame:
    """Concatenate the per-municipality files, taking `ano` from the file name."""
    frames = []
    for f in sorted(glob.glob(pattern)):
        # Only an empty field is missing: pandas' default NA strings would turn a
        # literal "NA" or "NULL" description into a null.
        df = pd.read_csv(
            f,
            sep=";",
            dtype=str,
            encoding="utf-8",
            keep_default_na=False,
            na_values=[""],
        )
        df["ano"] = Path(f).name[year_at : year_at + 4]
        frames.append(df)
    if not frames:
        raise FileNotFoundError(pattern)
    return pd.concat(frames, ignore_index=True)


def tender_ids(year: int) -> pd.DataFrame:
    """CE tender ids for one exercise, from the published `licitacao` table."""
    creds = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS)
    )
    client = bigquery.Client(credentials=creds, project=creds.project_id)
    sql = f"""
        select safe_cast(ano as string) ano, id_municipio, id_licitacao, id_licitacao_bd
        from `basedosdados.world_wb_mides.licitacao`
        where sigla_uf = 'CE' and ano = {year}
    """
    return client.query(sql).to_dataframe()


def build(
    raw_dir: Path, municipios: Path, licitacao: pd.DataFrame, year: int
) -> pd.DataFrame:
    """The notebook's CE transform for one exercise; every column returned as str."""
    ce1 = read_all(str(raw_dir / f"itens_licitacoes_{year}_*.csv"), 17)
    ce1 = ce1.drop(columns=["codigo_tipo_negociante"]).rename(
        columns={
            "numero_licitacao": "id_licitacao",
            "descricao_item_licitacao": "descricao",
            "descricao_unidade_item_licitacao": "unidade_medida",
            "valor_vencedor_item_licitacao": "valor_total",
            "valor_unitario_item_licitacao": "valor_unitario",
            "numero_quantidade_item_licitacao": "quantidade",
            "numero_documento_negociante": "documento",
            "numero_sequencial_item_licitacao": "numero",
        }
    )
    # Item files write the TCE municipality code unpadded ("2"), the supplier
    # files and the crosswalk padded ("002").
    ce1["codigo_municipio"] = ce1["codigo_municipio"].str.zfill(3)
    id_mun = pd.read_csv(
        municipios,
        sep=";",
        dtype=str,
        encoding="latin-1",
        usecols=["geoibgeId", "codigo_municipio"],
    ).rename(columns={"geoibgeId": "id_municipio"})
    id_mun["codigo_municipio"] = id_mun["codigo_municipio"].str.zfill(3)
    ce1 = ce1.merge(id_mun, how="left", on="codigo_municipio")
    ce1["documento"] = ce1["documento"].astype(str).str.strip()

    ce2 = read_all(str(raw_dir / f"licitantes_{year}_*.csv"), 11)
    ce2 = ce2[
        [
            "codigo_municipio",
            "ano",
            "numero_licitacao",
            "numero_documento_negociante",
            "nome_negociante",
            "data_realizacao_licitacao",
        ]
    ].rename(
        columns={
            "numero_licitacao": "id_licitacao",
            "nome_negociante": "nome_vencedor",
            "numero_documento_negociante": "documento",
        }
    )
    ce2["codigo_municipio"] = ce2["codigo_municipio"].str.zfill(3)
    ce2["documento"] = ce2["documento"].astype(str).str.strip()

    keys = [
        "codigo_municipio",
        "ano",
        "id_licitacao",
        "documento",
        "data_realizacao_licitacao",
    ]
    ce = ce1.merge(ce2, how="left", on=keys)
    ce["sigla_uf"] = "CE"
    ce["id_licitacao_bd"] = (
        ce["id_licitacao"]
        + ce["id_municipio"]
        + ce["ano"].str[2:4]
        + ce["sigla_uf"]
    )

    # Where `licitacao` holds the tender under a different id, the item's id is
    # not trusted and left missing, as in the notebook.
    ce3 = licitacao[licitacao["ano"] == str(year)].rename(
        columns={"id_licitacao_bd": "id_licitacao_bd_2"}
    )
    ce = ce.merge(
        ce3,
        how="left",
        on=["id_municipio", "ano", "id_licitacao"],
        indicator=True,
    )
    ce["id_licitacao_bd"] = np.where(
        (ce["_merge"] == "both")
        & (ce["id_licitacao_bd"] != ce["id_licitacao_bd_2"]),
        np.nan,
        ce["id_licitacao_bd"],
    )
    ce = ce.drop(columns="data_realizacao_licitacao")

    ce["id_item"] = (
        ce["numero"].astype(str)
        + ce["id_municipio"]
        + ce["id_licitacao"]
        + ce["ano"].str[2:4]
    )
    ce["id_item_bd"] = ce["id_item"] + ce["sigla_uf"]
    ce = ce.drop_duplicates()
    # Same item id with a different description, quantity or value: the id is
    # ambiguous and is set missing.
    for same, distinct in [
        (["id_item_bd"], ["id_item_bd", "descricao"]),
        (
            ["id_item_bd", "descricao"],
            ["id_item_bd", "descricao", "quantidade"],
        ),
        (
            ["id_item_bd", "descricao", "quantidade"],
            ["id_item_bd", "descricao", "quantidade", "valor_total"],
        ),
    ]:
        ce["id_item_bd"] = np.where(
            ce.duplicated(same, keep=False)
            & ~ce.duplicated(distinct, keep=False),
            np.nan,
            ce["id_item_bd"],
        )

    ce["numero"] = ce["numero"].astype(int)
    for col in ["quantidade", "valor_unitario", "valor_total"]:
        ce[col] = ce[col].astype(float)
    ce["documento"] = ce["documento"].str.lstrip("0").replace("", "0")
    ce["nome_vencedor"] = ce["nome_vencedor"].replace(
        "JOSÉ LUS DE LIMA - ME", "JOSÉ LUIS DE LIMA - ME"
    )
    ce = ce.drop_duplicates().reindex(columns=["ano", "sigla_uf", *COLUMNS])

    # Round-trip through the notebook's CSV writer so floats carry its "%.2f",
    # then hold every column as text.
    buf = io.StringIO()
    ce.to_csv(buf, index=False, na_rep="", float_format="%.2f")
    buf.seek(0)
    return pd.read_csv(buf, dtype=str, keep_default_na=False)


def check(df: pd.DataFrame, licitacao: pd.DataFrame, year: int) -> None:
    """Fail loudly on the failure that motivated this script: rows from another UF."""
    assert (df["ano"] == str(year)).all() and (df["sigla_uf"] == "CE").all()
    assert df["id_municipio"].str.startswith("23").all(), (
        "non-CE municipality in CE output"
    )
    ids = df.loc[df["id_item_bd"] != "", "id_item_bd"]
    assert ids.is_unique, "duplicate id_item_bd"
    tenders = df.loc[df["id_licitacao_bd"] != "", "id_licitacao_bd"]
    print(
        json.dumps(
            {
                "rows": len(df),
                "municipalities": df["id_municipio"].nunique(),
                "tenders_in_items": tenders.nunique(),
                "tenders_in_licitacao": len(licitacao),
                "items_matching_licitacao": round(
                    tenders.isin(set(licitacao["id_licitacao_bd"])).mean(), 4
                ),
                "nome_vencedor_filled": round(
                    (df["nome_vencedor"] != "").mean(), 4
                ),
            }
        )
    )


def upload(path: Path, year: int) -> None:
    creds = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS)
    )
    client = storage.Client(credentials=creds, project=creds.project_id)
    # Requester-pays bucket; without user_project every call is a 400.
    bucket = client.bucket(BUCKET, user_project=creds.project_id)
    blob = bucket.blob(STAGING.format(year=year))
    blob.upload_from_filename(str(path), content_type="text/csv")
    blob.reload()
    print(f"gs://{BUCKET}/{blob.name}", blob.size, blob.md5_hash)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--municipios", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument(
        "--upload",
        action="store_true",
        help=f"replace the partition in gs://{BUCKET}",
    )
    args = parser.parse_args()

    licitacao = tender_ids(args.year)
    df = build(
        args.raw_dir.expanduser(),
        args.municipios.expanduser(),
        licitacao,
        args.year,
    )
    check(df, licitacao, args.year)

    out = args.out_dir.expanduser()
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / f"licitacao_item_ce_{args.year}.parquet", index=False)
    csv = out / f"licitacao_item_ce_{args.year}.csv"
    df[COLUMNS].to_csv(csv, index=False, encoding="utf-8")
    print(csv)
    if args.upload:
        upload(csv, args.year)


if __name__ == "__main__":
    main()
