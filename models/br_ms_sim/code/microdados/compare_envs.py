"""
Análise comparativa: BQ dev vs BQ prod vs Datasus FTP (raw .dbc).

Uso:
    uv run python compare_envs.py               # resumo por ano (todos os anos)
    uv run python compare_envs.py --year 1996   # detalha UF a UF para um ano

Seções produzidas:
    1. BQ dev — contagem e nulos por ano
    2. BQ prod — contagem e nulos por ano
    3. Dev vs Prod — diff de contagem
    4. Raw Datasus — baixa .dbc do ano(s) indicados e conta registros + colunas presentes
    5. BQ dev vs Raw — diff de contagem por UF (só quando --year é passado)
"""

import argparse
import os
import sys
import tempfile
import urllib.request
from pathlib import Path

import basedosdados as bd
import pandas as pd

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

DEV_TABLE = "`basedosdados-dev.br_ms_sim.microdados`"
PROD_TABLE = "`basedosdados.br_ms_sim.microdados`"
BILLING = "basedosdados-dev"

FTP_URL = "ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/DORES/DO{uf}{ano}.dbc"

UFS = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]

KEY_COLS = ["data_obito", "sexo", "causa_basica", "sequencial_obito"]

# ---------------------------------------------------------------------------
# Helpers BQ
# ---------------------------------------------------------------------------


def bq(query: str) -> pd.DataFrame:
    return bd.read_sql(query, billing_project_id=BILLING, from_file=True)


def section(title: str) -> None:
    bar = "=" * 70
    print(f"\n{bar}\n{title}\n{bar}")


def summary_query(table: str, years_sql: str) -> str:
    null_exprs = "\n            ".join(
        f"COUNTIF({c} IS NULL) AS {c}_null," for c in KEY_COLS
    )
    return f"""
        SELECT
            ano,
            COUNT(*) AS n,
            {null_exprs}
            COUNT(DISTINCT sigla_uf) AS n_ufs
        FROM {table}
        WHERE ano IN ({years_sql})
        GROUP BY ano
        ORDER BY ano
    """


def detail_query(table: str, ano: int) -> str:
    null_exprs = "\n            ".join(
        f"COUNTIF({c} IS NULL) AS {c}_null," for c in KEY_COLS
    )
    return f"""
        SELECT
            sigla_uf,
            COUNT(*) AS n,
            {null_exprs}
            COUNT(DISTINCT sequencial_obito) AS sequencial_distintos
        FROM {table}
        WHERE ano = {ano}
        GROUP BY sigla_uf
        ORDER BY sigla_uf
    """


# ---------------------------------------------------------------------------
# Helpers raw .dbc
# ---------------------------------------------------------------------------


def _read_dbc(filepath: str) -> pd.DataFrame:
    try:
        from datasus_dbc import decompress as dbc2dbf
        from dbfread import DBF
    except ImportError:
        print(
            "  [erro] datasus_dbc / dbfread não instalados.", file=sys.stderr
        )
        return pd.DataFrame()

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".dbf")
    os.close(tmp_fd)
    try:
        dbc2dbf(str(filepath), tmp_path)
        table = DBF(tmp_path, encoding="iso-8859-1", load=True)
        return pd.DataFrame(iter(table))
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def fetch_raw_uf(uf: str, ano: int, cache_dir: Path) -> pd.DataFrame | None:
    dest = cache_dir / f"DO{uf}{ano}.dbc"
    if not dest.exists():
        url = FTP_URL.format(uf=uf, ano=ano)
        try:
            urllib.request.urlretrieve(url, filename=str(dest))
        except Exception as e:
            print(f"    FTP não disponível: UF={uf}, ano={ano} — {e}")
            return None
    df = _read_dbc(str(dest))
    if df.empty:
        return None
    df.columns = df.columns.str.upper()
    return df


def raw_summary(ano: int, ufs: list[str], cache_dir: Path) -> pd.DataFrame:
    rows = []
    for uf in ufs:
        print(f"  baixando/lendo DO{uf}{ano}.dbc ...")
        df = fetch_raw_uf(uf, ano, cache_dir)
        if df is None:
            rows.append(
                {
                    "sigla_uf": uf,
                    "n_raw": 0,
                    "tem_DTOBITO": False,
                    "tem_SEXO": False,
                    "tem_CAUSABAS": False,
                    "tem_CONTADOR": False,
                    "DTOBITO_nao_nulo": 0,
                }
            )
            continue

        n = len(df)
        tem_dt = "DTOBITO" in df.columns
        n_dt = int(df["DTOBITO"].notna().sum()) if tem_dt else 0

        rows.append(
            {
                "sigla_uf": uf,
                "n_raw": n,
                "tem_DTOBITO": tem_dt,
                "tem_SEXO": "SEXO" in df.columns,
                "tem_CAUSABAS": "CAUSABAS" in df.columns,
                "tem_CONTADOR": "CONTADOR" in df.columns,
                "DTOBITO_nao_nulo": n_dt,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Análise principal
# ---------------------------------------------------------------------------


def run_summary(years: list[int]) -> None:
    years_sql = ", ".join(str(y) for y in years)

    section("1. BQ dev — contagem e nulos por ano")
    try:
        df_dev = bq(summary_query(DEV_TABLE, years_sql))
        print(df_dev.to_string(index=False))
    except Exception as e:
        print(f"  [erro] {e}")
        df_dev = pd.DataFrame()

    section("2. BQ prod — contagem e nulos por ano")
    try:
        df_prod = bq(summary_query(PROD_TABLE, years_sql))
        print(df_prod.to_string(index=False))
    except Exception as e:
        print(f"  [erro] {e}")
        df_prod = pd.DataFrame()

    section("3. Dev vs Prod — diff de contagem")
    if not df_dev.empty and not df_prod.empty:
        merged = (
            df_dev[["ano", "n"]]
            .merge(
                df_prod[["ano", "n"]],
                on="ano",
                suffixes=("_dev", "_prod"),
                how="outer",
            )
            .fillna(0)
        )
        merged["diff"] = merged["n_dev"] - merged["n_prod"]
        merged["diff_%"] = (
            merged["diff"] / merged["n_prod"].replace(0, float("nan")) * 100
        ).round(2)
        print(merged.to_string(index=False))
    else:
        print("  Dados insuficientes para comparar.")


def run_detail(ano: int) -> None:
    section(f"4. BQ dev — detalhe por UF para ano={ano}")
    try:
        df_dev_uf = bq(detail_query(DEV_TABLE, ano))
        print(df_dev_uf.to_string(index=False))
    except Exception as e:
        print(f"  [erro] {e}")
        df_dev_uf = pd.DataFrame()

    section(f"5. BQ prod — detalhe por UF para ano={ano}")
    try:
        df_prod_uf = bq(detail_query(PROD_TABLE, ano))
        print(df_prod_uf.to_string(index=False))
    except Exception as e:
        print(f"  [erro] {e}")
        df_prod_uf = pd.DataFrame()

    cache_dir = Path("./input")
    cache_dir.mkdir(exist_ok=True)

    section(f"6. Raw Datasus FTP — contagem por UF para ano={ano}")
    ufs_com_dados = (
        df_dev_uf["sigla_uf"].tolist() if not df_dev_uf.empty else UFS
    )
    df_raw = raw_summary(ano, ufs_com_dados, cache_dir)
    print(df_raw.to_string(index=False))

    section(f"7. BQ dev vs Raw — diff de contagem por UF (ano={ano})")
    if not df_dev_uf.empty and not df_raw.empty:
        merged = (
            df_dev_uf[["sigla_uf", "n"]]
            .merge(df_raw[["sigla_uf", "n_raw"]], on="sigla_uf", how="outer")
            .fillna(0)
        )
        merged["diff"] = merged["n"].astype(int) - merged["n_raw"].astype(int)
        print(merged.to_string(index=False))

        section(f"8. Colunas-chave presentes no raw (ano={ano})")
        cols_check = df_raw[
            [
                "sigla_uf",
                "tem_DTOBITO",
                "tem_SEXO",
                "tem_CAUSABAS",
                "tem_CONTADOR",
                "DTOBITO_nao_nulo",
            ]
        ]
        print(cols_check.to_string(index=False))
    else:
        print("  Dados insuficientes para comparar.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Ano para análise detalhada UF a UF + download raw.",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=list(range(1996, 2025)),
        help="Lista de anos para o resumo dev vs prod (padrão: 1996-2024).",
    )
    args = parser.parse_args()

    run_summary(args.years)

    if args.year:
        run_detail(args.year)


if __name__ == "__main__":
    main()
