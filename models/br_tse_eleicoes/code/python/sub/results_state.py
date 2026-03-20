"""
Build: resultados_candidato_uf + resultados_partido_uf.
Equivalent of sub/resultados_uf.do.
Historical data: 1945-1990.
Produces TWO output tables per year (plus no partido for 1989).
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON
from utils.clean_election_type import clean_election_type_series
from utils.clean_party import clean_party_series
from utils.clean_result import clean_result_series
from utils.clean_string import clean_string_series
from utils.fix_candidate import fix_candidate

# fmt: off
UFS_CAND = {
    1945: ["AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "GP", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RB", "RJ", "RN", "RS", "SC", "SE", "SP", "Fernando de Noronha", "Iguacu", "Ponta Pora"],
    1947: ["AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "GP", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RB", "RJ", "RN", "RS", "SC", "SE", "SP"],
    1950: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "GP", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RB", "RJ", "RN", "RS", "SC", "SE", "SP"],
    1954: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "GP", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RB", "RJ", "RN", "RS", "SC", "SE", "SP"],
    1955: ["BR"],
    1958: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RB", "RJ", "RN", "RO", "RS", "SC", "SE", "SP"],
    1960: ["BR"],
    1962: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GB", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RS", "SC", "SE", "SP"],
    1965: ["BR"],
    1966: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GB", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    1970: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GB", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    1974: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GB", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    1978: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    1982: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    1986: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    1989: ["BR"],
    1990: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
}

UFS_PART = {
    1945: ["AC", "AL", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RS", "SC", "SE", "SP"],
    1947: ["AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "GP", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RB", "RJ", "RN", "RS", "SC", "SE", "SP"],
    1950: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "GP", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RB", "RJ", "RN", "RS", "SC", "SE", "SP"],
    1954: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "GP", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RB", "RJ", "RN", "RS", "SC", "SE", "SP"],
    1955: ["BR"],
    1958: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RB", "RJ", "RN", "RO", "RS", "SC", "SE", "SP"],
    1960: ["BR"],
    1962: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GB", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RS", "SC", "SE", "SP"],
    1965: ["BR"],
    1966: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GB", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    1970: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GB", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    1974: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GB", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    1978: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    1982: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    1986: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    # 1989: no partido output
    1990: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
}
# fmt: on

YEARS = sorted(UFS_CAND.keys())

# Null sentinels for historical data (includes #AVULSO#)
_HIST_NULLS = {"#NULO#", "#NULO", "#NE#", "#NE", "#AVULSO#", "-1", "-3"}


def _try_read(ano: int, uf: str, folder: str) -> pd.DataFrame:
    """Try multiple file naming patterns for historical result files."""
    candidates = [
        f"{folder}/{folder.upper()}_{ano}_br/{folder.upper()}_{ano}_{uf}",
        f"{folder}/{folder.upper()}_{ano}/{folder.upper()}_{ano}_{uf}",
        f"{folder}/{folder.upper()}_{ano}/{folder.upper()}_{ano}",
    ]
    for pattern in candidates:
        base = INPUT_DIR / pattern
        for ext in [".txt", ".csv"]:
            path = base.with_suffix(ext)
            if path.exists():
                return pd.read_csv(
                    path,
                    sep=";",
                    header=None,
                    dtype=str,
                    encoding="utf-8",
                    keep_default_na=False,
                    quotechar='"',
                    on_bad_lines="warn",
                )
    raise FileNotFoundError(f"No file found for {folder} {ano} {uf}")


def _clean_hist_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Replace historical null sentinels with empty string."""
    return df.replace(
        {col: {v: "" for v in _HIST_NULLS} for col in df.columns}
    )


def build_candidato(ano: int) -> pd.DataFrame:
    """Build candidate results at state level for a single year."""
    frames = []

    for uf in UFS_CAND[ano]:
        raw = _try_read(ano, uf, "votacao_candidato_uf")
        raw.columns = [f"v{i + 1}" for i in range(len(raw.columns))]

        # Schema depends on year
        if ano in (1955, 1960, 1965):
            # Single national file, special schema
            df = raw[
                [
                    "v3",
                    "v4",
                    "v5",
                    "v6",
                    "v11",
                    "v13",
                    "v19",
                    "v21",
                    "v24",
                    "v25",
                    "v26",
                ]
            ].copy()
            df.columns = [
                "ano",
                "turno",
                "tipo_eleicao",
                "sigla_uf",
                "nome_candidato",
                "cargo",
                "resultado",
                "sigla_partido",
                "coligacao",
                "composicao",
                "votos",
            ]
        elif ano == 1989 or ano in (1986, 1990):
            df = raw[
                [
                    "v3",
                    "v4",
                    "v5",
                    "v6",
                    "v9",
                    "v11",
                    "v13",
                    "v19",
                    "v20",
                    "v21",
                    "v24",
                    "v25",
                    "v26",
                ]
            ].copy()
            df.columns = [
                "ano",
                "turno",
                "tipo_eleicao",
                "sigla_uf",
                "numero_candidato",
                "nome_candidato",
                "cargo",
                "resultado",
                "numero_partido",
                "sigla_partido",
                "coligacao",
                "composicao",
                "votos",
            ]
        else:
            # 1933, 1934, 1945, 1947, and even years <= 1982 with mod(ano,4)==2
            df = raw[
                [
                    "v3",
                    "v4",
                    "v5",
                    "v6",
                    "v11",
                    "v13",
                    "v19",
                    "v21",
                    "v24",
                    "v25",
                    "v26",
                ]
            ].copy()
            df.columns = [
                "ano",
                "turno",
                "tipo_eleicao",
                "sigla_uf",
                "nome_candidato",
                "cargo",
                "resultado",
                "sigla_partido",
                "coligacao",
                "composicao",
                "votos",
            ]

        # destring
        for col in ["ano", "turno", "votos"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # clean nulls
        df = _clean_hist_nulls(df)

        # fix cargo value
        df.loc[df["cargo"] == "SUPLENTE DE SENADOR 1945", "cargo"] = (
            "SUPLENTE DE SENADOR"
        )

        # clean strings
        for col in ["tipo_eleicao", "cargo", "resultado"]:
            if col in df.columns:
                df[col] = clean_string_series(df[col])
        if "nome_candidato" in df.columns:
            df["nome_candidato"] = df["nome_candidato"].str.title()

        df["tipo_eleicao"] = clean_election_type_series(
            df["tipo_eleicao"], ano
        )
        df["sigla_partido"] = clean_party_series(df["sigla_partido"], ano)
        if "resultado" in df.columns:
            df["resultado"] = clean_result_series(df["resultado"])
        df = fix_candidate(df)

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    return result


def build_partido(ano: int) -> pd.DataFrame:
    """Build party results at state level for a single year."""
    if ano == 1989:
        return pd.DataFrame()

    frames = []

    for uf in UFS_PART[ano]:
        raw = _try_read(ano, uf, "votacao_partido_uf")
        raw.columns = [f"v{i + 1}" for i in range(len(raw.columns))]

        df = raw[
            ["v3", "v4", "v5", "v6", "v9", "v11", "v12", "v13", "v16", "v17"]
        ].copy()
        df.columns = [
            "ano",
            "turno",
            "tipo_eleicao",
            "sigla_uf",
            "cargo",
            "coligacao",
            "composicao",
            "sigla_partido",
            "votos_nominais",
            "votos_nao_nominais",
        ]

        # destring
        for col in ["ano", "turno", "votos_nominais", "votos_nao_nominais"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # clean nulls
        df = _clean_hist_nulls(df)

        # clean strings
        for col in ["tipo_eleicao", "cargo"]:
            df[col] = clean_string_series(df[col])
        df["tipo_eleicao"] = clean_election_type_series(
            df["tipo_eleicao"], ano
        )
        df["sigla_partido"] = clean_party_series(df["sigla_partido"], ano)

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def build_all():
    for ano in YEARS:
        print(f"  results_state {ano}")
        cand = build_candidato(ano)
        out = OUTPUT_PYTHON / f"resultados_candidato_uf_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        cand.to_parquet(out, index=False)

        if ano != 1989:
            part = build_partido(ano)
            out = OUTPUT_PYTHON / f"resultados_partido_uf_{ano}.parquet"
            part.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
