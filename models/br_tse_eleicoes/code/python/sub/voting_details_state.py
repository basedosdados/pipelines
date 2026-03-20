"""
Build: detalhes_votacao_uf (voting details at state level).
Equivalent of sub/detalhes_votacao_uf.do.
Historical data: 1945-1990.
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON
from utils.clean_election_type import clean_election_type_series
from utils.clean_string import clean_string_series

# fmt: off
UFS = {
    1945: ["AL", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RS", "SC", "SE", "SP"],
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
# fmt: on

# The Stata loop: 1945 1947 1955 1960 1965 1950(4)1990 1989
# Which expands to: 1945, 1947, 1955, 1960, 1965, 1950, 1954, 1958, 1962, 1966, 1970, 1974, 1978, 1982, 1986, 1990, 1989
YEARS = sorted(UFS.keys())


def build_voting_details_state(ano: int) -> pd.DataFrame:
    """Build voting details at state level for a single year."""
    frames = []

    for uf in UFS[ano]:
        # Try per-state file first, then single national file
        base_uf = (
            INPUT_DIR
            / f"detalhe_votacao_uf/DETALHE_VOTACAO_UF_{ano}/DETALHE_VOTACAO_UF_{ano}_{uf}"
        )
        base_nat = (
            INPUT_DIR
            / f"detalhe_votacao_uf/DETALHE_VOTACAO_UF_{ano}/DETALHE_VOTACAO_UF_{ano}"
        )

        df = None
        for base in [base_uf, base_nat]:
            for ext in [".txt", ".csv"]:
                path = base.with_suffix(ext)
                if path.exists():
                    df = pd.read_csv(
                        path,
                        sep=";",
                        header=None,
                        dtype=str,
                        encoding="utf-8",
                        keep_default_na=False,
                        quotechar='"',
                        on_bad_lines="warn",
                    )
                    df.columns = [f"v{i + 1}" for i in range(len(df.columns))]
                    break
            if df is not None:
                break

        if df is None:
            print(
                f"    WARNING: no file found for detalhes_votacao_uf {ano} {uf}"
            )
            continue

        # Schema: only <= 2012 era applies for historical data
        df = df[
            [
                "v3",
                "v4",
                "v5",
                "v6",
                "v9",
                "v10",
                "v11",
                "v12",
                "v13",
                "v14",
                "v15",
                "v16",
                "v17",
                "v18",
                "v19",
                "v20",
                "v21",
                "v22",
            ]
        ].copy()
        df.columns = [
            "ano",
            "turno",
            "tipo_eleicao",
            "sigla_uf",
            "cargo",
            "aptos",
            "comparecimento",
            "abstencoes",
            "votos_validos",
            "votos_brancos",
            "votos_nulos",
            "votos_legenda",
            "votos_anulados_apurado_separado",
            "secoes_totalizadas",
            "secoes_anuladas",
            "secoes_sem_funcionamento",
            "zonas_eleitorais",
            "juntas_apuradoras",
        ]

        # destring numeric columns
        num_cols = [
            "ano",
            "turno",
            "aptos",
            "comparecimento",
            "abstencoes",
            "votos_validos",
            "votos_brancos",
            "votos_nulos",
            "votos_legenda",
            "votos_anulados_apurado_separado",
            "secoes_totalizadas",
            "secoes_anuladas",
            "secoes_sem_funcionamento",
            "zonas_eleitorais",
            "juntas_apuradoras",
        ]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Replace -1 and -3 with NaN for numeric columns
        replace_cols = [c for c in num_cols if c not in ("ano", "turno")]
        for col in replace_cols:
            df.loc[df[col].isin([-1, -3]), col] = pd.NA

        # clean strings
        for col in ["tipo_eleicao", "cargo"]:
            df[col] = clean_string_series(df[col])
        df["tipo_eleicao"] = clean_election_type_series(
            df["tipo_eleicao"], ano
        )

        # computed proportions
        df["proporcao_comparecimento"] = (
            100 * df["comparecimento"] / df["aptos"]
        )
        df["proporcao_votos_validos"] = (
            100 * df["votos_validos"] / df["comparecimento"]
        )
        df["proporcao_votos_brancos"] = (
            100 * df["votos_brancos"] / df["comparecimento"]
        )
        df["proporcao_votos_nulos"] = (
            100 * df["votos_nulos"] / df["comparecimento"]
        )

        df = df.drop_duplicates()
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def build_all():
    for ano in YEARS:
        print(f"  voting_details_state {ano}")
        df = build_voting_details_state(ano)
        out = OUTPUT_PYTHON / f"detalhes_votacao_uf_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
