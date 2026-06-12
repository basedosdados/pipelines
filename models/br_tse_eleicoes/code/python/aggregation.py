"""
Phase 3b: Aggregation.
Equivalent of sub/agregacao.do.

1. resultados_candidato_municipio: collapse mun-zona to municipality (sum votos)
2. resultados_partido_municipio: same for partido
3. resultados_candidato: all-year aggregation (1945-2024) with historical data
4. detalhes_votacao_municipio: collapse from mun-zona with proportion recomputation
"""

import pandas as pd
from config import OUTPUT_PYTHON, YEARS_EVEN
from sub.results_state import YEARS as YEARS_HISTORICAL


def _read_partitioned_csv(
    table_name: str, ano: int, sigla_uf: str
) -> pd.DataFrame:
    """Read a partitioned CSV file."""
    path = (
        OUTPUT_PYTHON
        / table_name
        / f"ano={ano}"
        / f"sigla_uf={sigla_uf}"
        / f"{table_name}.csv"
    )
    if path.exists():
        return pd.read_csv(path, dtype=str, encoding="utf-8")
    return pd.DataFrame()


def _read_parquet(name: str, ano: int) -> pd.DataFrame:
    path = OUTPUT_PYTHON / f"{name}_{ano}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def _list_uf_dirs(table_name: str, ano: int) -> list[str]:
    """List sigla_uf values available for a partitioned table/year."""
    base = OUTPUT_PYTHON / table_name / f"ano={ano}"
    if not base.exists():
        return []
    ufs = []
    for d in sorted(base.iterdir()):
        if d.is_dir() and d.name.startswith("sigla_uf="):
            ufs.append(d.name.split("=", 1)[1])
    return ufs


# ---------------------------------------------------------------------------
# 1. resultados_candidato_municipio
# ---------------------------------------------------------------------------


def build_resultados_candidato_municipio():
    """Collapse mun-zona to municipality level (sum votos)."""
    print("  aggregating resultados_candidato_municipio...")

    group_cols = [
        "turno",
        "id_eleicao",
        "tipo_eleicao",
        "data_eleicao",
        "id_municipio",
        "id_municipio_tse",
        "cargo",
        "numero_partido",
        "sigla_partido",
        "titulo_eleitoral_candidato",
        "sequencial_candidato",
        "numero_candidato",
        "resultado",
    ]

    for ano in YEARS_EVEN:
        ufs = _list_uf_dirs("resultados_candidato_municipio_zona", ano)
        for uf in ufs:
            df = _read_partitioned_csv(
                "resultados_candidato_municipio_zona", ano, uf
            )
            if df.empty:
                continue

            df["votos"] = pd.to_numeric(df["votos"], errors="coerce")
            available = [c for c in group_cols if c in df.columns]
            agg = df.groupby(available, as_index=False, dropna=False)[
                "votos"
            ].sum()

            # Write output
            dest = (
                OUTPUT_PYTHON
                / "resultados_candidato_municipio"
                / f"ano={ano}"
                / f"sigla_uf={uf}"
            )
            dest.mkdir(parents=True, exist_ok=True)
            agg.to_csv(
                dest / "resultados_candidato_municipio.csv",
                index=False,
                encoding="utf-8",
            )


# ---------------------------------------------------------------------------
# 2. resultados_partido_municipio
# ---------------------------------------------------------------------------


def build_resultados_partido_municipio():
    """Collapse mun-zona to municipality level for partido."""
    print("  aggregating resultados_partido_municipio...")

    group_cols = [
        "turno",
        "id_eleicao",
        "tipo_eleicao",
        "data_eleicao",
        "id_municipio",
        "id_municipio_tse",
        "cargo",
        "numero_partido",
        "sigla_partido",
    ]

    for ano in YEARS_EVEN:
        ufs = _list_uf_dirs("resultados_partido_municipio_zona", ano)
        for uf in ufs:
            df = _read_partitioned_csv(
                "resultados_partido_municipio_zona", ano, uf
            )
            if df.empty:
                continue

            # Sum all votos_* columns
            vote_cols = [c for c in df.columns if c.startswith("votos")]
            for c in vote_cols:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            available = [c for c in group_cols if c in df.columns]
            agg = df.groupby(available, as_index=False, dropna=False)[
                vote_cols
            ].sum()

            dest = (
                OUTPUT_PYTHON
                / "resultados_partido_municipio"
                / f"ano={ano}"
                / f"sigla_uf={uf}"
            )
            dest.mkdir(parents=True, exist_ok=True)
            agg.to_csv(
                dest / "resultados_partido_municipio.csv",
                index=False,
                encoding="utf-8",
            )


# ---------------------------------------------------------------------------
# 3. resultados_candidato (all years: 1945-2024)
# ---------------------------------------------------------------------------


def _build_norm_cand_subsets():
    """Load norm_candidatos and build mod0/mod2 subsets for merging."""
    path = OUTPUT_PYTHON / "norm_candidatos.parquet"
    if not path.exists():
        return None, None, None

    nc = pd.read_parquet(path)
    nc["ano"] = pd.to_numeric(nc["ano"], errors="coerce")
    for col in ["numero", "numero_partido"]:
        if col in nc.columns:
            nc[col] = nc[col].astype(str)

    keep = [
        "ano",
        "tipo_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "cargo",
        "titulo_eleitoral",
        "numero",
        "numero_partido",
        "sigla_partido",
    ]
    nc = nc[[c for c in keep if c in nc.columns]].copy()

    mod0 = nc[nc["ano"] % 4 == 0].copy()
    mod2 = nc[nc["ano"] % 4 == 2].copy()
    mod2_est = mod2[mod2["cargo"] != "presidente"].copy()
    mod2_pres = mod2[mod2["cargo"] == "presidente"].copy()

    return mod0, mod2_est, mod2_pres


def build_resultados_candidato():
    """Build all-year resultados_candidato (1945-2024)."""
    print("  aggregating resultados_candidato...")

    group_cols = [
        "turno",
        "id_eleicao",
        "tipo_eleicao",
        "data_eleicao",
        "sigla_uf",
        "id_municipio",
        "id_municipio_tse",
        "cargo",
        "numero_partido",
        "sigla_partido",
        "titulo_eleitoral_candidato",
        "sequencial_candidato",
        "numero_candidato",
        "nome_candidato",
        "resultado",
    ]
    col_order = [*group_cols, "votos"]

    # --- Historical years (1945-1990) from results_state ---
    for ano in YEARS_HISTORICAL:
        df = _read_parquet("resultados_candidato_uf", ano)
        if df.empty:
            continue

        # Add missing columns
        if "sigla_uf" in df.columns and "cargo" in df.columns:
            df.loc[df["cargo"] == "presidente", "sigla_uf"] = ""
        for col in [
            "id_municipio",
            "id_municipio_tse",
            "id_eleicao",
            "data_eleicao",
            "numero_partido",
            "titulo_eleitoral_candidato",
            "sequencial_candidato",
            "numero_candidato",
        ]:
            if col not in df.columns:
                df[col] = ""

        df["votos"] = pd.to_numeric(df["votos"], errors="coerce")
        available = [c for c in group_cols if c in df.columns]
        agg = df.groupby(available, as_index=False, dropna=False)[
            "votos"
        ].sum()

        # Dedup
        dup_key = [
            "turno",
            "tipo_eleicao",
            "sigla_uf",
            "id_municipio_tse",
            "cargo",
            "sequencial_candidato",
            "numero_candidato",
            "nome_candidato",
        ]
        available_dup = [c for c in dup_key if c in agg.columns]
        dup_mask = agg.duplicated(subset=available_dup, keep=False)
        agg = agg[~dup_mask]

        # Order columns
        out_cols = [c for c in col_order if c in agg.columns]
        remaining = [c for c in agg.columns if c not in out_cols]
        agg = agg[out_cols + remaining]

        dest = OUTPUT_PYTHON / "resultados_candidato" / f"ano={ano}"
        dest.mkdir(parents=True, exist_ok=True)
        agg.to_csv(
            dest / "resultados_candidato.csv",
            index=False,
            encoding="utf-8",
        )

    # --- Modern years (1994-2024) from resultados_candidato_municipio_zona ---
    mod0, mod2_est, mod2_pres = _build_norm_cand_subsets()

    for ano in YEARS_EVEN:
        df = _read_parquet("resultados_candidato_municipio_zona", ano)
        if df.empty:
            continue

        # Rename columns to match expected names
        renames = {
            "sequencial": "sequencial_candidato",
            "nome": "nome_candidato",
            "nome_urna": "nome_urna_candidato",
        }
        for old, new in renames.items():
            if old in df.columns and new not in df.columns:
                df = df.rename(columns={old: new})

        # Merge titulo_eleitoral from norm_candidatos
        if mod0 is not None:
            has_numero_cand = "numero_candidato" in df.columns
            if has_numero_cand:
                df = df.rename(columns={"numero_candidato": "numero"})
            df["numero"] = df["numero"].astype(str)
            df["ano"] = pd.to_numeric(df["ano"], errors="coerce")

            if ano % 4 == 0:
                merge_keys = [
                    "ano",
                    "tipo_eleicao",
                    "sigla_uf",
                    "id_municipio_tse",
                    "cargo",
                    "numero",
                ]
                bring = ["titulo_eleitoral", "numero_partido", "sigla_partido"]
                bring = [c for c in bring if c in mod0.columns]
                sub = mod0[merge_keys + bring].drop_duplicates(
                    subset=merge_keys
                )
                df = df.merge(
                    sub, on=merge_keys, how="left", suffixes=("", "_norm")
                )
                for col in bring:
                    if f"{col}_norm" in df.columns:
                        df[col] = df[col].fillna(df[f"{col}_norm"])
                        df = df.drop(columns=[f"{col}_norm"])
            else:
                # Federal: split presidente vs others
                df_est = df[df["cargo"] != "presidente"].copy()
                df_pres = df[df["cargo"] == "presidente"].copy()

                merge_est = [
                    "ano",
                    "tipo_eleicao",
                    "sigla_uf",
                    "cargo",
                    "numero",
                ]
                merge_pres = ["ano", "tipo_eleicao", "cargo", "numero"]
                bring = ["titulo_eleitoral", "numero_partido", "sigla_partido"]
                bring = [c for c in bring if c in mod2_est.columns]

                sub_est = mod2_est[merge_est + bring].drop_duplicates(
                    subset=merge_est
                )
                df_est = df_est.merge(
                    sub_est, on=merge_est, how="left", suffixes=("", "_norm")
                )
                for col in bring:
                    if f"{col}_norm" in df_est.columns:
                        df_est[col] = df_est[col].fillna(df_est[f"{col}_norm"])
                        df_est = df_est.drop(columns=[f"{col}_norm"])

                sub_pres = mod2_pres[
                    [c for c in merge_pres + bring if c in mod2_pres.columns]
                ].drop_duplicates(subset=merge_pres)
                df_pres = df_pres.merge(
                    sub_pres, on=merge_pres, how="left", suffixes=("", "_norm")
                )
                for col in bring:
                    if f"{col}_norm" in df_pres.columns:
                        df_pres[col] = df_pres[col].fillna(
                            df_pres[f"{col}_norm"]
                        )
                        df_pres = df_pres.drop(columns=[f"{col}_norm"])

                df = pd.concat([df_est, df_pres], ignore_index=True)

            if has_numero_cand:
                df = df.rename(columns={"numero": "numero_candidato"})
            if "titulo_eleitoral" in df.columns:
                df = df.rename(
                    columns={"titulo_eleitoral": "titulo_eleitoral_candidato"}
                )

        # Clear sigla_uf for presidente, id_municipio for federal
        if "cargo" in df.columns:
            df.loc[df["cargo"] == "presidente", "sigla_uf"] = ""
        if ano % 4 == 2:
            for col in ["id_municipio", "id_municipio_tse"]:
                if col in df.columns:
                    df[col] = ""

        # Collapse
        df["votos"] = pd.to_numeric(df["votos"], errors="coerce")
        available = [c for c in group_cols if c in df.columns]
        agg = df.groupby(available, as_index=False, dropna=False)[
            "votos"
        ].sum()

        out_cols = [c for c in col_order if c in agg.columns]
        remaining = [c for c in agg.columns if c not in out_cols]
        agg = agg[out_cols + remaining]

        dest = OUTPUT_PYTHON / "resultados_candidato" / f"ano={ano}"
        dest.mkdir(parents=True, exist_ok=True)
        agg.to_csv(
            dest / "resultados_candidato.csv",
            index=False,
            encoding="utf-8",
        )


# ---------------------------------------------------------------------------
# 4. detalhes_votacao_municipio
# ---------------------------------------------------------------------------


def build_detalhes_votacao_municipio():
    """Collapse mun-zona to municipality, recompute proportions."""
    print("  aggregating detalhes_votacao_municipio...")

    group_cols = [
        "turno",
        "id_eleicao",
        "tipo_eleicao",
        "data_eleicao",
        "id_municipio",
        "id_municipio_tse",
        "cargo",
    ]

    sum_cols = [
        "aptos",
        "secoes",
        "secoes_agregadas",
        "aptos_totalizadas",
        "secoes_totalizadas",
        "comparecimento",
        "abstencoes",
    ]

    for ano in YEARS_EVEN:
        ufs = _list_uf_dirs("detalhes_votacao_municipio_zona", ano)
        for uf in ufs:
            df = _read_partitioned_csv(
                "detalhes_votacao_municipio_zona", ano, uf
            )
            if df.empty:
                continue

            # Identify all votos_* columns
            votos_cols = [c for c in df.columns if c.startswith("votos")]
            num_cols = [c for c in sum_cols + votos_cols if c in df.columns]
            for c in num_cols:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            available_grp = [c for c in group_cols if c in df.columns]
            agg = df.groupby(available_grp, as_index=False, dropna=False)[
                num_cols
            ].sum()

            # Recompute proportions
            if "comparecimento" in agg.columns and "aptos" in agg.columns:
                agg["proporcao_comparecimento"] = (
                    100 * agg["comparecimento"] / agg["aptos"]
                )
            if (
                "votos_validos" in agg.columns
                and "comparecimento" in agg.columns
            ):
                agg["proporcao_votos_validos"] = (
                    100 * agg["votos_validos"] / agg["comparecimento"]
                )
            if (
                "votos_brancos" in agg.columns
                and "comparecimento" in agg.columns
            ):
                agg["proporcao_votos_brancos"] = (
                    100 * agg["votos_brancos"] / agg["comparecimento"]
                )
            if (
                "votos_nulos" in agg.columns
                and "comparecimento" in agg.columns
            ):
                agg["proporcao_votos_nulos"] = (
                    100 * agg["votos_nulos"] / agg["comparecimento"]
                )

            dest = (
                OUTPUT_PYTHON
                / "detalhes_votacao_municipio"
                / f"ano={ano}"
                / f"sigla_uf={uf}"
            )
            dest.mkdir(parents=True, exist_ok=True)
            agg.to_csv(
                dest / "detalhes_votacao_municipio.csv",
                index=False,
                encoding="utf-8",
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def build_all():
    """Run all aggregation steps."""
    build_resultados_candidato_municipio()
    build_resultados_partido_municipio()
    build_resultados_candidato()
    build_detalhes_votacao_municipio()
    print("  aggregation complete.")


if __name__ == "__main__":
    build_all()
