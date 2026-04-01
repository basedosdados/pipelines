"""
Phase 3a: Normalization and partitioning.
Equivalent of sub/normalizacao_particao.do.

1. Candidate normalization (append all years, CPF-titulo mapping, multi-step dedup)
2. Party normalization
3. Merge titulo_eleitoral into results/finance tables
4. Partition ALL tables into Hive-style CSV output
"""

import pandas as pd
from config import OUTPUT_PYTHON, UFS_CANDIDATOS, UFS_PARTIDOS, YEARS_EVEN
from utils.helpers import save_partitioned

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_parquet(name: str, ano: int) -> pd.DataFrame:
    path = OUTPUT_PYTHON / f"{name}_{ano}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def _read_parquet_single(name: str) -> pd.DataFrame:
    path = OUTPUT_PYTHON / f"{name}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# 1. Candidate normalization
# ---------------------------------------------------------------------------


def normalize_candidates() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Normalize candidates table. Returns (norm_candidatos, cleaned_candidatos).

    norm_candidatos: deduplicated reference table (used to merge titulo_eleitoral
    into results/finance tables).
    cleaned_candidatos: full table after complex dedup (for the candidatos output).
    """
    print("  normalizing candidates...")

    # 1. Append all years
    frames = []
    for ano in sorted(UFS_CANDIDATOS.keys()):
        df = _read_parquet("candidatos", ano)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(), pd.DataFrame()

    all_cand = pd.concat(frames, ignore_index=True)

    # Drop turno == 2 and drop turno + resultado columns
    all_cand["turno"] = pd.to_numeric(all_cand["turno"], errors="coerce")
    all_cand = all_cand[all_cand["turno"] != 2].copy()
    all_cand = all_cand.drop(columns=["turno", "resultado"], errors="ignore")

    # Ensure string columns
    for col in ["cpf", "titulo_eleitoral"]:
        if col in all_cand.columns:
            all_cand[col] = all_cand[col].fillna("").astype(str)

    # --- Build unique CPF-titulo mapping ---
    cpf_titulo = all_cand[["cpf", "titulo_eleitoral"]].copy()
    cpf_titulo = cpf_titulo[
        (cpf_titulo["cpf"] != "") & (cpf_titulo["titulo_eleitoral"] != "")
    ]
    cpf_titulo = cpf_titulo.drop_duplicates()

    # Drop rows where key maps to multiple values (non-unique mapping)
    for col in ["cpf", "titulo_eleitoral"]:
        dup_counts = cpf_titulo[col].value_counts()
        non_unique = dup_counts[dup_counts > 1].index
        cpf_titulo = cpf_titulo[~cpf_titulo[col].isin(non_unique)]

    # Drop cpf from main, re-merge using titulo_eleitoral to get clean cpf
    all_cand = all_cand.drop(columns=["cpf"])
    all_cand = all_cand.merge(
        cpf_titulo[["titulo_eleitoral", "cpf"]],
        on="titulo_eleitoral",
        how="left",
    )
    all_cand["cpf"] = all_cand["cpf"].fillna("")

    all_cand = all_cand.reset_index(drop=True)
    all_cand["aux_id"] = all_cand.index

    # --- Step 2: Produce norm_candidatos (deduplicated reference) ---
    norm = all_cand[all_cand["titulo_eleitoral"] != ""].copy()

    # Dedup 1: full key with titulo_eleitoral, keep first
    key_full = [
        "ano",
        "tipo_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "numero",
        "cargo",
        "titulo_eleitoral",
    ]
    available_full = [c for c in key_full if c in norm.columns]
    norm = norm.drop_duplicates(subset=available_full, keep="first")

    # Dedup 2: on key without titulo_eleitoral, drop non-deferido
    key_base = [
        "ano",
        "tipo_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "numero",
        "cargo",
    ]
    available_base = [c for c in key_base if c in norm.columns]
    dup_mask = norm.duplicated(subset=available_base, keep=False)
    if "situacao" in norm.columns:
        norm = norm[~(dup_mask & (norm["situacao"] != "deferido"))]

    # Dedup 3: remaining dups on same key, drop all
    dup_mask = norm.duplicated(subset=available_base, keep=False)
    norm = norm[~dup_mask]

    # Dedup 4: force on (titulo_eleitoral, ano, tipo_eleicao)
    key_titulo = ["titulo_eleitoral", "ano", "tipo_eleicao"]
    norm = norm.drop_duplicates(subset=key_titulo, keep="first")

    norm_candidatos = norm.copy()

    # Save norm_candidatos
    norm_out = norm_candidatos.drop(columns=["aux_id"], errors="ignore")
    norm_out.to_parquet(OUTPUT_PYTHON / "norm_candidatos.parquet", index=False)
    norm_out.to_csv(
        OUTPUT_PYTHON / "norm_candidatos.csv", index=False, encoding="utf-8"
    )
    print(f"    norm_candidatos: {len(norm_out)} rows")

    # --- Step 3: Clean errors in final candidatos table ---
    # The full all_cand remains; we apply complex dedup on titulo_eleitoral groups

    # Compute group-level stats for (ano, tipo_eleicao, titulo_eleitoral)
    grp_key = ["ano", "tipo_eleicao", "titulo_eleitoral"]
    has_titulo = all_cand["titulo_eleitoral"] != ""

    # Tag duplicates (only for rows with titulo_eleitoral)
    all_cand["_is_dup"] = False
    titulo_rows = all_cand[has_titulo]
    dup_in_titulo = titulo_rows.duplicated(subset=grp_key, keep=False)
    all_cand.loc[titulo_rows.index[dup_in_titulo], "_is_dup"] = True

    # Compute N_cargo, N_numero, N_deferido, N_nome, N_cpf per group
    titulo_data = all_cand[has_titulo].copy()
    if not titulo_data.empty:
        # N_cargo: number of distinct cargos per group
        n_cargo = (
            titulo_data.groupby(grp_key)["cargo"].nunique().rename("_N_cargo")
        )
        # N_numero: number of distinct numeros per group
        n_numero = (
            titulo_data.groupby(grp_key)["numero"]
            .nunique()
            .rename("_N_numero")
        )
        # N_deferido: count of deferido rows per group
        titulo_data["_is_def"] = (
            titulo_data["situacao"] == "deferido"
            if "situacao" in titulo_data.columns
            else False
        )
        n_deferido = (
            titulo_data.groupby(grp_key)["_is_def"].sum().rename("_N_deferido")
        )
        # N_nome: number of distinct nomes per group
        n_nome = (
            titulo_data.groupby(grp_key)["nome"].nunique().rename("_N_nome")
        )
        # N_cpf: number of distinct cpfs per group
        n_cpf = titulo_data.groupby(grp_key)["cpf"].nunique().rename("_N_cpf")

        stats = pd.concat(
            [n_cargo, n_numero, n_deferido, n_nome, n_cpf], axis=1
        ).reset_index()
        all_cand = all_cand.merge(stats, on=grp_key, how="left")
    else:
        for col in [
            "_N_cargo",
            "_N_numero",
            "_N_deferido",
            "_N_nome",
            "_N_cpf",
        ]:
            all_cand[col] = 0

    # Fill NaN stats for non-titulo rows
    for col in [
        "_N_cargo",
        "_N_numero",
        "_N_deferido",
        "_N_nome",
        "_N_cpf",
    ]:
        all_cand[col] = all_cand[col].fillna(0).astype(int)

    is_dup = all_cand["_is_dup"]

    # Rule 1: N_deferido == 0 -> force dedup (keep first)
    mask1 = is_dup & (all_cand["_N_deferido"] == 0)
    drop1 = all_cand[mask1].duplicated(subset=grp_key, keep="first")
    all_cand = all_cand.drop(all_cand[mask1].index[drop1])

    # Rule 2: N_deferido == 1 -> drop non-deferido
    is_dup = all_cand.index.isin(
        all_cand[all_cand["titulo_eleitoral"] != ""]
        .loc[lambda d: d.duplicated(subset=grp_key, keep=False)]
        .index
    )
    mask2 = is_dup & (all_cand["_N_deferido"] == 1)
    if "situacao" in all_cand.columns:
        drop2 = mask2 & (all_cand["situacao"] != "deferido")
        all_cand = all_cand[~drop2]

    # Recompute is_dup
    is_dup = all_cand.index.isin(
        all_cand[all_cand["titulo_eleitoral"] != ""]
        .loc[lambda d: d.duplicated(subset=grp_key, keep=False)]
        .index
    )

    # Rule 3: N_deferido > 1, N_numero == 1, N_cargo == 1 -> force dedup
    mask3 = (
        is_dup
        & (all_cand["_N_deferido"] > 1)
        & (all_cand["_N_numero"] == 1)
        & (all_cand["_N_cargo"] == 1)
    )
    drop3 = all_cand[mask3].duplicated(subset=grp_key, keep="first")
    all_cand = all_cand.drop(all_cand[mask3].index[drop3])

    # Rule 4: N_deferido > 1, N_numero > 1, N_cargo == 1, N_nome == 1
    is_dup = all_cand.index.isin(
        all_cand[all_cand["titulo_eleitoral"] != ""]
        .loc[lambda d: d.duplicated(subset=grp_key, keep=False)]
        .index
    )
    mask4 = (
        is_dup
        & (all_cand["_N_deferido"] > 1)
        & (all_cand["_N_numero"] > 1)
        & (all_cand["_N_cargo"] == 1)
        & (all_cand["_N_nome"] == 1)
    )
    drop4 = all_cand[mask4].duplicated(subset=grp_key, keep="first")
    all_cand = all_cand.drop(all_cand[mask4].index[drop4])

    # Rule 5: N_deferido > 1, N_numero > 1, N_cargo > 1, N_nome == 1
    is_dup = all_cand.index.isin(
        all_cand[all_cand["titulo_eleitoral"] != ""]
        .loc[lambda d: d.duplicated(subset=grp_key, keep=False)]
        .index
    )
    mask5 = (
        is_dup
        & (all_cand["_N_deferido"] > 1)
        & (all_cand["_N_numero"] > 1)
        & (all_cand["_N_cargo"] > 1)
        & (all_cand["_N_nome"] == 1)
    )
    drop5 = all_cand[mask5].duplicated(subset=grp_key, keep="first")
    all_cand = all_cand.drop(all_cand[mask5].index[drop5])

    # Final catch-all dedup on (ano, tipo_eleicao, titulo_eleitoral)
    titulo_rows = all_cand[all_cand["titulo_eleitoral"] != ""]
    final_dups = titulo_rows.duplicated(subset=grp_key, keep="first")
    all_cand = all_cand.drop(titulo_rows.index[final_dups])

    # Final dedup on composite key
    final_key = [
        "ano",
        "sigla_uf",
        "id_municipio_tse",
        "sequencial",
        "numero",
        "titulo_eleitoral",
    ]
    available_final = [c for c in final_key if c in all_cand.columns]
    all_cand = all_cand.drop_duplicates(subset=available_final, keep="first")

    # Drop temp columns
    drop_cols = [
        "aux_id",
        "_is_dup",
        "_N_cargo",
        "_N_numero",
        "_N_deferido",
        "_N_nome",
        "_N_cpf",
        "_is_def",
    ]
    all_cand = all_cand.drop(
        columns=[c for c in drop_cols if c in all_cand.columns]
    )

    # Column ordering
    col_order = [
        "ano",
        "id_eleicao",
        "tipo_eleicao",
        "data_eleicao",
        "sigla_uf",
        "id_municipio",
        "id_municipio_tse",
        "titulo_eleitoral",
        "cpf",
        "sequencial",
        "numero",
        "nome",
        "nome_urna",
        "numero_partido",
        "sigla_partido",
        "cargo",
    ]
    existing_order = [c for c in col_order if c in all_cand.columns]
    remaining = [c for c in all_cand.columns if c not in existing_order]
    all_cand = all_cand[existing_order + remaining]

    print(f"    cleaned candidatos: {len(all_cand)} rows")
    return norm_out, all_cand


# ---------------------------------------------------------------------------
# 2. Party normalization
# ---------------------------------------------------------------------------


def normalize_parties() -> pd.DataFrame:
    """
    Build norm_partidos: append all years, keep (ano, numero, sigla), dedup.
    Also builds the partidos table for partitioning.
    """
    print("  normalizing parties...")

    frames = []
    for ano in sorted(UFS_PARTIDOS.keys()):
        df = _read_parquet("partidos", ano)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    all_part = pd.concat(frames, ignore_index=True)

    # norm_partidos: keep only (ano, numero, sigla), dedup
    norm_cols = ["ano", "numero", "sigla"]
    available = [c for c in norm_cols if c in all_part.columns]
    # Fallback: some builders may use sigla_partido instead of sigla
    if "sigla" not in all_part.columns and "sigla_partido" in all_part.columns:
        all_part = all_part.rename(columns={"sigla_partido": "sigla"})
        available = [c for c in norm_cols if c in all_part.columns]
    if (
        "numero" not in all_part.columns
        and "numero_partido" in all_part.columns
    ):
        all_part = all_part.rename(columns={"numero_partido": "numero"})
        available = [c for c in norm_cols if c in all_part.columns]

    norm = all_part[available].copy()

    # Drop specific known duplicate
    mask = (
        (norm["ano"].astype(str) == "2020")
        & (norm["numero"].astype(str) == "36")
        & (norm["sigla"] == "PTC")
    )
    norm = norm[~mask]
    norm = norm.drop_duplicates()

    norm.to_parquet(OUTPUT_PYTHON / "norm_partidos.parquet", index=False)
    print(f"    norm_partidos: {len(norm)} rows")

    return all_part


# ---------------------------------------------------------------------------
# 3. Merge titulo_eleitoral into results/finance tables
# ---------------------------------------------------------------------------


def _build_norm_cand_subsets(
    norm_cand: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Build three subsets of norm_candidatos for merging into results/finance:
    - mod0: municipal years (mod(ano,4)==0)
    - mod2_estadual: federal years, non-presidente
    - mod2_presid: federal years, presidente
    """
    norm_cand["ano"] = pd.to_numeric(norm_cand["ano"], errors="coerce")

    # Ensure numero is string for merge
    for col in ["numero", "numero_partido"]:
        if col in norm_cand.columns:
            norm_cand[col] = norm_cand[col].astype(str)

    mod0 = norm_cand[norm_cand["ano"] % 4 == 0].copy()
    mod2 = norm_cand[norm_cand["ano"] % 4 == 2].copy()
    mod2_est = mod2[mod2["cargo"] != "presidente"].copy()
    mod2_pres = mod2[mod2["cargo"] == "presidente"].copy()

    return mod0, mod2_est, mod2_pres


def _merge_titulo_into_results(
    df: pd.DataFrame,
    ano: int,
    mod0: pd.DataFrame,
    mod2_est: pd.DataFrame,
    mod2_pres: pd.DataFrame,
    merge_cols_mod0: list[str],
    merge_cols_mod2_est: list[str],
    merge_cols_mod2_pres: list[str],
    bring_cols: list[str],
) -> pd.DataFrame:
    """
    Merge norm_candidatos into a results/finance table using mod(ano,4) logic.
    """
    # Rename numero_candidato -> numero for merge
    has_numero_cand = "numero_candidato" in df.columns
    if has_numero_cand:
        df = df.rename(columns={"numero_candidato": "numero"})

    df["numero"] = df["numero"].astype(str)

    if ano % 4 == 0:
        subset = mod0[merge_cols_mod0 + bring_cols].drop_duplicates(
            subset=merge_cols_mod0
        )
        df = df.merge(subset, on=merge_cols_mod0, how="left")
    else:
        # Split presidente vs others
        df_est = df[df["cargo"] != "presidente"].copy()
        df_pres = df[df["cargo"] == "presidente"].copy()

        sub_est = mod2_est[merge_cols_mod2_est + bring_cols].drop_duplicates(
            subset=merge_cols_mod2_est
        )
        df_est = df_est.merge(sub_est, on=merge_cols_mod2_est, how="left")

        sub_pres = mod2_pres[
            merge_cols_mod2_pres + bring_cols
        ].drop_duplicates(subset=merge_cols_mod2_pres)
        df_pres = df_pres.merge(sub_pres, on=merge_cols_mod2_pres, how="left")

        df = pd.concat([df_est, df_pres], ignore_index=True)

    # Rename back
    if has_numero_cand:
        df = df.rename(columns={"numero": "numero_candidato"})
    if "titulo_eleitoral" in df.columns:
        df = df.rename(
            columns={"titulo_eleitoral": "titulo_eleitoral_candidato"}
        )

    # Fill NaN in brought columns
    for col in bring_cols:
        renamed = (
            col.replace("titulo_eleitoral", "titulo_eleitoral_candidato")
            if col == "titulo_eleitoral"
            else col
        )
        if renamed in df.columns:
            df[renamed] = df[renamed].fillna("")

    return df


# ---------------------------------------------------------------------------
# 4. Partition all tables
# ---------------------------------------------------------------------------


def _partition_candidatos(cleaned_cand: pd.DataFrame):
    """Partition candidatos by ano only (presidente has empty sigla_uf)."""
    print("  partitioning candidatos...")
    save_partitioned(cleaned_cand, "candidatos", ["ano"], OUTPUT_PYTHON)


def _partition_partidos(all_partidos: pd.DataFrame):
    """Partition partidos by ano. Keep only eleicao ordinaria."""
    print("  partitioning partidos...")
    if "tipo_eleicao" in all_partidos.columns:
        all_partidos = all_partidos[
            all_partidos["tipo_eleicao"] == "eleicao ordinaria"
        ].copy()
    save_partitioned(all_partidos, "partidos", ["ano"], OUTPUT_PYTHON)


def _partition_simple(table_name: str, years: list[int], by_uf: bool = True):
    """Partition a table that doesn't need merges."""
    print(f"  partitioning {table_name}...")
    partition_cols = ["ano", "sigla_uf"] if by_uf else ["ano"]
    for ano in years:
        df = _read_parquet(table_name, ano)
        if df.empty:
            continue
        save_partitioned(df, table_name, partition_cols, OUTPUT_PYTHON)


def _partition_results_mun_zone(
    norm_cand: pd.DataFrame,
):
    """Partition resultados_candidato_municipio_zona and partido variant."""
    print("  partitioning resultados_candidato_municipio_zona...")

    # Build norm subsets with appropriate columns
    keep_cols = [
        "titulo_eleitoral",
        "ano",
        "tipo_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "cargo",
        "numero",
    ]
    nc = norm_cand[[c for c in keep_cols if c in norm_cand.columns]].copy()
    mod0, mod2_est, mod2_pres = _build_norm_cand_subsets(nc)

    merge_mod0 = [
        "ano",
        "tipo_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "cargo",
        "numero",
    ]
    merge_mod2_est = ["ano", "tipo_eleicao", "sigla_uf", "cargo", "numero"]
    merge_mod2_pres = ["ano", "tipo_eleicao", "cargo", "numero"]
    bring = ["titulo_eleitoral"]

    for ano in YEARS_EVEN:
        df = _read_parquet("resultados_candidato_municipio_zona", ano)
        if df.empty:
            continue

        df = _merge_titulo_into_results(
            df,
            ano,
            mod0,
            mod2_est,
            mod2_pres,
            merge_mod0,
            merge_mod2_est,
            merge_mod2_pres,
            bring,
        )

        # Drop nome columns as in Stata
        df = df.drop(
            columns=["nome_candidato", "nome_urna_candidato"],
            errors="ignore",
        )

        save_partitioned(
            df,
            "resultados_candidato_municipio_zona",
            ["ano", "sigla_uf"],
            OUTPUT_PYTHON,
        )

    # Partido variant: no merge needed, just partition
    print("  partitioning resultados_partido_municipio_zona...")
    for ano in YEARS_EVEN:
        df = _read_parquet("resultados_partido_municipio_zona", ano)
        if df.empty:
            continue
        save_partitioned(
            df,
            "resultados_partido_municipio_zona",
            ["ano", "sigla_uf"],
            OUTPUT_PYTHON,
        )


def _partition_results_section(
    norm_cand: pd.DataFrame,
    norm_part: pd.DataFrame,
):
    """Partition resultados_candidato_secao and partido variant."""
    print("  partitioning resultados_candidato_secao...")

    # Candidato: merge titulo_eleitoral + sequencial + numero_partido + sigla_partido
    keep_cols = [
        "titulo_eleitoral",
        "sequencial",
        "numero_partido",
        "sigla_partido",
        "ano",
        "tipo_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "cargo",
        "numero",
    ]
    nc = norm_cand[[c for c in keep_cols if c in norm_cand.columns]].copy()
    mod0, mod2_est, mod2_pres = _build_norm_cand_subsets(nc)

    merge_mod0 = [
        "ano",
        "tipo_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "cargo",
        "numero",
    ]
    merge_mod2_est = ["ano", "tipo_eleicao", "sigla_uf", "cargo", "numero"]
    merge_mod2_pres = ["ano", "tipo_eleicao", "cargo", "numero"]
    bring = [
        "titulo_eleitoral",
        "sequencial",
        "numero_partido",
        "sigla_partido",
    ]
    # Only bring columns that exist
    bring = [c for c in bring if c in nc.columns]

    for ano in YEARS_EVEN:
        df = _read_parquet("resultados_candidato_secao", ano)
        if df.empty:
            continue

        df = _merge_titulo_into_results(
            df,
            ano,
            mod0,
            mod2_est,
            mod2_pres,
            merge_mod0,
            merge_mod2_est,
            merge_mod2_pres,
            bring,
        )

        # Rename sequencial -> sequencial_candidato if brought in
        if "sequencial" in df.columns:
            df = df.rename(columns={"sequencial": "sequencial_candidato"})

        save_partitioned(
            df,
            "resultados_candidato_secao",
            ["ano", "sigla_uf"],
            OUTPUT_PYTHON,
        )

    # Partido: merge with norm_partidos to get sigla_partido
    print("  partitioning resultados_partido_secao...")
    if not norm_part.empty:
        norm_part["numero"] = norm_part["numero"].astype(str)
        norm_part["ano"] = pd.to_numeric(norm_part["ano"], errors="coerce")
        part_lookup = norm_part[["ano", "numero", "sigla"]].drop_duplicates()

    for ano in YEARS_EVEN:
        df = _read_parquet("resultados_partido_secao", ano)
        if df.empty:
            continue

        # Merge sigla_partido from norm_partidos
        if not norm_part.empty and "numero_partido" in df.columns:
            df["numero_partido"] = df["numero_partido"].astype(str)
            df["ano"] = pd.to_numeric(df["ano"], errors="coerce")
            df = df.merge(
                part_lookup.rename(
                    columns={
                        "numero": "numero_partido",
                        "sigla": "sigla_partido",
                    }
                ),
                on=["ano", "numero_partido"],
                how="left",
                suffixes=("", "_norm"),
            )
            # Use norm value if exists
            if "sigla_partido_norm" in df.columns:
                df["sigla_partido"] = df["sigla_partido"].fillna(
                    df["sigla_partido_norm"]
                )
                df = df.drop(columns=["sigla_partido_norm"])

        save_partitioned(
            df,
            "resultados_partido_secao",
            ["ano", "sigla_uf"],
            OUTPUT_PYTHON,
        )


def _partition_bens(norm_cand: pd.DataFrame):
    """Partition bens_candidato with titulo_eleitoral merge."""
    print("  partitioning bens_candidato...")

    # For bens, merge on (ano, tipo_eleicao, sigla_uf, sequencial)
    keep = [
        "ano",
        "tipo_eleicao",
        "sigla_uf",
        "sequencial",
        "titulo_eleitoral",
    ]
    nc = norm_cand[[c for c in keep if c in norm_cand.columns]].copy()
    nc = nc[
        nc["ano"].apply(lambda x: pd.to_numeric(x, errors="coerce")) >= 2006
    ]
    nc["ano"] = pd.to_numeric(nc["ano"], errors="coerce")
    nc = nc.drop_duplicates(
        subset=["ano", "tipo_eleicao", "sigla_uf", "sequencial"]
    )

    for ano in range(2006, 2025, 2):
        df = _read_parquet("bens_candidato", ano)
        if df.empty:
            continue

        # Rename sequencial_candidato -> sequencial for merge
        if "sequencial_candidato" in df.columns:
            df = df.rename(columns={"sequencial_candidato": "sequencial"})

        df["ano"] = pd.to_numeric(df["ano"], errors="coerce")
        merge_keys = ["ano", "tipo_eleicao", "sigla_uf", "sequencial"]
        available_keys = [
            k for k in merge_keys if k in df.columns and k in nc.columns
        ]
        df = df.merge(
            nc[[*available_keys, "titulo_eleitoral"]].drop_duplicates(
                subset=available_keys
            ),
            on=available_keys,
            how="left",
        )
        df = df.rename(
            columns={
                "titulo_eleitoral": "titulo_eleitoral_candidato",
                "sequencial": "sequencial_candidato",
            }
        )
        if "titulo_eleitoral_candidato" in df.columns:
            df["titulo_eleitoral_candidato"] = df[
                "titulo_eleitoral_candidato"
            ].fillna("")

        save_partitioned(df, "bens_candidato", ["ano"], OUTPUT_PYTHON)


def _partition_finance(
    table_name: str,
    norm_cand: pd.DataFrame,
    years: range,
):
    """Partition receitas or despesas with titulo_eleitoral merge."""
    print(f"  partitioning {table_name}...")

    keep_cols = [
        "titulo_eleitoral",
        "ano",
        "tipo_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "cargo",
        "numero",
    ]
    nc = norm_cand[[c for c in keep_cols if c in norm_cand.columns]].copy()
    mod0, mod2_est, mod2_pres = _build_norm_cand_subsets(nc)

    merge_mod0 = [
        "ano",
        "tipo_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "cargo",
        "numero",
    ]
    merge_mod2_est = ["ano", "tipo_eleicao", "sigla_uf", "cargo", "numero"]
    merge_mod2_pres = ["ano", "tipo_eleicao", "cargo", "numero"]
    bring = ["titulo_eleitoral"]

    for ano in years:
        df = _read_parquet(table_name, ano)
        if df.empty:
            continue

        # For years <= 2012, fill empty tipo_eleicao
        if ano <= 2012 and "tipo_eleicao" in df.columns:
            df.loc[df["tipo_eleicao"] == "", "tipo_eleicao"] = (
                "eleicao ordinaria"
            )

        df = _merge_titulo_into_results(
            df,
            ano,
            mod0,
            mod2_est,
            mod2_pres,
            merge_mod0,
            merge_mod2_est,
            merge_mod2_pres,
            bring,
        )

        save_partitioned(df, table_name, ["ano"], OUTPUT_PYTHON)


def _partition_vagas():
    """Partition vagas: append all years, then partition by ano/sigla_uf."""
    print("  partitioning vagas...")
    frames = []
    for ano in YEARS_EVEN:
        df = _read_parquet("vagas", ano)
        if not df.empty:
            frames.append(df)
    if not frames:
        return
    all_vagas = pd.concat(frames, ignore_index=True)
    save_partitioned(all_vagas, "vagas", ["ano", "sigla_uf"], OUTPUT_PYTHON)


# ---------------------------------------------------------------------------
# Perfil eleitorado secao: year-specific UF lists
# ---------------------------------------------------------------------------

# fmt: off
_UFS_PERFIL_SECAO = {
    2008: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2010: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2012: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2014: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2016: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2018: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO", "ZZ"],
    2020: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2022: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO", "ZZ"],
    2024: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
}
# fmt: on


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def build_all():
    """Run all normalization and partitioning steps."""

    # 1. Normalize candidates
    norm_cand, cleaned_cand = normalize_candidates()

    # 2. Normalize parties
    all_partidos = normalize_parties()

    # Load norm tables for merging
    norm_part = pd.DataFrame()
    norm_part_path = OUTPUT_PYTHON / "norm_partidos.parquet"
    if norm_part_path.exists():
        norm_part = pd.read_parquet(norm_part_path)

    # 3. Partition candidatos
    if not cleaned_cand.empty:
        _partition_candidatos(cleaned_cand)

    # 4. Partition partidos
    if not all_partidos.empty:
        _partition_partidos(all_partidos)

    # 5. Partition results (mun-zona) with titulo merge
    if not norm_cand.empty:
        _partition_results_mun_zone(norm_cand)

    # 6. Partition results (secao) with titulo merge
    if not norm_cand.empty:
        _partition_results_section(norm_cand, norm_part)

    # 7. Partition simple tables (no merge needed)
    _partition_simple(
        "perfil_eleitorado_municipio_zona", YEARS_EVEN, by_uf=True
    )
    _partition_simple(
        "detalhes_votacao_municipio_zona", YEARS_EVEN, by_uf=True
    )
    _partition_simple("detalhes_votacao_secao", YEARS_EVEN, by_uf=True)

    # perfil_eleitorado_secao: year-specific UF lists
    print("  partitioning perfil_eleitorado_secao...")
    for ano in sorted(_UFS_PERFIL_SECAO.keys()):
        df = _read_parquet("perfil_eleitorado_secao", ano)
        if df.empty:
            continue
        save_partitioned(
            df,
            "perfil_eleitorado_secao",
            ["ano", "sigla_uf"],
            OUTPUT_PYTHON,
        )

    # perfil_eleitorado_local_votacao: single all-years file
    print("  partitioning perfil_eleitorado_local_votacao...")
    df_plv = _read_parquet_single("perfil_eleitorado_local_votacao")
    if not df_plv.empty:
        save_partitioned(
            df_plv,
            "perfil_eleitorado_local_votacao",
            ["ano", "sigla_uf"],
            OUTPUT_PYTHON,
        )

    # 8. Partition vagas
    _partition_vagas()

    # 9. Partition bens
    if not norm_cand.empty:
        _partition_bens(norm_cand)

    # 10. Partition receitas and despesas
    if not norm_cand.empty:
        _partition_finance(
            "receitas_candidato", norm_cand, range(2002, 2025, 2)
        )
        _partition_finance(
            "despesas_candidato", norm_cand, range(2002, 2025, 2)
        )

    print("  normalization & partitioning complete.")


if __name__ == "__main__":
    build_all()
