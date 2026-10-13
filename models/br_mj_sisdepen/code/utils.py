"""Pure download and cleaning functions for br_mj_sisdepen.

No Prefect imports: a recurring pipeline under pipelines/datasets/br_mj_sisdepen/
imports these directly rather than duplicating the transform.
"""

from __future__ import annotations

import re
import unicodedata
from itertools import pairwise
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from models.br_mj_sisdepen.code.constants import (
    BASE_URL,
    CAPACITY_REGIMES,
    CHARACTERISTIC_BLOCKS,
    COLUMN_ALIASES,
    CYCLE_FILES,
    DEACTIVATED_BLOCK,
    GENERATION_LABELS,
    INPUT_DIR,
    MATCH_AMBIGUITY_MARGIN,
    MATCH_CAPACITY_WEIGHT,
    MATCH_GAP_THRESHOLD,
    MATCH_NAME_WEIGHT,
    MATCH_THRESHOLD,
    NON_CATEGORY_LEVEL1,
    POPULATION_COURT,
    POPULATION_STATUS,
    RDD_COLUMN,
    RECORD_CONDITION,
    RECORD_CONDITION_FLAG,
    SCHEMA_GENERATION,
    SEX_LEVELS,
)

# --------------------------------------------------------------------------- #
# download
# --------------------------------------------------------------------------- #


def download_cycle(cycle: int, input_dir: Path = INPUT_DIR) -> Path:
    """Download one cycle's CSV, skipping if already present."""
    input_dir.mkdir(parents=True, exist_ok=True)
    fragment = CYCLE_FILES[cycle]
    dest = input_dir / Path(fragment).name
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    url = f"{BASE_URL}/{fragment}"
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return dest


def download_all(input_dir: Path = INPUT_DIR) -> dict[int, Path]:
    return {c: download_cycle(c, input_dir) for c in sorted(CYCLE_FILES)}


# --------------------------------------------------------------------------- #
# parsing helpers
# --------------------------------------------------------------------------- #


def read_header(path: Path) -> list[str]:
    with open(path, encoding="utf-8") as fh:
        return fh.readline().rstrip("\n").rstrip("\r").split(";")


def resolve(header: list[str], canon: str) -> str | None:
    """Resolve a canonical name to whichever spelling this cycle uses."""
    hset = set(header)
    return next((o for o in COLUMN_ALIASES[canon] if o in hset), None)


def to_number(s: pd.Series) -> pd.Series:
    """Parse a numeric column.

    The source writes numbers with '.' as the DECIMAL separator ('0.0', '759.0',
    IBGE codes as '1200401.0'). Stripping '.' as a thousands separator inflates
    every quantity tenfold.
    """
    return pd.to_numeric(s, errors="coerce")


def to_ibge(s: pd.Series) -> pd.Series:
    """'1200401.0' -> '1200401'."""

    def one(v):
        if pd.isna(v):
            return None
        try:
            return str(int(float(str(v).strip())))
        except (TypeError, ValueError):
            return None

    return s.map(one)


def segments(column: str) -> list[str]:
    return [p.strip() for p in column.split("|")]


_ROMAN = {
    "I": "1",
    "II": "2",
    "III": "3",
    "IV": "4",
    "V": "5",
    "VI": "6",
    "VII": "7",
    "VIII": "8",
}
_STOPWORDS = {
    "DE",
    "DA",
    "DO",
    "DOS",
    "DAS",
    "E",
    "EM",
    "A",
    "O",
    "UNIDADE",
    "PRISIONAL",
    "PENITENCIARIA",
    "PENITENCIARIO",
    "PRESIDIO",
    "CADEIA",
    "PUBLICA",
    "CENTRO",
    "ESTABELECIMENTO",
    "PENAL",
    "COMPLEXO",
    "DETENCAO",
    "PROVISORIA",
    "COLONIA",
    "AGRICOLA",
    "CASA",
    "ALBERGADO",
    "HOSPITAL",
    "CUSTODIA",
    "TRATAMENTO",
    "PSIQUIATRICO",
    "NUCLEO",
    "RESSOCIALIZACAO",
    "REGIONAL",
}


def name_tokens(name: object) -> frozenset[str]:
    """Token set used for record linkage.

    Roman wing numerals are normalised to arabic because 'Apac Arcos I' and
    'APAC ARCOS 1' are the same wing, but the numeral itself is kept: wings I and
    II are genuinely different units.
    """
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return frozenset()
    s = (
        unicodedata.normalize("NFKD", str(name))
        .encode("ascii", "ignore")
        .decode()
        .upper()
    )
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return frozenset(
        _ROMAN.get(t, t) for t in s.split() if t and t not in _STOPWORDS
    )


# --------------------------------------------------------------------------- #
# panel
# --------------------------------------------------------------------------- #


def read_cycle(cycle: int, path: Path) -> pd.DataFrame:
    """Read one cycle into a tidy per-establishment frame with nested measures."""
    header = read_header(path)
    hset = set(header)

    cap_cols, dem_cols, pop_cols, char_cols = [], [], [], []
    for c in header:
        seg = segments(c)
        if c.startswith("1.3 Capacidade do estabelecimento") and len(seg) >= 3:
            (dem_cols if seg[1] == DEACTIVATED_BLOCK else cap_cols).append(c)
        elif c.startswith("4.1 População prisional") and len(seg) >= 3:
            pop_cols.append(c)
        elif (
            any(c.startswith(f"{b} ") for b in CHARACTERISTIC_BLOCKS)
            and len(seg) >= 2
        ):
            char_cols.append(c)

    wanted = {v for k in COLUMN_ALIASES for v in [resolve(header, k)] if v}
    keep = (
        wanted | set(cap_cols) | set(dem_cols) | set(pop_cols) | set(char_cols)
    )
    keep |= {RDD_COLUMN} & hset

    df = pd.read_csv(
        path,
        sep=";",
        usecols=lambda c: c in keep,
        dtype=str,
        encoding="utf-8",
        low_memory=False,
    )
    df.attrs["cycle"] = cycle
    df.attrs["cap_cols"] = cap_cols
    df.attrs["dem_cols"] = dem_cols
    df.attrs["pop_cols"] = pop_cols
    df.attrs["char_cols"] = char_cols
    df.attrs["header"] = header
    return df


def base_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Establishment-level identifying columns, one row per establishment."""
    header = df.attrs["header"]
    cycle = df.attrs["cycle"]

    def col(canon):
        name = resolve(header, canon)
        if name is None or name not in df.columns:
            return pd.Series(pd.NA, index=df.index, dtype="object")
        return df[name].astype("string").str.strip()

    out = pd.DataFrame(
        {
            "_key": [f"{cycle}:{i}" for i in range(len(df))],
            "ciclo": str(cycle),
            "ano": to_number(col("ano")).astype("Int64"),
            "referencia": col("referencia"),
            "sigla_uf": col("uf"),
            "municipio": col("municipio"),
            "id_municipio": to_ibge(col("id_municipio")),
            "nome_unidade_original": col("nome"),
            "ambito": col("ambito"),
            "tipo_recolhimento": col("tipo_recolhimento"),
            "sexo_destinacao_original": col("sexo_destinacao_original"),
            "tipo_estabelecimento_original": col(
                "tipo_estabelecimento_original"
            ),
            "gestao": col("gestao"),
            "data_inauguracao": pd.to_datetime(
                col("data_inauguracao"), errors="coerce", dayfirst=True
            ),
            "descricao_outro_regime": col("descricao_outro_regime"),
        }
    )
    out["semestre"] = to_number(
        out["referencia"].str.extract(r"/(\d)")[0]
    ).astype("Int64")
    out["geracao_esquema"] = SCHEMA_GENERATION[cycle]

    # capacity total per establishment, from the seven regime totals only
    regime_totals = [
        c
        for c in df.attrs["cap_cols"]
        if segments(c)[1] in CAPACITY_REGIMES and segments(c)[2] == "Total"
    ]
    cap = (
        pd.concat([to_number(df[c]) for c in regime_totals], axis=1)
        if regime_totals
        else None
    )
    out["capacidade_total"] = (
        cap.sum(axis=1, min_count=1) if cap is not None else np.nan
    )

    deact = [
        c
        for c in df.attrs["dem_cols"]
        if segments(c)[2] == "Total de vagas desativadas"
    ]
    out["vagas_desativadas"] = to_number(df[deact[0]]) if deact else np.nan

    pop_cells = [
        c
        for c in df.attrs["pop_cols"]
        if segments(c)[1] in POPULATION_STATUS and segments(c)[2] != "Total"
    ]
    out["populacao_total"] = (
        pd.concat([to_number(df[c]) for c in pop_cells], axis=1).sum(
            axis=1, min_count=1
        )
        if pop_cells
        else np.nan
    )
    return out


def check_capacity_reconciles(df: pd.DataFrame, base: pd.DataFrame) -> float:
    """Block 1.3 publishes regime totals AND sex margins over the same quantity.

    They must agree. Returns the agreement rate; the caller asserts on it.
    """
    sex_cols = [
        c
        for c in df.attrs["cap_cols"]
        if segments(c)[1] in ("Masculino", "Feminino")
        and segments(c)[2] == "Total"
    ]
    if not sex_cols:
        return float("nan")
    by_sex = pd.concat([to_number(df[c]) for c in sex_cols], axis=1).sum(
        axis=1, min_count=1
    )
    return float(
        np.isclose(
            base["capacidade_total"].fillna(-1), by_sex.fillna(-1)
        ).mean()
    )


# --------------------------------------------------------------------------- #
# record linkage
# --------------------------------------------------------------------------- #


def _max_weight_assignment(scores: np.ndarray) -> list[tuple[int, int]]:
    """Exact maximum-weight one-to-one assignment on a small dense matrix.

    Hungarian via successive shortest augmenting paths (Jonker-Volgenant form),
    O(n^2 m). Written out rather than pulled from scipy: the repo declares no
    scipy dependency and nothing else uses it, while these matrices are tiny —
    the median municipality holds one establishment and the largest holds 38.
    """
    n, m = scores.shape
    transposed = n > m
    if transposed:
        scores = scores.T
        n, m = m, n
    cost = -scores.astype(float)
    inf = float("inf")
    u = [0.0] * (n + 1)
    v = [0.0] * (m + 1)
    p = [0] * (m + 1)  # p[j] = row matched to column j
    way = [0] * (m + 1)
    for i in range(1, n + 1):
        p[0] = i
        j0 = 0
        minv = [inf] * (m + 1)
        used = [False] * (m + 1)
        while True:
            used[j0] = True
            i0, delta, j1 = p[j0], inf, 0
            for j in range(1, m + 1):
                if used[j]:
                    continue
                cur = cost[i0 - 1][j - 1] - u[i0] - v[j]
                if cur < minv[j]:
                    minv[j], way[j] = cur, j0
                if minv[j] < delta:
                    delta, j1 = minv[j], j
            for j in range(m + 1):
                if used[j]:
                    u[p[j]] += delta
                    v[j] -= delta
                else:
                    minv[j] -= delta
            j0 = j1
            if p[j0] == 0:
                break
        while j0:
            j1 = way[j0]
            p[j0] = p[j1]
            j0 = j1
    pairs = [(p[j] - 1, j - 1) for j in range(1, m + 1) if p[j] != 0]
    return [(c, r) for r, c in pairs] if transposed else pairs


def _pair_score(tok_a, cap_a, tok_b, cap_b) -> float:
    jaccard = (
        len(tok_a & tok_b) / len(tok_a | tok_b) if (tok_a and tok_b) else 0.0
    )
    if pd.notna(cap_a) and pd.notna(cap_b) and max(cap_a, cap_b) > 0:
        proximity = 1 - abs(cap_a - cap_b) / max(cap_a, cap_b)
    else:
        proximity = 0.5
    return MATCH_NAME_WEIGHT * jaccard + MATCH_CAPACITY_WEIGHT * proximity


def link_units(panel: pd.DataFrame) -> pd.DataFrame:
    """Assign a stable id_unidade across cycles.

    The source publishes no establishment identifier and names are unstable, so
    identity is reconstructed. Matching is one-to-one between consecutive cycles
    within a municipality, which makes a same-cycle collision impossible by
    construction; a second pass links trajectory ends to later starts so that a
    unit missing for a semester is recognised as the same unit returning, rather
    than becoming two units and hiding the gap.
    """
    panel = panel.reset_index(drop=True).copy()
    panel["_tok"] = panel["nome_unidade_original"].map(name_tokens)
    cycles = sorted(panel["ciclo"].astype(int).unique())
    cyc_int = panel["ciclo"].astype(int)
    # positional arrays: the matching loop is O(units^2) per municipality-pair,
    # so it reads these rather than going through pandas indexers
    tok_arr = panel["_tok"].to_numpy()
    cap_arr = panel["capacidade_total"].to_numpy(dtype=float, na_value=np.nan)

    link: dict[int, int] = {}
    prov: dict[int, dict] = {}

    for t, t_next in pairwise(cycles):
        idx_a = panel.index[cyc_int == t]
        idx_b = panel.index[cyc_int == t_next]
        muns = set(panel.loc[idx_a, "id_municipio"].dropna()) & set(
            panel.loc[idx_b, "id_municipio"].dropna()
        )
        for mun in muns:
            ai = idx_a[panel.loc[idx_a, "id_municipio"] == mun].to_numpy()
            bi = idx_b[panel.loc[idx_b, "id_municipio"] == mun].to_numpy()
            if len(ai) == 0 or len(bi) == 0:
                continue
            scores = np.array(
                [
                    [
                        _pair_score(
                            tok_arr[a], cap_arr[a], tok_arr[b], cap_arr[b]
                        )
                        for b in bi
                    ]
                    for a in ai
                ]
            )
            for r, c in _max_weight_assignment(scores):
                s = scores[r, c]
                if s < MATCH_THRESHOLD:
                    continue
                link[bi[c]] = ai[r]
                rival_row = np.delete(scores[r], c)
                rival_col = np.delete(scores[:, c], r)
                rival = max(
                    rival_row.max() if rival_row.size else 0.0,
                    rival_col.max() if rival_col.size else 0.0,
                )
                prov[bi[c]] = {
                    "score_pareamento": round(float(s), 4),
                    "score_rival": round(float(rival), 4),
                    "pareamento_ambiguo": bool(
                        rival >= s - MATCH_AMBIGUITY_MARGIN
                    ),
                    "metodo_pareamento": "consecutivo",
                }

    # stage 1 trajectories
    successor = {}
    for b, a in link.items():
        successor.setdefault(a, b)
    traj = {}
    next_id = 0
    for i in panel.index:
        if i in link:
            continue
        cur, tid = i, next_id
        next_id += 1
        traj[cur] = tid
        while cur in successor:
            cur = successor[cur]
            traj[cur] = tid
    panel["_traj"] = panel.index.map(traj)

    # stage 2: bridge gaps so absences stay visible
    ends = panel.groupby("_traj").agg(
        first_c=("ciclo", lambda s: s.astype(int).min()),
        last_c=("ciclo", lambda s: s.astype(int).max()),
        mun=("id_municipio", "first"),
    )
    head_tail = {}
    for tid, g in panel.groupby("_traj"):
        gi = g.assign(_c=g["ciclo"].astype(int))
        a, b = gi.loc[gi._c.idxmin()], gi.loc[gi._c.idxmax()]
        head_tail[tid] = (
            (a["_tok"], a["capacidade_total"]),
            (b["_tok"], b["capacidade_total"]),
        )

    merge: dict[int, int] = {}
    for _mun, grp in ends.reset_index().groupby("mun", dropna=True):
        taken = set()
        for _, a in grp.sort_values("last_c").iterrows():
            if a.last_c == cycles[-1]:
                continue
            best, best_s = None, 0.0
            for _, b in grp.sort_values("first_c").iterrows():
                if (
                    b._traj in taken
                    or b._traj == a._traj
                    or b.first_c < a.last_c + 2
                ):
                    continue
                s = _pair_score(*head_tail[a._traj][1], *head_tail[b._traj][0])
                if s > best_s:
                    best_s, best = s, b._traj
            if best is not None and best_s >= MATCH_GAP_THRESHOLD:
                merge[best] = a._traj
                taken.add(best)

    def root(t):
        seen = set()
        while t in merge and t not in seen:
            seen.add(t)
            t = merge[t]
        return t

    panel["_unit"] = panel["_traj"].map(root)
    panel["id_unidade"] = (
        panel.groupby("_unit").ngroup().map(lambda n: f"{n + 1:05d}")
    )

    traj_arr = panel["_traj"].to_numpy()
    provenance = [
        prov.get(
            i,
            {
                "score_pareamento": np.nan,
                "score_rival": np.nan,
                "pareamento_ambiguo": False,
                "metodo_pareamento": (
                    "lacuna" if traj_arr[i] in merge else "inicio_trajetoria"
                ),
            },
        )
        for i in range(len(panel))
    ]
    panel = pd.concat(
        [panel, pd.DataFrame(provenance, index=panel.index)], axis=1
    )

    # the invariant the whole design exists to guarantee
    collisions = panel.groupby(["id_unidade", "ciclo"]).size()
    assert (collisions <= 1).all(), (
        f"{(collisions > 1).sum()} same-cycle collisions in the crosswalk"
    )

    # nome_unidade: the most recent name observed for the unit
    latest = (
        panel.assign(_c=panel["ciclo"].astype(int))
        .sort_values("_c")
        .groupby("id_unidade")["nome_unidade_original"]
        .last()
    )
    panel["nome_unidade"] = panel["id_unidade"].map(latest)
    return panel.drop(columns=["_tok", "_traj", "_unit"])


# --------------------------------------------------------------------------- #
# table extraction
# --------------------------------------------------------------------------- #

_ID_COLS = [
    "ano",
    "semestre",
    "sigla_uf",
    "id_municipio",
    "id_unidade",
    "ciclo",
    "geracao_esquema",
]


def _ids(panel: pd.DataFrame) -> pd.DataFrame:
    return panel[["_key", *_ID_COLS]]


def extract_unidade_prisional(
    raw: dict[int, pd.DataFrame], panel: pd.DataFrame
) -> pd.DataFrame:
    """One row per establishment x semester x regime, carrying declared capacity."""
    frames = []
    for cycle, df in raw.items():
        keys = pd.Series([f"{cycle}:{i}" for i in range(len(df))], name="_key")
        by_regime: dict[str, dict[str, pd.Series]] = {}
        for c in df.attrs["cap_cols"]:
            seg = segments(c)
            regime = CAPACITY_REGIMES.get(seg[1])
            if regime is None or seg[2] not in (
                "Masculino",
                "Feminino",
                "Total",
            ):
                continue
            by_regime.setdefault(regime, {})[seg[2]] = to_number(df[c])
        for regime, parts in by_regime.items():
            frames.append(
                pd.DataFrame(
                    {
                        "_key": keys,
                        "tipo_regime": regime,
                        "capacidade_masculina": parts.get(
                            "Masculino", pd.Series(np.nan, index=df.index)
                        ),
                        "capacidade_feminina": parts.get(
                            "Feminino", pd.Series(np.nan, index=df.index)
                        ),
                        "capacidade_total": parts.get(
                            "Total", pd.Series(np.nan, index=df.index)
                        ),
                    }
                )
            )
    long = pd.concat(frames, ignore_index=True)
    attrs = panel[
        [
            "_key",
            *_ID_COLS,
            "nome_unidade",
            "nome_unidade_original",
            "ambito",
            "tipo_recolhimento",
            "sexo_destinacao_original",
            "tipo_estabelecimento_original",
            "gestao",
            "data_inauguracao",
            "descricao_outro_regime",
        ]
    ]
    out = attrs.merge(long, on="_key", how="inner")
    out["descricao_outro_regime"] = out["descricao_outro_regime"].where(
        out["tipo_regime"] == "outro"
    )
    cols = [
        *_ID_COLS,
        "nome_unidade",
        "nome_unidade_original",
        "ambito",
        "tipo_recolhimento",
        "sexo_destinacao_original",
        "tipo_estabelecimento_original",
        "gestao",
        "data_inauguracao",
        "tipo_regime",
        "capacidade_masculina",
        "capacidade_feminina",
        "capacidade_total",
        "descricao_outro_regime",
    ]
    return out[cols].sort_values(
        ["ano", "semestre", "sigla_uf", "id_unidade", "tipo_regime"]
    )


def extract_populacao_prisional(
    raw: dict[int, pd.DataFrame], panel: pd.DataFrame
) -> pd.DataFrame:
    """Block 4.1: situacao_processual x regime x esfera_justica x sexo.

    Only component cells are kept. The source's published 'Total' rows and
    columns are dropped so that summing quantidade cannot double count.
    """
    frames = []
    for cycle, df in raw.items():
        keys = [f"{cycle}:{i}" for i in range(len(df))]
        rdd = (
            to_number(df[RDD_COLUMN])
            if RDD_COLUMN in df.columns
            else pd.Series(np.nan, index=df.index)
        )
        for c in df.attrs["pop_cols"]:
            seg = segments(c)
            status = POPULATION_STATUS.get(seg[1])
            if status is None or seg[2] == "Total":
                continue
            court = next(
                (
                    v
                    for k, v in POPULATION_COURT.items()
                    if seg[2].startswith(k)
                ),
                None,
            )
            if court is None:
                continue
            label = next(k for k in POPULATION_COURT if seg[2].startswith(k))
            sex = SEX_LEVELS.get(seg[2][len(label) :].strip())
            if sex is None:
                continue
            frames.append(
                pd.DataFrame(
                    {
                        "_key": keys,
                        "situacao_processual": status[0],
                        "regime": status[1],
                        "esfera_justica": court,
                        "sexo": sex,
                        "quantidade": to_number(df[c]).to_numpy(),
                        "quantidade_rdd": rdd.to_numpy(),
                    }
                )
            )
    long = pd.concat(frames, ignore_index=True)
    out = (
        _ids(panel).merge(long, on="_key", how="inner").drop(columns=["_key"])
    )
    return out.sort_values(
        [
            "ano",
            "semestre",
            "sigla_uf",
            "id_unidade",
            "situacao_processual",
            "regime",
            "esfera_justica",
            "sexo",
        ]
    )


def extract_populacao_caracteristica(
    raw: dict[int, pd.DataFrame], panel: pd.DataFrame
) -> pd.DataFrame:
    """Blocks 5.1/5.2/5.4/5.6 as separate marginals, each crossed only with sex.

    condicao_registro carries the establishment's own answer to whether it can
    obtain the characteristic from its records. When it is 'parte' the counts are
    real but do not sum to the establishment's total, so the flag travels with
    every row rather than living only in the coverage table.
    """
    frames = []
    for cycle, df in raw.items():
        keys = [f"{cycle}:{i}" for i in range(len(df))]
        flags: dict[str, pd.Series] = {}
        for c in df.attrs["char_cols"]:
            seg = segments(c)
            block = c.split(" ", 1)[0]
            if len(seg) == 2 and seg[1] == RECORD_CONDITION_FLAG:
                flags[block] = (
                    df[c].astype("string").str.strip().map(RECORD_CONDITION)
                )
        for c in df.attrs["char_cols"]:
            seg = segments(c)
            block = c.split(" ", 1)[0]
            characteristic = CHARACTERISTIC_BLOCKS.get(block)
            if characteristic is None or len(seg) < 3:
                continue
            if seg[1] in NON_CATEGORY_LEVEL1:
                continue
            sex = SEX_LEVELS.get(seg[2])
            if sex is None:
                continue
            frames.append(
                pd.DataFrame(
                    {
                        "_key": keys,
                        "caracteristica": characteristic,
                        "categoria": seg[1],
                        "sexo": sex,
                        "quantidade": to_number(df[c]).to_numpy(),
                        "condicao_registro": flags.get(
                            block,
                            pd.Series(pd.NA, index=df.index, dtype="string"),
                        ).to_numpy(),
                    }
                )
            )
    long = pd.concat(frames, ignore_index=True)
    out = (
        _ids(panel).merge(long, on="_key", how="inner").drop(columns=["_key"])
    )
    return out.sort_values(
        [
            "ano",
            "semestre",
            "sigla_uf",
            "id_unidade",
            "caracteristica",
            "categoria",
            "sexo",
        ]
    )


def build_uf_semestre(
    panel: pd.DataFrame, populacao: pd.DataFrame
) -> pd.DataFrame:
    """State-level convenience aggregate."""
    pop = (
        populacao.groupby(["ano", "semestre", "sigla_uf"])
        # pyrefly: ignore [no-matching-overload]
        .apply(
            lambda g: pd.Series(
                {
                    "populacao_total": g.quantidade.sum(min_count=1),
                    "populacao_masculina": g.loc[
                        g.sexo == "masculino", "quantidade"
                    ].sum(min_count=1),
                    "populacao_feminina": g.loc[
                        g.sexo == "feminino", "quantidade"
                    ].sum(min_count=1),
                    "populacao_provisoria": g.loc[
                        g.situacao_processual == "provisorio", "quantidade"
                    ].sum(min_count=1),
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )
    est = (
        panel.groupby(["ano", "semestre", "sigla_uf"])
        .agg(
            ciclo=("ciclo", "first"),
            geracao_esquema=("geracao_esquema", "first"),
            unidades=("id_unidade", "nunique"),
            capacidade_total=(
                "capacidade_total",
                lambda s: s.sum(min_count=1),
            ),
            vagas_desativadas=(
                "vagas_desativadas",
                lambda s: s.sum(min_count=1),
            ),
        )
        .reset_index()
    )
    out = est.merge(pop, on=["ano", "semestre", "sigla_uf"], how="left")
    out["taxa_ocupacao"] = (out.populacao_total / out.capacidade_total).round(
        6
    )
    cols = [
        "ano",
        "semestre",
        "sigla_uf",
        "ciclo",
        "geracao_esquema",
        "unidades",
        "populacao_total",
        "populacao_masculina",
        "populacao_feminina",
        "populacao_provisoria",
        "capacidade_total",
        "vagas_desativadas",
        "taxa_ocupacao",
    ]
    return out[cols].sort_values(["ano", "semestre", "sigla_uf"])


def build_unidade_crosswalk(panel: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "ano",
        "semestre",
        "sigla_uf",
        "id_municipio",
        "id_unidade",
        "ciclo",
        "nome_unidade_original",
        "score_pareamento",
        "score_rival",
        "pareamento_ambiguo",
        "metodo_pareamento",
    ]
    out = panel[cols].copy()
    out["pareamento_ambiguo"] = out["pareamento_ambiguo"].map(
        {True: "sim", False: "nao"}
    )
    return out.sort_values(["ano", "semestre", "sigla_uf", "id_unidade"])


def build_cobertura(
    panel: pd.DataFrame, caracteristica: pd.DataFrame
) -> pd.DataFrame:
    """Units expected vs reporting, and item non-response, by state and semester.

    'Expected' is reconstructed, not administrative: the source publishes only
    validated returns, so a non-reporting unit is absent and unflagged. A unit
    counts as expected in a semester when the crosswalk observes it both before
    and after, so it must have existed. unidades_ausentes is a lower bound.
    """
    cyc = panel["ciclo"].astype(int)
    span = (
        panel.assign(_c=cyc)
        .groupby("id_unidade")
        .agg(
            first_c=("_c", "min"),
            last_c=("_c", "max"),
            uf=("sigla_uf", lambda s: s.mode().iloc[0]),
        )
        .reset_index()
    )
    present = set(zip(panel["id_unidade"], cyc, strict=True))
    absences: list[tuple[str, int]] = [
        (u, c)
        for u, first_c, last_c in span[
            ["id_unidade", "first_c", "last_c"]
        ].itertuples(index=False)
        for c in range(first_c + 1, last_c)
        if (u, c) not in present
    ]
    abs_df = pd.DataFrame(absences, columns=["id_unidade", "_c"]).merge(
        span[["id_unidade", "uf"]], on="id_unidade", how="left"
    )
    abs_count = abs_df.groupby(["uf", "_c"]).size().rename("n").reset_index()

    # one condicao_registro per establishment x characteristic
    cond = caracteristica.drop_duplicates(
        ["ano", "semestre", "sigla_uf", "id_unidade", "caracteristica"]
    )[
        [
            "ano",
            "semestre",
            "sigla_uf",
            "id_unidade",
            "caracteristica",
            "condicao_registro",
        ]
    ]
    rows = []
    for (ano, sem, uf), g in panel.groupby(["ano", "semestre", "sigla_uf"]):
        c = int(g["ciclo"].iloc[0])
        miss = int(
            abs_count.loc[
                (abs_count.uf == uf) & (abs_count._c == c), "n"
            ].sum()
        )
        n = g["id_unidade"].nunique()
        rec = {
            "ano": ano,
            "semestre": sem,
            "sigla_uf": uf,
            "ciclo": str(c),
            "geracao_esquema": g["geracao_esquema"].iloc[0],
            "unidades_esperadas": n + miss,
            "unidades_reportando": n,
            "unidades_ausentes": miss,
            "taxa_presenca": round(n / (n + miss), 6)
            if (n + miss)
            else np.nan,
            "taxa_capacidade_informada": round(
                # pyrefly: ignore [unnecessary-type-conversion]
                float(g["capacidade_total"].notna().mean()),
                6,
            ),
        }
        sub = cond[
            (cond.ano == ano) & (cond.semestre == sem) & (cond.sigla_uf == uf)
        ]
        for char in ("faixa_etaria", "raca_cor", "escolaridade"):
            s = sub.loc[sub.caracteristica == char, "condicao_registro"]
            total = len(s)
            for suffix, key in (
                ("completa", "todas"),
                ("parcial", "parte"),
                ("ausente", "nao"),
            ):
                rec[f"taxa_{char}_{suffix}"] = (
                    # pyrefly: ignore [unnecessary-type-conversion]
                    round(float((s == key).sum() / total), 6)
                    if total
                    else np.nan
                )
        rec["populacao_total"] = g["populacao_total"].sum(min_count=1)
        rec["capacidade_total"] = g["capacidade_total"].sum(min_count=1)
        rows.append(rec)
    return pd.DataFrame(rows).sort_values(["ano", "semestre", "sigla_uf"])


def build_dicionario() -> pd.DataFrame:
    rows = [
        {
            "id_tabela": table,
            "nome_coluna": "geracao_esquema",
            "chave": key,
            "cobertura_temporal": "",
            "valor": label,
        }
        for table in (
            "unidade_prisional",
            "populacao_prisional",
            "populacao_caracteristica",
            "uf_semestre",
            "cobertura",
        )
        for key, label in GENERATION_LABELS.items()
    ]
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# output
# --------------------------------------------------------------------------- #


def write_partitioned(df: pd.DataFrame, table: str, output_dir: Path) -> int:
    """Write hive-partitioned parquet with EVERY column as STRING.

    Staging is all-STRING by house convention and the dbt model safe_casts each
    column to its architecture type. The cast goes through arrow rather than
    astype(str), which would render NULL as the literal 'nan' — a value safe_cast
    will not turn back into NULL. Real types are preserved up to this point so
    that ano serialises as '2016' and not '2016.0'.
    """
    out_root = output_dir / table
    written = 0
    partitions = (
        [(None, df)] if "ano" not in df.columns else list(df.groupby("ano"))
    )
    for ano, part in partitions:
        part = part.drop(columns=["ano"]) if ano is not None else part
        cols = list(part.columns)
        arrays = []
        for c in cols:
            s = part[c]
            if pd.api.types.is_datetime64_any_dtype(s):
                s = s.dt.strftime("%Y-%m-%d")
            arr = (
                pa.array(
                    s.to_numpy(dtype=object),
                    type=pa.string(),
                    from_pandas=True,
                )
                if s.dtype == object
                else pa.array(s, from_pandas=True).cast(pa.string())
            )
            arrays.append(arr)
        tbl = pa.Table.from_arrays(arrays, names=cols)
        dest = out_root / ("" if ano is None else f"ano={ano}")
        dest.mkdir(parents=True, exist_ok=True)
        pq.write_table(tbl, dest / "data.parquet", compression="snappy")
        written += len(part)
    return written
