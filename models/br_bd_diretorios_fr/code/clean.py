"""Clean INSEE reference files into 6 typed all-STRING Parquet directory tables.

Dataset: br_bd_diretorios_fr (French geography + classification directory).
Source: INSEE (Code Officiel Géographique, NAF rév. 2, NAF 2025, catégories juridiques).

These are DIRECTORY tables: not partitioned, all columns STRING, one plain
parquet per table. Missing values are written as real nulls (never the string
"nan"). Leading zeros and French accents are preserved.

Run:
    python clean.py
Reads from  ~/Downloads/br_bd_diretorios_fr_data/input/
Writes to   ~/Downloads/br_bd_diretorios_fr_data/output/<table>/data.parquet
Override the data root with env var BR_BD_DIRETORIOS_FR_DATA.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA_ROOT = Path(
    os.environ.get(
        "BR_BD_DIRETORIOS_FR_DATA",
        str(Path.home() / "Downloads" / "br_bd_diretorios_fr_data"),
    )
)
INPUT = DATA_ROOT / "input"
OUTPUT = DATA_ROOT / "output"

# Standard NACE division -> section letter ranges (2-digit division as int).
_SECTION_RANGES = [
    ("A", 1, 3),
    ("B", 5, 9),
    ("C", 10, 33),
    ("D", 35, 35),
    ("E", 36, 39),
    ("F", 41, 43),
    ("G", 45, 47),
    ("H", 49, 53),
    ("I", 55, 56),
    ("J", 58, 63),
    ("K", 64, 66),
    ("L", 68, 68),
    ("M", 69, 75),
    ("N", 77, 82),
    ("O", 84, 84),
    ("P", 85, 85),
    ("Q", 86, 88),
    ("R", 90, 93),
    ("S", 94, 96),
    ("T", 97, 98),
    ("U", 99, 99),
]


def division_to_section(div: str | None) -> str | None:
    """Map a 2-digit division code to its NACE section letter."""
    if div is None:
        return None
    try:
        n = int(div)
    except (TypeError, ValueError):
        return None
    for letter, lo, hi in _SECTION_RANGES:
        if lo <= n <= hi:
            return letter
    return None


def _clean_scalar(v):
    """Strip a value; map NaN / empty string to None. Never returns 'nan'."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if pd.isna(v):
        return None
    s = str(v).strip()
    return s if s != "" else None


def write_table(
    df: pd.DataFrame, columns: list[str], table: str, key: str
) -> int:
    """Write df[columns] as an all-STRING parquet; assert key uniqueness."""
    schema = pa.schema([(c, pa.string()) for c in columns])
    arrays = []
    for c in columns:
        cleaned = [_clean_scalar(v) for v in df[c].tolist()]
        arrays.append(pa.array(cleaned, type=pa.string()))
    tbl = pa.Table.from_arrays(arrays, schema=schema)

    keys = tbl.column(key).to_pylist()
    non_null = [k for k in keys if k is not None]
    assert len(non_null) == len(set(non_null)), (
        f"[{table}] key '{key}' not unique: "
        f"{len(non_null)} rows, {len(set(non_null))} distinct"
    )
    assert len(non_null) == len(keys), f"[{table}] key '{key}' has null values"

    out_dir = OUTPUT / table
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(tbl, out_dir / "data.parquet", compression="snappy")
    return tbl.num_rows


# ---------------------------------------------------------------------------
# Geography (COG CSVs)
# ---------------------------------------------------------------------------
def clean_regiao() -> int:
    df = pd.read_csv(
        INPUT / "v_region_2025.csv", dtype=str, keep_default_na=False
    )
    out = pd.DataFrame(
        {
            "id_regiao": df["REG"],
            "id_comuna_sede": df["CHEFLIEU"],
            "nome_regiao": df["LIBELLE"],
            "nome_regiao_maiusculo": df["NCCENR"],
        }
    )
    return write_table(
        out,
        [
            "id_regiao",
            "id_comuna_sede",
            "nome_regiao",
            "nome_regiao_maiusculo",
        ],
        "regiao",
        "id_regiao",
    )


def clean_departamento() -> int:
    df = pd.read_csv(
        INPUT / "v_departement_2025.csv", dtype=str, keep_default_na=False
    )
    out = pd.DataFrame(
        {
            "id_departamento": df["DEP"],
            "id_regiao": df["REG"],
            "id_comuna_sede": df["CHEFLIEU"],
            "nome_departamento": df["LIBELLE"],
        }
    )
    return write_table(
        out,
        [
            "id_departamento",
            "id_regiao",
            "id_comuna_sede",
            "nome_departamento",
        ],
        "departamento",
        "id_departamento",
    )


def clean_comuna() -> int:
    df = pd.read_csv(
        INPUT / "v_commune_2025.csv", dtype=str, keep_default_na=False
    )
    df = df[df["TYPECOM"].isin(["COM", "ARM"])].copy()
    out = pd.DataFrame(
        {
            "id_comuna": df["COM"],
            "id_departamento": df["DEP"],
            "id_regiao": df["REG"],
            "nome_comuna": df["LIBELLE"],
            "tipo_comuna": df["TYPECOM"],
        }
    )
    return write_table(
        out,
        [
            "id_comuna",
            "id_departamento",
            "id_regiao",
            "nome_comuna",
            "tipo_comuna",
        ],
        "comuna",
        "id_comuna",
    )


# ---------------------------------------------------------------------------
# NAF rév. 2 (2008)
# ---------------------------------------------------------------------------
def _read_liste(fname: str) -> dict[str, str]:
    """Read a naf2008_liste_nX.xls: header 'Code|Libellé' at row idx 2, data from row 3."""
    d = pd.read_excel(
        INPUT / fname, sheet_name="Feuil1", header=None, dtype=str
    )
    d = d.iloc[3:, [0, 1]].copy()
    d.columns = ["code", "libelle"]
    d = d.dropna(subset=["code"])
    return {
        str(c).strip(): (str(lib).strip() if pd.notna(lib) else None)
        for c, lib in zip(d["code"], d["libelle"], strict=False)
    }


def clean_naf_rev2(gaps: dict) -> int:
    codes = pd.read_excel(
        INPUT / "naf2008_5_niveaux.xls",
        sheet_name="naf2008_5_niveaux",
        header=0,
        dtype=str,
    )
    lbl5 = _read_liste("naf2008_liste_n5.xls")
    lbl4 = _read_liste("naf2008_liste_n4.xls")
    lbl3 = _read_liste("naf2008_liste_n3.xls")
    lbl2 = _read_liste("naf2008_liste_n2.xls")
    lbl1 = _read_liste("naf2008_liste_n1.xls")

    out = pd.DataFrame(
        {
            "naf_rev2": codes["NIV5"].str.strip(),
            "id_classe": codes["NIV4"].str.strip(),
            "id_grupo": codes["NIV3"].str.strip(),
            "id_divisao": codes["NIV2"].str.strip(),
            "id_secao": codes["NIV1"].str.strip(),
        }
    )
    out["descricao_naf_rev2"] = out["naf_rev2"].map(lbl5)
    out["descricao_classe"] = out["id_classe"].map(lbl4)
    out["descricao_grupo"] = out["id_grupo"].map(lbl3)
    out["descricao_divisao"] = out["id_divisao"].map(lbl2)
    out["descricao_secao"] = out["id_secao"].map(lbl1)

    for lvl, code_col, desc_col in [
        ("naf_rev2", "naf_rev2", "descricao_naf_rev2"),
        ("classe", "id_classe", "descricao_classe"),
        ("grupo", "id_grupo", "descricao_grupo"),
        ("divisao", "id_divisao", "descricao_divisao"),
        ("secao", "id_secao", "descricao_secao"),
    ]:
        n = int(out[code_col].notna().sum() - out[desc_col].notna().sum())
        if n:
            gaps[f"naf_rev2/{lvl}"] = n

    cols = [
        "naf_rev2",
        "descricao_naf_rev2",
        "id_classe",
        "descricao_classe",
        "id_grupo",
        "descricao_grupo",
        "id_divisao",
        "descricao_divisao",
        "id_secao",
        "descricao_secao",
    ]
    return write_table(out, cols, "naf_rev2", "naf_rev2")


# ---------------------------------------------------------------------------
# NAF 2025
# ---------------------------------------------------------------------------
def _read_naf2025_sheet(sheet: str) -> dict[str, str]:
    """NAF 2025 per-level sheet: header at row 0, data from row 1, cols code|libellé."""
    d = pd.read_excel(
        INPUT / "Structure_NAF_2025_Maj_2024-10-04.xlsx",
        sheet_name=sheet,
        header=0,
        dtype=str,
    )
    d = d.iloc[:, [0, 1]].copy()
    d.columns = ["code", "libelle"]
    d = d.dropna(subset=["code"])
    return {
        str(c).strip(): (str(lib).strip() if pd.notna(lib) else None)
        for c, lib in zip(d["code"], d["libelle"], strict=False)
    }


def clean_naf_2025(gaps: dict) -> int:
    lbl_sc = _read_naf2025_sheet("Sous-classes")
    lbl_cl = _read_naf2025_sheet("Classes")
    lbl_gr = _read_naf2025_sheet("Groupes")
    lbl_dv = _read_naf2025_sheet("Divisions")
    lbl_se = _read_naf2025_sheet("Sections")

    naf = pd.Series(sorted(lbl_sc.keys()), name="naf_2025")
    out = pd.DataFrame({"naf_2025": naf})
    out["id_classe"] = out["naf_2025"].str[:-1]  # 01.11Y -> 01.11
    out["id_grupo"] = out["id_classe"].str[:-1]  # 01.11  -> 01.1
    out["id_divisao"] = out["naf_2025"].str[:2]  # 01.11Y -> 01
    out["id_secao"] = out["id_divisao"].map(division_to_section)

    out["descricao_naf_2025"] = out["naf_2025"].map(lbl_sc)
    out["descricao_classe"] = out["id_classe"].map(lbl_cl)
    out["descricao_grupo"] = out["id_grupo"].map(lbl_gr)
    out["descricao_divisao"] = out["id_divisao"].map(lbl_dv)
    out["descricao_secao"] = out["id_secao"].map(lbl_se)

    for lvl, code_col, desc_col in [
        ("naf_2025", "naf_2025", "descricao_naf_2025"),
        ("classe", "id_classe", "descricao_classe"),
        ("grupo", "id_grupo", "descricao_grupo"),
        ("divisao", "id_divisao", "descricao_divisao"),
        ("secao", "id_secao", "descricao_secao"),
    ]:
        n = int(out[code_col].notna().sum() - out[desc_col].notna().sum())
        if n:
            gaps[f"naf_2025/{lvl}"] = n

    cols = [
        "naf_2025",
        "descricao_naf_2025",
        "id_classe",
        "descricao_classe",
        "id_grupo",
        "descricao_grupo",
        "id_divisao",
        "descricao_divisao",
        "id_secao",
        "descricao_secao",
    ]
    return write_table(out, cols, "naf_2025", "naf_2025")


# ---------------------------------------------------------------------------
# Catégories juridiques
# ---------------------------------------------------------------------------
def _read_cj_sheet(sheet: str) -> pd.DataFrame:
    """cj sheet: header 'Code|Libellé' at row idx 3, data from row 4."""
    d = pd.read_excel(
        INPUT / "cj_septembre_2022.xls",
        sheet_name=sheet,
        header=None,
        dtype=str,
    )
    d = d.iloc[4:, [0, 1]].copy()
    d.columns = ["code", "libelle"]
    d["code"] = d["code"].str.strip()
    d = d.dropna(subset=["code"])
    d = d[d["code"] != ""]
    return d


def clean_categoria_juridica(gaps: dict) -> int:
    n1 = _read_cj_sheet("Niveau I")
    n2 = _read_cj_sheet("Niveau II")
    n3 = _read_cj_sheet("Niveau III")
    lbl1 = dict(zip(n1["code"], n1["libelle"], strict=False))
    lbl2 = dict(zip(n2["code"], n2["libelle"], strict=False))

    out = pd.DataFrame(
        {
            "categoria_juridica": n3["code"].to_numpy(),
            "descricao_categoria_juridica": n3["libelle"].to_numpy(),
        }
    )
    out["id_nivel_2"] = out["categoria_juridica"].str[:2]
    out["descricao_nivel_2"] = out["id_nivel_2"].map(lbl2)
    out["id_nivel_1"] = out["categoria_juridica"].str[:1]
    out["descricao_nivel_1"] = out["id_nivel_1"].map(lbl1)

    for lvl, code_col, desc_col in [
        ("nivel_2", "id_nivel_2", "descricao_nivel_2"),
        ("nivel_1", "id_nivel_1", "descricao_nivel_1"),
    ]:
        n = int(out[code_col].notna().sum() - out[desc_col].notna().sum())
        if n:
            gaps[f"categoria_juridica/{lvl}"] = n

    cols = [
        "categoria_juridica",
        "descricao_categoria_juridica",
        "id_nivel_2",
        "descricao_nivel_2",
        "id_nivel_1",
        "descricao_nivel_1",
    ]
    return write_table(out, cols, "categoria_juridica", "categoria_juridica")


def main() -> None:
    gaps: dict[str, int] = {}
    counts = {
        "regiao": clean_regiao(),
        "departamento": clean_departamento(),
        "comuna": clean_comuna(),
        "naf_rev2": clean_naf_rev2(gaps),
        "naf_2025": clean_naf_2025(gaps),
        "categoria_juridica": clean_categoria_juridica(gaps),
    }

    print("\n=== br_bd_diretorios_fr — row counts ===")
    for t, n in counts.items():
        print(f"  {t:<20} {n:>7,} rows")

    if gaps:
        print("\n=== join gaps (codes without a label) ===")
        for k, n in gaps.items():
            print(f"  {k:<28} {n} unmatched")
    else:
        print("\nNo join gaps — every code matched a label.")


if __name__ == "__main__":
    main()
