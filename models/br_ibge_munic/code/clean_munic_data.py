#!/usr/bin/env python3
"""Clean MUNIC data into standardized section CSVs."""

from __future__ import annotations

import argparse
import io
import re
import unicodedata
import zipfile
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_BASE_DIR = BASE_DIR / "input" / "Base"
OUTPUT_DIR = BASE_DIR / "output"
SECTIONS_MAPPING_PATH = BASE_DIR / "code" / "sections_mapping.xlsx"
DIRECTORY_PATH = BASE_DIR / "input" / "br_bd_diretorios_brasil_municipio.csv"

DEFAULT_SECTIONS = ["atual_prefeito", "habitacao", "meio_ambiente", "recursos_gestao", "recursos_humanos"]
ARCH_SHEETS = {
    "atual_prefeito": "17ypDJTUevRapNofHiKA_EuofQ_yhwBEo1Okp_b-aD1w",
    "habitacao": "15XgH_sf5hQ_zaksS77stNu58ghwjga4JkTeP5rWge0E",
    "meio_ambiente": "1SF6lGdgfIxc9VJLpi1lzuwhhZseulK1qPSF1hKdIipQ",
    "recursos_gestao": "1EfS1g-OYIvKPF95b2Ln5bMCDCyYW8OK_U-dCGaSROHU",
    "recursos_humanos": "1mNyL_F0XxOylWqkKzJ1SFkqeyGRVVqc8-cVv3gLiaNY",
}


def normalize_text(text: object) -> str:
    if text is None:
        return ""
    text = str(text).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def is_dictionary_sheet(sheet_name: str) -> bool:
    sheet_norm = normalize_text(sheet_name)
    return "dicion" in sheet_norm or sheet_norm == "dictionary"


def find_data_file(year: int) -> Path | None:
    if not INPUT_BASE_DIR.exists():
        return None
    for ext in [".xlsx", ".xls", ".zip"]:
        for file in INPUT_BASE_DIR.iterdir():
            if file.name.startswith("~$"):
                continue
            if str(year) in file.name and file.suffix.lower() == ext:
                return file
    return None


def open_excel_file(path: Path) -> tuple[pd.ExcelFile, str]:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path, "r") as zf:
            for name in zf.namelist():
                if name.startswith("__MACOSX/"):
                    continue
                if name.lower().endswith((".xlsx", ".xls")):
                    data = zf.read(name)
                    engine = "openpyxl" if name.lower().endswith(".xlsx") else "xlrd"
                    return pd.ExcelFile(io.BytesIO(data), engine=engine), engine
        raise FileNotFoundError(f"No Excel file found inside {path.name}")
    engine = "openpyxl" if path.suffix.lower() == ".xlsx" else "xlrd"
    return pd.ExcelFile(path, engine=engine), engine


def detect_id_column(df: pd.DataFrame) -> str | None:
    preferred = {"idmunicipio", "codmun", "codmunic", "codmunicpio", "a1", "cod_munic"}
    best_col = None
    best_score = 0.0

    for col in df.columns:
        col_str = str(col)
        col_norm = normalize_text(col_str).replace(" ", "")
        series = df[col]
        sample = series.dropna().astype(str).str.strip().head(200)
        if sample.empty:
            continue
        sample = sample.str.replace(r"\.0$", "", regex=True)
        is_digits = sample.str.fullmatch(r"\d+")
        if is_digits.sum() == 0:
            continue
        lengths = sample[is_digits].str.len()
        ratio_digits = is_digits.mean()
        ratio_len = lengths.between(6, 7).mean()
        score = ratio_digits * 0.7 + ratio_len * 0.3
        if col_norm in preferred:
            score += 1.0
        elif "mun" in col_norm:
            score += 0.2
        if score > best_score:
            best_score = score
            best_col = col_str

    return best_col


def detect_sigla_uf_column(df: pd.DataFrame) -> str | None:
    best_col = None
    best_score = 0.0
    for col in df.columns:
        col_str = str(col)
        col_norm = normalize_text(col_str).replace(" ", "")
        if "uf" not in col_norm:
            continue
        series = df[col].dropna().astype(str).str.strip().head(200)
        if series.empty:
            continue
        is_uf = series.str.fullmatch(r"[A-Za-z]{2}")
        score = is_uf.mean()
        if col_norm in {"uf", "siglauf"}:
            score += 0.5
        if score > best_score:
            best_score = score
            best_col = col_str
    return best_col


def standardize_id(series: pd.Series) -> pd.Series:
    out = series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    out = out.replace({"nan": pd.NA, "NaN": pd.NA, "None": pd.NA, "": pd.NA})
    return out


def map_to_id_municipio_7(series: pd.Series, directory_df: pd.DataFrame) -> pd.Series:
    cleaned = standardize_id(series)
    id_map = {}
    if "id_municipio_6" in directory_df.columns and "id_municipio" in directory_df.columns:
        id_map = dict(
            zip(directory_df["id_municipio_6"].astype(str), directory_df["id_municipio"].astype(str))
        )

    result = cleaned.copy()
    result_str = result.astype(str)
    mask_7 = result.notna() & result_str.str.fullmatch(r"\d{7}")
    result.loc[mask_7] = result_str.loc[mask_7].str.zfill(7)

    mask_6 = result.notna() & result_str.str.fullmatch(r"\d{6}")
    if mask_6.any() and id_map:
        mapped = result_str.loc[mask_6].map(id_map)
        result.loc[mask_6] = mapped

    return result


def build_column_index(df: pd.DataFrame) -> tuple[dict[str, str], dict[str, list[str]]]:
    lower_map: dict[str, str] = {}
    norm_map: dict[str, list[str]] = {}
    for col in df.columns:
        col_str = str(col)
        lower = col_str.lower()
        if lower not in lower_map:
            lower_map[lower] = col_str
        norm = normalize_text(col_str).replace(" ", "")
        norm_map.setdefault(norm, []).append(col_str)
    return lower_map, norm_map


def find_column_name(
    target: object,
    columns: list[str],
    lower_map: dict[str, str],
    norm_map: dict[str, list[str]],
) -> str | None:
    if target is None or pd.isna(target):
        return None
    target_str = str(target).strip()
    if target_str in columns:
        return target_str
    lower = target_str.lower()
    if lower in lower_map:
        return lower_map[lower]
    norm = normalize_text(target_str).replace(" ", "")
    if norm in norm_map:
        return norm_map[norm][0]
    return None


def read_sections_mapping() -> pd.DataFrame:
    if not SECTIONS_MAPPING_PATH.exists():
        return pd.DataFrame()
    return pd.read_excel(SECTIONS_MAPPING_PATH)


def read_architecture(section: str) -> pd.DataFrame:
    sheet_id = ARCH_SHEETS.get(section)
    if not sheet_id:
        raise ValueError(f"No architecture sheet configured for: {section}")
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    return pd.read_excel(url)


def load_directory() -> pd.DataFrame:
    if not DIRECTORY_PATH.exists():
        raise FileNotFoundError(f"Directory file not found: {DIRECTORY_PATH}")
    df = pd.read_csv(DIRECTORY_PATH, dtype=str)
    df = df.rename(columns={c: c.strip() for c in df.columns})
    for col in ["id_municipio", "id_municipio_6"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    if "id_municipio" in df.columns:
        df["id_municipio"] = df["id_municipio"].str.zfill(7)
    if "id_municipio_6" in df.columns:
        df["id_municipio_6"] = df["id_municipio_6"].str.zfill(6)
    return df


def cast_series(series: pd.Series, bigquery_type: object) -> pd.Series:
    if bigquery_type is None or pd.isna(bigquery_type):
        return series
    dtype = str(bigquery_type).lower()
    if "int" in dtype:
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if "float" in dtype or "numeric" in dtype or "double" in dtype:
        return pd.to_numeric(series, errors="coerce")
    return series.astype("string")


def load_year_master(year: int) -> tuple[pd.DataFrame | None, dict[str, str]]:
    file_path = find_data_file(year)
    if not file_path:
        print(f"  Year {year}: data file not found")
        return None, {}

    try:
        xl, _engine = open_excel_file(file_path)
    except Exception as exc:
        print(f"  Year {year}: could not open file ({exc})")
        return None, {}

    sheet_names = [s for s in xl.sheet_names if not is_dictionary_sheet(s)]
    master_df = None
    id_col_used = None

    for sheet in sheet_names:
        try:
            df = xl.parse(sheet_name=sheet)
        except Exception as exc:
            print(f"  Year {year}: failed reading sheet '{sheet}' ({exc})")
            continue

        df = df.loc[:, [c for c in df.columns if str(c).lower() != "nan" and not str(c).startswith("Unnamed")]]
        df.columns = [str(c).strip() for c in df.columns]
        id_col = detect_id_column(df)
        if not id_col:
            print(f"  Year {year}: no id column detected in '{sheet}'")
            continue

        df = df.rename(columns={id_col: "__id_municipio__"})
        if master_df is None:
            master_df = df
            id_col_used = id_col
        else:
            new_cols = [c for c in df.columns if c not in master_df.columns or c == "__id_municipio__"]
            df = df[new_cols]
            master_df = master_df.merge(df, on="__id_municipio__", how="outer")

    if master_df is None:
        print(f"  Year {year}: no sheets merged")
        return None, {}

    meta_cols = {"id_column": id_col_used or "__id_municipio__"}
    return master_df, meta_cols


def build_section_year(
    section: str,
    year: int,
    arch_df: pd.DataFrame,
    master_df: pd.DataFrame,
    directory_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [str(c) for c in master_df.columns]
    lower_map, norm_map = build_column_index(master_df)
    if "__id_municipio__" in master_df:
        id_series = map_to_id_municipio_7(master_df["__id_municipio__"], directory_df)
    else:
        id_series = pd.Series(pd.NA, index=master_df.index)

    sigla_series = pd.Series(pd.NA, index=master_df.index)
    if "sigla_uf" in directory_df.columns and "id_municipio" in directory_df.columns:
        sigla_lookup = directory_df.set_index("id_municipio")["sigla_uf"]
        sigla_series = id_series.map(sigla_lookup)

    sigla_col = detect_sigla_uf_column(master_df)
    if sigla_col:
        sigla_series = sigla_series.combine_first(master_df[sigla_col])

    output_columns: dict[str, pd.Series] = {}
    year_col = f"original_name_{year}"

    for _, row in arch_df.iterrows():
        clean_name = row.get("name")
        bq_type = row.get("bigquery_type")
        original = row.get(year_col)

        # Skip rows with empty or null column names
        if clean_name is None or pd.isna(clean_name) or str(clean_name).strip() == "":
            continue

        if clean_name == "ano":
            series = pd.Series([year] * len(master_df), index=master_df.index)
        elif clean_name == "id_municipio":
            series = id_series
        elif clean_name == "sigla_uf":
            series = sigla_series
        else:
            col_name = find_column_name(original, columns, lower_map, norm_map)
            if (
                not col_name
                and section == "habitacao"
                and isinstance(clean_name, str)
                and clean_name.endswith("_ano_lei")
            ):
                fallback_candidates = [clean_name.replace("_ano_lei", "_ano")]
                if isinstance(original, str):
                    fallback_candidates.append(original.replace("_ano_lei", "_ano"))
                for candidate in fallback_candidates:
                    col_name = find_column_name(candidate, columns, lower_map, norm_map)
                    if col_name:
                        break
            if col_name:
                series = master_df[col_name]
            else:
                series = pd.Series(pd.NA, index=master_df.index)

        if clean_name == "idade" and str(bq_type).lower().startswith("int"):
            series = series.replace({"Ignorado": pd.NA, "ignorado": pd.NA})
        output_columns[clean_name] = cast_series(series, bq_type)

    return pd.DataFrame(output_columns)


def collect_years_from_arch(arch_df: pd.DataFrame) -> list[int]:
    years = []
    for col in arch_df.columns:
        if str(col).startswith("original_name_"):
            try:
                years.append(int(str(col).replace("original_name_", "")))
            except ValueError:
                continue
    return sorted(set(years))


def section_exists_in_year(mapping_df: pd.DataFrame, section: str, year: int) -> bool:
    if mapping_df.empty:
        return True
    year_col = str(year)
    if year_col not in mapping_df.columns:
        return True
    matches = mapping_df[mapping_df["cleaned_string"] == section]
    if matches.empty:
        return True
    return matches[year_col].notna().any()


def run(sections: list[str]) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    master_cache: dict[int, pd.DataFrame] = {}
    mapping_df = read_sections_mapping()
    directory_df = load_directory()
    text_replacements = {
        "Ensino médio completo": "Ensino médio (2º Grau) completo",
        "Ensino fundamental completo": "Ensino fundamental (1º Grau) completo",
        "Não aplicavel": "Não aplicável",
        "-": "",
    }

    for section in sections:
        print(f"\nProcessing section: {section}")
        arch_df = read_architecture(section)
        years = collect_years_from_arch(arch_df)
        if not years:
            print(f"  No year columns found for {section}")
            continue

        section_frames = []

        for year in years:
            year_col = f"original_name_{year}"
            if year_col not in arch_df.columns or not arch_df[year_col].notna().any():
                continue
            if not section_exists_in_year(mapping_df, section, year):
                continue

            if year not in master_cache:
                master_df, _meta = load_year_master(year)
                if master_df is None:
                    continue
                master_cache[year] = master_df

            year_df = build_section_year(section, year, arch_df, master_cache[year], directory_df)
            if not year_df.empty:
                section_frames.append(year_df)

        if not section_frames:
            print(f"  No data found for {section}")
            continue

        final_df = pd.concat(section_frames, ignore_index=True)
        # Filter out rows without a valid id_municipio
        if "id_municipio" in final_df.columns:
            final_df = final_df[final_df["id_municipio"].notna() & (final_df["id_municipio"].astype(str).str.strip() != "")]
        if "id_municipio" in final_df.columns and "ano" in final_df.columns:
            final_df = final_df.sort_values(["id_municipio", "ano"], na_position="last")
        string_cols = final_df.select_dtypes(include=["object", "string"]).columns
        if len(string_cols) > 0:
            final_df[string_cols] = final_df[string_cols].replace(text_replacements)
        out_path = OUTPUT_DIR / f"{section}.csv"
        final_df.to_csv(out_path, index=False)
        print(f"  Wrote {out_path.relative_to(BASE_DIR)} ({len(final_df)} rows)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean MUNIC data into section CSVs.")
    parser.add_argument(
        "--sections",
        type=str,
        default=",".join(DEFAULT_SECTIONS),
        help="Comma-separated section names (default: atual_prefeito,habitacao,meio_ambiente,recursos_gestao,recursos_humanos)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sections = [s.strip() for s in args.sections.split(",") if s.strip()]
    run(sections)


if __name__ == "__main__":
    main()
