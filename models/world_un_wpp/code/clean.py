"""Cleaning code for world_un_wpp (UN World Population Prospects 2024, Medium variant).

Tables:
    1. demographic_indicators   — country x year, 54 indicators
    2. population_age_group_sex — country x year x 5-year age group
    3. population_single_age_sex — country x year x single age (0-100)

Global rules:
    - Countries only: LocTypeID == 4 (equivalently non-empty ISO3_code; asserted).
    - Values kept in raw units (thousands). No unit conversion.
    - Output: snappy Parquet, hive-partitioned by year:
      output/<table_slug>/year=<YYYY>/data.parquet
      (partition column `year` is encoded in the path, not stored in the file).
"""

import gc
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "input"
OUTPUT = ROOT / "output"

FILE_INDICATORS = INPUT / "WPP2024_Demographic_Indicators_Medium.csv.gz"
FILE_AGE_GROUP = (
    INPUT / "WPP2024_Population1JanuaryByAge5GroupSex_Medium.csv.gz"
)
FILES_SINGLE_AGE = [
    INPUT / "WPP2024_PopulationBySingleAgeSex_Medium_1950-2023.csv.gz",
    INPUT / "WPP2024_PopulationBySingleAgeSex_Medium_2024-2100.csv.gz",
]

EXPECTED_N_COUNTRIES = 237

INDICATOR_RENAMES = {
    "TPopulation1Jan": "total_population_1_january",
    "TPopulation1July": "total_population_1_july",
    "TPopulationMale1July": "total_population_male_1_july",
    "TPopulationFemale1July": "total_population_female_1_july",
    "PopDensity": "population_density",
    "PopSexRatio": "population_sex_ratio",
    "MedianAgePop": "median_age",
    "NatChange": "natural_change",
    "NatChangeRT": "rate_natural_change",
    "PopChange": "population_change",
    "PopGrowthRate": "population_growth_rate",
    "DoublingTime": "population_doubling_time",
    "Births": "births",
    "Births1519": "births_women_age_15_19",
    "CBR": "crude_birth_rate",
    "TFR": "total_fertility_rate",
    "NRR": "net_reproduction_rate",
    "MAC": "mean_age_childbearing",
    "SRB": "sex_ratio_at_birth",
    "Deaths": "total_deaths",
    "DeathsMale": "total_deaths_male",
    "DeathsFemale": "total_deaths_female",
    "CDR": "crude_death_rate",
    "LEx": "life_expectancy_at_birth",
    "LExMale": "life_expectancy_at_birth_male",
    "LExFemale": "life_expectancy_at_birth_female",
    "LE15": "life_expectancy_at_15",
    "LE15Male": "life_expectancy_at_15_male",
    "LE15Female": "life_expectancy_at_15_female",
    "LE65": "life_expectancy_at_65",
    "LE65Male": "life_expectancy_at_65_male",
    "LE65Female": "life_expectancy_at_65_female",
    "LE80": "life_expectancy_at_80",
    "LE80Male": "life_expectancy_at_80_male",
    "LE80Female": "life_expectancy_at_80_female",
    "InfantDeaths": "infant_deaths",
    "IMR": "infant_mortality_rate",
    "LBsurvivingAge1": "live_births_surviving_age_1",
    "Under5Deaths": "under_5_deaths",
    "Q5": "under_5_mortality_rate",
    "Q0040": "mortality_before_age_40",
    "Q0040Male": "mortality_before_age_40_male",
    "Q0040Female": "mortality_before_age_40_female",
    "Q0060": "mortality_before_age_60",
    "Q0060Male": "mortality_before_age_60_male",
    "Q0060Female": "mortality_before_age_60_female",
    "Q1550": "mortality_between_age_15_50",
    "Q1550Male": "mortality_between_age_15_50_male",
    "Q1550Female": "mortality_between_age_15_50_female",
    "Q1560": "mortality_between_age_15_60",
    "Q1560Male": "mortality_between_age_15_60_male",
    "Q1560Female": "mortality_between_age_15_60_female",
    "NetMigrations": "net_migrations",
    "CNMR": "net_migration_rate",
}


def filter_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Keep countries only (LocTypeID == 4) and assert agreement with ISO3_code."""
    loc_type = pd.to_numeric(df["LocTypeID"], errors="coerce")
    iso3 = df["ISO3_code"].astype("string").str.strip()
    mask_type = (loc_type == 4).to_numpy(dtype=bool)
    mask_iso3 = (
        (iso3.notna() & (iso3 != "")).fillna(False).to_numpy(dtype=bool)
    )
    assert (mask_type == mask_iso3).all(), (
        f"LocTypeID==4 ({mask_type.sum()}) and non-empty ISO3_code "
        f"({mask_iso3.sum()}) filters disagree"
    )
    out = df.loc[mask_type].copy()
    n_countries = out["ISO3_code"].nunique()
    assert n_countries == EXPECTED_N_COUNTRIES, (
        f"expected {EXPECTED_N_COUNTRIES} countries, got {n_countries}"
    )
    return out


def write_partitions(
    df: pd.DataFrame, table_slug: str, schema: pa.Schema
) -> None:
    """Write one snappy parquet file per year: output/<slug>/year=<YYYY>/data.parquet.

    `year` is encoded in the path only; the file schema excludes it.
    """
    file_schema = pa.schema([f for f in schema if f.name != "year"])
    file_cols = [f.name for f in file_schema]
    for year, group in df.groupby("year", sort=True):
        part_dir = OUTPUT / table_slug / f"year={int(year)}"
        part_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(
            group[file_cols], schema=file_schema, preserve_index=False
        )
        pq.write_table(table, part_dir / "data.parquet", compression="snappy")


def clean_demographic_indicators() -> None:
    print("=== demographic_indicators ===")
    df = pd.read_csv(FILE_INDICATORS, encoding="utf-8-sig", low_memory=False)
    df = filter_countries(df)
    df = df.rename(columns={"Time": "year", "ISO3_code": "country_iso3_code"})
    df = df.rename(columns=INDICATOR_RENAMES)

    cols = ["year", "country_iso3_code", *INDICATOR_RENAMES.values()]
    df = df[cols]

    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["country_iso3_code"] = df["country_iso3_code"].astype(str).str.strip()
    for col in INDICATOR_RENAMES.values():
        df[col] = pd.to_numeric(df[col], errors="coerce")

    schema = pa.schema(
        [("year", pa.int64()), ("country_iso3_code", pa.string())]
        + [(col, pa.float64()) for col in INDICATOR_RENAMES.values()]
    )
    print(f"rows: {len(df):,} | years: {df['year'].min()}-{df['year'].max()}")
    write_partitions(df, "demographic_indicators", schema)
    del df
    gc.collect()


POP_SCHEMA_AGE_GROUP = pa.schema(
    [
        ("year", pa.int64()),
        ("country_iso3_code", pa.string()),
        ("age_group", pa.string()),
        ("population_male", pa.float64()),
        ("population_female", pa.float64()),
        ("population_total", pa.float64()),
    ]
)

POP_SCHEMA_SINGLE_AGE = pa.schema(
    [
        ("year", pa.int64()),
        ("country_iso3_code", pa.string()),
        ("age", pa.int64()),
        ("population_male", pa.float64()),
        ("population_female", pa.float64()),
        ("population_total", pa.float64()),
    ]
)

POP_USECOLS = [
    "ISO3_code",
    "LocTypeID",
    "Time",
    "AgeGrp",
    "AgeGrpStart",
    "PopMale",
    "PopFemale",
    "PopTotal",
]


def _read_population_file(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path, encoding="utf-8-sig", usecols=POP_USECOLS, low_memory=False
    )
    df = filter_countries(df)
    df = df.rename(
        columns={
            "Time": "year",
            "ISO3_code": "country_iso3_code",
            "PopMale": "population_male",
            "PopFemale": "population_female",
            "PopTotal": "population_total",
        }
    )
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["country_iso3_code"] = df["country_iso3_code"].astype(str).str.strip()
    for col in ["population_male", "population_female", "population_total"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def clean_population_age_group_sex() -> None:
    print("=== population_age_group_sex ===")
    df = _read_population_file(FILE_AGE_GROUP)
    df["age_group"] = df["AgeGrp"].astype(str).str.strip()
    df = df[[f.name for f in POP_SCHEMA_AGE_GROUP]]
    print(f"rows: {len(df):,} | years: {df['year'].min()}-{df['year'].max()}")
    write_partitions(df, "population_age_group_sex", POP_SCHEMA_AGE_GROUP)
    del df
    gc.collect()


def clean_population_single_age_sex() -> None:
    print("=== population_single_age_sex ===")
    # Files cover disjoint year ranges (1950-2023, 2024-2100); process sequentially.
    for path in FILES_SINGLE_AGE:
        print(f"processing {path.name} ...")
        df = _read_population_file(path)
        df["age"] = pd.to_numeric(df["AgeGrpStart"], errors="coerce").astype(
            "Int64"
        )
        df = df[[f.name for f in POP_SCHEMA_SINGLE_AGE]]
        print(
            f"  rows: {len(df):,} | years: {df['year'].min()}-{df['year'].max()}"
        )
        write_partitions(
            df, "population_single_age_sex", POP_SCHEMA_SINGLE_AGE
        )
        del df
        gc.collect()


def main() -> None:
    for slug in [
        "demographic_indicators",
        "population_age_group_sex",
        "population_single_age_sex",
    ]:
        target = OUTPUT / slug
        if target.exists():
            shutil.rmtree(target)
    clean_demographic_indicators()
    clean_population_age_group_sex()
    clean_population_single_age_sex()
    print("done.")


if __name__ == "__main__":
    main()
