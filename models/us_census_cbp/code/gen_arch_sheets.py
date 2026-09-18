"""Generate the wide CBP architecture-sheet rows (national, state, county) as TSV.

One TSV per table with the standard 10 BD architecture columns. The size-class
columns follow a mechanical grid, so we emit them programmatically to guarantee
consistency. dicionario is unchanged and not regenerated here.
"""

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

# 9 size bands present in every file (US/state carry emp+payroll per band;
# county carries establishment counts only). label = human phrase.
BANDS9 = [
    ("1_4", "1 to 4 employees"),
    ("5_9", "5 to 9 employees"),
    ("10_19", "10 to 19 employees"),
    ("20_49", "20 to 49 employees"),
    ("50_99", "50 to 99 employees"),
    ("100_249", "100 to 249 employees"),
    ("250_499", "250 to 499 employees"),
    ("500_999", "500 to 999 employees"),
    ("1000", "1,000 or more employees"),
]
# county-only finer 1,000+ bands (source n1000_1..n1000_4)
BANDS_CO_EXTRA = [
    ("1000_1499", "1,000 to 1,499 employees", "n1000_1"),
    ("1500_2499", "1,500 to 2,499 employees", "n1000_2"),
    ("2500_4999", "2,500 to 4,999 employees", "n1000_3"),
    ("5000_more", "5,000 or more employees", "n1000_4"),
]


def row(name, typ, desc, dict_=False, dir_="", unit="", obs="", orig=""):
    return [
        name,
        typ,
        desc,
        "",
        "yes" if dict_ else "no",
        dir_,
        unit,
        "no",
        obs,
        orig,
    ]


# shared key/dimension rows
YEAR = row(
    "year",
    "INT64",
    "Reference year",
    dir_="br_bd_diretorios_data_tempo.ano:ano",
    unit="year",
    orig="(file year)",
)
ID_STATE = row(
    "id_state",
    "STRING",
    "FIPS state code",
    dir_="br_bd_diretorios_us.state:id_state",
    orig="fipstate",
)
ID_COUNTY = row(
    "id_county",
    "STRING",
    "FIPS county code, five digits (state + county); a county part of 999 means establishments not allocated to a specific county (statewide)",
    dir_="br_bd_diretorios_us.county:id_county",
    orig="fipstate+fipscty",
    obs="Codes follow the vintage of each reference year, so counties that were later dissolved or renumbered (Connecticut counties before the 2022 planning regions, some Alaska boroughs, Dade County FL) are present in early years and do not match the current county directory.",
)
NAICS = row(
    "naics",
    "STRING",
    "NAICS industry code at its published aggregation level (2 to 6 digits); vintage varies by year, see naics_version",
    dir_="br_bd_diretorios_us.naics_2017:id_naics",
    orig="naics",
)
NAICS_V = row(
    "naics_version",
    "STRING",
    "NAICS classification vintage the code follows (1997, 2002, 2007, 2012, 2017)",
    dict_=True,
    obs="CBP lags the NAICS revisions: 2022 and 2023 data still follow NAICS 2017, so no 2022 vintage occurs in this dataset.",
    orig="(derived from year)",
)
LFO = row(
    "lfo",
    "STRING",
    "Legal form of organization of the establishments",
    dict_=True,
    orig="lfo",
)

# shared total (all-size) measure rows
TOTALS = [
    row(
        "establishments",
        "INT64",
        "Number of establishments",
        unit="establishment",
        orig="est",
    ),
    row(
        "employment",
        "INT64",
        "Number of paid employees during the week of March 12",
        unit="employee",
        orig="emp",
    ),
    row(
        "employment_flag",
        "STRING",
        "Data-suppression flag marking the employment-size range of a withheld cell (1998 to 2016 suppression era)",
        dict_=True,
        orig="empflag",
    ),
    row(
        "employment_noise_flag",
        "STRING",
        "Noise-infusion flag for employment from 2017 onward; G, H, J indicate increasing noise, D and S indicate withheld",
        dict_=True,
        orig="emp_nf",
    ),
    row(
        "payroll_first_quarter",
        "INT64",
        "First-quarter payroll in current U.S. dollars",
        unit="USD",
        orig="qp1 (x1000)",
    ),
    row(
        "payroll_first_quarter_noise_flag",
        "STRING",
        "Noise-infusion flag for first-quarter payroll",
        dict_=True,
        orig="qp1_nf",
    ),
    row(
        "payroll_annual",
        "INT64",
        "Annual payroll in current U.S. dollars",
        unit="USD",
        orig="ap (x1000)",
    ),
    row(
        "payroll_annual_noise_flag",
        "STRING",
        "Noise-infusion flag for annual payroll",
        dict_=True,
        orig="ap_nf",
    ),
]


def band_measure_rows(code, label):
    """8 columns per size band for national/state (values + flags)."""
    return [
        row(
            f"establishments_{code}",
            "INT64",
            f"Number of establishments with {label}",
            unit="establishment",
            orig=f"n{code}",
        ),
        row(
            f"employment_{code}",
            "INT64",
            f"Number of paid employees during the week of March 12 in establishments with {label}",
            unit="employee",
            orig=f"e{code}",
        ),
        row(
            f"employment_{code}_flag",
            "STRING",
            f"Data-suppression flag for employment in establishments with {label}",
            dict_=True,
            orig=f"f{code}",
        ),
        row(
            f"employment_{code}_noise_flag",
            "STRING",
            f"Noise-infusion flag for employment in establishments with {label}",
            dict_=True,
            orig=f"e{code}nf",
        ),
        row(
            f"payroll_first_quarter_{code}",
            "INT64",
            f"First-quarter payroll in current U.S. dollars for establishments with {label}",
            unit="USD",
            orig=f"q{code} (x1000)",
        ),
        row(
            f"payroll_first_quarter_{code}_noise_flag",
            "STRING",
            f"Noise-infusion flag for first-quarter payroll in establishments with {label}",
            dict_=True,
            orig=f"q{code}nf",
        ),
        row(
            f"payroll_annual_{code}",
            "INT64",
            f"Annual payroll in current U.S. dollars for establishments with {label}",
            unit="USD",
            orig=f"a{code} (x1000)",
        ),
        row(
            f"payroll_annual_{code}_noise_flag",
            "STRING",
            f"Noise-infusion flag for annual payroll in establishments with {label}",
            dict_=True,
            orig=f"a{code}nf",
        ),
    ]


def national_rows():
    rows = [YEAR, NAICS, NAICS_V, LFO, *TOTALS]
    for code, label in BANDS9:
        rows += band_measure_rows(code, label)
    return rows


def state_rows():
    rows = [YEAR, ID_STATE, NAICS, NAICS_V, LFO, *TOTALS]
    for code, label in BANDS9:
        rows += band_measure_rows(code, label)
    return rows


def county_rows():
    # county has no lfo and only establishment counts per band (no per-band $)
    rows = [YEAR, ID_STATE, ID_COUNTY, NAICS, NAICS_V, *TOTALS]
    for code, label in BANDS9:
        orig = "n<5" if code == "1_4" else f"n{code}"
        rows.append(
            row(
                f"establishments_{code}",
                "INT64",
                f"Number of establishments with {label}",
                unit="establishment",
                orig=orig,
            )
        )
    for code, label, orig in BANDS_CO_EXTRA:
        rows.append(
            row(
                f"establishments_{code}",
                "INT64",
                f"Number of establishments with {label}",
                unit="establishment",
                orig=orig,
            )
        )
    return rows


def write_tsv(name, rows):
    p = HERE / f"sheet_{name}.tsv"
    # lineterminator is explicit: csv.writer defaults to CRLF, which the repo's
    # mixed-line-ending pre-commit hook then rewrites, so every regeneration would
    # otherwise show the whole file as changed.
    with open(p, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(HEADER)
        w.writerows(rows)
    print(f"{name}: {len(rows)} columns -> {p.name}")


if __name__ == "__main__":
    write_tsv("national", national_rows())
    write_tsv("state", state_rows())
    write_tsv("county", county_rows())
