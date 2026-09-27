"""Generate the dicionario parquet for us_cfpb_hmda from the CFPB HMDA code sheets.

  uv run --with duckdb python gen_dicionario.py

One row per (id_tabela, nome_coluna, chave, valor) for every coded categorical column
(covered_by_dictionary=yes) in the two LAR tables. `valor` holds the canonical CFPB label
in English (the data language). Codes are transcribed from the public HMDA Filing
Instructions Guide / LAR data-fields documentation (modern, 2018+) and the historic LAR
record code sheet (legacy, 2007-2017). cobertura_temporal is left blank (same as table).

Output: output/dicionario/data.parquet
"""

import pandas as pd
from common import LEGACY, MODERN, OUTPUT, load_cols

# ---- shared code sets (modern 2018+) -------------------------------------------------
ETHNICITY = {  # applicant/co-applicant ethnicity-1..5
    "1": "Hispanic or Latino",
    "11": "Mexican",
    "12": "Puerto Rican",
    "13": "Cuban",
    "14": "Other Hispanic or Latino",
    "2": "Not Hispanic or Latino",
    "3": "Information not provided by applicant in mail, internet, or telephone application",
    "4": "Not applicable",
    "5": "No co-applicant",
}
ETH_OBSERVED = {
    "1": "Collected on the basis of visual observation or surname",
    "2": "Not collected on the basis of visual observation or surname",
    "3": "Not applicable",
    "4": "No co-applicant",
}
RACE = {
    "1": "American Indian or Alaska Native",
    "2": "Asian",
    "21": "Asian Indian",
    "22": "Chinese",
    "23": "Filipino",
    "24": "Japanese",
    "25": "Korean",
    "26": "Vietnamese",
    "27": "Other Asian",
    "3": "Black or African American",
    "4": "Native Hawaiian or Other Pacific Islander",
    "41": "Native Hawaiian",
    "42": "Guamanian or Chamorro",
    "43": "Samoan",
    "44": "Other Pacific Islander",
    "5": "White",
    "6": "Information not provided by applicant in mail, internet, or telephone application",
    "7": "Not applicable",
    "8": "No co-applicant",
}
RACE_OBSERVED = ETH_OBSERVED
SEX = {
    "1": "Male",
    "2": "Female",
    "3": "Information not provided by applicant in mail, internet, or telephone application",
    "4": "Not applicable",
    "5": "No co-applicant",
    "6": "Applicant selected both male and female",
}
SEX_OBSERVED = ETH_OBSERVED
AGE = {
    "1": "<25",
    "2": "25-34",
    "3": "35-44",
    "4": "45-54",
    "5": "55-64",
    "6": "65-74",
    "7": ">74",
    "8888": "Not applicable",
    "9999": "No co-applicant",
}
YESNO_NA = {"Yes": "Yes", "No": "No", "NA": "Not applicable"}
YN_EXEMPT = {"1": "Yes", "2": "No", "1111": "Exempt"}
CREDIT_SCORE = {
    "1": "Equifax Beacon 5.0",
    "2": "Experian Fair Isaac",
    "3": "FICO Risk Score Classic 04",
    "4": "FICO Risk Score Classic 98",
    "5": "VantageScore 2.0",
    "6": "VantageScore 3.0",
    "7": "More than one credit scoring model",
    "8": "Other credit scoring model",
    "9": "Not applicable",
    "10": "No co-applicant",
    "1111": "Exempt",
}
AUS = {
    "1": "Desktop Underwriter (DU)",
    "2": "Loan Prospector (LP) / Loan Product Advisor",
    "3": "Technology Open to Approved Lenders (TOTAL) Scorecard",
    "4": "Guaranteed Underwriting System (GUS)",
    "5": "Other",
    "6": "Not applicable",
    "7": "Internal Proprietary System",
    "1111": "Exempt",
}
DENIAL = {
    "1": "Debt-to-income ratio",
    "2": "Employment history",
    "3": "Credit history",
    "4": "Collateral",
    "5": "Insufficient cash (downpayment, closing costs)",
    "6": "Unverifiable information",
    "7": "Credit application incomplete",
    "8": "Mortgage insurance denied",
    "9": "Other",
    "10": "Not applicable",
    "1111": "Exempt",
}

MODERN_CODES = {
    "conforming_loan_limit": {
        "C": "Conforming",
        "NC": "Nonconforming",
        "U": "Undetermined",
        "NA": "Not applicable",
    },
    "action_taken": {
        "1": "Loan originated",
        "2": "Application approved but not accepted",
        "3": "Application denied",
        "4": "Application withdrawn by applicant",
        "5": "File closed for incompleteness",
        "6": "Purchased loan",
        "7": "Preapproval request denied",
        "8": "Preapproval request approved but not accepted",
    },
    "purchaser_type": {
        "0": "Not applicable",
        "1": "Fannie Mae",
        "2": "Ginnie Mae",
        "3": "Freddie Mac",
        "4": "Farmer Mac",
        "5": "Private securitizer",
        "6": "Commercial bank, savings bank, or savings association",
        "71": "Credit union, mortgage company, or finance company",
        "72": "Life insurance company",
        "8": "Affiliate institution",
        "9": "Other type of purchaser",
    },
    "preapproval": {
        "1": "Preapproval requested",
        "2": "Preapproval not requested",
    },
    "loan_type": {
        "1": "Conventional",
        "2": "Federal Housing Administration insured (FHA)",
        "3": "Veterans Affairs guaranteed (VA)",
        "4": "USDA Rural Housing Service or Farm Service Agency guaranteed (RHS/FSA)",
    },
    "loan_purpose": {
        "1": "Home purchase",
        "2": "Home improvement",
        "31": "Refinancing",
        "32": "Cash-out refinancing",
        "4": "Other purpose",
        "5": "Not applicable",
    },
    "lien_status": {
        "1": "Secured by a first lien",
        "2": "Secured by a subordinate lien",
    },
    "reverse_mortgage": YN_EXEMPT,
    "open_end_line_of_credit": {
        "1": "Open-end line of credit",
        "2": "Not an open-end line of credit",
        "1111": "Exempt",
    },
    "business_or_commercial_purpose": {
        "1": "Primarily for a business or commercial purpose",
        "2": "Not primarily for a business or commercial purpose",
        "1111": "Exempt",
    },
    "hoepa_status": {
        "1": "High-cost mortgage",
        "2": "Not a high-cost mortgage",
        "3": "Not applicable",
    },
    "negative_amortization": {
        "1": "Negative amortization",
        "2": "No negative amortization",
        "1111": "Exempt",
    },
    "interest_only_payment": {
        "1": "Interest-only payments",
        "2": "No interest-only payments",
        "1111": "Exempt",
    },
    "balloon_payment": {
        "1": "Balloon payment",
        "2": "No balloon payment",
        "1111": "Exempt",
    },
    "other_nonamortizing_features": {
        "1": "Other non-fully amortizing features",
        "2": "No other non-fully amortizing features",
        "1111": "Exempt",
    },
    "construction_method": {"1": "Site-built", "2": "Manufactured home"},
    "occupancy_type": {
        "1": "Principal residence",
        "2": "Second residence",
        "3": "Investment property",
    },
    "manufactured_home_secured_property_type": {
        "1": "Manufactured home and land",
        "2": "Manufactured home and not land",
        "3": "Not applicable",
        "1111": "Exempt",
    },
    "manufactured_home_land_property_interest": {
        "1": "Direct ownership",
        "2": "Indirect ownership",
        "3": "Paid leasehold",
        "4": "Unpaid leasehold",
        "5": "Not applicable",
        "1111": "Exempt",
    },
    "total_units": {
        "1": "1",
        "2": "2",
        "3": "3",
        "4": "4",
        "5-24": "5-24",
        "25-49": "25-49",
        "50-99": "50-99",
        "100-149": "100-149",
        ">149": ">149",
    },
    "debt_to_income_ratio": {
        "<20%": "<20%",
        "20%-<30%": "20%-<30%",
        "30%-<36%": "30%-<36%",
        "50%-60%": "50%-60%",
        ">60%": ">60%",
        "NA": "Not applicable",
        "Exempt": "Exempt",
    },
    "applicant_credit_score_type": CREDIT_SCORE,
    "co_applicant_credit_score_type": CREDIT_SCORE,
    "submission_of_application": {
        "1": "Submitted directly to your institution",
        "2": "Not submitted directly to your institution",
        "3": "Not applicable",
        "1111": "Exempt",
    },
    "initially_payable_to_institution": {
        "1": "Initially payable to your institution",
        "2": "Not initially payable to your institution",
        "3": "Not applicable",
        "1111": "Exempt",
    },
    "applicant_age": AGE,
    "co_applicant_age": AGE,
    "applicant_age_above_62": YESNO_NA,
    "co_applicant_age_above_62": YESNO_NA,
    "applicant_sex": SEX,
    "co_applicant_sex": SEX,
    "applicant_sex_observed": SEX_OBSERVED,
    "co_applicant_sex_observed": SEX_OBSERVED,
    "applicant_ethnicity_observed": ETH_OBSERVED,
    "co_applicant_ethnicity_observed": ETH_OBSERVED,
    "applicant_race_observed": RACE_OBSERVED,
    "co_applicant_race_observed": RACE_OBSERVED,
    **{f"applicant_ethnicity_{i}": ETHNICITY for i in range(1, 6)},
    **{f"co_applicant_ethnicity_{i}": ETHNICITY for i in range(1, 6)},
    **{f"applicant_race_{i}": RACE for i in range(1, 6)},
    **{f"co_applicant_race_{i}": RACE for i in range(1, 6)},
    **{f"aus_{i}": AUS for i in range(1, 6)},
    **{f"denial_reason_{i}": DENIAL for i in range(1, 5)},
}

# ---- legacy code sets (2007-2017) ----------------------------------------------------
L_RACE = {
    "1": "American Indian or Alaska Native",
    "2": "Asian",
    "3": "Black or African American",
    "4": "Native Hawaiian or Other Pacific Islander",
    "5": "White",
    "6": "Information not provided by applicant",
    "7": "Not applicable",
    "8": "No co-applicant",
}
L_ETH = {
    "1": "Hispanic or Latino",
    "2": "Not Hispanic or Latino",
    "3": "Information not provided by applicant",
    "4": "Not applicable",
    "5": "No co-applicant",
}
L_SEX = {
    "1": "Male",
    "2": "Female",
    "3": "Information not provided by applicant",
    "4": "Not applicable",
    "5": "No co-applicant",
}
L_DENIAL = {
    "1": "Debt-to-income ratio",
    "2": "Employment history",
    "3": "Credit history",
    "4": "Collateral",
    "5": "Insufficient cash (downpayment, closing costs)",
    "6": "Unverifiable information",
    "7": "Credit application incomplete",
    "8": "Mortgage insurance denied",
    "9": "Other",
}
LEGACY_CODES = {
    "agency_code": {
        "1": "Office of the Comptroller of the Currency (OCC)",
        "2": "Federal Reserve System (FRS)",
        "3": "Federal Deposit Insurance Corporation (FDIC)",
        "5": "National Credit Union Administration (NCUA)",
        "7": "Department of Housing and Urban Development (HUD)",
        "9": "Consumer Financial Protection Bureau (CFPB)",
    },
    "loan_type": {
        "1": "Conventional",
        "2": "FHA insured",
        "3": "VA guaranteed",
        "4": "FSA/RHS guaranteed",
    },
    "property_type": {
        "1": "One to four-family (other than manufactured housing)",
        "2": "Manufactured housing",
        "3": "Multifamily",
    },
    "loan_purpose": {
        "1": "Home purchase",
        "2": "Home improvement",
        "3": "Refinancing",
    },
    "owner_occupancy": {
        "1": "Owner-occupied as a principal dwelling",
        "2": "Not owner-occupied as a principal dwelling",
        "3": "Not applicable",
    },
    "preapproval": {
        "1": "Preapproval was requested",
        "2": "Preapproval was not requested",
        "3": "Not applicable",
    },
    "action_taken": {
        "1": "Loan originated",
        "2": "Application approved but not accepted",
        "3": "Application denied by financial institution",
        "4": "Application withdrawn by applicant",
        "5": "File closed for incompleteness",
        "6": "Loan purchased by the institution",
        "7": "Preapproval request denied by financial institution",
        "8": "Preapproval request approved but not accepted",
    },
    "purchaser_type": {
        "0": "Loan was not originated or was not sold in the year",
        "1": "Fannie Mae",
        "2": "Ginnie Mae",
        "3": "Freddie Mac",
        "4": "Farmer Mac",
        "5": "Private securitization",
        "6": "Commercial bank, savings bank or savings association",
        "7": "Life insurance company, credit union, mortgage bank, or finance company",
        "8": "Affiliate institution",
        "9": "Other type of purchaser",
    },
    "hoepa_status": {"1": "HOEPA loan", "2": "Not a HOEPA loan"},
    "lien_status": {
        "1": "Secured by a first lien",
        "2": "Secured by a subordinate lien",
        "3": "Not secured by a lien",
        "4": "Not applicable",
    },
    "applicant_ethnicity": L_ETH,
    "co_applicant_ethnicity": L_ETH,
    "applicant_sex": L_SEX,
    "co_applicant_sex": L_SEX,
    **{f"applicant_race_{i}": L_RACE for i in range(1, 6)},
    **{f"co_applicant_race_{i}": L_RACE for i in range(1, 6)},
    **{f"denial_reason_{i}": L_DENIAL for i in range(1, 4)},
}


def build() -> pd.DataFrame:
    rows = []
    for table, codes in ((MODERN, MODERN_CODES), (LEGACY, LEGACY_CODES)):
        arch = {c.name for c in load_cols(table) if c.name != "year"}
        for col, mapping in codes.items():
            if col not in arch:
                raise SystemExit(
                    f"{table}: dicionario column {col!r} not in architecture"
                )
            for chave, valor in mapping.items():
                rows.append((table, col, str(chave), "", valor))
    return pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )


def main() -> None:
    df = build()
    out_dir = OUTPUT / "dicionario"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "data.parquet"
    df.to_parquet(out, compression="snappy", index=False)
    print(f"wrote {out}  rows={len(df):,}")
    print(
        "per-table coded columns:",
        df.groupby("id_tabela")["nome_coluna"].nunique().to_dict(),
    )


if __name__ == "__main__":
    main()
