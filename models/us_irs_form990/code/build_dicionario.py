"""Build ``dicionario.csv`` from the IRS code sheets and the NTEE code list.

Code tables are transcribed from the EO BMF information sheet
(https://www.irs.gov/pub/foia/ig/tege/eo-info.pdf, January 2026 revision);
NTEE labels come from the Nonprofit Open Data Collective's
``mission-taxonomies`` repository (``NTEE/ntee.csv``, kept here as
``ntee_codes.csv``). The resulting CSV is committed; the cleaning transform
only converts it to parquet.

    python build_dicionario.py
"""

import csv
from pathlib import Path

HERE = Path(__file__).parent
OUT = HERE / "dicionario.csv"
NTEE = HERE / "ntee_codes.csv"

FORM_TYPE = {
    "990": "Form 990, Return of Organization Exempt From Income Tax",
    "990EZ": "Form 990-EZ, Short Form Return of Organization Exempt From Income Tax",
}

SUBSECTION = {
    "01": "501(c)(1) Government instrumentality",
    "02": "501(c)(2) Title-holding corporation",
    "03": "501(c)(3) Charitable, educational, religious, scientific or literary organization",
    "04": "501(c)(4) Civic league or social welfare organization",
    "05": "501(c)(5) Labor, agricultural or horticultural organization",
    "06": "501(c)(6) Business league, chamber of commerce or board of trade",
    "07": "501(c)(7) Social or recreational club",
    "08": "501(c)(8) Fraternal beneficiary society",
    "09": "501(c)(9) Voluntary employees' beneficiary association",
    "10": "501(c)(10) Domestic fraternal society",
    "11": "501(c)(11) Teachers' retirement fund association",
    "12": "501(c)(12) Benevolent life insurance association, mutual ditch or irrigation company, mutual or cooperative telephone company",
    "13": "501(c)(13) Cemetery company or burial association",
    "14": "501(c)(14) Credit union or other mutual financial organization",
    "15": "501(c)(15) Mutual insurance company or association",
    "16": "501(c)(16) Corporation financing crop operations",
    "17": "501(c)(17) Supplemental unemployment compensation trust",
    "18": "501(c)(18) Employee-funded pension trust (created before 25 June 1959)",
    "19": "501(c)(19) Post or organization of war veterans",
    "20": "501(c)(20) Legal service organization",
    "21": "501(c)(21) Black lung benefit trust",
    "22": "501(c)(22) Multiemployer pension plan",
    "23": "501(c)(23) Veterans' association formed before 1880",
    "24": "501(c)(24) Trust described in section 4049 of ERISA",
    "25": "501(c)(25) Title-holding company for pensions and other exempt entities",
    "26": "501(c)(26) State-sponsored high-risk health insurance organization",
    "27": "501(c)(27) State-sponsored workers' compensation reinsurance organization",
    "29": "501(c)(29) ACA section 1322 qualified nonprofit health insurance issuer",
    "40": "501(d) Apostolic and religious organization",
    "50": "501(e) Cooperative hospital service organization",
    "60": "501(f) Cooperative service organization of operating educational organizations",
    "70": "501(k) Child care organization",
    "71": "501(n) Charitable risk pool",
    "81": "529 Qualified state-sponsored tuition program",
    "92": "4947(a)(1) Non-exempt charitable trust treated as a private foundation (Form 990-PF filer)",
    "00": "Not exempt under any subsection (revoked or not yet determined)",
    # Present in the data but absent from the IRS information sheet.
    "82": "Code 82, not documented in the IRS EO BMF information sheet",
    "91": "Code 91, not documented in the IRS EO BMF information sheet",
}

# subsection + classification digit -> label (EO BMF info sheet table)
CLASSIFICATION = {
    "011": "Government instrumentality",
    "021": "Title-holding corporation",
    "031": "Charitable organization",
    "032": "Educational organization",
    "033": "Literary organization",
    "034": "Organization to prevent cruelty to animals",
    "035": "Organization to prevent cruelty to children",
    "036": "Organization for public safety testing",
    "037": "Religious organization",
    "038": "Scientific organization",
    "041": "Civic league",
    "042": "Local association of employees",
    "043": "Social welfare organization",
    "051": "Agricultural organization",
    "052": "Horticultural organization",
    "053": "Labor organization",
    "061": "Board of trade",
    "062": "Business league",
    "063": "Chamber of commerce",
    "064": "Real estate board",
    "071": "Pleasure, recreational or social club",
    "081": "Fraternal beneficiary society, order or association",
    "091": "Voluntary employees' beneficiary association (non-government employees)",
    "092": "Voluntary employees' beneficiary association (government employees)",
    "101": "Domestic fraternal society or association",
    "111": "Teachers' retirement fund association",
    "121": "Benevolent life insurance association",
    "122": "Mutual ditch or irrigation company",
    "123": "Mutual cooperative telephone company",
    "124": "Organization like those on the three preceding lines",
    "131": "Burial association",
    "132": "Cemetery company",
    "141": "Credit union",
    "142": "Other mutual corporation or association",
    "151": "Mutual insurance company or association other than life or marine",
    "161": "Corporation financing crop operations",
    "171": "Supplemental unemployment compensation trust or plan",
    "181": "Employee-funded pension trust (created before 25 June 1959)",
    "191": "Post or organization of war veterans",
    "201": "Legal service organization",
    "211": "Black lung trust",
    "221": "Multiemployer pension plan",
    "231": "Veterans' association formed prior to 1880",
    "241": "Trust described in section 4049 of ERISA",
    "251": "Title-holding company for pensions and similar entities",
    "261": "State-sponsored high-risk health insurance organization",
    "271": "State-sponsored workers' compensation reinsurance organization",
    "291": "ACA section 1322 qualified nonprofit health insurance issuer",
    "401": "Apostolic and religious organization (501(d))",
    "501": "Cooperative hospital service organization (501(e))",
    "601": "Cooperative service organization of operating educational organizations (501(f))",
    "701": "Child care organization (501(k))",
    "711": "Charitable risk pool",
    "811": "Qualified state-sponsored tuition program",
    "921": "4947(a)(1) private foundation (Form 990-PF filer)",
}

AFFILIATION = {
    "1": "Central organization (no group exemption) of a national, regional or geographic grouping",
    "2": "Intermediate organization (no group exemption) of a national, regional or geographic grouping",
    "3": "Independent organization or independent auxiliary",
    "6": "Central organization holding a group ruling, not a church or 501(c)(1) organization",
    "7": "Intermediate organization of a group exemption",
    "8": "Central organization holding a group ruling that is a church or 501(c)(1) organization",
    "9": "Subordinate organization in a group ruling",
    "0": "Not reported",
}

DEDUCTIBILITY = {
    "1": "Contributions are deductible",
    "2": "Contributions are not deductible",
    "4": "Contributions are deductible by treaty (foreign organizations)",
    "0": "Not reported",
}

FOUNDATION = {
    "00": "All organizations except 501(c)(3)",
    "02": "Private operating foundation exempt from paying excise taxes on investment income",
    "03": "Private operating foundation (other)",
    "04": "Private non-operating foundation",
    "09": "Suspense",
    "10": "Church 170(b)(1)(A)(i)",
    "11": "School 170(b)(1)(A)(ii)",
    "12": "Hospital or medical research organization 170(b)(1)(A)(iii)",
    "13": "Organization operated for the benefit of a governmentally owned college or university 170(b)(1)(A)(iv)",
    "14": "Governmental unit 170(b)(1)(A)(v)",
    "15": "Organization receiving a substantial part of its support from a governmental unit or the general public 170(b)(1)(A)(vi)",
    "16": "Organization receiving no more than one-third of its support from gross investment income and unrelated business income and more than one-third from contributions, fees and exempt-purpose receipts 509(a)(2)",
    "17": "Organization operated solely for the benefit of organizations described in codes 10 through 16, 509(a)(3)",
    "18": "Organization organized and operated to test for public safety 509(a)(4)",
    "21": "509(a)(3) Type I supporting organization",
    "22": "509(a)(3) Type II supporting organization",
    "23": "509(a)(3) Type III functionally integrated supporting organization",
    "24": "509(a)(3) Type III not functionally integrated supporting organization",
    "25": "Agricultural research organization 170(b)(1)(A)(ix)",
    # Present in the data but absent from the IRS information sheet.
    "06": "Code 06, not documented in the IRS EO BMF information sheet",
    "07": "Code 07, not documented in the IRS EO BMF information sheet",
}

ORGANIZATION = {
    "1": "Corporation",
    "2": "Trust",
    "3": "Co-operative",
    "4": "Partnership",
    "5": "Association",
    "0": "Not reported",
    "6": "Other",
}

STATUS = {
    "01": "Unconditional exemption",
    "02": "Conditional exemption",
    "12": "Trust described in section 4947(a)(2) of the Internal Revenue Code",
    "25": "Organization terminating its private foundation status under section 507(b)(1)(B)",
}

ASSET_INCOME = {
    "0": "0",
    "1": "1 to 9,999",
    "2": "10,000 to 24,999",
    "3": "25,000 to 99,999",
    "4": "100,000 to 499,999",
    "5": "500,000 to 999,999",
    "6": "1,000,000 to 4,999,999",
    "7": "5,000,000 to 9,999,999",
    "8": "10,000,000 to 49,999,999",
    "9": "50,000,000 or greater",
}

FILING_REQ = {
    "01": "Form 990 (all other) or 990-EZ return required",
    "02": "Required to file Form 990-N (e-Postcard); income under $50,000 per year",
    "03": "Form 990 group return",
    "04": "Required to file Form 990-BL (black lung trusts)",
    "06": "Not required to file (church)",
    "07": "Government 501(c)(1)",
    "13": "Not required to file (religious organization)",
    "14": "Not required to file (instrumentality of a state or political subdivision)",
    "00": "Not required to file (all other)",
}

PF_FILING_REQ = {
    "1": "Form 990-PF return required",
    "0": "No Form 990-PF return required",
    "2": "Form 990-PF return required (code 2, undocumented by the IRS)",
    "3": "Form 990-PF return required (code 3, undocumented by the IRS)",
}


def rows_for(table: str, column: str, codes: dict) -> list[dict]:
    return [
        {
            "id_tabela": table,
            "nome_coluna": column,
            "chave": k,
            "cobertura_temporal": "",
            "valor": v,
        }
        for k, v in codes.items()
    ]


def main() -> None:
    rows: list[dict] = []
    for table in ("return_financial", "compensation"):
        rows += rows_for(table, "form_type", FORM_TYPE)
    rows += rows_for("organization", "subsection_code", SUBSECTION)
    rows += rows_for(
        "organization", "subsection_classification_code", CLASSIFICATION
    )
    rows += rows_for("organization", "affiliation_code", AFFILIATION)
    rows += rows_for("organization", "deductibility_code", DEDUCTIBILITY)
    rows += rows_for("organization", "foundation_code", FOUNDATION)
    rows += rows_for("organization", "organization_code", ORGANIZATION)
    rows += rows_for("organization", "status_code", STATUS)
    rows += rows_for("organization", "asset_code", ASSET_INCOME)
    rows += rows_for("organization", "income_code", ASSET_INCOME)
    rows += rows_for("organization", "filing_requirement_code", FILING_REQ)
    rows += rows_for(
        "organization", "pf_filing_requirement_code", PF_FILING_REQ
    )
    with open(NTEE, newline="", encoding="utf-8") as fh:
        ntee = {r["ntee"]: r["description"] for r in csv.DictReader(fh)}
    rows += rows_for("organization", "ntee_code", ntee)
    rows += rows_for("revocation", "exemption_type", SUBSECTION)
    with open(OUT, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=[
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            ],
        )
        w.writeheader()
        w.writerows(rows)
    print(f"{OUT.name}: {len(rows)} rows")


if __name__ == "__main__":
    main()
