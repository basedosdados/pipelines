"""Build the us_fec_campaign_finance dicionario table.

    uv run python build_dicionario.py

Emits output/dicionario/data.parquet: one row per (table, column, code), mapping every
FEC code stored in the dataset to its published description.

Codes and descriptions are transcribed from the FEC's own code-description pages:
  https://www.fec.gov/campaign-finance-data/party-code-descriptions/
  https://www.fec.gov/campaign-finance-data/committee-type-code-descriptions/
  https://www.fec.gov/campaign-finance-data/transaction-type-code-descriptions/
  https://www.fec.gov/campaign-finance-data/report-type-code-descriptions/
  https://www.fec.gov/campaign-finance-data/disbursement-category-code-descriptions/
plus the candidate and committee master file descriptions for the single-letter flags.

Values are kept in English: the dataset, its columns and its source documentation are
all English (.claude/rules/data-basis-style.md).
"""

import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT = (
    Path(
        os.environ.get(
            "FEC_DATA_DIR",
            Path.home() / "Downloads" / "us_fec_campaign_finance_data",
        )
    )
    / "output"
)

# --------------------------------------------------------------------------- #
# Code books
# --------------------------------------------------------------------------- #

# A handful of values in otherwise-closed code fields appear in no FEC code list —
# a few rows each out of millions. Naming them honestly keeps custom_dictionary_coverage
# strict rather than loosening the test to accommodate them.
UNDOCUMENTED = "Code not documented by the Federal Election Commission"

OFFICE = {
    "H": "U.S. House of Representatives",
    "S": "U.S. Senate",
    "P": "President of the United States",
}

INCUMBENT_CHALLENGER = {
    "I": "Incumbent",
    "C": "Challenger",
    "O": "Open seat",
}

CANDIDATE_STATUS = {
    "C": "Statutory candidate",
    "F": "Statutory candidate for future election",
    "N": "Not yet a statutory candidate",
    "P": "Statutory candidate in prior cycle",
    "A": UNDOCUMENTED,
    "I": UNDOCUMENTED,
    "Q": UNDOCUMENTED,
}

COMMITTEE_DESIGNATION = {
    "A": "Authorized by a candidate",
    "B": "Lobbyist/registrant PAC",
    "D": "Leadership PAC",
    "J": "Joint fundraising committee",
    "P": "Principal campaign committee of a candidate",
    "U": "Unauthorized",
}

COMMITTEE_TYPE = {
    "C": "Communication cost",
    "D": "Delegate committee",
    "E": "Electioneering communication",
    "H": "House campaign committee",
    "I": "Independent expenditor (person or group)",
    "N": "PAC - nonqualified",
    "O": "Independent expenditure-only committee (Super PAC)",
    "P": "Presidential campaign committee",
    "Q": "PAC - qualified",
    "S": "Senate campaign committee",
    "U": "Single-candidate independent expenditure",
    "V": "Hybrid PAC - nonqualified, with non-contribution account",
    "W": "Hybrid PAC - qualified, with non-contribution account",
    "X": "Party - nonqualified",
    "Y": "Party - qualified",
    "Z": "National party nonfederal account",
}

FILING_FREQUENCY = {
    "A": "Administratively terminated",
    "D": "Debt",
    "M": "Monthly filer",
    "Q": "Quarterly filer",
    "T": "Terminated",
    "W": "Waived",
}

ORGANIZATION_TYPE = {
    "C": "Corporation",
    "L": "Labor organization",
    "M": "Membership organization",
    "T": "Trade association",
    "V": "Cooperative",
    "W": "Corporation without capital stock",
    "H": UNDOCUMENTED,
    "I": UNDOCUMENTED,
}

ENTITY_TYPE = {
    "CAN": "Candidate",
    "CCM": "Candidate committee",
    "COM": "Committee",
    "IND": "Individual (a person)",
    "ORG": "Organization (not a committee and not a person)",
    "PAC": "Political action committee",
    "PTY": "Party organization",
    # Not on the FEC's published entity-type list, but unambiguous: it pairs with
    # transaction type 11, "Native American tribe contribution".
    "TRB": "Native American tribe",
    "B": UNDOCUMENTED,
    "C": UNDOCUMENTED,
    "I": UNDOCUMENTED,
}

AMENDMENT_INDICATOR = {
    "N": "New report",
    "A": "Amendment to a previously filed report",
    "T": "Termination report",
}

MEMO_CODE = {
    "X": "Amount is not included in the report total, or the entry describes a "
    "special circumstance",
    "Y": UNDOCUMENTED,
    "H": UNDOCUMENTED,
    "M": UNDOCUMENTED,
    "0": UNDOCUMENTED,
    "*": UNDOCUMENTED,
}

REPORT_TYPE = {
    "12C": "Pre-convention report",
    "12G": "Pre-general report",
    "12P": "Pre-primary report",
    "12R": "Pre-runoff report",
    "12S": "Pre-special report",
    "24H": "24-hour independent expenditure report",
    "30D": "Post-election report",
    "30G": "Post-general report",
    "30P": "Post-primary report",
    "30R": "Post-runoff report",
    "30S": "Post-special report",
    "48H": "48-hour independent expenditure report",
    "60D": "Post-convention report of convention expenses",
    "90D": "Post-inaugural report",
    "90S": "Post-inaugural supplement report",
    "ADJ": "Comprehensive adjusted amendment",
    "CA": "Comprehensive amendment",
    "M2": "February monthly report",
    "M3": "March monthly report",
    "M4": "April monthly report",
    "M5": "May monthly report",
    "M6": "June monthly report",
    "M7": "July monthly report",
    "M8": "August monthly report",
    "M9": "September monthly report",
    "M10": "October monthly report",
    "M11": "November monthly report",
    "M12": "December monthly report",
    "MY": "Mid-year report",
    "Q1": "First quarter report",
    "Q2": "Second quarter report",
    "Q3": "Third quarter report",
    "TER": "Termination report",
    "YE": "Year-end report",
    # Legacy codes still present in pre-2000 filings but dropped from the FEC's
    # current code-description page. Together they cover ~46,000 rows.
    "10D": "Pre-election report due 10 days before the election (legacy)",
    "10G": "Pre-general report due 10 days before the general election (legacy)",
    "10P": "Pre-primary report due 10 days before the primary election (legacy)",
    "10R": "Pre-runoff report due 10 days before the runoff election (legacy)",
    "10S": "Pre-special report due 10 days before the special election (legacy)",
    "24": "24-hour independent expenditure report (legacy)",
}

DISBURSEMENT_CATEGORY = {
    "000": "Uncategorized — no disbursement category reported",
    "001": "Administrative, salary and overhead expenses",
    "002": "Travel expenses, including travel reimbursements",
    "003": "Solicitation and fundraising expenses",
    "004": "Advertising expenses, including general public political advertising",
    "005": "Polling expenses",
    "006": "Campaign materials",
    "007": "Campaign event expenses",
    "008": "Transfers to other authorized committees of the same candidate",
    "009": "Loan repayments",
    "010": "Refunds of contributions",
    "011": "Political contributions to other federal candidates and committees",
    "012": "Donations to charitable or civic organizations",
    "101": "Non-allocable expenses (presidential filers)",
    "102": "Media expenditures (presidential filers)",
    "103": "Mass mailings and other campaign materials (presidential filers)",
    "104": "Overhead of state offices and their facilities (presidential filers)",
    "105": "Special telephone programs (presidential filers)",
    "106": "Public opinion polls (presidential filers)",
    "107": "Fundraising expenditures (presidential filers)",
}

TRANSACTION_TYPE = {
    "10": "Contribution to an independent expenditure-only committee, hybrid PAC or "
    "nonfederal party account from a person",
    "10J": "Memo - recipient committee's percentage of a nonfederal receipt from a person",
    "11": "Native American tribe contribution",
    "11J": "Memo - recipient committee's percentage of a Native American tribe "
    "contribution to a joint fundraising committee",
    "12": "Nonfederal other receipt - Levin account",
    "13": "Inaugural donation accepted",
    "15": "Contribution to a political committee from an individual, partnership or "
    "limited liability company",
    "15C": "Contribution from the candidate",
    "15E": "Earmarked contribution to a political committee from an individual, "
    "partnership or limited liability company",
    "15F": "Loans forgiven by the candidate",
    "15I": "Earmarked contribution received by an intermediary committee and passed on "
    "via the contributor's check",
    "15J": "Memo - recipient committee's percentage of a contribution from an "
    "individual, partnership or limited liability company to a joint fundraising committee",
    "15K": "Contribution received from a registered filer, disclosed on an authorized "
    "committee report",
    "15T": "Earmarked contribution received by an intermediary committee and entered "
    "into its treasury",
    "15Z": "In-kind contribution received from a registered filer",
    "16C": "Loan received from the candidate",
    "16F": "Loan received from a bank",
    "16G": "Loan received from an individual",
    "16H": "Loan received from a registered filer",
    "16J": "Loan repayment received from an individual",
    "16K": "Loan repayment received from a registered filer",
    "16L": "Loan repayment received from an unregistered entity",
    "16R": "Loan received from a registered filer",
    "16U": "Loan received from an unregistered entity",
    "17R": "Contribution refund received from a registered entity",
    "17U": "Refund, rebate or return received from an unregistered entity",
    "17Y": "Refund, rebate or return received from an individual or corporation",
    "17Z": "Refund, rebate or return received from a candidate or committee",
    "18G": "Transfer in from an affiliated committee",
    "18H": "Honorarium received",
    "18J": "Memo - recipient committee's percentage of a contribution from a registered "
    "committee to a joint fundraising committee",
    "18K": "Contribution received from a registered filer",
    "18L": "Bundled contribution",
    "18U": "Contribution received from an unregistered committee",
    "19": "Electioneering communication donation received",
    "19J": "Memo - recipient committee's percentage of an electioneering communication "
    "donation to a joint fundraising committee",
    "20": "Nonfederal disbursement from a party soft money account (1991-2002)",
    "20A": "Nonfederal disbursement - Levin account, voter registration",
    "20B": "Nonfederal disbursement - Levin account, voter identification",
    "20C": "Loan repayment made to the candidate",
    "20D": "Nonfederal disbursement - Levin account, generic campaign activity",
    "20F": "Loan repayment made to a bank",
    "20G": "Loan repayment made to an individual",
    "20R": "Loan repayment made to a registered filer",
    "20V": "Nonfederal disbursement - Levin account, get out the vote",
    "20Y": "Nonfederal refund",
    "21Y": "Native American tribe refund",
    "22G": "Loan made to an individual",
    "22H": "Loan made to a candidate or committee",
    "22J": "Loan repayment made to an individual",
    "22K": "Loan repayment made to a candidate or committee",
    "22L": "Loan repayment made to a bank",
    "22R": "Contribution refund to an unregistered entity",
    "22U": "Loan repaid to an unregistered entity",
    "22X": "Loan made to an unregistered entity",
    "22Y": "Contribution refund to an individual, partnership or limited liability company",
    "22Z": "Contribution refund to a candidate or committee",
    "23Y": "Inaugural donation refund",
    "24A": "Independent expenditure opposing the election of a candidate",
    "24C": "Coordinated party expenditure",
    "24E": "Independent expenditure advocating the election of a candidate",
    "24F": "Communication cost for a candidate",
    "24G": "Transfer out to an affiliated committee",
    "24H": "Honorarium to a candidate",
    "24I": "Earmarked contributor's check passed on by an intermediary committee to the "
    "intended recipient",
    "24K": "Contribution made to a nonaffiliated committee",
    "24N": "Communication cost against a candidate",
    "24P": "Contribution made to a possible federal candidate, including in-kind",
    "24R": "Election recount disbursement",
    "24T": "Earmarked contribution passed to the intended recipient from the "
    "intermediary's treasury",
    "24U": "Contribution made to an unregistered entity",
    "24Z": "In-kind contribution made to a registered filer",
    "28L": "Refund of a bundled contribution",
    "29": "Electioneering communication disbursement or obligation",
    "30": "Convention account receipt from an individual, partnership or limited "
    "liability company",
    "30E": "Convention account earmarked receipt",
    "30F": "Convention account memo - recipient committee's percentage from a registered "
    "committee to a joint fundraising committee",
    "30G": "Convention account transfer in from an affiliated committee",
    "30J": "Convention account memo - recipient committee's percentage from an "
    "individual, partnership or limited liability company to a joint fundraising committee",
    "30K": "Convention account receipt from a registered filer",
    "30T": "Convention account receipt from a Native American tribe",
    "31": "Headquarters account receipt from an individual, partnership or limited "
    "liability company",
    "31E": "Headquarters account earmarked receipt",
    "31F": "Headquarters account memo - recipient committee's percentage from a "
    "registered committee to a joint fundraising committee",
    "31G": "Headquarters account transfer in from an affiliated committee",
    "31J": "Headquarters account memo - recipient committee's percentage from an "
    "individual, partnership or limited liability company to a joint fundraising committee",
    "31K": "Headquarters account receipt from a registered filer",
    "31T": "Headquarters account receipt from a Native American tribe",
    "32": "Recount account receipt from an individual, partnership or limited liability "
    "company",
    "32E": "Recount account earmarked receipt",
    "32F": "Recount account memo - recipient committee's percentage from a registered "
    "committee to a joint fundraising committee",
    "32G": "Recount account transfer in from an affiliated committee",
    "32J": "Recount account memo - recipient committee's percentage from an individual, "
    "partnership or limited liability company to a joint fundraising committee",
    "32K": "Recount account receipt from a registered filer",
    "32T": "Recount account receipt from a Native American tribe",
    "40": "Convention account disbursement",
    "40T": "Convention account refund to a Native American tribe",
    "40Y": "Convention account refund to an individual, partnership or limited liability "
    "company",
    "40Z": "Convention account refund to a registered filer",
    "41": "Headquarters account disbursement",
    "41T": "Headquarters account refund to a Native American tribe",
    "41Y": "Headquarters account refund to an individual, partnership or limited "
    "liability company",
    "41Z": "Headquarters account refund to a registered filer",
    "42": "Recount account disbursement",
    "42T": "Recount account refund to a Native American tribe",
    "42Y": "Recount account refund to an individual, partnership or limited liability "
    "company",
    "42Z": "Recount account refund to a registered filer",
}

PARTY = {
    "ACE": "Ace Party",
    "AKI": "Alaskan Independence Party",
    "AIC": "American Independent Conservative",
    "AIP": "American Independent Party",
    "AMP": "American Party",
    "APF": "American People's Freedom Party",
    "AE": "Americans Elect",
    "CIT": "Citizens' Party",
    "CMD": "Commandments Party",
    "CMP": "Commonwealth Party of the U.S.",
    "COM": "Communist Party",
    "CNC": "Concerned Citizens Party of Connecticut",
    "CRV": "Conservative Party",
    "CON": "Constitution Party",
    "CST": "Constitutional",
    "COU": "Country",
    "DCG": "D.C. Statehood Green Party",
    "DNL": "Democratic-Nonpartisan League",
    "DEM": "Democratic Party",
    "D/C": "Democratic/Conservative",
    "DFL": "Democratic-Farmer-Labor",
    "DGR": "Desert Green Party",
    "FED": "Federalist",
    "FLP": "Freedom Labor Party",
    "FRE": "Freedom Party",
    "GWP": "George Wallace Party",
    "GRT": "Grassroots",
    "GRE": "Green Party",
    "GR": "Green-Rainbow",
    "HRP": "Human Rights Party",
    "IDP": "Independence Party",
    "IND": "Independent",
    "IAP": "Independent American Party",
    "ICD": "Independent Conservative Democratic",
    "IGR": "Independent Green",
    "IP": "Independent Party",
    "IDE": "Independent Party of Delaware",
    "IGD": "Industrial Government Party",
    "JCN": "Jewish/Christian National",
    "JUS": "Justice Party",
    "LRU": "La Raza Unida",
    "LBR": "Labor Party",
    "LFT": "Less Federal Taxes",
    "LBL": "Liberal Party",
    "LIB": "Libertarian Party",
    "LBU": "Liberty Union Party",
    "MTP": "Mountain Party",
    "NDP": "National Democratic Party",
    "NLP": "Natural Law Party",
    "NA": "New Alliance",
    "NJC": "New Jersey Conservative Party",
    "NPP": "New Progressive Party",
    "NPA": "No Party Affiliation",
    "NOP": "No Party Preference",
    "NNE": "None",
    "N": "Nonpartisan",
    "NON": "Non-Party",
    "OE": "One Earth Party",
    "OTH": "Other",
    "PG": "Pacific Green",
    "PSL": "Party for Socialism and Liberation",
    "PAF": "Peace and Freedom",
    "PFP": "Peace and Freedom Party",
    "PFD": "Peace Freedom Party",
    "POP": "People Over Politics",
    "PPY": "People's Party",
    "PCH": "Personal Choice Party",
    "PPD": "Popular Democratic Party",
    "PRO": "Progressive Party",
    "NAP": "Prohibition Party",
    "PRI": "Puerto Rican Independence Party",
    "RUP": "Raza Unida Party",
    "REF": "Reform Party",
    "REP": "Republican Party",
    "RES": "Resource Party",
    "RTL": "Right to Life",
    "SEP": "Socialist Equality Party",
    "SLP": "Socialist Labor Party",
    "SUS": "Socialist Party",
    "SOC": "Socialist Party U.S.A.",
    "SWP": "Socialist Workers Party",
    "TX": "Taxpayers",
    "TWR": "Taxpayers Without Representation",
    "TEA": "Tea Party",
    "THD": "Theo-Democratic",
    "LAB": "U.S. Labor Party",
    "USP": "U.S. People's Party",
    "UST": "U.S. Taxpayers Party",
    "UN": "Unaffiliated",
    "UC": "United Citizen",
    "UNI": "United Party",
    "UNK": "Unknown",
    "VET": "Veterans Party",
    "WTP": "We the People",
    "W": "Write-In",
}

# --------------------------------------------------------------------------- #
# Which coded column of which table draws on which code book.
# Must stay in sync with covered_by_dictionary=yes in architecture/*.csv.
# --------------------------------------------------------------------------- #

TXN_CODES = {
    "amendment_indicator": AMENDMENT_INDICATOR,
    "report_type": REPORT_TYPE,
    "transaction_type": TRANSACTION_TYPE,
    "entity_type": ENTITY_TYPE,
    "memo_code": MEMO_CODE,
}

ASSIGNMENTS = {
    "candidate": {
        "party": PARTY,
        "office": OFFICE,
        "incumbent_challenger_status": INCUMBENT_CHALLENGER,
        "candidate_status": CANDIDATE_STATUS,
    },
    "committee": {
        "committee_designation": COMMITTEE_DESIGNATION,
        "committee_type": COMMITTEE_TYPE,
        "party": PARTY,
        "filing_frequency": FILING_FREQUENCY,
        "organization_type": ORGANIZATION_TYPE,
    },
    "candidate_committee_link": {
        "committee_type": COMMITTEE_TYPE,
        "committee_designation": COMMITTEE_DESIGNATION,
    },
    "contribution_individual": TXN_CODES,
    "contribution_committee": TXN_CODES,
    "committee_transaction": TXN_CODES,
    "disbursement": {
        "amendment_indicator": AMENDMENT_INDICATOR,
        "report_type": REPORT_TYPE,
        "entity_type": ENTITY_TYPE,
        "category": DISBURSEMENT_CATEGORY,
        "memo_code": MEMO_CODE,
    },
}


def build() -> pd.DataFrame:
    rows = []
    for table, columns in ASSIGNMENTS.items():
        for column, codebook in columns.items():
            for key, value in codebook.items():
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": key,
                        "cobertura_temporal": "",
                        "valor": value,
                    }
                )
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


def main():
    df = build()
    dest = OUTPUT / "dicionario"
    dest.mkdir(parents=True, exist_ok=True)
    # Staging is all-STRING by house convention; write via arrow, never astype(str).
    schema = pa.schema([(c, pa.string()) for c in df.columns])
    pq.write_table(
        pa.Table.from_pandas(df, schema=schema, preserve_index=False),
        dest / "data.parquet",
        compression="snappy",
    )
    print(f"dicionario: {len(df):,} rows -> {dest / 'data.parquet'}")
    print(df.groupby(["id_tabela", "nome_coluna"]).size().to_string())


if __name__ == "__main__":
    main()
