"""Build the ``dicionario`` table for us_stanford_dime.

Every mapping here is transcribed from a documented source — the DIME v4.0
codebook (sections 5, 7 and 8) or the FEC's published transaction-type and
committee-type code lists. Nothing is inferred from the data, because a guessed
label is worse than an absent one.

Two code spaces are deliberately only partly covered, and both say so in the
column's ``observations``:

* ``party`` mixes documented ICPSR numeric codes (100, 200, 328) with
  undocumented numeric codes and three-letter party abbreviations. The three
  documented codes account for 99.4% of non-null rows.
* ``seat``, ``nimsp_office``, ``nimsp_party`` and ``nimsp_candidate_status``
  carry readable labels rather than codes and are not dictionary-covered at all.

    python gen_dicionario.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import architecture as arch
import clean

OUT = clean.OUTPUT / "dicionario"

# FEC transaction type codes, DIME codebook section 8. The codebook truncates a
# few descriptions at its line width; those are completed from the FEC's own
# published list and marked with the full wording.
TRANSACTION_TYPE = {
    "10": "Non-federal receipt from persons, Levin (L-1A)",
    "11": "Tribal contribution",
    "12": "Non-federal other receipt, Levin (L-2)",
    "13": "Inaugural donation accepted",
    "15": "Contribution",
    "15C": "Contribution from candidate",
    "15E": "Earmarked contribution",
    "15F": "Loans forgiven by candidate",
    "15I": "Earmarked intermediary in",
    "15J": "Memo, filer's percentage of contribution given to joint fundraising committee",
    "15T": "Earmarked intermediary treasury in",
    "15Z": "In-kind contribution received from registered filer",
    "16C": "Loans received from the candidate",
    "16F": "Loans received from banks",
    "16G": "Loan from individual",
    "16H": "Loan from candidate or committee",
    "16J": "Loan repayments from individual",
    "16K": "Loan repayments from candidate or committee",
    "16L": "Loan repayments received from registered entity",
    "16R": "Loans received from registered filers",
    "16U": "Loan received from unregistered entity",
    "17R": "Contribution refund received from registered entity",
    "17U": "Refund, rebate or return received from unregistered entity",
    "17Y": "Refund, rebate or return from individual or corporation",
    "17Z": "Refund, rebate or return from candidate or committee",
    "18G": "Transfer in, affiliated",
    "18H": "Honorarium received",
    "18J": "Memo, filer's percentage of contribution given to joint fundraising committee",
    "18K": "Contribution received from registered filer",
    "18S": "Receipts from secretary of state",
    "18U": "Contribution received from unregistered committee",
    "19": "Electioneering communication donation received",
    "19J": "Memo, electioneering communication percentage of donation",
    "20": "Disbursement, exempt from limits",
    "20A": "Non-federal disbursement, Levin (L-4A), voter registration",
    "20B": "Non-federal disbursement, Levin (L-4B), voter identification",
    "20C": "Loan repayments made to candidate",
    "20D": "Non-federal disbursement, Levin (L-4D), generic campaign activity",
    "20F": "Loan repayments made to banks",
    "20G": "Loan repayments made to individual",
    "20R": "Loan repayments made to registered filer",
    "20V": "Non-federal disbursement, Levin (L-4C), get out the vote",
    "22G": "Loan to individual",
    "22H": "Loan to candidate or committee",
    "22J": "Loan repayment to individual",
    "22K": "Loan repayment to candidate or committee",
    "22L": "Loan repayment to bank",
    "22R": "Contribution refund to unregistered entity",
    "22U": "Loan repaid to unregistered entity",
    "22X": "Loan made to unregistered entity",
    "22Y": "Contribution refund to individual",
    "22Z": "Contribution refund to candidate or committee",
    "23Y": "Inaugural donation refund",
    "24A": "Independent expenditure against",
    "24C": "Coordinated expenditure",
    "24E": "Independent expenditure for",
    "24F": "Communication cost for candidate (C7)",
    "24G": "Transfer out, affiliated",
    "24H": "Honorarium to candidate",
    "24I": "Earmarked intermediary out",
    "24K": "Contribution made to non-affiliated",
    "24N": "Communication cost against candidate (C7)",
    "24P": "Contribution made to possible candidate",
    "24R": "Election recount disbursement",
    "24T": "Earmarked intermediary treasury out",
    "24U": "Contribution made to unregistered entity",
    "24Z": "In-kind contribution made to registered filer",
    "29": "Electioneering communication disbursement",
    # Non-FEC codes, assigned by DIME for state and local records.
    "15S": "Contribution to state elections (catchall)",
    "15L": "Contribution to local elections (catchall)",
    "15PD": "Contribution made as payroll deduction",
    "PF": "Public funding (state level)",
    "PFR": "Public funding returned (state level)",
}

PARTY = {
    "100": "Democrat",
    "200": "Republican",
    "328": "Independent",
}

# DIME codebook section 5.
CONTRIBUTOR_TYPE = {"I": "Individual", "C": "Committee or organization"}
GENDER = {"M": "Male", "F": "Female", "U": "Unknown"}
ELECTION_TYPE = {
    "P": "Primary election",
    "G": "General election",
    "S": "Special election",
    "R": "Run-off election",
}
RECIPIENT_TYPE_UPPER = {
    "CAND": "Candidate",
    "COMM": "PAC, organization or party committee",
}
RECIPIENT_TYPE_LOWER = {"cand": "Candidate", "comm": "Committee"}
INCUMBENCY = {
    "I": "Incumbent",
    "C": "Challenger",
    "O": "Open seat candidate",
    "U": "Unknown",
}
NIMSP_INCUMBENCY = {
    "I": "Incumbent",
    "C": "Challenger",
    "O": "Open seat candidate",
    "IO": "Incumbent, open seat",
    "IC": "Incumbent, challenger",
    "UNK": "Unknown",
    "NA": "Not applicable",
}
WINNER = {"W": "Won election", "L": "Lost election"}
ELEC_STAT = {
    "W": "Won",
    "L": "Lost",
    "R": "Run-off",
    "?": "Outcome not recorded",
}
FEC_CANDIDATE_STATUS = {
    "C": "Statutory candidate",
    "F": "Statutory candidate for future election",
    "N": "Not yet a statutory candidate",
    "P": "Statutory candidate in prior cycle",
}
INTEREST_GROUP = {
    "C": "Corporation",
    "L": "Labor organization",
    "M": "Membership organization",
    "T": "Trade association",
    "V": "Cooperative",
    "W": "Corporation without capital stock",
}
# FEC committee type codes.
COMMITTEE_TYPE = {
    "C": "Communication cost",
    "D": "Delegate committee",
    "E": "Electioneering communication",
    "H": "House candidate committee",
    "I": "Independent expenditor, person or group",
    "N": "PAC, nonqualified",
    "O": "Independent expenditure-only committee (super PAC)",
    "P": "Presidential candidate committee",
    "Q": "PAC, qualified",
    "S": "Senate candidate committee",
    "U": "Single-candidate independent expenditure committee",
    "V": "PAC with non-contribution account, nonqualified",
    "W": "PAC with non-contribution account, qualified",
    "X": "Party committee, nonqualified",
    "Y": "Party committee, qualified",
    "Z": "National party non-federal account",
}
BOOL_SCALING_EXCLUDED = {
    "1": "Excluded from the CFscore estimation",
    "0": "Included in the CFscore estimation",
}
BOOL_SCALING_INCLUDED = {
    "1": "Met the requirements for inclusion in the CFscore estimation",
    "0": "Did not meet the requirements for inclusion in the CFscore estimation",
}
BOOL_PROJECTED = {
    "1": "Projected onto the recovered space as a supplementary observation",
    "0": "Estimated within the scaling",
}

# (table, column, mapping)
ENTRIES = [
    ("contribution", "transaction_type", TRANSACTION_TYPE),
    ("contribution", "election_type", ELECTION_TYPE),
    ("contribution", "contributor_type", CONTRIBUTOR_TYPE),
    ("contribution", "contributor_gender", GENDER),
    ("contribution", "recipient_party", PARTY),
    ("contribution", "recipient_type", RECIPIENT_TYPE_UPPER),
    ("contribution", "excluded_from_scaling", BOOL_SCALING_EXCLUDED),
    ("recipient", "party", PARTY),
    ("recipient", "party_original", PARTY),
    ("recipient", "candidate_gender", GENDER),
    ("recipient", "incumbency_status", INCUMBENCY),
    ("recipient", "nimsp_incumbency_status", NIMSP_INCUMBENCY),
    ("recipient", "recipient_type", RECIPIENT_TYPE_LOWER),
    ("recipient", "primary_winner", WINNER),
    ("recipient", "general_winner", WINNER),
    ("recipient", "special_election_status", ELEC_STAT),
    ("recipient", "runoff_election_status", ELEC_STAT),
    ("recipient", "fec_candidate_status", FEC_CANDIDATE_STATUS),
    ("recipient", "interest_group_category", INTEREST_GROUP),
    ("recipient", "committee_type", COMMITTEE_TYPE),
    ("recipient", "included_in_scaling", BOOL_SCALING_INCLUDED),
    ("contributor", "contributor_type", CONTRIBUTOR_TYPE),
    ("contributor", "contributor_gender", GENDER),
    ("contributor", "is_projected", BOOL_PROJECTED),
]

COVERAGE = "1980(2)2024"


def check_consistency() -> None:
    """Fail if the dictionary and the architecture disagree about coverage.

    ``covered_by_dictionary = yes`` is a promise that this table defines the
    column's codes. A column flagged without an entry sends the reader to an
    empty lookup; an entry for an unflagged column is dead weight the site never
    surfaces. Neither shows up in a dbt test, so it is checked here.
    """
    flagged = {
        (table, col[0])
        for table, cols in arch.TABLES.items()
        for col in cols
        if col[4] == "yes"
    }
    covered = {(table, col) for table, col, _ in ENTRIES}
    missing = sorted(flagged - covered)
    extra = sorted(covered - flagged)
    if missing or extra:
        raise SystemExit(
            f"dictionary/architecture mismatch\n"
            f"  flagged but not defined: {missing}\n"
            f"  defined but not flagged: {extra}"
        )
    print(f"coverage check: {len(flagged)} coded columns, all defined")


def build() -> list[dict]:
    rows = []
    for table, column, mapping in ENTRIES:
        for key, value in mapping.items():
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": COVERAGE,
                    "valor": value,
                }
            )
    return rows


def main() -> None:
    check_consistency()
    rows = build()
    OUT.mkdir(parents=True, exist_ok=True)
    csv_path = OUT / "dicionario.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
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

    import duckdb

    con = duckdb.connect()
    con.execute(
        f"copy (select * from read_csv('{csv_path}', header=true, all_varchar=true)) "
        f"to '{OUT / 'dicionario_000.parquet'}' (format parquet, compression snappy)"
    )
    con.close()
    csv_path.unlink()

    by_table: dict[str, int] = {}
    for r in rows:
        by_table[r["id_tabela"]] = by_table.get(r["id_tabela"], 0) + 1
    print(f"dicionario: {len(rows)} entries across {len(ENTRIES)} columns")
    for t, n in sorted(by_table.items()):
        print(f"  {t}: {n}")


if __name__ == "__main__":
    main()
