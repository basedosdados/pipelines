"""Write the architecture CSVs for us_ucla_voteview (one per table).

Usage:
    uv run --no-sync python models/us_ucla_voteview/code/build_architecture.py

The CSVs under ``architecture/`` are the source of truth for column order, types
and descriptions. The dbt models, the cleaning code
(``pipelines/datasets/us_ucla_voteview/utils.py``) and the recurring pipeline
all read them.

Column names are Voteview's own, unchanged, so that users of the Voteview
exports and R package recognise them. Descriptions follow the Voteview codebook
pages (https://voteview.com/articles/data_help_<table>).
"""

import csv
from pathlib import Path

OUT = Path(__file__).parent / "architecture"
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
STATE = "br_bd_diretorios_us.state:abbreviation"
MEMBER = "br_bd_diretorios_us.congress_member:id_bioguide"


def col(name, type_, desc, dic="no", directory="", unit="", obs=""):
    return [name, type_, desc, "", dic, directory, unit, "no", obs, name]


CONGRESS = col(
    "congress",
    "INT64",
    "Number of the Congress, e.g. 115 for the 115th Congress (2017-2019)",
    obs=(
        "Ordinal Congress number, starting at 1 in 1789. Each Congress spans two "
        "years. Partition column of the partitioned tables"
    ),
)
CHAMBER = col("chamber", "STRING", "Chamber: House, Senate or President")
ROLLNUMBER = col(
    "rollnumber",
    "STRING",
    "Sequential number of the roll call within the Congress and chamber",
    obs=(
        "Starts at 1 in the first roll call of each Congress. Excludes quorum "
        "calls and vacated votes, so it does not match clerk_rollnumber. Stored "
        "as text: cast to INT64 to sort numerically"
    ),
)
ICPSR = col(
    "icpsr",
    "STRING",
    "ICPSR identifier of the member",
    obs=(
        "In general one identifier per member for their whole career. Members "
        "who switched parties, and members who later became president, receive "
        "a new identifier so that a separate NOMINATE estimate can be produced. "
        "Use bioguide_id to identify the person"
    ),
)
NOM = "NOMINATE (DW-NOMINATE) "


def nominate(name, what, obs=""):
    return col(name, "FLOAT64", f"{NOM}{what}", unit="index", obs=obs)


SCALE_OBS = (
    "Ideal-point coordinate, roughly between -1 and 1. The first dimension is "
    "usually read as economic liberal (negative) to conservative (positive). "
    "Re-estimated over the whole history when new votes arrive, so past values "
    "can change between releases"
)

TABLES = {
    "members": [
        CONGRESS,
        CHAMBER,
        ICPSR,
        col(
            "state_icpsr",
            "STRING",
            "ICPSR code of the state the member represents",
            obs=(
                "ICPSR state codes. 92-96 are territories (PR, AS, GU, MP, VI) "
                "and 99 is the whole country (presidents). The state directory "
                "has no ICPSR code for territories, so the directory link is on "
                "state_abbrev"
            ),
        ),
        col(
            "district_code",
            "STRING",
            "Congressional district the member represents within their state",
            obs=(
                "0 for senators and presidents. Historical at-large districts are "
                "coded 99, 98 or 1"
            ),
        ),
        col(
            "state_abbrev",
            "STRING",
            "Two-letter postal abbreviation of the state the member represents",
            directory=STATE,
            obs="USA for presidents, which is not in the state directory",
        ),
        col(
            "party_code",
            "STRING",
            "Voteview code of the member's party",
            obs="Party names are in the parties table (congress, chamber, party_code)",
        ),
        col(
            "occupancy",
            "STRING",
            "ICPSR occupancy code: order of the member among the occupants of the seat",
            obs=(
                "0 if the only occupant, 1 if the first, 2 if the second, and so "
                "on. Legacy ICPSR field, not verified by Voteview"
            ),
        ),
        col(
            "last_means",
            "STRING",
            "ICPSR attain-office code: the member's last means of attaining office",
            dic="yes",
            obs="Legacy ICPSR field, not verified by Voteview",
        ),
        col("bioname", "STRING", "Name of the member, surname first"),
        col(
            "bioguide_id",
            "STRING",
            "Identifier of the member in the Biographical Directory of the US Congress",
            directory=MEMBER,
            obs="Empty for most presidents",
        ),
        col("born", "INT64", "Year of the member's birth", unit="year"),
        col("died", "INT64", "Year of the member's death", unit="year"),
        nominate("nominate_dim1", "first-dimension estimate", SCALE_OBS),
        nominate("nominate_dim2", "second-dimension estimate", SCALE_OBS),
        col(
            "nominate_log_likelihood",
            "FLOAT64",
            f"Log-likelihood of the member's {NOM}estimate",
            obs="Dimensionless fit statistic",
        ),
        col(
            "nominate_geo_mean_probability",
            "FLOAT64",
            f"Geometric mean probability of the member's {NOM}estimate",
            unit="proportion",
        ),
        col(
            "nominate_number_of_votes",
            "INT64",
            "Number of votes cast by the member in the Congress",
            unit="vote",
        ),
        col(
            "nominate_number_of_errors",
            "INT64",
            f"Number of the member's votes misclassified by the {NOM}estimate",
            unit="vote",
        ),
        col(
            "conditional",
            "STRING",
            f"Whether the {NOM}estimate was produced conditionally",
            dic="yes",
            obs=(
                "Conditional estimates are provisional and are updated at the next "
                "full estimation. Empty in every row of the current release"
            ),
        ),
        col(
            "nokken_poole_dim1",
            "FLOAT64",
            "Nokken-Poole first-dimension estimate",
            unit="index",
            obs="Estimated separately for each Congress, unlike nominate_dim1",
        ),
        col(
            "nokken_poole_dim2",
            "FLOAT64",
            "Nokken-Poole second-dimension estimate",
            unit="index",
            obs="Estimated separately for each Congress, unlike nominate_dim2",
        ),
    ],
    "rollcalls": [
        CONGRESS,
        CHAMBER,
        ROLLNUMBER,
        col("date", "DATE", "Date of the roll call"),
        col(
            "session",
            "STRING",
            "Session of the Congress in which the roll call took place",
            obs="Empty for most historical roll calls",
        ),
        col(
            "clerk_rollnumber",
            "STRING",
            "Roll call number assigned by the clerk of the chamber",
            obs=(
                "Includes quorum calls and vacated votes and restarts at 1 every "
                "session, not every Congress"
            ),
        ),
        col(
            "majority_requirement",
            "STRING",
            "Fraction of votes required to pass, e.g. 1/2, 2/3 or 3/5",
        ),
        col("yea_count", "INT64", "Number of Yea votes", unit="vote"),
        col("nay_count", "INT64", "Number of Nay votes", unit="vote"),
        nominate(
            "nominate_mid_1",
            "first-dimension midpoint estimate of the roll call",
        ),
        nominate(
            "nominate_mid_2",
            "second-dimension midpoint estimate of the roll call",
        ),
        nominate(
            "nominate_spread_1",
            "first-dimension spread estimate of the roll call",
        ),
        nominate(
            "nominate_spread_2",
            "second-dimension spread estimate of the roll call",
        ),
        col(
            "nominate_log_likelihood",
            "FLOAT64",
            f"Log-likelihood of the roll call's {NOM}estimate",
            obs="Dimensionless fit statistic",
        ),
        col(
            "bill_number",
            "STRING",
            "Number of the bill on which the roll call was held",
        ),
        col("vote_result", "STRING", "Official result of the roll call"),
        col(
            "vote_desc",
            "STRING",
            "Description of the roll call assigned by congressional staff",
        ),
        col(
            "vote_question",
            "STRING",
            "Question put to the vote, e.g. On Agreeing to the Amendment",
        ),
        col(
            "dtl_desc",
            "STRING",
            "Description of the roll call from historical sources",
        ),
    ],
    "votes": [
        CONGRESS,
        CHAMBER,
        ROLLNUMBER,
        col(
            "date",
            "DATE",
            "Date of the roll call",
            obs=(
                "Not in the Voteview votes file. Added by Data Basis from the "
                "rollcalls table (congress, chamber, rollnumber)"
            ),
        ),
        ICPSR,
        col(
            "cast_code",
            "STRING",
            "How the member voted",
            dic="yes",
        ),
        col(
            "prob",
            "FLOAT64",
            f"Estimated probability, from {NOM.strip()}, of the member casting the vote as recorded",
            unit="percent",
        ),
    ],
    "parties": [
        CONGRESS,
        CHAMBER,
        col("party_code", "STRING", "Voteview code of the party"),
        col("party_name", "STRING", "Name of the party"),
        col(
            "n_members",
            "INT64",
            "Number of party members in the chamber in the Congress",
            unit="person",
        ),
        nominate(
            "nominate_dim1_median",
            "first-dimension median of the party's members",
        ),
        nominate(
            "nominate_dim2_median",
            "second-dimension median of the party's members",
        ),
        nominate(
            "nominate_dim1_mean", "first-dimension mean of the party's members"
        ),
        nominate(
            "nominate_dim2_mean",
            "second-dimension mean of the party's members",
        ),
    ],
    "dictionary": [
        col("table_id", "STRING", "Table identifier"),
        col("column_name", "STRING", "Column name"),
        col("key", "STRING", "Code"),
        col("temporal_coverage", "STRING", "Temporal coverage of the code"),
        col("value", "STRING", "Meaning of the code"),
    ],
}

# Codes from the Voteview codebook. last_means 0, 6 and 7 occur in the data but are
# not documented; they are kept and labelled as such rather than dropped.
DICTIONARY = [
    (
        "votes",
        "cast_code",
        "0",
        "Not a member of the chamber when the vote was taken",
    ),
    ("votes", "cast_code", "1", "Yea"),
    ("votes", "cast_code", "2", "Paired Yea"),
    ("votes", "cast_code", "3", "Announced Yea"),
    ("votes", "cast_code", "4", "Announced Nay"),
    ("votes", "cast_code", "5", "Paired Nay"),
    ("votes", "cast_code", "6", "Nay"),
    ("votes", "cast_code", "7", "Present (some Congresses)"),
    ("votes", "cast_code", "8", "Present (some Congresses)"),
    ("votes", "cast_code", "9", "Not Voting (abstention)"),
    ("members", "last_means", "0", "Not documented by Voteview"),
    ("members", "last_means", "1", "Elected in a general election"),
    ("members", "last_means", "2", "Elected in a special election"),
    ("members", "last_means", "3", "Elected directly by a state legislature"),
    ("members", "last_means", "5", "Appointed"),
    ("members", "last_means", "6", "Not documented by Voteview"),
    ("members", "last_means", "7", "Not documented by Voteview"),
    (
        "members",
        "conditional",
        "0",
        "Estimated in the full NOMINATE estimation",
    ),
    ("members", "conditional", "1", "Estimated conditionally (provisional)"),
]


def main():
    OUT.mkdir(exist_ok=True)
    for table, rows in TABLES.items():
        names = [r[0] for r in rows]
        assert len(names) == len(set(names)), f"duplicate column in {table}"
        assert all(len(r) == len(HEADER) for r in rows), table
        with open(OUT / f"{table}.csv", "w", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(HEADER)
            w.writerows(rows)
    # The dictionary table's rows, read by the cleaning code.
    with open(OUT.parent / "dictionary.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow([r[0] for r in TABLES["dictionary"]])
        w.writerows((t, c, k, "", v) for t, c, k, v in DICTIONARY)
    print(
        f"wrote {len(TABLES)} architecture CSVs and dictionary.csv to {OUT.parent}"
    )


if __name__ == "__main__":
    main()
