"""Build the per-table auxiliary-file bundles for us_oi_opportunity_atlas.

Each bundle holds the source codebook for the table and a README with the
citation, per-file provenance and the transformations applied at load.

Writes $US_OI_OPPORTUNITY_ATLAS_DATA_DIR/auxiliary_files/<table>/auxiliary_files.zip,
to be published at
gs://basedosdados-public/auxiliary_files/us_oi_opportunity_atlas/<table>/auxiliary_files.zip.

Usage:
    python models/us_oi_opportunity_atlas/code/build_auxiliary_files.py
"""

import datetime as dt
import shutil
import urllib.request
import zipfile

from clean_data import DATA_DIR
from table_meta import TABLE_ORDER

UPLOADS = "https://opportunityinsights.org/wp-content/uploads"
DOCS = DATA_DIR / "docs"
OUT = DATA_DIR / "auxiliary_files"

# Bundle file name -> source URL.
CODEBOOKS = {
    "tract": ("codebook_tract_outcomes.pdf", f"{UPLOADS}/2019/07/Codebook-for-Table-4.pdf"),
    "county": ("codebook_county_outcomes.pdf", f"{UPLOADS}/2019/07/Codebook-for-Table-5.pdf"),
    "commuting_zone": ("codebook_commuting_zone_outcomes.pdf", f"{UPLOADS}/2019/07/Codebook-for-Table-6.pdf"),
    "migration": ("migration_patterns_data_dictionary.pdf", f"{UPLOADS}/2022/07/Data-Dictionary.pdf"),
}  # fmt: skip
DATA_FILES = {
    "tract": "https://opportunityinsightsstatic.s3.us-east-1.amazonaws.com/assets/tract_outcomes_early.csv (linked from opportunityinsights.org/data as tract_outcomes.zip)",
    "county": f"{UPLOADS}/2018/10/county_outcomes.zip",
    "commuting_zone": f"{UPLOADS}/2018/10/cz_outcomes.csv",
    "migration": ", ".join(
        f"{UPLOADS}/2022/07/{f}"
        for f in ("od_pooled.csv", "od_race.csv", "od_inc.csv", "od.csv")
    ),
}

CITATION_ATLAS = (
    "Chetty, Raj, John N. Friedman, Nathaniel Hendren, Maggie R. Jones and Sonya "
    'R. Porter. "The Opportunity Atlas: Mapping the Childhood Roots of Social '
    'Mobility." American Economic Review 116 (1): 1-51, 2026. Earlier version: '
    "NBER Working Paper 25147, 2018."
)
CITATION_MIGRATION = (
    'Sprung-Keyser, Ben, Nathaniel Hendren and Sonya R. Porter. "The Radius of '
    'Economic Opportunity: Evidence from Migration and Local Labor Markets." '
    "US Census Bureau CES Working Paper 22-27, 2022."
)
LINKS = [
    ("The Opportunity Atlas (paper and data)", "https://opportunityinsights.org/paper/the-opportunity-atlas/"),
    ("Opportunity Atlas replication package (Harvard Dataverse, CC BY 4.0)", "https://doi.org/10.7910/DVN/NKCQM1"),
    ("2010 to 2020 census tract crosswalk", f"{UPLOADS}/2021/05/us_tract_2010_2020_crosswalk.csv"),
    ("The Radius of Economic Opportunity (paper and data)", "https://opportunityinsights.org/paper/the-radius-of-economic-opportunity/"),
]  # fmt: skip

ATLAS_NOTES = """\
- The source file has one row per place and one column per outcome x race x
  gender x statistic. It is reshaped to one row per place x race x gender x
  parental income percentile (`_outcome` tables) or place x race x gender
  (`_sample` tables). Race codes are spelled out (hisp -> hispanic, natam ->
  native_american).
- `parent_income_percentile` takes the values 1, 10, 25, 50, 75, 100 and `mean`
  (the source's `_mean` columns, not conditioning on parental income). The source
  publishes the 10th percentile although the codebook does not list it.
- Standard errors (`<outcome>_se`) are published only at the 25th and 75th
  percentiles and the mean; they are NULL at the other percentiles.
- Rows whose values are all missing are dropped; the source suppresses most
  race x gender cells in small places.
- `state_id`, `county_id` and `tract_id` are zero-padded 2010 FIPS codes built
  from the source's numeric `state`, `county` and `tract` columns.
- The source adds a small amount of noise to every estimate to protect privacy,
  so some fractions are slightly negative. Values are loaded as published.
"""
MIGRATION_NOTES = """\
- The four source files (pooled, by race, by parental income quintile, and by
  both) are stacked; `race` and `parent_income_quintile` take the value `pooled`
  for the dimension a file does not split by. The source's `pool` column (e.g.
  `BlackQ1`) is split into the two columns.
- Columns are renamed: n -> individuals, n_tot_o -> origin_individuals, n_tot_d
  -> destination_individuals, pr_d_o -> probability_destination_given_origin,
  pr_o_d -> probability_origin_given_destination.
- Counts include noise for differential privacy, so some are negative.
"""


def fetch(name: str, url: str) -> None:
    DOCS.mkdir(parents=True, exist_ok=True)
    path = DOCS / name
    if not path.exists():
        urllib.request.urlretrieve(url, path)


def readme(
    table: str, kind: str, codebook: tuple[str, str], today: str
) -> str:
    migration = kind == "migration"
    lines = [
        f"# us_oi_opportunity_atlas.{table}",
        "",
        "Source: Opportunity Insights, https://opportunityinsights.org/data/",
        "",
        "## Citation",
        "",
        CITATION_MIGRATION if migration else CITATION_ATLAS,
        "",
        "Opportunity Insights asks users to credit Opportunity Insights and the US "
        "Census Bureau.",
        "",
        "## Files in this bundle",
        "",
        f"- `{codebook[0]}`: the source's codebook for this table. Downloaded on "
        f"{today} from {codebook[1]}",
        "",
        "## Source data",
        "",
        f"Downloaded on {today}: {DATA_FILES[kind]}",
        "",
        "## Transformations applied at load",
        "",
        MIGRATION_NOTES if migration else ATLAS_NOTES,
        "## Related documents (not bundled)",
        "",
    ]
    lines += [f"- {title}: {url}" for title, url in LINKS]
    return "\n".join(lines) + "\n"


def main() -> None:
    today = dt.date.today().isoformat()
    for name, url in CODEBOOKS.values():
        fetch(name, url)
    for table in TABLE_ORDER:
        kind = (
            "migration"
            if table == "commuting_zone_migration"
            else table.rsplit("_", 1)[0]
        )
        codebook = CODEBOOKS[kind]
        d = OUT / table
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)
        with zipfile.ZipFile(
            d / "auxiliary_files.zip", "w", zipfile.ZIP_DEFLATED
        ) as z:
            z.writestr("README.md", readme(table, kind, codebook, today))
            z.write(DOCS / codebook[0], codebook[0])
        print(f"{table}: {d / 'auxiliary_files.zip'}")


if __name__ == "__main__":
    main()
