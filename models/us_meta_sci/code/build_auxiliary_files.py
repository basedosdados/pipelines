"""Build the per-table auxiliary-file bundles for us_meta_sci (README + release note PDF).

Writes $US_META_SCI_DATA_DIR/auxiliary_files/<table>/auxiliary_files.zip. Publish to
gs://basedosdados-public/auxiliary_files/us_meta_sci/<table>/auxiliary_files.zip.
"""

import os
import shutil
import zipfile
from pathlib import Path

D = Path(
    os.environ.get(
        "US_META_SCI_DATA_DIR", Path.home() / "Library/Caches/us_meta_sci_data"
    )
)
PDF = D / "input/the-social-connectedness-index.pdf"
HDX = "https://data.humdata.org/dataset/social-connectedness-index"
RES = "https://data.humdata.org/dataset/e9988552-74e4-4ff4-943f-c782ac8bca87/resource"
SRC = {
    "country": (
        "country.csv",
        f"{RES}/652cf9c9-541f-47de-8d53-ff818062bd0c/download/country.csv",
    ),
    "gadm1": (
        "gadm1.csv",
        f"{RES}/8e1b8b59-c12e-48ea-9af3-41dde75916d5/download/gadm1.csv",
    ),
    "gadm2": (
        "gadm2.zip (12 shards)",
        "https://drive.google.com/file/d/1M3XTjZG_bgzGkEZ1tJgZ6qLcuPJU5Ck4 (linked from the HDX page)",
    ),
    "geoboundaries_adm1": (
        "geoboundaries_adm1.csv",
        f"{RES}/6419cafc-5edb-4355-9577-b086ecc8d21d/download/geoboundaries_adm1.csv",
    ),
    "geoboundaries_adm2": (
        "geoboundaries_adm2.zip (12 shards)",
        "https://drive.google.com/file/d/1y6DHFyFpmadbDKYQ0a4UaI7EK-FuJgK8 (linked from the HDX page)",
    ),
    "us_county": (
        "us_counties.csv",
        f"{RES}/97dc352f-c9c5-47d6-a6ef-88709e14006c/download/us_counties.csv",
    ),
    "us_zcta": (
        "us_zcta.zip (10 shards)",
        "https://drive.google.com/file/d/13gbCdgHD-xfkogDoSzcAGKhNLHMPy8tR (linked from the HDX page)",
    ),
    "nuts_2024": (
        "nuts_2024.zip (nuts1_2024.csv, nuts2_2024.csv, nuts3_2024.csv)",
        f"{RES}/b691d1d1-b286-456d-9a23-16e2f2d463cc/download/nuts_2024.zip",
    ),
    "region_to_country": (
        "all_region_to_country.zip (9 files, <level>_to_country.csv)",
        f"{RES}/953e8683-bcf7-49f9-908f-1e14209e98d3/download/all_region_to_country.zip",
    ),
}
NOTES = {
    "country": "- The source's `user_region`/`friend_region` columns repeat the country code in this file and are dropped.",
    "us_county": "- All rows are US-US pairs; the source's constant `user_country`/`friend_country` columns are dropped and `user_region`/`friend_region` are renamed `user_county_id`/`friend_county_id` (5-digit FIPS).",
    "us_zcta": "- All rows are US-US pairs; the constant country columns are dropped and the region columns are renamed `user_zcta_id`/`friend_zcta_id`.\n- 10 of the 20,195 ZCTAs are not in the Data Basis 2020 ZCTA directory: 10281, 23806, 30320, 40506, 83601, 84132, 85144, 85288, 92878, 98205.",
    "nuts_2024": "- The three level files are stacked; `nuts_level` (nuts1, nuts2, nuts3) records the source file. `scaled_sci` is rescaled separately within each level, so compare values only within one level.",
    "region_to_country": "- The nine `<level>_to_country.csv` files are stacked; `region_level` records the source file (gadm1, gadm2, geoboundaries_adm1, geoboundaries_adm2, nuts1_2024, nuts2_2024, nuts3_2024, us_county, us_zcta). `scaled_sci` is rescaled separately within each level.\n- Pairs are directed region -> country only. The source's `friend_region` column equals `friend_country` in every row and is dropped.",
}
for t in ["gadm1", "gadm2", "geoboundaries_adm1", "geoboundaries_adm2"]:
    NOTES[t] = (
        "- Region identifiers are kept as published ("
        + (
            "GADM GIDs, e.g. KOR.15_1"
            if "gadm" in t
            else "geoBoundaries shapeIDs, e.g. 66186276B64762166704956"
        )
        + "). Data Basis has no directory for this geography yet."
    )
README = """# Social Connectedness Index (Meta) — `us_meta_sci.{table}`

Auxiliary files for the Data Basis table `us_meta_sci.{table}`.

## How to cite

Johnston, D., Kuchler, T., Kulkarni, M., and Stroebel, J. (2026). "The Social Connectedness Index." Data release note, Meta Platforms and NYU Stern. Included here as `social_connectedness_index_2026_release_note.pdf`.

Bailey, M., Cao, R., Kuchler, T., Stroebel, J., and Wong, A. (2018). "Social Connectedness: Measurement, Determinants, and Effects." *Journal of Economic Perspectives* 32(3): 259-280. https://doi.org/10.1257/jep.32.3.259

Data: Meta Data for Good, Social Connectedness Index, via the Humanitarian Data Exchange ({hdx}). Licence: Creative Commons Zero (CC0).

## Files in this bundle

| File | What it is | Source URL | Downloaded |
|---|---|---|---|
| `social_connectedness_index_2026_release_note.pdf` | 6-page release note: definition of the SCI, sample (Facebook users active in the 30 days before 2026-01-25 with more than 20 friends), privacy safeguards (units with fewer than 500 users excluded; mu-Gaussian differential privacy), geographic coverage, aggregation formula and limitations | {pdf_url} | {date} |
| `README.md` | This file | — | — |

## Source data for this table

- Raw file: {src_file}
- URL: {src_url}
- Downloaded: {date}
- Snapshot: Facebook friendship links as of 2026-01-25 (single release; Meta published earlier releases between 2018 and 2021, and the release note reports a correlation of 0.97 with the 2021 release at the country and US county levels).

## Reading the table

- `scaled_sci` = Friendships(i,j) / (Users(i) x Users(j)), rescaled by Meta to lie between 1 and 1,000,000,000 within each source file. It is a relative measure: if SCI(A,B) is twice SCI(A,C), a Facebook user in A is twice as likely to be friends with a user in B as with a user in C. Values are not comparable across tables.
- Region-to-region and country-to-country tables list every ordered pair, both directions, including each unit with itself; the index is symmetric.
- Country codes are ISO 3166-1 alpha-2 as published. Namibia is `NA` (a literal string, not a missing value) and Kosovo is `XK` (a user-assigned code).
- No values were altered at load; columns were only renamed, dropped when redundant, or (for stacked files) labelled with their level.
{notes}

## Link-only documentation

- Methodology: https://dataforgood.facebook.com/dfg/docs/methodology-social-connectedness-index
- Code and documentation repository: https://github.com/social-connectedness-index/social-connectedness-index
- HDX dataset page: {hdx}
- Bailey et al. (2018), JEP: https://doi.org/10.1257/jep.32.3.259
"""
date = (D / "download_date.txt").read_text().strip()[:10]
root = D / "auxiliary_files"
if root.exists():
    shutil.rmtree(root)
for t, (f, u) in SRC.items():
    d = root / t
    d.mkdir(parents=True)
    (d / "README.md").write_text(
        README.format(
            table=t,
            hdx=HDX,
            date=date,
            src_file=f,
            src_url=u,
            notes=NOTES.get(t, ""),
            pdf_url=f"{RES}/d26548fb-8935-4018-937b-4c0cacf99007/download/the-social-connectedness-index.pdf",
        )
    )
    with zipfile.ZipFile(
        d / "auxiliary_files.zip", "w", zipfile.ZIP_DEFLATED
    ) as z:
        z.write(d / "README.md", "README.md")
        z.write(PDF, "social_connectedness_index_2026_release_note.pdf")
    print(t, (d / "auxiliary_files.zip").stat().st_size)
