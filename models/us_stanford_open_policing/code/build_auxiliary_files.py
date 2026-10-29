"""Build the auxiliary-file bundle for the stop table.

The bundle holds the project's data README (per-jurisdiction column coverage,
date ranges and processing notes, without which the sparse stop table is hard
to read) and a README with citation and provenance. Long-form material (the
paper, the shapefiles, the RDS files) is linked, not rehosted.

Writes ``<DATA_DIR>/auxiliary_files/stop/auxiliary_files.zip``. Uploading it
to ``gs://basedosdados-public/auxiliary_files/us_stanford_open_policing/stop/``
is a separate, approved step.

Usage:
    python models/us_stanford_open_policing/code/build_auxiliary_files.py
"""

import datetime as dt
import urllib.request
import zipfile

try:
    from models.us_stanford_open_policing.code import constants
except ModuleNotFoundError:  # run as a script
    import constants  # type: ignore[no-redef]

README_COMMIT = "c9c730d68cb7a5591aa3884290f4fe9b20ddd536"
README_URL = (
    "https://raw.githubusercontent.com/stanford-policylab/opp/"
    f"{README_COMMIT}/data_readme.md"
)

BUNDLE_README = """# Stanford Open Policing Project: auxiliary files for `stop`

Data Basis table `basedosdados.us_stanford_open_policing.stop`.

## Citation

E. Pierson, C. Simoiu, J. Overgoor, S. Corbett-Davies, D. Jenson, A. Shoemaker,
V. Ramachandran, P. Barghouty, C. Phillips, R. Shroff, and S. Goel. "A
large-scale analysis of racial disparities in police stops across the United
States". Nature Human Behaviour, Vol. 4, 2020.

Data: Stanford Open Policing Project, https://openpolicing.stanford.edu/data/,
released under the Open Data Commons Attribution License (ODC-BY 1.0).

## Files

| File | What it is | Source | Downloaded |
|---|---|---|---|
| `opp_data_readme.md` | The project's data README: the standardized columns, best practices, and for every jurisdiction its date range, the coverage rate of each column and processing notes | {readme_url} (commit `{commit}`, last changed 2023-01-05) | {today} |

## Linked, not bundled

- Paper: https://www.nature.com/articles/s41562-020-0858-1
- Data page, with the RDS files and shapefiles for 22 cities: https://openpolicing.stanford.edu/data/
- Processing code: https://github.com/stanford-policylab/opp
- Stanford Digital Repository records: https://purl.stanford.edu/yg821jf8611 (2020-04-01 release) and https://purl.stanford.edu/wb225bk3255 (2023-01-26 release)

## Reading the table

- One source file per jurisdiction (`jurisdiction_id`, e.g. `tx_statewide`);
  each fills a different subset of the columns. Read the jurisdiction's section
  in `opp_data_readme.md` before comparing locations.
- Columns prefixed `raw_` in the source files are location-specific
  pass-throughs of the agency's raw data. They are not in this table.
- `year`, `state_abbreviation`, `state_id` and `jurisdiction_id` are added by
  Data Basis from the stop date and the file name. `year` and `date` are null
  for the few stops whose agency recorded no date (most of them in Oregon).
- Boolean columns keep the source's `TRUE`/`FALSE` text.
"""


def main() -> None:
    out = constants.DATA_DIR / "auxiliary_files" / "stop"
    out.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(README_URL, timeout=60) as r:
        data_readme = r.read()
    readme = BUNDLE_README.format(
        readme_url=README_URL,
        commit=README_COMMIT,
        today=dt.date.today().isoformat(),
    )
    path = out / "auxiliary_files.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("README.md", readme)
        z.writestr("opp_data_readme.md", data_readme)
    print(f"{path} ({path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
