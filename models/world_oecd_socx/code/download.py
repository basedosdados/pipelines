"""Download the SOCX Aggregated cube from the SDMX API into the scratch tree.

Chunked by year. One request per year returns every reporting area for that year;
the API rate-limits by IP, so a year at a time keeps each request small (~7 MB)
and lets the run resume. The year range and the areas that actually hold data
come from the ``Actual`` content constraint cached by ``fetch_structure.py``, so
no request is issued for a period the source has already said is empty.

Resumability, and the trap it avoids: a chunk that failed must never later be
mistaken for a chunk that was legitimately empty, or the gap becomes permanent
and silent. So a chunk file is written only after a 200 response has been parsed,
via a ``.part`` file renamed into place; any non-200 raises. ``manifest.json``
records each chunk's row count.

Run: ``python download.py``   (only one table, ``expenditure``)
"""

import csv
import io
import json
import sys
import xml.etree.ElementTree as ET

from common import INPUT, SDMX, STRUCTURE, get
from tables import TABLES

S = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"
C = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}"


def actual_constraint(flow, version):
    """(areas, first_year, last_year) the source says this flow actually holds."""
    for path in sorted(STRUCTURE.glob("constraints_*.xml")):
        root = ET.parse(path).getroot()
        for cc in root.iter(f"{S}ContentConstraint"):
            if cc.get("type") != "Actual":
                continue
            ref = cc.find(f"{S}ConstraintAttachment/{S}Dataflow/Ref")
            if (
                ref is None
                or ref.get("id") != flow
                or ref.get("version") != version
            ):
                continue
            areas, start, end = [], None, None
            for region in cc.iter(f"{S}CubeRegion"):
                for kv in region.iter(f"{C}KeyValue"):
                    if kv.get("id") == "REF_AREA":
                        areas = [
                            v.text for v in kv.iter(f"{C}Value") if v.text
                        ]
                for tr in region.iter(f"{C}TimeRange"):
                    sp, ep = (
                        tr.find(f"{C}StartPeriod"),
                        tr.find(f"{C}EndPeriod"),
                    )
                    if sp is not None and sp.text:
                        start = int(sp.text[:4])
                    if ep is not None and ep.text:
                        end = int(ep.text[:4])
            return areas, start, end
    return [], None, None


# Written for a year the source reports as holding nothing, so an empty chunk is
# an explicit recorded fact rather than an absent file.
EMPTY_MARKER = "# NoRecordsFound\n"


def fetch_chunk(dest, flow, version, agency, year):
    """Download one year to ``dest``, or return its existing row count."""
    if dest.exists():
        text = dest.read_text()
        if text == EMPTY_MARKER:
            return 0
        return text.count("\n") - 1
    resp = get(
        f"{SDMX}/data/{agency},{flow},{version}/all",
        params={
            "format": "csvfile",
            "startPeriod": str(year),
            "endPeriod": str(year),
        },
        allow_empty=True,
    )
    if resp is None:
        dest.write_text(EMPTY_MARKER)
        return 0
    text = resp.text
    # Parse before writing: a truncated or error body must not land as a chunk.
    rows = sum(1 for _ in csv.reader(io.StringIO(text))) - 1
    if rows < 0:
        raise ValueError(f"empty response body for {flow} {version} {year}")
    part = dest.with_suffix(".part")
    part.write_text(text)
    part.rename(dest)
    return rows


def download_table(slug, spec, manifest):
    agency = spec["agency"]
    out_dir = INPUT / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    for version in spec["versions"]:
        _areas, start, end = actual_constraint(spec["flow"], version)
        if start is None or end is None:
            raise SystemExit(
                f"no Actual constraint year range for {spec['flow']} {version} "
                "-- run fetch_structure.py first"
            )
        tag = f"{spec['flow'].split('@')[-1]}_{version}"
        for year in range(start, end + 1):
            dest = out_dir / f"{tag}_{year}.csv"
            n = fetch_chunk(dest, spec["flow"], version, agency, year)
            manifest[f"{slug}/{dest.name}"] = n
            total += n
            print(f"    {dest.name:44s} {n:10,d} rows", flush=True)
    return total


def main():
    wanted = sys.argv[1:] or list(TABLES)
    manifest_path = INPUT / "manifest.json"
    INPUT.mkdir(parents=True, exist_ok=True)
    manifest = (
        json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    )
    grand = 0
    for slug in wanted:
        print(f"{slug}:", flush=True)
        grand += download_table(slug, TABLES[slug], manifest)
        manifest_path.write_text(
            json.dumps(manifest, indent=1, sort_keys=True)
        )
    print(f"\n{len(wanted)} table(s), {grand:,} rows downloaded")


if __name__ == "__main__":
    main()
