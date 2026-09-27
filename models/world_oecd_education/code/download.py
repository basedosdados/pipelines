"""Download the OECD education cubes from the SDMX API into the scratch tree.

Chunked by year, because the whole ``UOE_FIN`` cube is 5.35 GB in a single
request and the API rate-limits by IP. The year range and the list of areas that
actually hold data both come from the ``Actual`` content constraint cached by
``fetch_structure.py``, so no request is issued for a combination the source has
already said is empty.

Resumability, and the trap it has to avoid: a chunk that failed must never be
mistaken later for a chunk that was legitimately empty, or the gap becomes
permanent and silent. So a chunk file is written only after a 200 response has
been parsed, via a ``.part`` file renamed into place, and any non-200 raises
rather than leaving a short file behind. ``manifest.json`` records the row count
of every chunk so the totals can be checked against the ``sdmx_metrics``
observation counts the API advertises.

Run: ``python download.py``            all tables
     ``python download.py student``    one table
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


# Written for a year the source reports as holding nothing, so that an empty
# chunk is an explicit recorded fact rather than an absent file.
EMPTY_MARKER = "# NoRecordsFound\n"


def fetch_chunk(dest, flow, version, agency, year=None):
    """Download one chunk to ``dest``, or return its existing row count."""
    if dest.exists():
        text = dest.read_text()
        if text == EMPTY_MARKER:
            return 0
        return text.count("\n") - 1
    params = {"format": "csvfile"}
    if year is not None:
        params["startPeriod"] = str(year)
        params["endPeriod"] = str(year)
    resp = get(
        f"{SDMX}/data/{agency},{flow},{version}/all",
        params=params,
        allow_empty=True,
    )
    if resp is None:
        # The source says this year holds no observations. Record it as a real,
        # header-only chunk so a later run cannot confuse "known empty" with
        # "never fetched" and leave a permanent hole.
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


def flows_for(spec):
    """(flow, version, year_filter) triples that make up one table."""
    out = [(spec["flow"], v, None) for v in spec["versions"]]
    for flow, version in spec.get("stack", []):
        out.append((flow, version, None))
    if spec.get("backfill"):
        version, start, end = spec["backfill"]
        out.append((spec["flow"], version, (start, end)))
    return out


def download_table(slug, spec, manifest):
    agency = "OECD.EDU.ECS" if slug.startswith("talis") else "OECD.EDU.IMEP"
    out_dir = INPUT / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    for flow, version, year_filter in flows_for(spec):
        _areas, start, end = actual_constraint(flow, version)
        if year_filter:
            start, end = year_filter
        tag = f"{flow.split('@')[-1]}_{version}"
        if start is None or end is None:
            # No time dimension: the whole cube in one request. These are small,
            # the largest being 26,040 observations.
            dest = out_dir / f"{tag}.csv"
            n = fetch_chunk(dest, flow, version, agency)
            manifest[f"{slug}/{dest.name}"] = n
            total += n
            print(f"    {dest.name:44s} {n:10,d} rows", flush=True)
            continue
        for year in range(start, end + 1):
            dest = out_dir / f"{tag}_{year}.csv"
            n = fetch_chunk(dest, flow, version, agency, year=year)
            manifest[f"{slug}/{dest.name}"] = n
            total += n
            print(f"    {dest.name:44s} {n:10,d} rows", flush=True)
    return total


def main():
    wanted = sys.argv[1:] or list(TABLES)
    manifest_path = INPUT / "manifest.json"
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
    print(f"\n{len(wanted)} tables, {grand:,} rows downloaded")


if __name__ == "__main__":
    main()
