"""Download and cache every SDMX structure artefact the onboarding needs.

Writes into ``<scratch>/input/structure/``:

* ``dataflows_<agency>.xml``  — every dataflow, ALL versions (not just ``latest``)
* ``constraints_<agency>.xml``— every content constraint, ALL versions. These carry
  both numbers worth having, so two requests replace one per flow: the
  ``sdmx_metrics`` annotation is the exact observation count, and ``validFrom`` on
  the ``Actual`` constraint is the last-update timestamp (the Data Explorer's
  "Last updated"). Neither appears on a bare ``/dataflow/<agency>/<flow>/<ver>``
  call — only when constraints are pulled in.
* ``dsd_<DSD>_<ver>.xml``     — each data structure definition plus its codelists

Two things this exists to get right. First, ``/latest`` hides live older vintages:
OECD.EDU.IMEP returns 127 flows under ``latest`` and 198 under ``all``, and for the
snapshot cubes those older versions carry *disjoint* reference periods (actual
salaries v1.1 = 2020-2022, v2.1 = 2023-2025), so taking only the newest silently
drops years. Second, everything is cached, because the API rate-limits by IP.

Run: ``python fetch_structure.py``  (add ``--refresh`` to re-download)
"""

import argparse
import xml.etree.ElementTree as ET

from common import SDMX, STRUCTURE, get

AGENCIES = ("OECD.EDU.IMEP", "OECD.EDU.ECS")
S = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"
C = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}"
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"


def cached(name, url, *, refresh=False):
    """Fetch ``url`` into ``STRUCTURE/name`` unless already present."""
    path = STRUCTURE / name
    if path.exists() and not refresh and path.stat().st_size > 0:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f"  fetching {name}", flush=True)
    path.write_bytes(get(url).content)
    return path


def english(node, tag):
    """The en-language child ``tag`` of ``node``, or ''."""
    for n in node.findall(f"{C}{tag}"):
        if (n.get(XML_LANG) or "").startswith("en"):
            return n.text or ""
    return ""


def list_flows(refresh=False):
    """Every dataflow in the education agencies, all versions."""
    flows = []
    for agency in AGENCIES:
        path = cached(
            f"dataflows_{agency}.xml",
            f"{SDMX}/dataflow/{agency}/all/all?detail=full",
            refresh=refresh,
        )
        root = ET.parse(path).getroot()
        for df in root.iter(f"{S}Dataflow"):
            ref = df.find(f"{S}Structure/Ref")
            flows.append(
                {
                    "agency": df.get("agencyID"),
                    "flow": df.get("id"),
                    "version": df.get("version"),
                    "dsd": ref.get("id") if ref is not None else "",
                    "dsd_version": ref.get("version")
                    if ref is not None
                    else "",
                    "name": english(df, "Name"),
                    "description": english(df, "Description"),
                }
            )
    return flows


def constraint_info(refresh=False):
    """(agency, flow, version) -> {"obs": int, "last_update": str}.

    Both facts live on the content constraints, so this is two requests rather
    than one per flow. ``sdmx_metrics`` is the exact observation count — verified
    against a full download of DF_LSO_NEAC_ALL, where the annotation said 7729373
    and the download had 7,729,373 rows. ``validFrom`` on the ``Actual``
    constraint is the last-update timestamp, and it is the only freshness signal
    the API exposes: ``updatedAfter`` on a data query is honoured but always
    returns an empty message, and the dataflow carries no update field.

    Uses ``/all/all`` rather than ``/all/latest`` because the older vintages are
    the point — see the module docstring.
    """
    out = {}
    for agency in AGENCIES:
        path = cached(
            f"constraints_{agency}.xml",
            f"{SDMX}/contentconstraint/{agency}/all/all",
            refresh=refresh,
        )
        root = ET.parse(path).getroot()
        for cc in root.iter(f"{S}ContentConstraint"):
            ref = cc.find(f"{S}ConstraintAttachment/{S}Dataflow/Ref")
            if ref is None:
                continue
            rec = out.setdefault(
                (ref.get("agencyID"), ref.get("id"), ref.get("version")), {}
            )
            for a in cc.iter(f"{C}Annotation"):
                kind = a.find(f"{C}AnnotationType")
                if kind is None or kind.text != "sdmx_metrics":
                    continue
                title = a.find(f"{C}AnnotationTitle")
                text = (title.text or "") if title is not None else ""
                if text.strip().isdigit():
                    rec["obs"] = int(text)
            if cc.get("type") == "Actual" and cc.get("validFrom"):
                rec["last_update"] = cc.get("validFrom")
    return out


def fetch_dsd(agency, dsd, version, refresh=False):
    """The DSD plus every codelist it enumerates."""
    return cached(
        f"dsd_{dsd}_{version}.xml",
        f"{SDMX}/datastructure/{agency}/{dsd}/{version}?references=children",
        refresh=refresh,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--refresh", action="store_true", help="re-download cached files"
    )
    args = ap.parse_args()

    STRUCTURE.mkdir(parents=True, exist_ok=True)
    flows = list_flows(refresh=args.refresh)
    print(f"{len(flows)} dataflows across {len(AGENCIES)} education agencies")

    info = constraint_info(refresh=args.refresh)
    print(f"{len(info)} flow-versions described by content constraints")
    for f in flows:
        rec = info.get((f["agency"], f["flow"], f["version"]), {})
        f["obs"] = rec.get("obs")
        f["last_update"] = rec.get("last_update", "")
    missing = [f for f in flows if f["obs"] is None]
    if missing:
        print(
            f"  {len(missing)} flows have no observation count (empty or withdrawn)"
        )

    dsds = {(f["agency"], f["dsd"], f["dsd_version"]) for f in flows}
    print(f"fetching {len(dsds)} data structure definitions with codelists")
    for agency, dsd, version in sorted(dsds):
        fetch_dsd(agency, dsd, version, refresh=args.refresh)

    import json

    (STRUCTURE / "flows.json").write_text(json.dumps(flows, indent=1))
    print(f"wrote {STRUCTURE / 'flows.json'}")


if __name__ == "__main__":
    main()
