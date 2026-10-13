"""Enumerate the VEC public-files Azure blob container.

The VEC publishes its whole file estate in a publicly listable Azure blob
container. Listing it is the authoritative inventory: complete, with sizes and
modification times, and it does not depend on scraping the website.

Writes ``inventory/blobs.json`` under the scratch root.
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

CONTAINER = "https://itsitecoreblobvecprd01.blob.core.windows.net/public-files"
SCRATCH = os.environ.get(
    "VEC_DATA_ROOT",
    os.path.expanduser("~/Downloads/au_vic_vec_elections_data"),
)


def list_container() -> list[dict]:
    out: list[dict] = []
    marker = None
    page = 0
    while True:
        query = {"restype": "container", "comp": "list", "maxresults": "5000"}
        if marker:
            query["marker"] = marker
        url = CONTAINER + "?" + urllib.parse.urlencode(query)
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=180) as response:
            body = response.read()
        root = ET.fromstring(body.decode("utf-8-sig"))
        blobs = root.find("Blobs")
        if blobs is None:
            raise RuntimeError("no <Blobs> element in the container listing")
        count = 0
        for blob in blobs.findall("Blob"):
            props = blob.find("Properties")
            if props is None:
                continue
            out.append(
                {
                    "name": blob.findtext("Name"),
                    "last_modified": props.findtext("Last-Modified"),
                    "size": int(props.findtext("Content-Length") or 0),
                    "content_type": props.findtext("Content-Type"),
                }
            )
            count += 1
        page += 1
        marker = root.findtext("NextMarker") or None
        print(
            f"page {page}: {count} blobs, running total {len(out)}, "
            f"more={'yes' if marker else 'no'}",
            flush=True,
        )
        if not marker:
            break
        time.sleep(0.2)
    return out


def main() -> None:
    os.makedirs(os.path.join(SCRATCH, "inventory"), exist_ok=True)
    blobs = list_container()
    path = os.path.join(SCRATCH, "inventory", "blobs.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(blobs, handle)
    print(f"TOTAL {len(blobs)} blobs -> {path}")


if __name__ == "__main__":
    main()
