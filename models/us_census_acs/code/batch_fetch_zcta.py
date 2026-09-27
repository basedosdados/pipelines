"""Backfill ZCTA profile cells whose full group() response the API truncates.

For each missing (year, profile), request the profile's E/M/PE/PM variables in
chunks of <=49 (small responses the API returns reliably), then reassemble them
into the same [header, *rows] group()-format JSON the melt consumes.
"""

import gzip
import json
import re
import time
import urllib.parse
import urllib.request

PROF = "/Users/rdahis/acs_data/profiles"
with open("/Users/rdahis/acs_data/.census_api_key") as _f:
    KEY = _f.read().strip()
MISSING = [(2011, "DP03"), (2020, "DP02"), (2021, "DP02"), (2022, "DP02")]
FORC = "zip code tabulation area:*"
CHUNK = 49


def codes_for(year, profile):
    with gzip.open(f"{PROF}/vars_acs5_{year}.json.gz", "rt") as f:
        v = json.load(f)["variables"]
    est = sorted(c for c in v if re.match(rf"^{profile}_\d{{3,4}}P?E$", c))
    full = []
    for c in est:  # E -> [E, M]; PE -> [PE, PM]
        full += [c, c[:-1] + "M"]
    return est, full


def fetch_chunk(year, profile, chunk, tries=4):
    get = ",".join(["NAME", *chunk])
    params = urllib.parse.urlencode({"get": get, "for": FORC, "key": KEY})
    url = f"https://api.census.gov/data/{year}/acs/acs5/profile?{params}"
    for t in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=180) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            print(f"      chunk retry {t + 1}: {str(e)[:60]}", flush=True)
            time.sleep(4)
    raise RuntimeError(f"chunk failed after {tries} tries")


def backfill(year, profile):
    est, full = codes_for(year, profile)
    print(
        f"  {year} {profile}: {len(est)} lines, {len(full)} codes, {-(-len(full) // CHUNK)} chunks",
        flush=True,
    )
    merged = {}  # geo_key -> {code: value}
    name_of = {}  # geo_key -> NAME
    geo_names = None
    geo_order = []
    for i in range(0, len(full), CHUNK):
        chunk = full[i : i + CHUNK]
        data = fetch_chunk(year, profile, chunk)
        header = data[0]
        idx = {c: j for j, c in enumerate(header)}
        gnames = [
            c for c in header if c != "NAME" and c not in chunk
        ]  # trailing geo cols
        geo_names = gnames
        for row in data[1:]:
            gk = tuple(row[idx[g]] for g in gnames)
            if gk not in merged:
                merged[gk] = {}
                name_of[gk] = row[idx["NAME"]]
                geo_order.append(gk)
            for c in chunk:
                merged[gk][c] = row[idx[c]]
        print(
            f"      chunk {i // CHUNK + 1} ok ({len(data) - 1} rows)",
            flush=True,
        )
    # reconstruct group-format JSON: header = NAME + all codes + geo cols
    out_header = ["NAME", *full, *geo_names]
    out = [out_header]
    for gk in geo_order:
        out.append(
            [name_of[gk]] + [merged[gk].get(c) for c in full] + list(gk)
        )
    path = f"{PROF}/acs5/{year}/zcta_{profile}.json.gz"
    with gzip.open(path, "wt") as f:
        json.dump(out, f)
    with gzip.open(path, "rt") as f:
        json.load(f)  # validate
    print(
        f"    -> wrote {path} ({len(out) - 1} rows, {len(out_header)} cols)",
        flush=True,
    )


if __name__ == "__main__":
    for y, p in MISSING:
        backfill(y, p)
    print("BACKFILL COMPLETE", flush=True)
