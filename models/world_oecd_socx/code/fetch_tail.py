"""One-off: fetch the year range the chunked download could not reach.

The per-year download tripped a Cloudflare managed challenge on the OECD API
(403 "Just a moment..."), which a scripted client cannot solve and which only
relaxes after the IP is quiet for a while. This fetches the whole missing range
in a SINGLE request, with long quiet cooldowns between attempts so the IP is not
kept warm. The result is written as one multi-year chunk that clean.py picks up
alongside the per-year files (clean partitions by the TIME_PERIOD column, so a
multi-year chunk is fine).

    python fetch_tail.py 2018 2022
"""

import sys
import time

import requests
from common import HEADERS, INPUT, SDMX

FLOW = "DSD_SOCX_AGG@DF_SOCX_AGG"
VERSION = "1.0"
AGENCY = "OECD.ELS.SPD"
COOLDOWNS = [0, 1800, 1800, 2400]  # seconds of silence before each attempt


def main():
    start, end = sys.argv[1], sys.argv[2]
    dest = (
        INPUT
        / "expenditure"
        / f"{FLOW.split('@')[-1]}_{VERSION}_{start}_{end}.csv"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{SDMX}/data/{AGENCY},{FLOW},{VERSION}/all"
    params = {"format": "csvfile", "startPeriod": start, "endPeriod": end}
    for i, wait in enumerate(COOLDOWNS):
        if wait:
            print(f"  cooldown {wait}s before attempt {i + 1}", flush=True)
            time.sleep(wait)
        resp = requests.get(url, params=params, headers=HEADERS, timeout=1800)
        if resp.status_code == 200:
            n = resp.text.count("\n") - 1
            dest.write_text(resp.text)
            print(f"OK {start}-{end}: {n:,} rows -> {dest.name}", flush=True)
            return
        cf = "Just a moment" in resp.text[:2000]
        print(
            f"  attempt {i + 1}: HTTP {resp.status_code}"
            f"{' (cloudflare challenge)' if cf else ''}",
            flush=True,
        )
    raise SystemExit(
        f"still blocked for {start}-{end} after {len(COOLDOWNS)} attempts"
    )


if __name__ == "__main__":
    main()
