"""BEA API client + pure download/clean transforms for the us_bea dataset.

Pure functions (no Prefect, no side effects beyond HTTP GET and the caller-chosen
output dir), so the recurring pipeline (step 12) can import them directly.

Row schemas (from beaapi/get_data.py, verified against the live API):
  NIPA:          TableName, SeriesCode, LineNumber, LineDescription, TimePeriod,
                 METRIC_NAME, CL_UNIT, UNIT_MULT, DataValue, NoteRef
  Regional:      Code, GeoFips, GeoName, TimePeriod, CL_UNIT, UNIT_MULT, DataValue, NoteRef
  GDPbyIndustry: TableID, Frequency, Year, Quarter, Industry, IndustrYDescription,
                 DataValue, NoteRef
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://apps.bea.gov/api/data/"
MISSING_TOKENS = {"", "(NA)", "(NM)", "(D)", "(L)", "(*)", "NA", "n/a"}


def _key() -> str:
    k = os.environ.get("BEA_API_KEY", "").strip()
    if not k:
        raise RuntimeError("BEA_API_KEY not set in environment")
    return k


_MIN_INTERVAL = (
    0.65  # s between call starts -> ~92/min, under BEA's 100/min cap
)
_last_call = [0.0]


def _throttle():
    dt = time.monotonic() - _last_call[0]
    if dt < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - dt)
    _last_call[0] = time.monotonic()


def call(
    method: str, dataset: str | None = None, *, max_retries: int = 6, **params
) -> dict:
    """One BEA API call. Handles 429/Retry-After and transient errors. Returns Results."""
    _throttle()
    q = {"UserID": _key(), "method": method, "ResultFormat": "JSON"}
    if dataset:
        q["datasetname"] = dataset
    q.update({k: v for k, v in params.items() if v is not None})
    url = BASE + "?" + urllib.parse.urlencode(q)
    delay = 2.0
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url, timeout=180) as r:
                body = json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = int(e.headers.get("Retry-After", "60")) + 1
                time.sleep(wait)
                continue
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            raise
        res = body.get("BEAAPI", {}).get("Results", {})
        # BEA sometimes wraps Results in a list
        if isinstance(res, list):
            res = res[0] if res else {}
        err = res.get("Error") if isinstance(res, dict) else None
        if err:
            code = str(err.get("APIErrorCode", ""))
            desc = err.get("APIErrorDescription", "") or err.get(
                "ErrorDetail", {}
            ).get("Description", "")
            if code == "7":  # throttled
                time.sleep(61)
                continue
            raise BEAError(code, desc, dataset, params)
        return res
    raise RuntimeError(
        f"BEA call failed after retries: {method} {dataset} {params}"
    )


class BEAError(RuntimeError):
    def __init__(self, code, desc, dataset, params):
        self.code, self.desc = code, desc
        super().__init__(
            f"BEA APIErrorCode {code}: {desc} [{dataset} {params}]"
        )


def _norm_pv(node: dict, param: str) -> dict:
    """ParamValue nodes are keyed differently per dataset (NIPA: TableName/Description;
    Regional/GDPbyIndustry: Key/Desc). Normalize to {'key','desc'}."""
    key = node.get("Key") if "Key" in node else None
    if key is None:
        key = (
            node.get(param)
            or node.get("TableName")
            or node.get("TableID")
            or node.get("LineCode")
            or node.get("Industry")
            or node.get("FrequencyID")
        )
    desc = node.get("Desc") or node.get("Description") or ""
    return {"key": str(key) if key is not None else None, "desc": desc}


def param_values(dataset: str, param: str) -> list[dict]:
    res = call("GetParameterValues", dataset, ParameterName=param)
    v = res.get("ParamValue", res)
    v = v if isinstance(v, list) else [v]
    return [_norm_pv(n, param) for n in v]


def param_values_filtered(dataset: str, target: str, **filters) -> list[dict]:
    res = call(
        "GetParameterValuesFiltered",
        dataset,
        TargetParameter=target,
        **filters,
    )
    v = res.get("ParamValue", res)
    v = v if isinstance(v, list) else [v]
    return [_norm_pv(n, target) for n in v]


def get_data(dataset: str, **params) -> list[dict]:
    """GetData -> list of data rows (possibly empty)."""
    res = call("GetData", dataset, **params)
    data = res.get("Data", [])
    if isinstance(data, dict):
        data = [data]
    return data


# --------------------------------------------------------------------------- #
# pure cleaning helpers
# --------------------------------------------------------------------------- #
def clean_value(raw) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if s in MISSING_TOKENS:
        return None
    s = s.replace(",", "")
    # parenthesised negatives -> -x (rare in API payload, but be safe)
    if (
        s.startswith("(")
        and s.endswith(")")
        and s[1:-1].replace(".", "").isdigit()
    ):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return None


def split_time_period(
    tp: str | None,
) -> tuple[int | None, str | None, str | None]:
    """TimePeriod -> (year, quarter, month). YYYY / YYYYQn / YYYYMnn."""
    if tp is None:
        return None, None, None
    tp = tp.strip()
    try:
        if "Q" in tp:
            y, q = tp.split("Q")
            return int(y), q, None
        if "M" in tp:
            y, m = tp.split("M")
            return int(y), None, m.zfill(2)
        return int(tp[:4]), None, None
    except (ValueError, IndexError):
        return None, None, None


def line_from_code(code: str) -> str | None:
    """Regional 'Code' like 'SAGDP2N-1' -> line_code '1'."""
    if not code:
        return None
    return code.rsplit("-", 1)[-1] if "-" in code else None


def norm_gi_quarter(frequency: str, quarter) -> str | None:
    """GDPbyIndustry: annual rows have Quarter==Year; quarterly rows are I..IV."""
    if frequency == "A":
        return None
    roman = {"I": "1", "II": "2", "III": "3", "IV": "4"}
    return roman.get(str(quarter).strip(), str(quarter).strip() or None)


if __name__ == "__main__":
    # smoke test
    print(
        "datasets:",
        [d["DatasetName"] for d in call("GetDataSetList")["Dataset"]][:3],
        "...",
    )
    print(
        "clean_value tests:",
        clean_value("1,234.5"),
        clean_value("(NA)"),
        clean_value("(D)"),
        clean_value(""),
    )
    print(
        "split_time_period:",
        split_time_period("2020"),
        split_time_period("2020Q3"),
        split_time_period("2020M07"),
    )
    print(
        "line_from_code:",
        line_from_code("SAGDP2N-1"),
        line_from_code("CAINC1-30"),
    )
