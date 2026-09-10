"""Download and parse the Victorian Electoral Commission political donations register.

The register is a Power Apps (Dataverse) portal on ``disclosures.vec.vic.gov.au`` — not
``www.vec.vic.gov.au``, which 404s. It is published as two saved views over one entity,
``pit_donation``: ``/public-donations/`` covers disclosures from 1 July 2020 on, and
``/public-donations-before-2020/`` covers the earlier ones. The grid labels describe the
views, not the data extent, so both must be pulled.

Two properties of the endpoint are load-bearing:

1. Without **both** the session cookie and the ``__RequestVerificationToken`` header it
   answers HTTP 200 with a zero-byte body. That is a silent failure, so
   :func:`_pull_portal` raises on an empty response rather than returning no records.
2. The response repeats the full Dataverse attribute metadata on every record, roughly
   55 KB per row. Records are stripped to Name/Type/Value/FormattedValue on ingest, which
   takes the cached pull from ~240 MB to ~5 MB.

Dates arrive as ``/Date(epoch_ms)/`` with ``FormattedValue`` in Australian ``d/MM/yyyy``
order. Only the epoch is parsed; the formatted string is used to verify the parse, never
to produce it.
"""

from __future__ import annotations

import base64
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import pandas as pd
import requests

from pipelines.datasets.au_vic_vec_elections import schema

# --------------------------------------------------------------------------------------
# Source constants
# --------------------------------------------------------------------------------------

USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
BASE_URL: Final[str] = "https://disclosures.vec.vic.gov.au"
GRID_ENDPOINT: Final[str] = (
    BASE_URL
    + "/_services/entity-grid-data.json/d78574f9-20c3-4dcc-8d8d-85cf5b7ac141"
)
TOKEN_URL: Final[str] = BASE_URL + "/_layout/tokenhtml"

PAGE_SIZE: Final[int] = 1000
REQUEST_TIMEOUT: Final[int] = 600

#: The two saved views, and the cache file each is written to.
PORTALS: Final[dict[str, str]] = {
    "after_2020": "/public-donations/",
    "before_2020": "/public-donations-before-2020/",
}

#: Attribute keys kept from each record. Everything else is repeated metadata.
KEPT_ATTRIBUTE_KEYS: Final[tuple[str, ...]] = (
    "Name",
    "Type",
    "Value",
    "FormattedValue",
)

#: The gap between the two views: the earlier view stops before 1 July 2020 and the later
#: one starts in September 2020, so no disclosure should fall inside this window.
GAP_START: Final[date] = date(2020, 7, 1)
GAP_END: Final[date] = date(2020, 9, 2)

TABLE: Final[str] = "disclosure_gift"

_DATAVERSE_DATE_RE: Final[re.Pattern[str]] = re.compile(r"^/Date\((-?\d+)\)/$")
_VIEW_LAYOUTS_RE: Final[re.Pattern[str]] = re.compile(
    r"data-view-layouts='([^']+)'"
)
_TOKEN_RE: Final[re.Pattern[str]] = re.compile(r'value="([^"]+)"')
_NON_ALNUM_RE: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9]+")


class SourceError(RuntimeError):
    """The portal answered in a way that cannot be trusted as data."""


# --------------------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------------------


def _strip_record(record: dict[str, Any], portal: str) -> dict[str, Any]:
    """Keep the record id, its portal, and the four useful keys of each attribute."""
    return {
        "Portal": portal,
        "Id": record["Id"],
        "Attributes": [
            {key: attribute.get(key) for key in KEPT_ATTRIBUTE_KEYS}
            for attribute in record["Attributes"]
        ],
    }


def _pull_portal(portal: str, path: str) -> list[dict[str, Any]]:
    """Pull every page of one saved view, verifying the reported ``ItemCount``."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    print(f"[{portal}] GET {path}", flush=True)
    page_html = session.get(BASE_URL + path, timeout=60).text
    layout_match = _VIEW_LAYOUTS_RE.search(page_html)
    if layout_match is None:
        raise SourceError(f"[{portal}] no data-view-layouts on {path}")
    # Scraped fresh every run: the secure configuration is signed and rotates.
    config = json.loads(base64.b64decode(layout_match.group(1)))[0]

    token_match = _TOKEN_RE.search(session.get(TOKEN_URL, timeout=60).text)
    if token_match is None:
        raise SourceError(
            f"[{portal}] no request verification token at {TOKEN_URL}"
        )
    token = token_match.group(1)

    records: list[dict[str, Any]] = []
    item_count: int | None = None
    page = 1
    while True:
        body = {
            "base64SecureConfiguration": config["Base64SecureConfiguration"],
            "sortExpression": config["SortExpression"],
            "search": None,
            "page": page,
            "pageSize": PAGE_SIZE,
            "filter": None,
            "metaFilter": None,
            "timezoneOffset": 0,
            "customParameters": [],
        }
        response = session.post(
            GRID_ENDPOINT,
            json=body,
            timeout=REQUEST_TIMEOUT,
            headers={
                "__RequestVerificationToken": token,
                "X-Requested-With": "XMLHttpRequest",
                "Referer": BASE_URL + path,
            },
        )
        if not response.content:
            raise SourceError(
                f"[{portal}] page {page} returned HTTP {response.status_code} with an "
                "empty body — the session cookie or verification token was rejected"
            )
        payload = response.json()
        item_count = payload["ItemCount"]
        batch = payload["Records"]
        records.extend(_strip_record(record, portal) for record in batch)
        print(
            f"[{portal}] page {page}: {len(batch)} records "
            f"({len(records)}/{item_count}), more={payload['MoreRecords']}",
            flush=True,
        )
        if not payload["MoreRecords"]:
            break
        page += 1

    if item_count is None or len(records) != item_count:
        raise SourceError(
            f"[{portal}] pulled {len(records)} records but ItemCount says {item_count}"
        )
    return records


def fetch_raw(cache_dir: str, refresh: bool = False) -> list[dict[str, Any]]:
    """Return the stripped records of both portals, caching each as JSON.

    Set ``refresh`` to re-pull a portal whose cache file already exists.
    """
    directory = Path(cache_dir)
    directory.mkdir(parents=True, exist_ok=True)

    records: list[dict[str, Any]] = []
    for portal, path in PORTALS.items():
        cache_file = directory / f"donations_{portal}.json"
        if cache_file.exists() and not refresh:
            print(f"[{portal}] reading cache {cache_file}", flush=True)
            portal_records = json.loads(cache_file.read_text(encoding="utf-8"))
        else:
            portal_records = _pull_portal(portal, path)
            cache_file.write_text(
                json.dumps(portal_records, ensure_ascii=False),
                encoding="utf-8",
            )
            print(
                f"[{portal}] wrote {len(portal_records)} records to {cache_file}",
                flush=True,
            )
        records.extend(portal_records)

    print(
        f"fetch_raw: {len(records)} records across {len(PORTALS)} portals",
        flush=True,
    )
    return records


# --------------------------------------------------------------------------------------
# Attribute readers
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Attribute:
    type_: str | None
    value: Any
    formatted: str | None


def _attributes(record: dict[str, Any]) -> dict[str, _Attribute]:
    return {
        attribute["Name"]: _Attribute(
            attribute.get("Type"),
            attribute.get("Value"),
            attribute.get("FormattedValue"),
        )
        for attribute in record["Attributes"]
    }


def _clean_text(value: Any) -> str | None:
    """Trim; an empty or whitespace-only string becomes NULL, never ``""``."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _snake(value: str | None) -> str | None:
    if value is None:
        return None
    return _NON_ALNUM_RE.sub("_", value.strip().lower()).strip("_") or None


def _parse_date(attribute: _Attribute | None) -> date | None:
    """Parse ``/Date(epoch_ms)/`` and cross-check it against the ``d/MM/yyyy`` label."""
    if attribute is None or attribute.value is None:
        return None
    match = _DATAVERSE_DATE_RE.match(str(attribute.value))
    if match is None:
        raise ValueError(f"unrecognised Dataverse date {attribute.value!r}")
    parsed = datetime.fromtimestamp(int(match.group(1)) / 1000, tz=UTC).date()

    label = _clean_text(attribute.formatted)
    if label:
        # Australian day-first order; never parsed as US month-first.
        expected = datetime.strptime(label, "%d/%m/%Y").date()
        if expected != parsed:
            raise ValueError(
                f"epoch date {parsed} disagrees with the portal's label {label!r}"
            )
    return parsed


def _entity_reference(
    attribute: _Attribute | None,
) -> tuple[str | None, str | None]:
    if attribute is None or not isinstance(attribute.value, dict):
        return None, None
    return _clean_text(attribute.value.get("Id")), _clean_text(
        attribute.value.get("Name")
    )


def _money(attribute: _Attribute | None) -> float | None:
    if attribute is None or not isinstance(attribute.value, dict):
        return None
    raw = attribute.value.get("Value")
    return None if raw is None else float(raw)


def _option_label(attribute: _Attribute | None) -> str | None:
    return None if attribute is None else _clean_text(attribute.formatted)


# --------------------------------------------------------------------------------------
# Row construction
# --------------------------------------------------------------------------------------


def _build_row(record: dict[str, Any]) -> dict[str, Any]:
    attributes = _attributes(record)
    portal = record["Portal"]

    donation_id = _clean_text(record["Id"])
    attribute_id = _clean_text(
        getattr(attributes.get("pit_donationid"), "value", None)
    )
    if (
        attribute_id is not None
        and donation_id is not None
        and attribute_id.lower() != donation_id.lower()
    ):
        raise ValueError(
            f"record Id {donation_id} disagrees with pit_donationid {attribute_id}"
        )

    date_made = _parse_date(attributes.get("vec_datedonationmade"))
    date_received = _parse_date(attributes.get("vec_datedonationreceived"))
    if portal == "before_2020":
        # The earlier view publishes a single donation date; it is the received date.
        date_received = _parse_date(attributes.get("pit_donationdate"))
        date_made = None

    donor_id, donor_name = _entity_reference(attributes.get("vec_donor"))
    recipient_id, recipient_name = _entity_reference(
        attributes.get("vec_recipient")
    )
    party_id, party_name = _entity_reference(
        attributes.get("pit_recipientrpp")
    )

    effective = date_received or date_made
    if effective is None:
        raise ValueError(
            f"donation {donation_id} has neither a made nor a received date"
        )

    return {
        "year": effective.year,
        "donation_id": donation_id,
        "date_made": date_made,
        "date_received": date_received,
        "donor_id": donor_id,
        "donor_name": donor_name,
        "donor_suburb": _clean_text(
            getattr(attributes.get("pit_donorsuburb"), "value", None)
        ),
        "donor_state": _clean_text(
            getattr(attributes.get("pit_donorstate"), "value", None)
        ),
        "recipient_id": recipient_id,
        "recipient_name": recipient_name,
        "recipient_party_id": party_id,
        "recipient_party_name": party_name,
        "gift_value": _money(attributes.get("pit_amount")),
        "donation_type": _snake(
            _option_label(attributes.get("pit_donationtype"))
        ),
        "disclosure_status": _snake(
            _option_label(attributes.get("statuscode"))
        ),
        "electorate_name": _clean_text(
            getattr(attributes.get("pit_electorate"), "value", None)
        ),
        "_portal": portal,
    }


def build_disclosure_gift(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Turn stripped portal records into the ``disclosure_gift`` frame."""
    frame = pd.DataFrame([_build_row(record) for record in records])
    columns = schema.column_names(TABLE)
    frame = frame[[*columns, "_portal"]]
    frame["year"] = frame["year"].astype("int64")
    frame["gift_value"] = frame["gift_value"].astype("float64")
    for column in ("date_made", "date_received"):
        # Kept as ``datetime.date`` objects so arrow infers DATE, not TIMESTAMP.
        frame[column] = (
            frame[column].astype("object").where(frame[column].notna(), None)
        )
    return frame


# --------------------------------------------------------------------------------------
# Consistency checks
# --------------------------------------------------------------------------------------


def check_portal_totals(frame: pd.DataFrame) -> dict[str, int]:
    """Total rows must be the sum of the two portals' rows."""
    per_portal = frame["_portal"].value_counts().to_dict()
    total = int(sum(per_portal.values()))
    if total != len(frame):
        raise SourceError(
            f"portal counts {per_portal} do not sum to {len(frame)} rows"
        )
    print(f"check: {total} rows = {per_portal}", flush=True)
    return {portal: int(count) for portal, count in per_portal.items()}


def check_no_id_collisions(frame: pd.DataFrame) -> None:
    """A donation id must appear once, and never in both portals."""
    duplicated = frame.loc[
        frame["donation_id"].duplicated(keep=False), "donation_id"
    ]
    if not duplicated.empty:
        raise SourceError(
            f"{duplicated.nunique()} donation_id values are duplicated, e.g. "
            f"{sorted(duplicated.unique())[:3]}"
        )
    print("check: no donation_id collisions between the portals", flush=True)


def check_gap_is_empty(frame: pd.DataFrame) -> int:
    """No disclosure should fall in the window between the two views."""
    effective = frame["date_received"].where(
        frame["date_received"].notna(), frame["date_made"]
    )
    inside = effective.map(
        lambda value: isinstance(value, date) and GAP_START <= value <= GAP_END
    )
    count = int(inside.sum())
    print(f"check: {count} rows in the {GAP_START}..{GAP_END} gap", flush=True)
    return count


def check_null_dates_match_status(frame: pd.DataFrame) -> dict[str, Any]:
    """Null dates should line up exactly with the unreconciled sides.

    Documented property of the register: a donation is declared by both the donor and the
    recipient, and an unreconciled side supplies no date. Before-2020 rows carry a single
    donation date by construction and are excluded from the ``date_made`` half.
    """
    after = frame[frame["_portal"] == "after_2020"]
    report: dict[str, Any] = {}
    for column, status in (
        ("date_made", "donor_unreconciled"),
        ("date_received", "recipient_unreconciled"),
    ):
        null_mask = after[column].isna()
        status_mask = after["disclosure_status"] == status
        report[column] = {
            "null_rows": int(null_mask.sum()),
            f"{status}_rows": int(status_mask.sum()),
            "null_and_status": int((null_mask & status_mask).sum()),
            "null_not_status": int((null_mask & ~status_mask).sum()),
            "status_not_null": int((status_mask & ~null_mask).sum()),
            "holds": bool((null_mask == status_mask).all()),
        }
        print(f"check: {column} vs {status} -> {report[column]}", flush=True)
    report["before_2020_date_made_nulls"] = int(
        frame[frame["_portal"] == "before_2020"]["date_made"].isna().sum()
    )
    return report


def option_set_labels(
    records: list[dict[str, Any]],
) -> dict[str, Counter[str]]:
    """Distinct raw ``FormattedValue`` labels for the two option sets, with counts."""
    labels: dict[str, Counter[str]] = {
        "pit_donationtype": Counter(),
        "statuscode": Counter(),
    }
    for record in records:
        attributes = _attributes(record)
        for name, counter in labels.items():
            label = _option_label(attributes.get(name))
            counter[label or "<null>"] += 1
    return labels


# --------------------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------------------


def parse_all(cache_dir: str) -> dict[str, pd.DataFrame]:
    """Return ``{"disclosure_gift": frame}`` from the cached (or freshly pulled) pull."""
    records = fetch_raw(cache_dir)
    frame = build_disclosure_gift(records)

    check_portal_totals(frame)
    check_no_id_collisions(frame)
    check_gap_is_empty(frame)
    check_null_dates_match_status(frame)

    frame = frame.drop(columns=["_portal"]).reset_index(drop=True)
    print(f"parse_all: {TABLE} has {len(frame)} rows", flush=True)
    return {TABLE: frame}
