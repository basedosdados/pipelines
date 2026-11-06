"""Pure download + cleaning functions for us_congress_legislators (no Prefect).

Source: https://github.com/unitedstates/congress-legislators (CC0). The repo
republishes the full set of YAML files on every commit, so a refresh is always a
full rebuild of every table.

Shared by the Prefect flow and the one-shot bootstrap scripts under
``models/us_congress_legislators/code/`` and ``models/br_bd_diretorios_us/code/``
(the ``congress_member`` directory is built from the same files).

Every table is written as all-STRING parquet: staging is all-STRING by house
convention and the dbt models ``safe_cast`` each column to its final type.
"""

import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yaml

from pipelines.datasets.us_congress_legislators.constants import constants


def download_sources(input_dir: Path) -> None:
    """Download every source YAML file into ``input_dir``."""
    input_dir.mkdir(parents=True, exist_ok=True)
    for name in constants.SOURCE_FILES.value:
        url = f"{constants.RAW_BASE_URL.value}/{name}.yaml"
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        (input_dir / f"{name}.yaml").write_bytes(response.content)


def latest_source_commit_date() -> datetime.date:
    """Date (UTC) of the most recent commit on the source repo's default branch.

    The repo has no release cadence; a commit is its only publication signal.
    One unauthenticated GitHub API call (limit: 60 per hour per IP).
    """
    response = requests.get(
        constants.COMMITS_API_URL.value,
        params={"per_page": 1},
        headers={"Accept": "application/vnd.github+json"},
        timeout=60,
    )
    response.raise_for_status()
    committed = response.json()[0]["commit"]["committer"]["date"]
    return datetime.date.fromisoformat(committed[:10])


def _load(input_dir: Path, name: str):
    return yaml.safe_load((input_dir / f"{name}.yaml").read_text())


def s(v) -> str | None:
    """Normalize any scalar to a stripped string, or None."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    v = str(v).strip()
    return v if v != "" else None


def joinlist(v) -> str | None:
    if not v:
        return None
    return ",".join(str(x) for x in v)


def _legislators(input_dir: Path) -> list[tuple[bool, dict]]:
    """Current legislators first, then historical, tagged with is_current."""
    return [
        (is_current, r)
        for is_current, fn in [
            (True, "legislators-current"),
            (False, "legislators-historical"),
        ]
        for r in _load(input_dir, fn)
    ]


# ---------------------------------------------------------------- legislators
def build_legislator(input_dir: Path) -> pd.DataFrame:
    rows = []
    for is_current, r in _legislators(input_dir):
        i = r.get("id", {})
        n = r.get("name", {})
        b = r.get("bio", {})
        rows.append(
            {
                "id_bioguide": s(i.get("bioguide")),
                "id_thomas": s(i.get("thomas")),
                "id_lis": s(i.get("lis")),
                "id_govtrack": s(i.get("govtrack")),
                "id_icpsr": s(i.get("icpsr")),
                "id_opensecrets": s(i.get("opensecrets")),
                "id_votesmart": s(i.get("votesmart")),
                "id_fec": joinlist(i.get("fec")),
                "id_cspan": s(i.get("cspan")),
                "id_maplight": s(i.get("maplight")),
                "id_house_history": s(i.get("house_history")),
                "id_pictorial": s(i.get("pictorial")),
                "id_wikidata": s(i.get("wikidata")),
                "id_google_entity": s(i.get("google_entity_id")),
                "name_wikipedia": s(i.get("wikipedia")),
                "name_ballotpedia": s(i.get("ballotpedia")),
                "first_name": s(n.get("first")),
                "middle_name": s(n.get("middle")),
                "last_name": s(n.get("last")),
                "suffix": s(n.get("suffix")),
                "nickname": s(n.get("nickname")),
                "full_name": s(n.get("official_full")),
                "birthday": s(b.get("birthday")),
                "gender": s(b.get("gender")),
                "is_current": "true" if is_current else "false",
            }
        )
    return pd.DataFrame(rows).drop_duplicates(
        subset=["id_bioguide"], keep="first"
    )


def build_congress_member(input_dir: Path) -> pd.DataFrame:
    """The ``br_bd_diretorios_us.congress_member`` directory table."""
    rows = []
    for _is_current, r in _legislators(input_dir):
        i = r.get("id", {})
        n = r.get("name", {})
        b = r.get("bio", {})
        rows.append(
            {
                "id_bioguide": s(i.get("bioguide")),
                "name_first": s(n.get("first")),
                "name_middle": s(n.get("middle")),
                "name_last": s(n.get("last")),
                "name_suffix": s(n.get("suffix")),
                "name_official_full": s(n.get("official_full")),
                "birthday": s(b.get("birthday")),
                "gender": s(b.get("gender")),
                "id_govtrack": s(i.get("govtrack")),
                "id_icpsr": s(i.get("icpsr")),
                "id_lis": s(i.get("lis")),
                "id_thomas": s(i.get("thomas")),
                "id_opensecrets": s(i.get("opensecrets")),
                "id_votesmart": s(i.get("votesmart")),
                "id_cspan": s(i.get("cspan")),
                "id_fec": joinlist(i.get("fec")),
                "id_wikidata": s(i.get("wikidata")),
                "wikipedia": s(i.get("wikipedia")),
                "ballotpedia": s(i.get("ballotpedia")),
            }
        )
    return pd.DataFrame(rows).drop_duplicates(
        subset=["id_bioguide"], keep="first"
    )


def build_term(input_dir: Path) -> pd.DataFrame:
    rows = []
    for _is_current, r in _legislators(input_dir):
        bio = s(r.get("id", {}).get("bioguide"))
        for t in r.get("terms", []):
            rows.append(
                {
                    "id_bioguide": bio,
                    "term_type": s(t.get("type")),
                    "term_start": s(t.get("start")),
                    "term_end": s(t.get("end")),
                    "state": s(t.get("state")),
                    "district": s(t.get("district")),
                    "senate_class": s(t.get("class")),
                    "state_rank": s(t.get("state_rank")),
                    "party": s(t.get("party")),
                    "caucus": s(t.get("caucus")),
                    "how": s(t.get("how")),
                    "end_type": s(t.get("end-type")),
                    "url": s(t.get("url")),
                    "address": s(t.get("address")),
                    "phone": s(t.get("phone")),
                    "fax": s(t.get("fax")),
                    "contact_form": s(t.get("contact_form")),
                    "office": s(t.get("office")),
                    "rss_url": s(t.get("rss_url")),
                }
            )
    return pd.DataFrame(rows)


def build_leadership_role(input_dir: Path) -> pd.DataFrame:
    rows = []
    for _is_current, r in _legislators(input_dir):
        bio = s(r.get("id", {}).get("bioguide"))
        for lr in r.get("leadership_roles", []):
            rows.append(
                {
                    "id_bioguide": bio,
                    "title": s(lr.get("title")),
                    "chamber": s(lr.get("chamber")),
                    "role_start": s(lr.get("start")),
                    "role_end": s(lr.get("end")),
                }
            )
    return pd.DataFrame(rows)


def build_social_media(input_dir: Path) -> pd.DataFrame:
    rows = []
    for r in _load(input_dir, "legislators-social-media"):
        bio = s(r.get("id", {}).get("bioguide"))
        soc = r.get("social", {})
        rows.append(
            {
                "id_bioguide": bio,
                "twitter": s(soc.get("twitter")),
                "twitter_id": s(soc.get("twitter_id")),
                "facebook": s(soc.get("facebook")),
                "instagram": s(soc.get("instagram")),
                "instagram_id": s(soc.get("instagram_id")),
                "youtube": s(soc.get("youtube")),
                "youtube_id": s(soc.get("youtube_id")),
                "mastodon": s(soc.get("mastodon")),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- committees
def build_committee(input_dir: Path) -> pd.DataFrame:
    committees: dict[str | None, dict] = {}  # id_committee -> row

    def add(full_id, parent, thomas_id, name, ctype, is_sub, is_current, rec):
        if full_id in committees:
            return  # current wins (processed first)
        committees[full_id] = {
            "id_committee": full_id,
            "id_committee_parent": parent,
            "thomas_id": thomas_id,
            "name": name,
            "committee_type": ctype,
            "is_subcommittee": "true" if is_sub else "false",
            "is_current": "true" if is_current else "false",
            "house_committee_id": s(rec.get("house_committee_id")),
            "senate_committee_id": s(rec.get("senate_committee_id")),
            "jurisdiction": s(rec.get("jurisdiction")),
            "url": s(rec.get("url")),
            "address": s(rec.get("address")),
            "phone": s(rec.get("phone")),
            "wikipedia": s(rec.get("wikipedia")),
        }

    for is_current, fn in [
        (True, "committees-current"),
        (False, "committees-historical"),
    ]:
        for c in _load(input_dir, fn):
            tid = s(c.get("thomas_id"))
            ctype = s(c.get("type"))
            add(tid, None, tid, s(c.get("name")), ctype, False, is_current, c)
            for sub in c.get("subcommittees", []):
                sid = s(sub.get("thomas_id"))
                full = f"{tid}{sid}"
                add(
                    full,
                    tid,
                    sid,
                    s(sub.get("name")),
                    ctype,
                    True,
                    is_current,
                    sub,
                )

    return pd.DataFrame(list(committees.values()))


def build_committee_membership(input_dir: Path) -> pd.DataFrame:
    rows = []
    for cid, members in _load(
        input_dir, "committee-membership-current"
    ).items():
        for m in members:
            rows.append(
                {
                    "id_committee": s(cid),
                    "id_bioguide": s(m.get("bioguide")),
                    "party": s(m.get("party")),
                    "rank": s(m.get("rank")),
                    "title": s(m.get("title")),
                    "chamber": s(m.get("chamber")),
                }
            )
    return pd.DataFrame(rows)


def build_district_office(input_dir: Path) -> pd.DataFrame:
    rows = []
    for r in _load(input_dir, "legislators-district-offices"):
        bio = s(r.get("id", {}).get("bioguide"))
        for o in r.get("offices", []):
            rows.append(
                {
                    "id_office": s(o.get("id")),
                    "id_bioguide": bio,
                    "building": s(o.get("building")),
                    "address": s(o.get("address")),
                    "suite": s(o.get("suite")),
                    "city": s(o.get("city")),
                    "state": s(o.get("state")),
                    "zip_code": s(o.get("zip")),
                    "phone": s(o.get("phone")),
                    "fax": s(o.get("fax")),
                    "hours": s(o.get("hours")),
                    "latitude": s(o.get("latitude")),
                    "longitude": s(o.get("longitude")),
                }
            )
    return pd.DataFrame(rows)


def build_executive_term(input_dir: Path) -> pd.DataFrame:
    rows = []
    for r in _load(input_dir, "executive"):
        i = r.get("id", {})
        n = r.get("name", {})
        b = r.get("bio", {})
        for t in r.get("terms", []):
            rows.append(
                {
                    "id_bioguide": s(i.get("bioguide")),
                    "id_icpsr_prez": s(i.get("icpsr_prez")),
                    "id_govtrack": s(i.get("govtrack")),
                    "first_name": s(n.get("first")),
                    "middle_name": s(n.get("middle")),
                    "last_name": s(n.get("last")),
                    "suffix": s(n.get("suffix")),
                    "nickname": s(n.get("nickname")),
                    "full_name": s(n.get("official_full")),
                    "birthday": s(b.get("birthday")),
                    "gender": s(b.get("gender")),
                    "term_type": s(t.get("type")),
                    "term_start": s(t.get("start")),
                    "term_end": s(t.get("end")),
                    "party": s(t.get("party")),
                    "how": s(t.get("how")),
                }
            )
    return pd.DataFrame(rows)


# us_congress_legislators tables, in the order they are uploaded.
BUILDERS = {
    "legislator": build_legislator,
    "term": build_term,
    "social_media": build_social_media,
    "leadership_role": build_leadership_role,
    "committee": build_committee,
    "committee_membership": build_committee_membership,
    "district_office": build_district_office,
    "executive_term": build_executive_term,
}


def write_table(df: pd.DataFrame, path: Path) -> Path:
    """Write ``df`` as all-STRING snappy parquet to ``path``/data.parquet.

    Cast via arrow rather than ``astype(str)``, which would render NULL as the
    literal ``"nan"``.
    """
    path.mkdir(parents=True, exist_ok=True)
    for p in path.glob("*.parquet"):
        p.unlink()
    # from_pandas=True maps NaN to null, so no NaN reaches the file as "nan".
    table = pa.Table.from_arrays(
        [
            pa.array(df[c].tolist(), type=pa.string(), from_pandas=True)
            for c in df.columns
        ],
        names=list(df.columns),
    )
    out = path / "data.parquet"
    pq.write_table(table, out, compression="snappy")
    return out


def clean_all(input_dir: Path, output_dir: Path) -> dict[str, str]:
    """Build every table and write it under ``output_dir/<dataset>/<table>``.

    Returns:
        A mapping of ``"<dataset_id>.<table_id>"`` to the table's output
        directory, in upload order: the ``congress_member`` directory first,
        since ``legislator`` carries a relationships test against it.
    """
    out: dict[str, str] = {}
    directory_ds = constants.DIRECTORY_DATASET_ID.value
    path = output_dir / directory_ds / "congress_member"
    write_table(build_congress_member(input_dir), path)
    out[f"{directory_ds}.congress_member"] = str(path)

    dataset_id = constants.DATASET_ID.value
    for table, build in BUILDERS.items():
        path = output_dir / dataset_id / table
        write_table(build(input_dir), path)
        out[f"{dataset_id}.{table}"] = str(path)
    return out
