"""Clean unitedstates/congress-legislators YAML into 8 Data Basis tables.

Source: https://github.com/unitedstates/congress-legislators (CC0).
Output: one unpartitioned snappy Parquet per table under output/<table>/data.parquet.
All columns are written as STRING; dbt applies safe_cast to final types.
"""

import pathlib
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

HERE = pathlib.Path(__file__).resolve().parent
INPUT = HERE / "input"
OUTPUT = HERE.parent / "output"


def load(name):
    return yaml.safe_load((INPUT / f"{name}.yaml").read_text())


def s(v):
    """Normalize any scalar to a stripped string, or None."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    v = str(v).strip()
    return v if v != "" else None


def joinlist(v):
    if not v:
        return None
    return ",".join(str(x) for x in v)


# ---------------------------------------------------------------- legislators
def build_legislator():
    rows = []
    for is_current, fn in [
        (True, "legislators-current"),
        (False, "legislators-historical"),
    ]:
        for r in load(fn):
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
    df = pd.DataFrame(rows).drop_duplicates(
        subset=["id_bioguide"], keep="first"
    )
    return df


def build_term():
    rows = []
    for fn in ["legislators-current", "legislators-historical"]:
        for r in load(fn):
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


def build_leadership_role():
    rows = []
    for fn in ["legislators-current", "legislators-historical"]:
        for r in load(fn):
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


def build_social_media():
    rows = []
    for r in load("legislators-social-media"):
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
def build_committee():
    committees = {}  # id_committee -> row

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
        for c in load(fn):
            tid = s(c.get("thomas_id"))
            ctype = s(c.get("type"))
            add(tid, None, tid, s(c.get("name")), ctype, False, is_current, c)
            for sub in c.get("subcommittees", []):
                sid = s(sub.get("thomas_id"))
                full = tid + sid
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


def build_committee_membership():
    rows = []
    for cid, members in load("committee-membership-current").items():
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


def build_district_office():
    rows = []
    for r in load("legislators-district-offices"):
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


def build_executive_term():
    rows = []
    for r in load("executive"):
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


def write_table(slug, df):
    out = OUTPUT / slug
    out.mkdir(parents=True, exist_ok=True)
    for p in out.glob("*.parquet"):
        p.unlink()
    df = df.astype(object).where(pd.notna(df), None)
    schema = pa.schema([(c, pa.string()) for c in df.columns])
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, out / "data.parquet", compression="snappy")
    print(
        f"  {slug:22s} {len(df):>6d} rows  {len(df.columns):>2d} cols -> {out}/data.parquet"
    )


def main():
    targets = sys.argv[1:] or list(BUILDERS)
    print("Cleaning congress-legislators tables:")
    for slug in targets:
        if slug not in BUILDERS:
            print(f"  ! unknown table {slug}")
            continue
        write_table(slug, BUILDERS[slug]())


if __name__ == "__main__":
    main()
