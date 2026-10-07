"""Pure functions for world_openalex: read the OpenAlex snapshot and flatten it.

No Prefect imports here. The recurring flow (``flows.py``) and the one-shot
bootstrap (``models/world_openalex/code/load.py``) both call these.

The snapshot is nested Parquet (one row per entity, with lists of structs).
Each ``flatten_<entity>`` turns one record batch into a dict of relational
tables, typed as the architecture says. ``to_staging`` then orders the columns
by the architecture CSV and casts every one to STRING, the house convention
for staging; the dbt models ``safe_cast`` them back.
"""

import csv
import datetime
import json
import urllib.request
from collections.abc import Callable, Iterator
from functools import cache
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as _pc
import pyarrow.parquet as pq
from pyarrow import fs

from pipelines.datasets.world_openalex.constants import constants

# pyarrow.compute builds its functions at import time, so type checkers see none
# of them. Typing the module as Any once beats an ignore on every call.
pc: Any = _pc

DOI_PREFIX = "https://doi.org/"
# Greedy: everything up to the last slash of a URL. Turns
# https://openalex.org/W123 into W123 and https://openalex.org/subfields/1702
# into 1702. DOIs contain slashes, so they go through strip_doi instead.
_URL_HEAD = r"^https?://.*/"


# --------------------------------------------------------------------------
# Snapshot access
# --------------------------------------------------------------------------


def fetch_manifest(url: str = constants.MANIFEST_URL.value) -> dict:
    """Download the snapshot's Parquet manifest (about 0.7 MB, no credentials).

    The manifest is written last, so its presence means the release is complete.
    """
    with urllib.request.urlopen(url, timeout=120) as resp:
        return json.load(resp)


def release_date(manifest: dict) -> str:
    """Return the release date of a manifest, as ``YYYY-MM-DD``."""
    return manifest["date"]


def entity_files(manifest: dict, entity: str) -> list[tuple[str, int]]:
    """List ``(s3 path without scheme, record count)`` for one entity."""
    for e in manifest["entities"]:
        if e["entity"] == entity:
            return [
                (f["url"].removeprefix("s3://"), f["meta"]["record_count"])
                for f in e["files"]
            ]
    raise KeyError(f"entity {entity!r} not in manifest")


def entity_record_count(manifest: dict, entity: str) -> int:
    """Total records the manifest declares for one entity."""
    return next(
        e["record_count"]
        for e in manifest["entities"]
        if e["entity"] == entity
    )


@cache
def s3_filesystem() -> fs.S3FileSystem:
    """Anonymous S3 filesystem for the public OpenAlex bucket."""
    return fs.S3FileSystem(anonymous=True, region=constants.S3_REGION.value)


def iter_batches(
    path: str,
    columns: list[str] | None = None,
    batch_size: int = constants.BATCH_SIZE.value,
    filesystem: fs.FileSystem | None = None,
) -> Iterator[pa.Table]:
    """Stream one snapshot file as Arrow tables of ``batch_size`` rows.

    Reads only the requested columns, so memory follows the batch, not the file.
    ``filesystem`` defaults to the anonymous S3 bucket; tests pass a local one.
    """
    with (filesystem or s3_filesystem()).open_input_file(path) as fh:
        pf = pq.ParquetFile(fh)
        for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
            yield pa.Table.from_batches([batch])


# --------------------------------------------------------------------------
# Architecture
# --------------------------------------------------------------------------


@cache
def architecture(table: str) -> list[tuple[str, str]]:
    """Return ``[(column, bigquery_type), ...]`` in architecture order."""
    path = Path(constants.ARCHITECTURE_DIR.value) / f"{table}.csv"
    with path.open(encoding="utf-8") as fh:
        return [(r["name"], r["bigquery_type"]) for r in csv.DictReader(fh)]


def to_staging(table: str, t: pa.Table) -> pa.Table:
    """Order columns by the architecture and cast every one to STRING.

    Casting goes through Arrow so NULL stays NULL (``astype(str)`` would write
    the literal ``"nan"``) and integers keep no decimal point. Timestamps are
    reduced to dates first, as the architecture types them DATE.

    Raises:
        ValueError: if the flattened table lacks a column the architecture
            declares, or carries one it does not.
    """
    names = [n for n, _ in architecture(table)]
    missing = set(names) - set(t.column_names)
    extra = set(t.column_names) - set(names)
    if missing or extra:
        raise ValueError(
            f"{table}: missing {sorted(missing)}, unexpected {sorted(extra)}"
        )
    cols = []
    for name in names:
        c = t[name]
        if pa.types.is_timestamp(c.type):
            c = pc.cast(c, pa.date32())
        cols.append(pc.cast(c, pa.string()))
    return pa.table(cols, names=names)


# --------------------------------------------------------------------------
# Arrow helpers
# --------------------------------------------------------------------------


def short_id(a: pa.Array) -> pa.Array:
    """Strip the URL head from an id: https://openalex.org/W123 -> W123."""
    return pc.replace_substring_regex(a, _URL_HEAD, "")


def strip_doi(a: pa.Array) -> pa.Array:
    """Strip the https://doi.org/ prefix from a DOI."""
    return pc.replace_substring(a, DOI_PREFIX, "", max_replacements=1)


def field(a: pa.Array, path: str) -> pa.Array:
    """Return a nested struct field by dotted path (nulls propagate)."""
    for p in path.split("."):
        a = pc.struct_field(a, p)
    return a


def col(t: pa.Table, name: str) -> pa.Array:
    """Return a table column as one contiguous array."""
    return t[name].combine_chunks()


def explode(
    lst: pa.Array,
) -> tuple[pa.Array, pa.Array, pa.Array]:
    """Flatten a list array.

    Returns:
        ``(values, parent_index, position)`` — the flattened values, the row of
        ``lst`` each value came from, and its 1-based position in that row's list.
    """
    values = pc.list_flatten(lst)
    parent = pc.list_parent_indices(lst)
    if len(values) == 0:
        empty = pa.array([], pa.int64())
        return values, empty, empty
    offsets = lst.offsets.to_numpy()
    par = parent.to_numpy()
    pos = np.arange(len(values)) + offsets[0] - offsets[par] + 1
    return values, parent, pa.array(pos, pa.int64())


def join_list(lst: pa.Array, short: bool = False) -> pa.Array:
    """Join a list<string> into ``a;b;c``; empty or null lists become NULL."""
    values = lst.values
    if short:
        values = short_id(values)
    rebuilt = pa.ListArray.from_arrays(lst.offsets, values, mask=lst.is_null())
    joined = pc.binary_join(rebuilt, ";")
    return pc.if_else(
        pc.equal(joined, ""), pa.scalar(None, pa.string()), joined
    )


def take(a: pa.Array, idx: pa.Array) -> pa.Array:
    """Broadcast a parent column onto exploded child rows."""
    return pc.take(a, idx)


def table(**cols: pa.Array) -> pa.Table:
    """Build a table from keyword arrays, preserving argument order."""
    return pa.table(list(cols.values()), names=list(cols))


def rebuild_abstract(inverted_index: str | None) -> str | None:
    """Rebuild abstract text from OpenAlex's JSON inverted index.

    The index maps each word to the positions it occupies; ordering the words by
    position restores the text. Returns None for a missing or empty index.
    """
    if not inverted_index:
        return None
    idx = json.loads(inverted_index)
    if not idx:
        return None
    words = {p: w for w, positions in idx.items() for p in positions}
    return " ".join(words[k] for k in sorted(words)) or None


# --------------------------------------------------------------------------
# Flatteners: one record batch of an entity -> {table: typed pa.Table}
# --------------------------------------------------------------------------


def clean_publication_year(year: pa.Array) -> pa.Array:
    """Null publication years later than next year.

    A few hundred works carry impossible years (up to 2050). Left in place they
    land in far-future partitions and stretch the table's coverage. The raw
    value survives in ``work.publication_date``.
    """
    cap = datetime.date.today().year + 1
    return pc.if_else(pc.greater(year, cap), pa.scalar(None, year.type), year)


def flatten_works(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of works into the 16 work tables."""
    out: dict[str, pa.Table] = {}
    wid = short_id(col(t, "id"))
    year = clean_publication_year(col(t, "publication_year"))
    ids = col(t, "ids")
    pl = col(t, "primary_location")
    oa = col(t, "open_access")
    bib = col(t, "biblio")
    cnp = col(t, "citation_normalized_percentile")

    def ident(key: str) -> pa.Array:
        return short_id(pc.map_lookup(ids, pa.scalar(key), "first"))

    out["work"] = table(
        publication_year=year,
        work_id=wid,
        doi=strip_doi(col(t, "doi")),
        pmid=ident("pmid"),
        pmcid=ident("pmcid"),
        mag_id=ident("mag"),
        title=col(t, "title"),
        publication_date=col(t, "publication_date"),
        type=col(t, "type"),
        language=col(t, "language"),
        primary_source_id=short_id(field(pl, "source.id")),
        primary_topic_id=short_id(field(col(t, "primary_topic"), "id")),
        is_open_access=field(oa, "is_oa"),
        open_access_status=field(oa, "oa_status"),
        open_access_url=field(oa, "oa_url"),
        any_repository_has_fulltext=field(oa, "any_repository_has_fulltext"),
        is_paratext=col(t, "is_paratext"),
        is_retracted=col(t, "is_retracted"),
        is_xpac=col(t, "is_xpac"),
        volume=field(bib, "volume"),
        issue=field(bib, "issue"),
        first_page=field(bib, "first_page"),
        last_page=field(bib, "last_page"),
        authors_count=col(t, "authors_count"),
        institutions_distinct_count=col(t, "institutions_distinct_count"),
        countries_distinct_count=col(t, "countries_distinct_count"),
        locations_count=col(t, "locations_count"),
        referenced_works_count=col(t, "referenced_works_count"),
        cited_by_count=col(t, "cited_by_count"),
        fwci=col(t, "fwci"),
        citation_normalized_percentile=field(cnp, "value"),
        is_in_top_1_percent=field(cnp, "is_in_top_1_percent"),
        is_in_top_10_percent=field(cnp, "is_in_top_10_percent"),
        apc_list_usd=field(col(t, "apc_list"), "value_usd"),
        apc_paid_usd=field(col(t, "apc_paid"), "value_usd"),
        has_fulltext=col(t, "has_fulltext"),
        has_pdf=field(col(t, "has_content"), "pdf"),
        created_date=col(t, "created_date"),
        updated_date=col(t, "updated_date"),
    )

    # Abstracts: rebuilt in Python, one JSON parse per work.
    abstracts = pa.array(
        [
            rebuild_abstract(s)
            for s in col(t, "abstract_inverted_index").to_pylist()
        ],
        pa.string(),
    )
    keep = pc.is_valid(abstracts)
    out["work_abstract"] = table(
        publication_year=year.filter(keep),
        work_id=wid.filter(keep),
        abstract=abstracts.filter(keep),
    )

    # Authorships and their nested lists.
    au, au_par, au_pos = explode(col(t, "authorships"))
    au_year, au_wid = take(year, au_par), take(wid, au_par)
    out["work_authorship"] = table(
        publication_year=au_year,
        work_id=au_wid,
        author_sequence=au_pos,
        author_id=short_id(field(au, "author.id")),
        author_position=field(au, "author_position"),
        author_name=field(au, "author.display_name"),
        orcid=short_id(field(au, "author.orcid")),
        raw_author_name=field(au, "raw_author_name"),
        raw_orcid=field(au, "raw_orcid"),
        is_corresponding=field(au, "is_corresponding"),
    )
    if len(au):
        inst, inst_par, _ = explode(field(au, "institutions"))
        out["work_authorship_institution"] = table(
            publication_year=take(au_year, inst_par),
            work_id=take(au_wid, inst_par),
            author_sequence=take(au_pos, inst_par),
            institution_id=short_id(field(inst, "id")),
        )
        aff, aff_par, aff_pos = explode(field(au, "affiliations"))
        out["work_authorship_affiliation"] = table(
            publication_year=take(au_year, aff_par),
            work_id=take(au_wid, aff_par),
            author_sequence=take(au_pos, aff_par),
            affiliation_sequence=aff_pos,
            raw_affiliation_string=field(aff, "raw_affiliation_string"),
            institution_ids=join_list(
                field(aff, "institution_ids"), short=True
            ),
        )
        ctry, ctry_par, _ = explode(field(au, "countries"))
        out["work_authorship_country"] = table(
            publication_year=take(au_year, ctry_par),
            work_id=take(au_wid, ctry_par),
            author_sequence=take(au_pos, ctry_par),
            country_code=ctry,
        )

    loc, loc_par, loc_pos = explode(col(t, "locations"))
    loc_id = field(loc, "id")
    primary_id = take(field(pl, "id"), loc_par)
    best_id = take(field(col(t, "best_oa_location"), "id"), loc_par)
    out["work_location"] = table(
        publication_year=take(year, loc_par),
        work_id=take(wid, loc_par),
        location_sequence=loc_pos,
        location_id=loc_id,
        source_id=short_id(field(loc, "source.id")),
        is_primary=pc.fill_null(pc.equal(loc_id, primary_id), False),
        is_best_open_access=pc.fill_null(pc.equal(loc_id, best_id), False),
        is_open_access=field(loc, "is_oa"),
        is_published=field(loc, "is_published"),
        is_accepted=field(loc, "is_accepted"),
        version=field(loc, "version"),
        license=field(loc, "license"),
        landing_page_url=field(loc, "landing_page_url"),
        pdf_url=field(loc, "pdf_url"),
        raw_source_name=field(loc, "raw_source_name"),
        raw_type=field(loc, "raw_type"),
        provenance=field(loc, "provenance"),
    )

    # The snapshot lists topics by descending score, primary first, so list
    # position is the rank.
    tp, tp_par, tp_pos = explode(col(t, "topics"))
    out["work_topic"] = table(
        publication_year=take(year, tp_par),
        work_id=take(wid, tp_par),
        topic_id=short_id(field(tp, "id")),
        topic_rank=tp_pos,
        score=field(tp, "score"),
    )

    kw, kw_par, _ = explode(col(t, "keywords"))
    out["work_keyword"] = table(
        publication_year=take(year, kw_par),
        work_id=take(wid, kw_par),
        keyword_id=short_id(field(kw, "id")),
        score=field(kw, "score"),
    )

    sdg, sdg_par, _ = explode(col(t, "sustainable_development_goals"))
    out["work_sdg"] = table(
        publication_year=take(year, sdg_par),
        work_id=take(wid, sdg_par),
        sdg_id=short_id(field(sdg, "id")),
        score=field(sdg, "score"),
    )

    me, me_par, _ = explode(col(t, "mesh"))
    out["work_mesh"] = table(
        publication_year=take(year, me_par),
        work_id=take(wid, me_par),
        descriptor_id=field(me, "descriptor_ui"),
        descriptor_name=field(me, "descriptor_name"),
        qualifier_id=field(me, "qualifier_ui"),
        qualifier_name=field(me, "qualifier_name"),
        is_major_topic=field(me, "is_major_topic"),
    )

    aw, aw_par, _ = explode(col(t, "awards"))
    out["work_award"] = table(
        publication_year=take(year, aw_par),
        work_id=take(wid, aw_par),
        award_id=short_id(field(aw, "id")),
        funder_id=short_id(field(aw, "funder_id")),
        funder_award_id=field(aw, "funder_award_id"),
    )

    fu, fu_par, _ = explode(col(t, "funders"))
    out["work_funder"] = table(
        publication_year=take(year, fu_par),
        work_id=take(wid, fu_par),
        funder_id=short_id(field(fu, "id")),
    )

    rf, rf_par, _ = explode(col(t, "referenced_works"))
    out["work_reference"] = table(
        publication_year=take(year, rf_par),
        work_id=take(wid, rf_par),
        referenced_work_id=short_id(rf),
    )

    cy, cy_par, _ = explode(col(t, "counts_by_year"))
    out["work_counts_by_year"] = table(
        publication_year=take(year, cy_par),
        work_id=take(wid, cy_par),
        year=field(cy, "year"),
        cited_by_count=field(cy, "cited_by_count"),
    )

    ix, ix_par, _ = explode(col(t, "indexed_in"))
    out["work_indexed_in"] = table(
        publication_year=take(year, ix_par),
        work_id=take(wid, ix_par),
        index_name=ix,
    )
    return out


def _summary(t: pa.Table) -> dict[str, pa.Array]:
    ss = col(t, "summary_stats")
    return {
        "two_year_mean_citedness": field(ss, "2yr_mean_citedness"),
        "h_index": field(ss, "h_index"),
        "i10_index": field(ss, "i10_index"),
    }


def flatten_authors(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of authors into the 6 author tables."""
    aid = short_id(col(t, "id"))
    out = {
        "author": table(
            author_id=aid,
            display_name=col(t, "display_name"),
            full_name=col(t, "full_name"),
            orcid=short_id(col(t, "orcid")),
            works_count=col(t, "works_count"),
            cited_by_count=col(t, "cited_by_count"),
            **_summary(t),
            created_date=col(t, "created_date"),
            updated_date=col(t, "updated_date"),
        )
    }
    alt, alt_par, _ = explode(col(t, "display_name_alternatives"))
    out["author_alternative_name"] = table(
        author_id=take(aid, alt_par), alternative_name=alt
    )
    aff, aff_par, _ = explode(col(t, "affiliations"))
    yrs, yrs_par, _ = explode(field(aff, "years"))
    out["author_affiliation"] = table(
        year=yrs,
        author_id=take(take(aid, aff_par), yrs_par),
        institution_id=take(short_id(field(aff, "institution.id")), yrs_par),
    )
    lk, lk_par, _ = explode(col(t, "last_known_institutions"))
    out["author_last_known_institution"] = table(
        author_id=take(aid, lk_par), institution_id=short_id(field(lk, "id"))
    )
    out["author_topic"] = _author_topics(t, aid)
    cy, cy_par, _ = explode(col(t, "counts_by_year"))
    out["author_counts_by_year"] = table(
        year=field(cy, "year"),
        author_id=take(aid, cy_par),
        works_count=field(cy, "works_count"),
        oa_works_count=field(cy, "oa_works_count"),
        cited_by_count=field(cy, "cited_by_count"),
    )
    return out


def _author_topics(t: pa.Table, aid: pa.Array) -> pa.Table:
    """Join each author's topic counts with its topic shares, by topic id."""
    tp, tp_par, _ = explode(col(t, "topics"))
    counts = table(
        author_id=take(aid, tp_par),
        topic_id=short_id(field(tp, "id")),
        works_count=field(tp, "count"),
    )
    sh, sh_par, _ = explode(col(t, "topic_share"))
    shares = table(
        author_id=take(aid, sh_par),
        topic_id=short_id(field(sh, "id")),
        topic_share=field(sh, "value"),
    )
    joined = counts.join(
        shares, ["author_id", "topic_id"], join_type="full outer"
    )
    return joined.select(
        ["author_id", "topic_id", "works_count", "topic_share"]
    )


def flatten_awards(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of awards into the 4 award tables."""
    gid = short_id(col(t, "id"))
    out = {
        "award": table(
            award_id=gid,
            display_name=col(t, "display_name"),
            description=col(t, "description"),
            funder_id=short_id(field(col(t, "funder"), "id")),
            funder_award_id=col(t, "funder_award_id"),
            funding_type=col(t, "funding_type"),
            funder_scheme=col(t, "funder_scheme"),
            amount=col(t, "amount"),
            currency=col(t, "currency"),
            start_date=col(t, "start_date"),
            end_date=col(t, "end_date"),
            start_year=col(t, "start_year"),
            end_year=col(t, "end_year"),
            primary_topic_id=short_id(field(col(t, "primary_topic"), "id")),
            funded_outputs_count=col(t, "funded_outputs_count"),
            doi=strip_doi(col(t, "doi")),
            landing_page_url=col(t, "landing_page_url"),
            provenance=col(t, "provenance"),
            created_date=col(t, "created_date"),
            updated_date=col(t, "updated_date"),
        )
    }

    def people(arr: pa.Array, par: pa.Array, role: str) -> pa.Table:
        return table(
            award_id=take(gid, par),
            role=pa.array([role] * len(arr), pa.string()),
            given_name=field(arr, "given_name"),
            family_name=field(arr, "family_name"),
            orcid=short_id(field(arr, "orcid")),
            role_start_date=field(arr, "role_start"),
            affiliation_name=field(arr, "affiliation.name"),
            country_code=field(arr, "affiliation.country"),
        )

    parts = []
    for name, role in (
        ("lead_investigator", "lead"),
        ("co_lead_investigator", "co_lead"),
    ):
        s = col(t, name)
        keep = pc.is_valid(s)
        idx = pc.indices_nonzero(keep)
        parts.append(people(s.filter(keep), idx, role))
    iv, iv_par, _ = explode(col(t, "investigators"))
    parts.append(people(iv, iv_par, "investigator"))
    inv = pa.concat_tables(parts).sort_by([("award_id", "ascending")])
    # Number investigators within each award, lead first.
    gids = inv["award_id"].to_numpy(zero_copy_only=False)
    seq = np.ones(len(gids), dtype=np.int64)
    for i in range(1, len(gids)):
        if gids[i] == gids[i - 1]:
            seq[i] = seq[i - 1] + 1
    out["award_investigator"] = inv.add_column(
        1, "investigator_sequence", pa.array(seq)
    )

    ia, ia_par, _ = explode(col(t, "institution_awarded"))
    out["award_institution"] = table(
        award_id=take(gid, ia_par), institution_id=short_id(field(ia, "id"))
    )
    tp, tp_par, _ = explode(col(t, "topics"))
    out["award_topic"] = table(
        award_id=take(gid, tp_par),
        topic_id=short_id(field(tp, "id")),
        score=field(tp, "score"),
    )
    return out


def flatten_institutions(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of institutions."""
    iid = short_id(col(t, "id"))
    geo = col(t, "geo")
    out = {
        "institution": table(
            institution_id=iid,
            ror_id=short_id(col(t, "ror")),
            display_name=col(t, "display_name"),
            country_code=col(t, "country_code"),
            type=col(t, "type"),
            city=field(geo, "city"),
            region=field(geo, "region"),
            geonames_city_id=field(geo, "geonames_city_id"),
            latitude=field(geo, "latitude"),
            longitude=field(geo, "longitude"),
            is_super_system=col(t, "is_super_system"),
            status=col(t, "status"),
            homepage_url=col(t, "homepage_url"),
            wikidata_id=short_id(field(col(t, "ids"), "wikidata")),
            works_count=col(t, "works_count"),
            cited_by_count=col(t, "cited_by_count"),
            **_summary(t),
            created_date=col(t, "created_date"),
            updated_date=col(t, "updated_date"),
        )
    }
    asc, asc_par, _ = explode(col(t, "associated_institutions"))
    out["institution_association"] = table(
        institution_id=take(iid, asc_par),
        associated_institution_id=short_id(field(asc, "id")),
        relationship=field(asc, "relationship"),
    )
    return out


def flatten_sources(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of sources."""
    sid = short_id(col(t, "id"))
    out = {
        "source": table(
            source_id=sid,
            issn_l=col(t, "issn_l"),
            display_name=col(t, "display_name"),
            type=col(t, "type"),
            host_organization_id=short_id(col(t, "host_organization")),
            country_code=col(t, "country_code"),
            homepage_url=col(t, "homepage_url"),
            is_open_access=col(t, "is_oa"),
            is_in_doaj=col(t, "is_in_doaj"),
            is_in_doaj_since_year=col(t, "is_in_doaj_since_year"),
            is_in_scielo=col(t, "is_in_scielo"),
            is_ojs=col(t, "is_ojs"),
            is_core=col(t, "is_core"),
            is_preprint_repository=col(t, "is_preprint_repository"),
            oa_flip_year=col(t, "oa_flip_year"),
            first_publication_year=col(t, "first_publication_year"),
            last_publication_year=col(t, "last_publication_year"),
            apc_usd=col(t, "apc_usd"),
            works_count=col(t, "works_count"),
            oa_works_count=col(t, "oa_works_count"),
            cited_by_count=col(t, "cited_by_count"),
            **_summary(t),
            created_date=col(t, "created_date"),
            updated_date=col(t, "updated_date"),
        )
    }
    iss, iss_par, _ = explode(col(t, "issn"))
    out["source_issn"] = table(source_id=take(sid, iss_par), issn=iss)
    return out


def flatten_publishers(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of publishers."""
    return {
        "publisher": table(
            publisher_id=short_id(col(t, "id")),
            display_name=col(t, "display_name"),
            hierarchy_level=col(t, "hierarchy_level"),
            parent_publisher_id=short_id(
                field(col(t, "parent_publisher"), "id")
            ),
            country_codes=join_list(col(t, "country_codes")),
            ror_id=short_id(col(t, "ror_id")),
            wikidata_id=short_id(col(t, "wikidata_id")),
            homepage_url=col(t, "homepage_url"),
            works_count=col(t, "works_count"),
            cited_by_count=col(t, "cited_by_count"),
            **_summary(t),
            created_date=col(t, "created_date"),
            updated_date=col(t, "updated_date"),
        )
    }


def flatten_funders(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of funders."""
    ids = col(t, "ids")
    return {
        "funder": table(
            funder_id=short_id(col(t, "id")),
            display_name=col(t, "display_name"),
            country_code=col(t, "country_code"),
            description=col(t, "description"),
            ror_id=short_id(field(ids, "ror")),
            wikidata_id=short_id(field(ids, "wikidata")),
            crossref_funder_id=field(ids, "crossref"),
            homepage_url=col(t, "homepage_url"),
            works_count=col(t, "works_count"),
            cited_by_count=col(t, "cited_by_count"),
            awards_count=col(t, "awards_count"),
            **_summary(t),
            created_date=col(t, "created_date"),
            updated_date=col(t, "updated_date"),
        )
    }


def _taxonomy(
    t: pa.Table, id_name: str, parents: list[str]
) -> dict[str, pa.Array]:
    cols = {
        id_name: short_id(col(t, "id")),
        "display_name": col(t, "display_name"),
        "description": col(t, "description"),
    }
    for p in parents:
        cols[f"{p}_id"] = short_id(field(col(t, p), "id"))
    return cols


def _counts_dates(t: pa.Table) -> dict[str, pa.Array]:
    return {
        "works_count": col(t, "works_count"),
        "cited_by_count": col(t, "cited_by_count"),
        "created_date": col(t, "created_date"),
        "updated_date": col(t, "updated_date"),
    }


def flatten_topics(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of topics."""
    return {
        "topic": table(
            **_taxonomy(t, "topic_id", ["subfield", "field", "domain"]),
            keywords=join_list(col(t, "keywords")),
            wikipedia_url=field(col(t, "ids"), "wikipedia"),
            **_counts_dates(t),
        )
    }


def flatten_subfields(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of subfields."""
    return {
        "subfield": table(
            **_taxonomy(t, "subfield_id", ["field", "domain"]),
            **_counts_dates(t),
        )
    }


def flatten_fields(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of fields."""
    return {
        "field": table(
            **_taxonomy(t, "field_id", ["domain"]), **_counts_dates(t)
        )
    }


def flatten_domains(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of domains."""
    return {
        "domain": table(**_taxonomy(t, "domain_id", []), **_counts_dates(t))
    }


def flatten_keywords(t: pa.Table) -> dict[str, pa.Table]:
    """Flatten a batch of keywords."""
    return {
        "keyword": table(
            keyword_id=short_id(col(t, "id")),
            display_name=col(t, "display_name"),
            **_counts_dates(t),
        )
    }


FLATTENERS: dict[str, Callable[[pa.Table], dict[str, pa.Table]]] = {
    "works": flatten_works,
    "authors": flatten_authors,
    "awards": flatten_awards,
    "institutions": flatten_institutions,
    "sources": flatten_sources,
    "publishers": flatten_publishers,
    "funders": flatten_funders,
    "topics": flatten_topics,
    "subfields": flatten_subfields,
    "fields": flatten_fields,
    "domains": flatten_domains,
    "keywords": flatten_keywords,
}


def build_dicionario(manifest: dict) -> pa.Table:
    """Build the dicionario from the language, license and SDG lookup entities."""
    rows: dict[str, list[str | None]] = {
        "id_tabela": [],
        "nome_coluna": [],
        "chave": [],
        "cobertura_temporal": [],
        "valor": [],
    }
    for entity, targets in constants.DICTIONARY_ENTITIES.value.items():
        keys: dict[str, str] = {}
        for path, _ in entity_files(manifest, entity):
            for t in iter_batches(path, columns=["id", "display_name"]):
                for k, v in zip(
                    short_id(col(t, "id")).to_pylist(),
                    col(t, "display_name").to_pylist(),
                    strict=True,
                ):
                    keys[k] = v
        for table_id, column in targets:
            for k in sorted(keys, key=lambda x: (len(x), x)):
                rows["id_tabela"].append(table_id)
                rows["nome_coluna"].append(column)
                rows["chave"].append(k)
                rows["cobertura_temporal"].append(None)
                rows["valor"].append(keys[k])
    return pa.table(rows)


# --------------------------------------------------------------------------
# File-level processing
# --------------------------------------------------------------------------


def process_file(
    entity: str,
    path: str,
    out_dir: Path,
    file_tag: str,
    filesystem: fs.FileSystem | None = None,
) -> dict[str, tuple[Path, int]]:
    """Stream one snapshot file, flatten it, write one staging file per table.

    Args:
        entity: Snapshot entity (``works``, ``authors``, ...).
        path: S3 path of the file, without scheme.
        out_dir: Local directory; files land in ``out_dir/<table>/<file_tag>.parquet``.
        file_tag: Unique name for this source file within the entity.
        filesystem: Where ``path`` lives; defaults to the OpenAlex S3 bucket.

    Returns:
        ``{table: (local file, rows written)}`` for tables with at least one row.
    """
    writers: dict[str, pq.ParquetWriter] = {}
    rows: dict[str, int] = {}
    paths: dict[str, Path] = {}
    try:
        for batch in iter_batches(path, filesystem=filesystem):
            for tbl, data in FLATTENERS[entity](batch).items():
                if data.num_rows == 0:
                    continue
                staged = to_staging(tbl, data)
                if tbl not in writers:
                    paths[tbl] = out_dir / tbl / f"{file_tag}.parquet"
                    paths[tbl].parent.mkdir(parents=True, exist_ok=True)
                    writers[tbl] = pq.ParquetWriter(
                        paths[tbl], staged.schema, compression="snappy"
                    )
                writers[tbl].write_table(staged)
                rows[tbl] = rows.get(tbl, 0) + staged.num_rows
    finally:
        for w in writers.values():
            w.close()
    return {tbl: (paths[tbl], rows[tbl]) for tbl in writers}


def file_tag(path: str) -> str:
    """Name a source file uniquely: ``updated_date=2026-09-03/part_0006`` -> ``2026-09-03_part_0006``."""
    parent, name = path.split("/")[-2:]
    return f"{parent.removeprefix('updated_date=')}_{name.removesuffix('.parquet')}"
