#!/usr/bin/env python3
"""Shared wide->long reshape engine for GERDA vote-result files.

Design (approved plan):
  * Long, geography-first, all years/parties stacked. English columns.
  * Melt individual party / genuine-residual columns to rows (party, vote_share
    [+ votes/seats/... where the source provides them]).
  * DROP cross-cutting aggregate columns (cdu_csu, far_right, far_left,
    far_left_w_linke, total_vote_share) so the per-unit sum of vote_share stays
    ~1; their party->group membership is recorded in the party directory.
  * MELT-THEN-DROP-NA: a source-NA party cell yields NO row (never a fabricated
    0/NULL). A source 0 is preserved as a 0 row (0 != NA).
  * Non-party bookkeeping is dropped: mail-in allocation inputs, covariates,
    crosswalk weights, numeric vote-total diagnostics, the vestigial ags_21
    column, and the non-voter column (nichtwahler / nichtwaehler).

Output: one all-STRING snappy Parquet per table (dbt safe_casts to final types).
"""

import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# --- Non-party columns (union across modules). Party = anything NOT here and
#     NOT in DROP_AGG. Kept vs dropped is decided per-table by the RENAME map;
#     this set only governs party detection. ---
NON_PARTY = {
    # identifiers / geography / time / ballot
    "ags",
    "ags_name",
    "ags_21",
    "ags_25",
    "ags_name_21",
    "ags_name_25",
    "county",
    "county_code",
    "county_code_21",
    "county_name",
    "county_name_21",
    "county_type",
    "state",
    "state_name",
    "state_abbr",
    "year",
    "election_year",
    "election_date",
    "wkr_nr",
    "wkr_name",
    "stimme",
    "boundary_change",
    "elected_party",
    "government_party",
    "person_id",
    "election_type",
    # turnout / counts
    "eligible_voters",
    "eligible_voters_orig",
    "number_voters",
    "number_voters_orig",
    "valid_votes",
    "invalid_votes",
    "turnout",
    "turnout_wo_mailin",
    # non-voters (not a party; disjoint from vote shares)
    "nichtwahler",
    "nichtwaehler",
    # mail-in bookkeeping
    "voters_wo_blockingnotice",
    "voters_blockingnotice",
    "voters_par25_2",
    "blocked_voters_orig",
    "voters_w_ballot",
    "unique_mailin",
    "unique_multi_mailin",
    "blocked_weight",
    "voters_weight",
    "pop_weight",
    "area_weight",
    "voters_wo_sperrvermerk",
    "voters_w_sperrvermerk",
    "voters_par24_2",
    "voters_w_wahlschein",
    # covariates / crosswalk weights
    "pop",
    "area",
    "population",
    "employees",
    "pop_density",
    "area_ags",
    "population_ags",
    "employees_ags",
    "pop_density_ags",
    "area_cw",
    "pop_cw",
    "emp_cw",
    "weights",
    "n_predecessors",
    # numeric diagnostics (GERDA ships both the misspelled "incogruence" and the
    # correctly spelled "incongruence" depending on the module)
    "total_votes",
    "total_votes_incogruence",
    "perc_total_votes_incogruence",
    "total_votes_incongruence",
    "perc_total_votes_incongruence",
    # flags (kept where a table lists them in `flags`, else dropped)
    "flag_naive_turnout_above_1",
    "flag_harm_turnout_above_1",
    "flag_turnout_above_1",
    "flag_unsuccessful_naive_merge",
    "flag_aggregated",
    "flag_total_votes_incongruent",
    "flag_briefwahl_agg",
    "flag_briefwahl_only",
    "flag_no_valid_votes",
    "flag_other_party_residual",
    # already-long payload
    "party",
    "votes",
    "vote_share",
    "seats",
    # county-council seats panel non-party columns
    "seats_total",
    "seats_regional",
    "seats_other",
    "seats_local_other",
    "flag_seats_total_incongruent",
    "comment",
    "source",
    "last_checked",
}

DROP_AGG = {
    "cdu_csu",
    "far_right",
    "far_left",
    "far_left_w_linke",
    "far_left_wlinke",
    "total_vote_share",
}

# Column prefixes that are never party keys: interpretive flags, per-party seat
# counts, per-party zero->NA recode flags, and bookkeeping counts.
NON_PARTY_PREFIXES = ("flag_", "seats_", "replaced_0_with_na", "n_")

BALLOT = {"erststimme": "first_vote", "zweitstimme": "second_vote"}


def party_columns(cols):
    cols = list(cols)
    drop = set(DROP_AGG)
    # cdu_csu is a derived aggregate only where cdu and csu are separate columns
    # (federal, state). In municipal / county-council it is the base party and
    # must be kept.
    if "cdu" not in cols and "csu" not in cols:
        drop.discard("cdu_csu")
    return [
        c
        for c in cols
        if c not in NON_PARTY
        and c not in drop
        and not c.startswith(NON_PARTY_PREFIXES)
    ]


def norm_code(s, width):
    """Strip spurious leading zeros so a code longer than `width` (e.g. a 9-digit
    AGS or 3-digit state code, a padding artifact GERDA emits for states 10-16 in
    some harmonized files) reduces to `width` digits. Codes already <= width are
    unchanged, so a genuine leading zero (Schleswig-Holstein 01..., Hamburg 02...)
    is preserved. Non-string values (NaN) pass through."""

    def fix(v):
        if not isinstance(v, str):
            return v
        while len(v) > width and v.startswith("0"):
            v = v[1:]
        return v

    return s.map(fix)


def norm_ags(s):
    """Normalize a municipality AGS to 8 digits. GERDA emits two distinct padding
    artifacts: a spurious LEADING zero (9-digit, states 10-16 in the harm_21
    federal file: '010041100' -> '10041100') and a spurious TRAILING block
    (11-digit AGS + '000', Hessen county elections: '06431001000' -> '06431001')."""

    def fix(v):
        if not isinstance(v, str):
            return v
        n = len(v)
        if n == 9 and v[0] == "0":
            return v[1:]
        if n >= 10:
            return v[:8]
        return v

    return s.map(fix)


def detect_encoding(path):
    """GERDA ships some modules (municipal, county) as cp1252 and others as
    UTF-8. Decide per file: UTF-8 if the whole file decodes cleanly, else cp1252."""
    with open(path, "rb") as fh:
        raw = fh.read()
    try:
        raw.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        # latin-1 (ISO-8859-1) maps all 256 bytes so it never raises, and decodes
        # German umlauts identically to cp1252; some GERDA files contain stray
        # bytes (0x81) undefined in cp1252.
        return "latin-1"


def read_csv_str(path, **kw):
    """read_csv as all-strings with the right encoding and empty/NA -> NaN."""
    return pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        na_values=["", "NA"],
        encoding=detect_encoding(path),
        **kw,
    )


def _read_chunks(path, chunksize):
    return read_csv_str(path, chunksize=chunksize)


def reshape_wide(
    path,
    derive,
    keep,
    flags,
    party_value="vote_share",
    const=None,
    chunksize=50000,
):
    """Melt a wide GERDA file to long.

    derive(chunk) -> DataFrame of canonical structural columns (same row index).
    keep          -> ordered list of canonical structural column names to output.
    flags         -> source flag columns to carry (already handled inside derive).
    party_value   -> name of the melted value column ("vote_share").
    const         -> dict of constant columns to add (e.g. {"year": "2021"}).
    Returns (long_df, n_nonempty_cells) for the validity check.
    """
    parts, n_cells = [], 0
    for chunk in _read_chunks(path, chunksize):
        pcols = party_columns(chunk.columns)
        struct = derive(chunk).reset_index(drop=True)
        if const:
            for k, v in const.items():
                struct[k] = v
        pv = chunk[pcols].reset_index(drop=True)
        n_cells += int(pv.notna().sum().sum())
        stacked = pv.stack(future_stack=True).dropna()  # noqa: PD013  keep 0s, drop NA
        long = stacked.reset_index()
        long.columns = ["_row", "party", party_value]
        long = long.join(struct, on="_row").drop(columns="_row")
        parts.append(long)
    out = pd.concat(parts, ignore_index=True)
    ordered = [*keep, "party", party_value]
    out = out[[c for c in ordered if c in out.columns]]
    out = out.drop_duplicates(
        ignore_index=True
    )  # source sometimes repeats a unit
    return out, n_cells


def passthrough_long(path, derive, keep, value_cols):
    """For files already in long form: derive structural cols, keep party +
    value_cols (e.g. ['votes','vote_share']). Drops non-party rows GERDA carries
    as `party` values in its long files (nichtwaehler, cross-cutting aggregates),
    to match the wide-file reshape and the party directory."""
    df = read_csv_str(path)
    df = df[~df["party"].isin(NON_PARTY | DROP_AGG)]
    # drop rows with no result at all (every value column NA); a party with a
    # result in at least one value column is kept
    has_val = pd.concat([df[v].notna() for v in value_cols], axis=1).any(
        axis=1
    )
    df = df[has_val].reset_index(drop=True)
    struct = derive(df).reset_index(drop=True)
    struct["party"] = df["party"].to_numpy()
    for v in value_cols:
        struct[v] = df[v].to_numpy()
    ordered = [*keep, "party", *value_cols]
    out = struct[[c for c in ordered if c in struct.columns]]
    out = out.drop_duplicates(ignore_index=True)
    return out, None


def write_parquet(df, out_dir, name):
    """Write df as a single all-STRING snappy Parquet: out_dir/<name>.parquet.
    turnout and vote_share are stored as percentages (0-100)."""
    os.makedirs(out_dir, exist_ok=True)
    for col in ("turnout", "vote_share"):
        if col in df.columns:
            df[col] = (pd.to_numeric(df[col], errors="coerce") * 100).astype(
                "string"
            )
    df = df.astype(object).where(df.notna(), None)
    schema = pa.schema([(c, pa.string()) for c in df.columns])
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    path = os.path.join(out_dir, f"{name}.parquet")
    pq.write_table(table, path, compression="snappy")
    return path
