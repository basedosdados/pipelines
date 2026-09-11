"""Pure transform for au_vic_vec_elections.

Every function here is Prefect-free so a later recurring pipeline can import the same
transform instead of duplicating it. The four ``parse_*`` modules each own one source
family; this module concatenates them, resolves the geography link, builds the data
dictionary and writes the partitioned parquet.
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa

from pipelines.datasets.au_vic_vec_elections import schema
from pipelines.datasets.au_vic_vec_elections.constants import constants

_ARROW_TYPES = {
    "STRING": "string",
    "INT64": "int64",
    "FLOAT64": "float64",
    "DATE": "date32",
    "DATETIME": "timestamp",
}

# No table may carry a null partition year: every row in this dataset belongs to a
# dated electoral event or a dated donation, so a null year is a defect, not a fact.
NULL_PARTITION_ALLOWED: set[str] = set()


def normalise_district(name: str) -> str:
    """Fold a district name to the crosswalk's comparison key."""
    # pyrefly: ignore [unnecessary-type-conversion]
    name = re.sub(r"\s*\(.*\)\s*$", "", str(name))
    name = name.replace("-", " ").replace("'", "")
    name = re.sub(r"[^a-z0-9 ]", "", name.lower())
    return re.sub(r"\s+", " ", name).strip()


def apply_sed_crosswalk(
    frame: pd.DataFrame, crosswalk: dict[str, str]
) -> pd.DataFrame:
    """Fill ``state_electoral_division_id`` for Legislative Assembly contests only.

    A district with no counterpart in the vintage is left null rather than forced onto
    a same-named division with different boundaries.
    """
    if "state_electoral_division_id" not in frame.columns:
        return frame
    frame = frame.copy()
    is_assembly = frame["chamber"] == "legislative_assembly"
    keys = frame["district_name"].map(normalise_district)
    frame["state_electoral_division_id"] = (
        keys.map(crosswalk).where(is_assembly, other=None).astype("object")
    )
    return frame


def _to_all_string_table(frame: pd.DataFrame, columns) -> pa.Table:
    """Build an all-STRING Arrow table with a stable column order.

    Staging is all-STRING by house convention and the dbt model ``safe_cast``s every
    column, so the staging schema carries order, not types. Two details are load-bearing:

    * cast through Arrow, never ``astype(str)`` — the latter renders NULL as the literal
      ``"nan"``, which ``safe_cast`` will not turn back into NULL;
    * pass the architecture's real type through first, so ``year`` serialises as
      ``"2022"`` and not ``"2022.0"``.
    """
    arrays = []
    names = []
    for column in columns:
        series = (
            frame[column.name]
            if column.name in frame
            else pd.Series([None] * len(frame))
        )
        kind = _ARROW_TYPES[column.bigquery_type]
        if kind == "int64":
            typed = pa.array(
                pd.to_numeric(series, errors="coerce").astype("Int64"),
                type=pa.int64(),
            )
        elif kind == "float64":
            typed = pa.array(
                pd.to_numeric(series, errors="coerce"), type=pa.float64()
            )
        elif kind == "date32":
            values = pd.to_datetime(series, errors="coerce")
            typed = pa.array(values.dt.date, type=pa.date32())
        elif kind == "timestamp":
            values = pd.to_datetime(series, errors="coerce")
            typed = pa.array(values, type=pa.timestamp("s"))
        else:
            # An explicit object-dtype mask, not ``where(..., other=None)``. A
            # non-object series cannot hold ``None``, so pandas coerces it back to
            # that dtype's own missing marker — ``NaN`` on a float column, ``pd.NA``
            # on ``Int64``, ``NaT`` on a datetime — and the ``str(v)`` below would
            # then write the literal ``"nan"``, ``"<NA>"`` or ``"NaT"``, none of
            # which ``safe_cast`` turns back into a NULL. Casting to ``object``
            # first lets the mask store a real ``None``. No such value ever reached
            # the data — zero rows across all eight tables in dev — so this is a
            # prospective fix, not a repair.
            values = series.astype(object)
            values[series.isna()] = None
            typed = pa.array(
                [None if v is None else str(v) for v in values],
                type=pa.string(),
            )
        arrays.append(typed.cast(pa.string()))
        names.append(column.name)
    return pa.Table.from_arrays(arrays, names=names)


def write_partitioned(
    frame: pd.DataFrame, table: str, output_dir: Path
) -> int:
    """Write one table as Snappy parquet, hive-partitioned by ``year``."""
    import pyarrow.parquet as pq

    columns = schema.TABLES[table]
    target = output_dir / table
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)

    if not schema.PARTITION_COLUMNS[table]:
        arrow = _to_all_string_table(frame, columns)
        pq.write_table(arrow, target / "data.parquet", compression="snappy")
        return arrow.num_rows

    if frame["year"].isna().any() and table not in NULL_PARTITION_ALLOWED:
        # pyrefly: ignore [unnecessary-type-conversion]
        bad = int(frame["year"].isna().sum())
        raise ValueError(
            f"{table}: {bad} rows have a null partition year; decide explicitly "
            "whether to drop or repair them"
        )

    written = 0
    body_columns = [c for c in columns if c.name != "year"]
    for year, chunk in frame.groupby(frame["year"], dropna=False):
        partition = target / f"year={int(year)}"
        partition.mkdir(parents=True, exist_ok=True)
        arrow = _to_all_string_table(
            chunk.drop(columns=["year"]), body_columns
        )
        if arrow.num_rows == 0:
            # An empty first partition makes dump_header infer INTEGER and poisons the
            # staging schema. Never emit one.
            continue
        pq.write_table(arrow, partition / "data.parquet", compression="snappy")
        written += arrow.num_rows
    return written


def concat_frames(parts: list[pd.DataFrame], table: str) -> pd.DataFrame:
    """Concatenate one table's contributions, enforcing the architecture's columns."""
    names = schema.column_names(table)
    usable = [p for p in parts if p is not None and len(p)]
    if not usable:
        return pd.DataFrame(columns=names)
    aligned = []
    for part in usable:
        missing = [c for c in names if c not in part.columns]
        if missing:
            raise ValueError(f"{table}: a source is missing columns {missing}")
        extra = [c for c in part.columns if c not in names]
        if extra:
            raise ValueError(
                f"{table}: a source has unexpected columns {extra}"
            )
        aligned.append(part[names])
    return pd.concat(aligned, ignore_index=True)


def load_crosswalk(path: Path) -> dict[str, str]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def build_dicionario(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Build the data dictionary from the cleaned tables.

    The dictionary is derived from the facts rather than from a hand-kept lookback
    window, so a value that appears in the data but has no declared label fails the
    build instead of shipping unexplained.
    """
    rows = []
    for table in constants.TABLES.value:
        if table == "dicionario" or table not in frames:
            continue
        frame = frames[table]
        for column in schema.TABLES[table]:
            if column.covered_by_dictionary != "yes":
                continue
            if column.name not in frame.columns:
                continue
            observed = sorted(
                {
                    str(v)
                    for v in frame[column.name].dropna().unique()
                    if str(v) != ""
                }
            )
            for key in observed:
                label = VOCABULARIES.get(column.name, {}).get(key)
                if label is None:
                    raise ValueError(
                        f"{table}.{column.name}: value {key!r} has no declared label "
                        "in VOCABULARIES"
                    )
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column.name,
                        "chave": key,
                        "cobertura_temporal": "",
                        "valor": label,
                    }
                )
    return pd.DataFrame(rows, columns=schema.column_names("dicionario"))


# Declared label sets for every column marked covered_by_dictionary. Labels are the
# Portuguese the dicionario publishes.
VOCABULARIES: dict[str, dict[str, str]] = {
    "chamber": {
        "legislative_assembly": "Assembleia Legislativa",
        "legislative_council": "Conselho Legislativo",
    },
    "government_level": {"state": "Estadual"},
    "contest_type": {
        "state_district": "Distrito estadual",
        "state_region": "Região do Conselho Legislativo",
        "state_province": "Província do Conselho Legislativo",
    },
    "voting_system": {
        "compulsory_preferential": "Voto preferencial obrigatório",
        "single_transferable_vote": "Voto único transferível",
    },
    "election_type": {
        "state_general": "Eleição geral estadual",
        "state_by_election": "Eleição suplementar estadual",
    },
    "is_elected": {"yes": "Sim", "no": "Não"},
    "count_type": {
        "first_preference": "Primeira preferência",
        "two_candidate_preferred": "Preferência entre duas candidaturas",
        "two_party_preferred": "Preferência entre dois partidos",
    },
    "vote_type": {
        "ordinary": "Voto ordinário, apurado no local de votação",
        "absent": "Voto de eleitor fora do distrito",
        "early": "Voto antecipado",
        "postal": "Voto postal",
        "provisional": "Voto provisório",
        "marked_as_voted": "Voto de eleitor já marcado como tendo votado",
        "declaration": (
            "Voto por declaração, sem classificação mais fina. Categoria usada apenas "
            "em 2006, quando a VEC não separava voto provisório de voto de eleitor já "
            "marcado como tendo votado"
        ),
    },
    "donation_type": {
        "money": "Dinheiro",
        "service": "Serviço",
        "property": "Bem",
        "loan": "Empréstimo",
    },
    "disclosure_status": {
        "reconciled": "Conciliada por ambas as partes",
        "donor_unreconciled": "Declarada apenas por quem doou",
        "recipient_unreconciled": "Declarada apenas por quem recebeu",
    },
}


def _source_url(election_id: str) -> str:
    """Canonical published location of an event's results.

    The blob container is the source for everything up to 2018; the 2022 general
    election and every by-election since are only complete on the CMS, because the
    two-candidate-preferred counts and the distributions of preferences for
    Legislative Assembly districts are not published as files at all.
    """
    blob = constants.BLOB_CONTAINER.value
    if election_id == "state2002":
        return f"{blob}/historical-results/general/"
    if election_id in {"state2006", "state2010", "state2014", "state2018"}:
        return f"{blob}/historical-results/{election_id}/"
    return "https://www.vec.vic.gov.au/results/state-election-results"


def build_election(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Assemble the election dimension from the events the facts actually reference.

    Built from the union of ``election_id`` across every fact table rather than from
    any single parser, so an event that only one source covers still gets a row and
    every fact keeps a resolvable foreign key.
    """
    meta = constants.ELECTION_META.value
    seen: set[str] = set()
    for table, frame in frames.items():
        if table == "dicionario" or "election_id" not in frame.columns:
            continue
        seen.update(frame["election_id"].dropna().unique())
    unknown = sorted(seen - set(meta))
    if unknown:
        raise ValueError(
            f"fact tables reference electoral events with no entry in "
            f"constants.ELECTION_META: {unknown}"
        )
    rows = []
    for election_id in sorted(seen, key=lambda k: meta[k][2]):
        name, election_type, election_date = meta[election_id]
        rows.append(
            {
                "year": int(election_date[:4]),
                "election_id": election_id,
                "election_name": name,
                "election_type": election_type,
                "government_level": "state",
                "election_date": election_date,
                "source_url": _source_url(election_id),
            }
        )
    return pd.DataFrame(rows, columns=schema.column_names("election"))


def check_referential_integrity(frames: dict[str, pd.DataFrame]) -> None:
    """Every fact must point at an event the election table publishes."""
    known = set(frames["election"]["election_id"])
    for table, frame in frames.items():
        if (
            table in {"election", "dicionario"}
            or "election_id" not in frame.columns
        ):
            continue
        orphan = sorted(set(frame["election_id"].dropna()) - known)
        if orphan:
            raise ValueError(
                f"{table}: election_id not in election table: {orphan}"
            )


def clean_all(input_dir: Path, output_dir: Path) -> dict[str, int]:
    """Full transform. Returns a row count per table."""
    from pipelines.datasets.au_vic_vec_elections import (
        parse_donations,
        parse_excel,
        parse_html,
        parse_website,
    )

    sources: dict[str, dict[str, pd.DataFrame]] = {
        "html": parse_html.parse_all(str(input_dir)),
        "excel": parse_excel.parse_all(str(input_dir)),
        "website": parse_website.parse_all(str(input_dir / "website")),
        "donations": parse_donations.parse_all(str(input_dir / "donations")),
    }

    # Keys held apart on purpose: an independently derived copy kept for comparison,
    # and the 2023 indicative distributions, which are statistical rather than legal
    # counts and are not published in the same table as the counts that elected anyone.
    held_apart = {
        "result_voting_centre_2018_xlsx",
        "distribution_of_preferences_indicative",
    }

    frames: dict[str, pd.DataFrame] = {}
    for table in constants.TABLES.value:
        if table in {"election", "dicionario"}:
            continue
        parts = [
            source[table]
            for source in sources.values()
            if table in source and table not in held_apart
        ]
        frames[table] = concat_frames(parts, table)

    crosswalk = load_crosswalk(
        input_dir.parent
        / "inventory"
        / f"sed_crosswalk_{constants.SED_VINTAGE.value}.json"
    )
    frames = {t: apply_sed_crosswalk(f, crosswalk) for t, f in frames.items()}

    frames["election"] = build_election(frames)
    check_referential_integrity(frames)
    frames["dicionario"] = build_dicionario(frames)

    output_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for table in constants.TABLES.value:
        counts[table] = write_partitioned(frames[table], table, output_dir)
        print(f"  wrote {table:32s} {counts[table]:>9,} rows", flush=True)
    return counts
