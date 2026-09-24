"""Rebuild `orgao` as `cod_orgao` for the MG municipalities TCE-MG withdrew from 2017/2018.

WHY THIS EXISTS
---------------
`clean_mg.py` maps `orgao` from `cod_orgao` (see the long comment there). TCE-MG has
retroactively withdrawn municipalities from exercises 2017 and 2018 -- the portal now
serves 711 of 853 municipalities for `empenho` 2017, 696 for `despesa` 2017, and 752
for each in 2018. Those municipalities therefore have NO source rows in this vintage,
and `cod_orgao` exists only in the source.

Their parquet files do exist in the staging bucket, from the original MiDES ingest,
but carry `orgao = seq_orgao`. Leaving them there would put two key schemes inside a
single exercise and give `orgao` two meanings in one column. Deleting them would drop
6,565,540 staging rows (17-18% of MG 2017, 12-13% of MG 2018).

This script takes the third path: reconstruct `cod_orgao` for those files from a
crosswalk built out of the SAME municipality's source data in adjacent exercises.

WHY THE CROSSWALK IS SOUND
--------------------------
`seq_orgao` is a global portal sequence (values run 40,41,42 ... 101,102,103 ...);
`cod_orgao` is a per-municipality ordinal (01, 02, 03). The two value spaces are
disjoint, so the direction of the mapping is unambiguous. Measured 2026-09-23:

  * 455 (category, municipality) pairs needed a map; all 455 built from donors
    2015/2016/2019/2020, with ZERO seq->cod conflicts between donor years.
  * All 1,002 affected files covered: 0 unmapped values, 0 NULL `orgao`.
  * Direction confirmed: 1,001 of 1,002 files have `orgao` values drawn only from
    the seq_orgao space and none from the cod_orgao space (the remaining file's
    values sit in both, and is mapped correctly anyway since the map is keyed on
    seq_orgao).
  * GROUND TRUTH: on 600 municipality-years that DO still have 2017/2018 source
    data, a crosswalk built only from 2015/2016/2019/2020 predicted the real
    2017/2018 `cod_orgao` for 1,475 of 1,475 orgao values -- 100%, 0 wrong,
    0 absent. That tests the assumption in the exact years being reconstructed.

Re-run `--validate` to reproduce that last check before trusting a re-run.

WHAT IT DOES NOT CHANGE
-----------------------
`id_empenho` for these municipalities stays the published vintage's `seq_empenho`.
That is correct: all four mirrors for a withdrawn municipality-exercise come from
that one vintage, so empenho -> liquidacao -> pagamento linkage stays internally
consistent. Only `orgao` is rewritten, so the key scheme matches the rest of MG.

Usage:
    python remap_mg_2017_2018_orgao.py --validate     # ground-truth check only
    python remap_mg_2017_2018_orgao.py --dry-run
    python remap_mg_2017_2018_orgao.py                # backup, remap, upload
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import random
import re
import sys
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.oauth2 import service_account

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]  # sibling module via sys.path
import clean_mg

BUCKET = "basedosdados-dev"
PREFIX = "staging/world_wb_mides"
BACKUP = "backup/world_wb_mides_mg_2017_2018_published_vintage_20260923"
CREDENTIALS = Path.home() / ".basedosdados/credentials/staging.json"
WORK = (
    Path(
        os.environ.get(
            "MIDES_DATA_DIR", Path.home() / "Downloads/world_wb_mides_data"
        )
    )
    / "remap_2017_2018"
)

MIRROR_CATEGORY = {
    "raw_empenho_mg": "empenho",
    "raw_rsp_mg": "empenho",
    "raw_liquidacao_mg": "despesa",
    "raw_pagamento_mg": "despesa",
}
# The phase whose CSV carries both seq_orgao and cod_orgao for each category.
CARRIER_PHASE = {"empenho": "empenho", "despesa": "liquidacao"}
DONOR_YEARS = (2016, 2019, 2015, 2020)
TARGET_YEARS = (2017, 2018)


def client_and_bucket():
    info = json.loads(CREDENTIALS.read_text())
    creds = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS)
    )
    client = storage.Client(credentials=creds, project=info["project_id"])
    return client, client.bucket(BUCKET, user_project=info["project_id"])


def source_pairs(
    year: int, category: str, ibge: str
) -> set[tuple[str, str]] | None:
    """Distinct (seq_orgao, cod_orgao) in one municipality-exercise, or None."""
    path = (
        clean_mg.MG_INPUT
        / str(year)
        / category
        / f"SICOM.{year}.{ibge}.{category}.zip"
    )
    if not path.is_file():
        return None
    phase = CARRIER_PHASE[category]
    try:
        with zipfile.ZipFile(path) as archive:
            hit = [
                m
                for m in archive.namelist()
                if m.endswith(f".{category}.{phase}.csv")
            ]
            if not hit:
                return None
            table, _ = clean_mg.read_source_csv(archive.read(hit[0]), hit[0])
            if (
                "seq_orgao" not in table.column_names
                or "cod_orgao" not in table.column_names
            ):
                return None
            return {
                (s, c)
                for s, c in zip(
                    table.column("seq_orgao").to_pylist(),
                    table.column("cod_orgao").to_pylist(),
                    # same table, so the two columns are the same length
                    strict=True,
                )
                if s is not None
            }
    except Exception:
        return None


def _group1(pattern: str, text: str) -> str:
    """`re.search(...).group(1)`, but with a message instead of an AttributeError."""
    match = re.search(pattern, text)
    if match is None:
        raise ValueError(f"{text!r} does not match {pattern!r}")
    return match.group(1)


def donor_map(
    category: str, ibge: str, years=DONOR_YEARS
) -> tuple[dict[str, str], dict]:
    """seq_orgao -> cod_orgao for one municipality, plus any cross-year conflicts."""
    seen: dict[str, dict[int, str]] = collections.defaultdict(dict)
    for year in years:
        pairs = source_pairs(year, category, ibge)
        if not pairs:
            continue
        for seq, cod in pairs:
            seen[seq][year] = cod
    conflicts = {s: v for s, v in seen.items() if len(set(v.values())) > 1}
    # Most recent donor wins; conflicts are reported, never silently resolved.
    return {s: sorted(v.items())[-1][1] for s, v in seen.items()}, conflicts


def validate(sample: int = 150) -> bool:
    """Predict real 2017/2018 cod_orgao from donor years only. The honest test."""
    random.seed(7)
    jobs = []
    for category in ("empenho", "despesa"):
        for target in TARGET_YEARS:
            directory = clean_mg.MG_INPUT / str(target) / category
            ids = sorted(p.name.split(".")[2] for p in directory.glob("*.zip"))
            for ibge in random.sample(ids, min(sample, len(ids))):
                jobs.append((category, ibge, target))
    print(
        f"validating on {len(jobs)} municipality-years that still have source data"
    )
    lock = threading.Lock()
    tally = collections.Counter()
    wrong: list[tuple] = []

    def check(job):
        category, ibge, target = job
        truth = source_pairs(target, category, ibge)
        if not truth:
            return
        mapping, _ = donor_map(category, ibge)
        if not mapping:
            return
        for seq, cod in truth:
            with lock:
                if seq not in mapping:
                    tally["absent"] += 1
                elif mapping[seq] == cod:
                    tally["correct"] += 1
                else:
                    tally["wrong"] += 1
                    if len(wrong) < 10:
                        wrong.append(
                            (category, ibge, target, seq, mapping[seq], cod)
                        )

    with ThreadPoolExecutor(6) as pool:
        list(pool.map(check, jobs))
    total = tally["correct"] + tally["wrong"]
    print(
        f"  correct {tally['correct']:,}/{total:,}   wrong {tally['wrong']:,}   absent {tally['absent']:,}"
    )
    for w in wrong:
        print("  MISMATCH", w)
    return tally["wrong"] == 0 and tally["absent"] == 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--validate",
        action="store_true",
        help="run the ground-truth check and stop",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--skip-backup", action="store_true")
    args = parser.parse_args()

    if args.validate:
        raise SystemExit(0 if validate() else 1)

    client, bucket = client_and_bucket()

    # 1. The affected objects are exactly those in the bucket with no local
    #    counterpart -- i.e. the municipality-exercises the portal withdrew.
    output = (
        Path(
            os.environ.get(
                "MIDES_DATA_DIR", Path.home() / "Downloads/world_wb_mides_data"
            )
        )
        / "output"
    )
    keys: list[str] = []
    for mirror in MIRROR_CATEGORY:
        local = {p.name for p in (output / mirror).glob("*.parquet")}
        for blob in client.list_blobs(bucket, prefix=f"{PREFIX}/{mirror}/"):
            name = blob.name.rsplit("/", 1)[-1]
            if name not in local:
                keys.append(blob.name)
    years = collections.Counter(_group1(r"_(\d{4})_", k) for k in keys)
    print(
        f"{len(keys)} objects with no local counterpart; by year: {dict(sorted(years.items()))}"
    )
    stray = {y for y in years if int(y) not in TARGET_YEARS}
    if stray:
        raise SystemExit(
            f"refusing to run: objects outside {TARGET_YEARS} have no local counterpart: {stray}"
        )
    if args.dry_run:
        print("dry run, nothing written")
        return

    # 2. Back up before touching anything -- the dev bucket has no versioning.
    if not args.skip_backup:
        existing = sum(
            1 for _ in client.list_blobs(bucket, prefix=BACKUP + "/")
        )
        if existing >= len(keys):
            print(f"backup already present ({existing} objects), leaving it")
        else:

            def copy(key):
                bucket.copy_blob(
                    bucket.blob(key),
                    bucket,
                    f"{BACKUP}/{key.split(PREFIX + '/', 1)[1]}",
                )

            with ThreadPoolExecutor(args.workers) as pool:
                list(pool.map(copy, keys))
            print(f"backed up {len(keys)} objects -> gs://{BUCKET}/{BACKUP}/")

    # 3. Remap and upload in place.
    WORK.mkdir(parents=True, exist_ok=True)
    lock = threading.Lock()
    stats = collections.Counter()
    unmapped: list[str] = []

    def remap(key: str):
        mirror = key.split("/")[2]
        category = MIRROR_CATEGORY[mirror]
        ibge = _group1(r"_(\d{7})\.parquet$", key)
        mapping, conflicts = donor_map(category, ibge)
        if conflicts:
            with lock:
                unmapped.append(f"{key}: donor-year conflict {conflicts}")
            return
        local = WORK / mirror / key.rsplit("/", 1)[-1]
        local.parent.mkdir(parents=True, exist_ok=True)
        bucket.blob(key).download_to_filename(str(local))
        table = pq.read_table(local)
        index = table.column_names.index("orgao")
        field = table.schema.field(index)
        old = table.column("orgao").to_pylist()
        if any(v is not None and v not in mapping for v in old):
            with lock:
                unmapped.append(f"{key}: orgao value absent from donor map")
            return
        new = [None if v is None else mapping[v] for v in old]
        rebuilt = table.set_column(
            index, field, pa.array(new, type=field.type)
        )
        assert rebuilt.schema == table.schema, key
        pq.write_table(rebuilt, local, compression="snappy")
        bucket.blob(key).upload_from_filename(str(local))
        with lock:
            stats["files"] += 1
            stats["rows"] += rebuilt.num_rows

    with ThreadPoolExecutor(args.workers) as pool:
        list(pool.map(remap, keys))

    print(
        f"remapped and uploaded {stats['files']} files, {stats['rows']:,} rows"
    )
    if unmapped:
        print(f"SKIPPED {len(unmapped)} -- these still carry seq_orgao:")
        for u in unmapped[:10]:
            print("  ", u)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
