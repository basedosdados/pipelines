"""Tests for the skip decision in `promote_staging.py`."""

import sys
from pathlib import Path

# The scripts are run as files, not imported as a package, and import their
# siblings by bare name -- put that directory on the path the same way.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from promote_staging import (  # pyrefly: ignore [missing-import]
    BlobDigest,
    needs_copy,
)


def blob(name, md5="m", crc32c="c", size=10):
    return BlobDigest(name=name, md5=md5, crc32c=crc32c, size=size)


def test_identical_mirror_is_skipped():
    source = [blob("a.parquet", md5="m1"), blob("b.parquet", md5="m2")]
    copy, reason = needs_copy(source, list(reversed(source)))
    assert not copy
    assert reason == "all 2 blobs match (2 by md5)"


def test_equal_count_different_content_is_copied():
    # orgao_unidade_gestora: one blob per bucket, prod an older revision.
    source = [
        blob("data.csv", md5="2mEwk82jJJXRRVMDQENlpA==", size=31_690_000)
    ]
    destination = [
        blob("data.csv", md5="kVfl8sO9bhFvTdqQA8ghBg==", size=31_360_000)
    ]
    copy, reason = needs_copy(source, destination)
    assert copy
    assert "1 differ in content: data.csv (md5)" in reason


def test_equal_count_different_names_is_copied():
    copy, reason = needs_copy([blob("ano=2024/a")], [blob("ano=2023/a")])
    assert copy
    assert "missing from destination: ano=2024/a" in reason
    assert "only in destination: ano=2023/a" in reason


def test_empty_destination_is_copied():
    assert needs_copy([blob("a")], []) == (True, "destination is empty")


def test_composite_object_falls_back_to_crc32c():
    # A composite object carries crc32c but no md5.
    source = [blob("a", md5=None, crc32c="c1")]
    assert needs_copy(source, [blob("a", md5=None, crc32c="c1")]) == (
        False,
        "all 1 blobs match (1 by crc32c)",
    )
    copy, reason = needs_copy(source, [blob("a", md5="m", crc32c="c2")])
    assert copy
    assert "a (crc32c)" in reason


def test_no_checksum_falls_back_to_size():
    source = [blob("a", md5=None, crc32c=None, size=10)]
    assert not needs_copy(source, [blob("a", md5=None, crc32c=None, size=10)])[
        0
    ]
    copy, reason = needs_copy(
        source, [blob("a", md5=None, crc32c=None, size=11)]
    )
    assert copy
    assert "a (size)" in reason


def test_long_differences_are_summarised():
    source = [blob(f"f{i}", md5=f"s{i}") for i in range(5)]
    destination = [blob(f"f{i}", md5=f"d{i}") for i in range(5)]
    _, reason = needs_copy(source, destination)
    assert reason.endswith("and 2 more")
