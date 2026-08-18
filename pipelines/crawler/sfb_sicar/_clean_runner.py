"""Subprocess entrypoint: clean ONE feature range into ONE parquet part.

The clean churns millions of tiny per-feature GEOS/shapely/WKT allocations. In a
single long-lived process on the Linux worker, glibc never fully returns that
freed memory (heap fragmentation, and up to ``8 x ncores`` retaining arenas), so
RSS creeps up range after range and OOMs the 32 GiB pod on a dense Amazonas
``app`` — a growth that never appears on (non-glibc) macOS.

The recurring flow therefore runs each range here, in its own short-lived
process that ``tasks.clean_uf_theme`` launches once per range and lets exit. When
the process exits the OS reclaims everything, so peak RSS is permanently one
range, regardless of allocator, arena count, or core count. ``MALLOC_ARENA_MAX``
is set in the child's environment too (glibc reads it only before its first
malloc) as a cheap second bound.

Invoked as::

    python -m pipelines.crawler.sfb_sicar._clean_runner \
        <shp> <start> <count> <part_dir> <table> <snapshot_iso> <sigla_uf> <idx>

Prints one ``RESULT rows=<n> dropped=<n> peak_rss_mb=<n>`` line on success and
exits non-zero (with a traceback on stderr) on failure.
"""

import resource
import sys

from pipelines.crawler.sfb_sicar.utils import clean_shp_range


def main() -> int:
    shp, start, count, part_dir, table, snapshot_iso, sigla_uf, idx = sys.argv[
        1:9
    ]
    rows, dropped = clean_shp_range(
        shp=shp,
        start=int(start),
        count=int(count),
        part_dir=part_dir,
        table=table,
        snapshot_iso=snapshot_iso,
        sigla_uf=sigla_uf,
        file_idx=int(idx),
    )
    # ru_maxrss is KB on Linux, bytes on macOS; the worker is Linux.
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_mb = (
        peak_kb / 1024
        if sys.platform.startswith("linux")
        else peak_kb / 1024**2
    )
    print(f"RESULT rows={rows} dropped={dropped} peak_rss_mb={peak_mb:.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
