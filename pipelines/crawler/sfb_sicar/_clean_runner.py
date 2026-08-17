"""Subprocess entrypoint for cleaning one UF x theme with a tuned allocator.

The clean churns millions of tiny per-feature GEOS/shapely/WKT allocations. On
the Linux worker, glibc's default ``8 x ncores`` malloc arenas each retain freed
memory and RSS balloons ~10x the real working set — a dense Amazonas ``app``
chunk that peaks at ~3.6 GB on macOS OOMed the 32 GiB pod. ``MALLOC_ARENA_MAX``
fixes it, but glibc only reads it *before the first malloc*, so it must be in the
environment when the process starts — not set from Python after import. The
recurring flow therefore runs the clean here, in a child process it launches with
``MALLOC_ARENA_MAX`` already in ``env`` (see ``tasks.clean_uf_theme``), rather
than depending on the k8s work-pool job template to inject a pod env var.

Invoked as::

    python -m pipelines.crawler.sfb_sicar._clean_runner \
        <zip_path> <out_root> <table> <snapshot_iso> <sigla_uf> [budget_mb]

Prints one ``RESULT rows=<n> dropped=<n> peak_rss_mb=<n>`` line on success and
exits non-zero (with a traceback on stderr) on failure.
"""

import resource
import sys

from pipelines.crawler.sfb_sicar.utils import clean_theme_chunked


def main() -> int:
    zip_path, out_root, table, snapshot_iso, sigla_uf = sys.argv[1:6]
    budget_bytes = (
        int(sys.argv[6]) * 1024 * 1024
        if len(sys.argv) > 6
        else 96 * 1024 * 1024
    )
    rows, dropped = clean_theme_chunked(
        zip_path=zip_path,
        out_root=out_root,
        table=table,
        snapshot_iso=snapshot_iso,
        sigla_uf=sigla_uf,
        budget_bytes=budget_bytes,
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
