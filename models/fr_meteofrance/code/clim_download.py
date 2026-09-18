"""Download the Météo-France *Données climatologiques de base* archives.

    uv run python models/fr_meteofrance/code/clim_download.py [--only quot|mens]
                                                              [--latest-only]

A thin CLI. The transform itself lives in
``pipelines/datasets/fr_meteofrance/clim_utils.py`` so the one-shot bootstrap and
the recurring flow share one copy. Files land in ``$MFC_INPUT``
(default ``~/Downloads/fr_meteofrance_clim/input``).
"""

import argparse

from pipelines.datasets.fr_meteofrance.clim_utils import INPUT, download


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only", choices=["quot", "mens"], help="download one series"
    )
    parser.add_argument(
        "--latest-only",
        action="store_true",
        help="fetch only the latest-<years> slice, the one the source rewrites",
    )
    args = parser.parse_args()
    print(f"input {INPUT}")
    for kind in ("mens", "quot"):
        if args.only in (None, kind):
            download(kind, only_latest=args.latest_only)


if __name__ == "__main__":
    main()
