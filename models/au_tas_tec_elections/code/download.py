"""Download every au_tas_tec_elections source artefact into the scratch input tree.

Run: PYTHONPATH=. ~/.venvs/bd-pipelines-tas/bin/python \
        models/au_tas_tec_elections/code/download.py

Writes ``$TEC_DATA_ROOT/input/<election_id>/`` plus ``input/manifest.json``.

Every fetch raises on a non-2xx. Without that a 404 body is written under the data
file's name and an ``if not target.exists()`` guard then skips it forever, so the
build proceeds on an HTML error page it believes is a spreadsheet.
"""

from __future__ import annotations

import json
import urllib.parse

from pipelines.datasets.au_tas_tec_elections.constants import (
    constants,
    data_root,
)
from pipelines.datasets.au_tas_tec_elections.discovery import (
    discover_contests,
    fetch,
)

BASE = constants.BASE_URL.value


def save(url: str, target) -> int:
    status, body = fetch(url)
    if status != 200:
        raise RuntimeError(f"HTTP {status} for {url}")
    if not body:
        raise RuntimeError(f"empty body for {url}")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    return len(body)


def main() -> int:
    root = data_root() / "input"
    root.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, dict] = {}
    dead: list[str] = []
    total_bytes = 0

    for election_id, meta in constants.ELECTION_META.value.items():
        name, chamber, etype, date, prefix = meta
        index = urllib.parse.urljoin(
            BASE, constants.ELECTION_INDEX.value[election_id]
        )
        contests = {
            div: c
            for div, c in discover_contests(index).items()
            if prefix in c.page
        }
        out_dir = root / election_id
        entry: dict = {
            "name": name,
            "chamber": chamber,
            "election_type": etype,
            "election_date": date,
            "contests": {},
        }
        for div, c in sorted(contests.items()):
            files: dict[str, str] = {}
            try:
                total_bytes += save(c.page, out_dir / div / "page.html")
            except RuntimeError as exc:
                # A results page can link a sibling division that does not exist
                # on that event's path — the 2017 Pembroke by-election microsite
                # links Murchison and Rumney, which 404. Drop the contest rather
                # than inventing one with no data behind it.
                dead.append(str(exc))
                print(f"  [dead contest] {div}: {exc}", flush=True)
                continue
            files["page"] = f"{div}/page.html"
            if c.fp_fragment:
                total_bytes += save(c.fp_fragment, out_dir / div / "fp.html")
                files["fp"] = f"{div}/fp.html"
            if c.dist_fragment:
                total_bytes += save(
                    c.dist_fragment, out_dir / div / "dist.html"
                )
                files["dist"] = f"{div}/dist.html"
            for doc in c.documents:
                fname = urllib.parse.unquote(doc.rsplit("/", 1)[-1])
                try:
                    total_bytes += save(doc, out_dir / "docs" / fname)
                except RuntimeError as exc:
                    # A document link can be stale while the page around it is
                    # live: the 2018 results pages still link
                    # ``PPs/Braddon.xlsx``, superseded by
                    # ``2018-Braddon-first-prefs-by-polling-places.xlsx``. Record
                    # it and carry on, but never write the error body under the
                    # data file's name and never skip it silently.
                    dead.append(str(exc))
                    print(f"  [dead] {exc}", flush=True)
                    continue
                files.setdefault("docs", []).append(f"docs/{fname}")  # type: ignore[union-attr]
            entry["contests"][div] = {
                "page_url": c.page,
                "fp_url": c.fp_fragment,
                "dist_url": c.dist_fragment,
                "inline": c.inline,
                "document_urls": c.documents,
                "files": files,
            }
        manifest[election_id] = entry
        print(
            f"{election_id:16s} {len(contests)} contests -> {out_dir}",
            flush=True,
        )

    (root / "manifest.json").write_text(
        json.dumps({"elections": manifest, "dead_urls": dead}, indent=2),
        encoding="utf-8",
    )
    if dead:
        print(f"\n{len(dead)} dead document link(s) on live pages:")
        for d in dead:
            print(f"  {d}")
    n = sum(len(e["contests"]) for e in manifest.values())
    print(
        f"\n{len(manifest)} elections, {n} contests, "
        f"{total_bytes / 1e6:.1f} MB -> {root}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
