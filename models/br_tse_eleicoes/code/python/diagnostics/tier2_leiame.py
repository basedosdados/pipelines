"""Tier 2 — acquire the official TSE layout per family x year (schema only).

TSE republishes every election year's files in the modern layout, and those
files carry a header row (verified back to 1994). The header row IS the
authoritative ordered layout (position N == raw column ``vN``). The leiame
PDF is the fallback for files without headers.

Source preference per (family, ano):
  1. ``tier2_overrides.json``               (hand-transcribed)
  2. header row of a local raw data file    (TSE_DATA_DIR/input)
  3. header row of a remote data member     (HTTP Range; ~KBs, never the data)
  4. leiame PDF/TXT, local then remote      (text-based parse)

Parsed layouts are cached as ``artifacts/layouts/{family}_{ano}.json``.
"""

from __future__ import annotations

import io
import json
import re
import urllib.error
import urllib.request
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

from config import INPUT_DIR

from .spec import FAMILIES, MEMBER_PREFER

ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
LAYOUTS_DIR = ARTIFACTS / "layouts"
CACHE_DIR = ARTIFACTS / "leiame_cache"
OVERRIDES_PATH = Path(__file__).resolve().parent / "tier2_overrides.json"

# official TSE variable names: UPPERCASE with at least one underscore
VAR_RE = re.compile(r"^[A-Z][A-Z0-9]*(_[A-Z0-9]+)+$")
_LEIAME_RE = re.compile(r"leia[\s_-]*me.*\.(pdf|txt)$", re.IGNORECASE)
_DATA_EXT_RE = re.compile(r"\.(csv|txt)$", re.IGNORECASE)
_UF_PROBES = ("AC", "AL", "SE", "RR")


@dataclass
class Layout:
    family: str
    ano: int
    source: str  # "<kind>:<where>"  kind in {override, header, leiame}
    columns: list[str]  # ordered official TSE variable names (1-based = vN)
    has_header: bool = False  # True when taken from a real file header row
    notes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# HTTP range file — lets zipfile read a remote zip without downloading it
# ---------------------------------------------------------------------------


class HttpRangeFile(io.RawIOBase):
    """Seekable read-only file over HTTP Range requests with block caching."""

    BLOCK = 256 * 1024

    def __init__(self, url: str):
        self.url = url
        self._pos = 0
        self._blocks: dict[int, bytes] = {}
        self._size = self._fetch_size()

    def _fetch_size(self) -> int:
        req = urllib.request.Request(self.url, method="HEAD")
        with urllib.request.urlopen(req, timeout=60) as resp:
            length = resp.headers.get("Content-Length")
            if length is None:
                raise OSError(f"no Content-Length for {self.url}")
            return int(length)

    def _fetch_block(self, idx: int) -> bytes:
        if idx in self._blocks:
            return self._blocks[idx]
        start = idx * self.BLOCK
        end = min(start + self.BLOCK, self._size) - 1
        req = urllib.request.Request(
            self.url, headers={"Range": f"bytes={start}-{end}"}
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
        self._blocks[idx] = data
        return data

    # io protocol -----------------------------------------------------------

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._size + offset
        return self._pos

    def read(self, n=-1):
        if n < 0:
            n = self._size - self._pos
        n = max(0, min(n, self._size - self._pos))
        out = bytearray()
        while n > 0:
            idx, off = divmod(self._pos, self.BLOCK)
            chunk = self._fetch_block(idx)[off : off + n]
            if not chunk:
                break
            out.extend(chunk)
            self._pos += len(chunk)
            n -= len(chunk)
        return bytes(out)

    def readinto(self, b):  # BufferedReader uses readinto, not read
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)


def open_remote_zip(url: str) -> zipfile.ZipFile:
    return zipfile.ZipFile(
        io.BufferedReader(HttpRangeFile(url), buffer_size=64 * 1024)
    )


# ---------------------------------------------------------------------------
# header-row extraction
# ---------------------------------------------------------------------------


_NUMERIC_CELL_RE = re.compile(r"^-?[\d.,/:\s]+$")


def _header_columns(first_line: str) -> list[str] | None:
    """Return columns if the line is a TSE header row, else None.

    Two header styles exist: official variable names (``SQ_CANDIDATO``) and,
    in some generations (e.g. prestacao 2010), human-readable names
    (``"Sequencial Candidato";"UF";...``).
    """
    cells = [
        c.strip().strip('"') for c in first_line.rstrip("\r\n").split(";")
    ]
    if len(cells) < 3:
        return None
    matches = sum(1 for c in cells if VAR_RE.match(c))
    if matches / len(cells) >= 0.8:
        return cells
    # human-readable header: no numeric/date-like cells, mostly alphabetic
    if len(cells) >= 5 and not any(
        _NUMERIC_CELL_RE.match(c) for c in cells if c
    ):
        alpha = sum(1 for c in cells if re.search(r"[A-Za-zÀ-ÿ]{2,}", c))
        if alpha / len(cells) >= 0.8:
            return cells
    return None


def _member_ok(name: str, member_hint: str) -> bool:
    if _LEIAME_RE.search(Path(name).name) or "leioute" in name.lower():
        return False
    return not (member_hint and not re.search(member_hint, name))


def _local_header(family: str, ano: int) -> tuple[list[str], str] | None:
    fam = FAMILIES[family]
    for tpl in fam.local_dirs:
        for uf in _UF_PROBES if "{uf}" in tpl else ("",):
            d = INPUT_DIR / tpl.format(ano=ano, uf=uf)
            if not d.is_dir():
                continue
            files = [
                p
                for p in sorted(d.rglob("*"))
                if p.is_file()
                and _DATA_EXT_RE.search(p.name)
                and _member_ok(str(p.relative_to(d)), fam.member_hint)
            ]
            for p in files:
                try:
                    with open(p, encoding="latin-1") as fh:
                        cols = _header_columns(fh.readline())
                except OSError:
                    continue
                if cols:
                    return cols, f"header:{p}"
    return None


def _zip_header(
    zf: zipfile.ZipFile, url: str, member_hint: str, prefer: str = ""
) -> tuple[list[str], str] | None:
    members = [
        m
        for m in zf.infolist()
        if _DATA_EXT_RE.search(m.filename)
        and _member_ok(m.filename, member_hint)
    ]
    if not members:
        return None
    if prefer:
        preferred = [m for m in members if re.search(prefer, m.filename)]
        members = preferred or members
    member = min(members, key=lambda m: m.file_size)
    with zf.open(member) as fh:
        first = io.TextIOWrapper(fh, encoding="latin-1").readline()
    cols = _header_columns(first)
    if cols:
        return cols, f"header:{url}!{member.filename}"
    return None


# ---------------------------------------------------------------------------
# leiame parsing
# ---------------------------------------------------------------------------


_SECTION_RE = re.compile(r"TIPO\s+DO\s+ARQUIVO\s+(.+)", re.IGNORECASE)


def _vars_from_lines(lines: list[str], section_hint: str) -> list[str]:
    """Ordered variable names from leiame text lines.

    Multi-section leiames delimit file types with "TIPO DO ARQUIVO <name>";
    when ``section_hint`` is set only the matching section is read. Tokens
    split by PDF line-wrapping ("QTDE_VOTOS" + "_NOMINAIS") are merged.
    """
    has_sections = any(_SECTION_RE.search(ln) for ln in lines)
    in_section = not (has_sections and section_hint)
    cols: list[str] = []
    seen: set[str] = set()
    for line in lines:
        m = _SECTION_RE.search(line)
        if m:
            if has_sections and section_hint:
                in_section = bool(re.search(section_hint, m.group(1)))
            if in_section:
                cols, seen = [], set()  # restart at the section header
            continue
        if not in_section or not line.strip():
            continue
        parts = line.strip().split()
        tok = parts[0].strip('";:')
        if len(parts) > 1 and parts[1].startswith("_"):
            merged = tok + parts[1].strip('";:')
            if VAR_RE.match(merged):
                tok = merged
        if VAR_RE.match(tok) and tok not in seen:
            cols.append(tok)
            seen.add(tok)
    return cols


def parse_leiame_pdf(data: bytes, section_hint: str = "") -> list[str]:
    """Ordered variable list from a leiame PDF (text lines, table fallback)."""
    import pdfplumber  # deferred: run harness with `uv run --with pdfplumber`

    with pdfplumber.open(io.BytesIO(data)) as pdf:
        lines: list[str] = []
        for page in pdf.pages:
            lines.extend((page.extract_text() or "").splitlines())
        text_cols = _vars_from_lines(lines, section_hint)

        table_lines: list[str] = []
        for page in pdf.pages:
            for table in page.extract_tables():
                for row in table:
                    cells = [(c or "").replace("\n", " ") for c in row]
                    table_lines.append(" ".join(cells).strip())
        table_cols = _vars_from_lines(table_lines, section_hint)
    return text_cols if len(text_cols) >= len(table_cols) else table_cols


def parse_leiame_txt(data: bytes, section_hint: str = "") -> list[str]:
    text = data.decode("latin-1", errors="replace")
    return _vars_from_lines(text.splitlines(), section_hint)


def _parse_leiame(data: bytes, name: str, section_hint: str = "") -> list[str]:
    if name.lower().endswith(".pdf") or data[:4] == b"%PDF":
        return parse_leiame_pdf(data, section_hint)
    return parse_leiame_txt(data, section_hint)


def _local_leiame(family: str, ano: int) -> tuple[bytes, str] | None:
    fam = FAMILIES[family]
    for tpl in fam.local_dirs:
        for uf in _UF_PROBES if "{uf}" in tpl else ("",):
            d = INPUT_DIR / tpl.format(ano=ano, uf=uf)
            if not d.is_dir():
                continue
            for p in sorted(d.iterdir()):
                if p.is_file() and _LEIAME_RE.search(p.name):
                    return p.read_bytes(), f"leiame:{p}"
    return None


# ---------------------------------------------------------------------------
# acquisition
# ---------------------------------------------------------------------------


def _remote(family: str, ano: int) -> Layout | None:
    fam = FAMILIES[family]
    for tpl in fam.url_templates:
        for uf in _UF_PROBES if "{uf}" in tpl else ("",):
            url = tpl.format(ano=ano, uf=uf)
            try:
                zf = open_remote_zip(url)
            except (urllib.error.HTTPError, urllib.error.URLError, OSError):
                continue
            try:
                got = _zip_header(
                    zf,
                    url,
                    fam.member_hint,
                    MEMBER_PREFER.get((family, ano), ""),
                )
                if got:
                    cols, source = got
                    return Layout(family, ano, source, cols, has_header=True)
                members = [
                    m for m in zf.namelist() if _LEIAME_RE.search(Path(m).name)
                ]
                if members:
                    member = sorted(members, key=len)[0]
                    data = zf.read(member)
                    CACHE_DIR.mkdir(parents=True, exist_ok=True)
                    (CACHE_DIR / f"{family}_{ano}.bin").write_bytes(data)
                    cols = _parse_leiame(data, member, fam.section_hint)
                    return Layout(family, ano, f"leiame:{url}!{member}", cols)
            except (OSError, zipfile.BadZipFile):
                continue
    return None


def _load_overrides() -> dict:
    if OVERRIDES_PATH.exists():
        return json.loads(OVERRIDES_PATH.read_text())
    return {}


def layout_path(family: str, ano: int) -> Path:
    return LAYOUTS_DIR / f"{family}_{ano}.json"


def load_layout(family: str, ano: int) -> Layout | None:
    p = layout_path(family, ano)
    if p.exists():
        return Layout(**json.loads(p.read_text()))
    return None


def acquire(family: str, ano: int, force: bool = False) -> Layout | None:
    if not force:
        cached = load_layout(family, ano)
        if cached is not None:
            return cached

    overrides = _load_overrides()
    key = f"{family}_{ano}"
    layout: Layout | None = None
    if key in overrides:
        ov = overrides[key]
        layout = Layout(
            family=family,
            ano=ano,
            source=ov.get("source", "override:manual"),
            columns=ov["columns"],
            has_header=ov.get("has_header", False),
            notes=ov.get("notes", []),
        )
    if layout is None:
        got = _local_header(family, ano)
        if got:
            layout = Layout(family, ano, got[1], got[0], has_header=True)
    if layout is None:
        got = _local_leiame(family, ano)
        if got:
            data, source = got
            layout = Layout(
                family,
                ano,
                source,
                _parse_leiame(data, source, FAMILIES[family].section_hint),
            )
    if layout is None:
        layout = _remote(family, ano)
    if layout is None:
        return None

    if len(layout.columns) < 3:
        layout.notes.append(
            "parse produced <3 variables — needs tier2_overrides.json entry"
        )
    LAYOUTS_DIR.mkdir(parents=True, exist_ok=True)
    layout_path(family, ano).write_text(
        json.dumps(asdict(layout), indent=1, ensure_ascii=False)
    )
    return layout


def acquire_all(
    pairs: list[tuple[str, int]], force: bool = False
) -> dict[tuple[str, int], Layout | None]:
    results: dict[tuple[str, int], Layout | None] = {}
    for family, ano in pairs:
        try:
            layout = acquire(family, ano, force=force)
        except Exception as exc:
            print(f"  {family} {ano}: ERROR {type(exc).__name__}: {exc}")
            results[(family, ano)] = None
            continue
        if layout is None:
            print(f"  {family} {ano}: NOT FOUND")
        else:
            kind = layout.source.split(":", 1)[0]
            flag = "" if len(layout.columns) >= 3 else "  !! needs override"
            print(
                f"  {family} {ano}: {len(layout.columns)} vars ({kind}){flag}"
            )
        results[(family, ano)] = layout
    return results


def needed_pairs(table: str | None = None) -> list[tuple[str, int]]:
    """All (family, ano) pairs referenced by the tier-1 spec."""
    from .spec import funcs_for

    pairs: set[tuple[str, int]] = set()
    for fs in funcs_for(table):
        for ano in fs.years:
            pairs.add((fs.family, ano))
    return sorted(pairs)


if __name__ == "__main__":
    acquire_all(needed_pairs())
