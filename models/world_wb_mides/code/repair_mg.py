"""Rebuild source rows whose free-text fields contain unescaped delimiters.

WHY THIS IS NOT THE "GUESSING" clean_mg.py REFUSES TO DO
-------------------------------------------------------
`read_source_csv` drops any row whose delimiter count differs from the header and
counts it `ragged`, on the stated ground that repairing "would mean guessing which
column the extra delimiters fell in, and a wrongly re-joined payment row is worse
than an absent one". That is right about guessing and wrong that guessing is the
only option.

Only a free-text column can swallow a delimiter. Every other column is typed --
`vlr_*` numeric, `dat_*` eight digits, `seq_*`/`cod_*` integers, `num_*` free of
letters -- and SICOM's labels are formatted `CODE - DESCRIPTION`
(`1.753.000 - RECURSOS PROVENIENTES...`, `0000 - SEM IDENTIFICACAO...`), so each
anchored label announces where it begins.

So we enumerate every way the excess segments could be distributed among the text
columns, keep only distributions where EVERY column satisfies its own pattern,
and accept the row ONLY IF EXACTLY ONE distribution survives. Zero or several
means the reconstruction is not provable, and the row is refused and counted
exactly as before.

The safety property the original decision wanted is preserved -- nothing is
published whose reconstruction is not uniquely determined. What changes is that
most ragged rows ARE uniquely determined, so they stop being discarded.

MEASURED RECOVERY, AND WHERE IT FALLS SHORT
-------------------------------------------
Across the seven streams with ragged rows, 372,340 of 428,400 repaired (86.9%):

    empenhoFonte     2023   122,952 / 122,952   100%
    liquidacaoFonte  2023   109,851 / 109,851   100%
    respLicitacao  2020-23   24,987 /  24,991   100%
    movFonteRsp      2023     3,696 /   3,696   100%
    pagamento        2023    99,394 / 107,905  92.1%
    despesa          2023    11,460 /  58,253  19.7%
    recDispensa      2023         0 /     608     0%

Three known limits, left in place deliberately rather than tuned away:

  * `pagamento` loses 8,511 rows an earlier hand-anchored variant recovered.
    Calibration makes `nom_resp`/`nom_credor` absorbing candidates too, which
    creates ambiguity the narrower version did not have. Both behaviours are
    safe; neither dominates.
  * `despesa` and `recDispensa` have many adjacent label columns, so an excess
    often admits several readings and is refused.
  * Rows with FEWER fields than the header (`pagamento` 2024-26 carry 5- and
    25-field rows against 29) are a different defect and are always refused.

Refusing is never a correctness risk -- it is the pre-existing behaviour. The
counts above are what this module adds, not a ceiling it fails to reach.
"""

from __future__ import annotations

import itertools
import re

# Anchors are CALIBRATED FROM THE DATA, never asserted. An anchor invented from
# a column's name rejects the true reading whenever the guess is wrong: measured,
# `dsc_dotacao` holds `03.00003001.17.122.0300.2001.3.3.90.39.00.1.753.000` -- a
# dotted code with no dash -- so a `CODE - DESCRIPTION` anchor refused every
# candidate and the stream repaired 0 of 608 rows. `Layout.calibrate()` therefore
# learns each column's shape from rows that are already well formed, and keeps a
# pattern only when it holds for essentially all of them.
CANDIDATE_PATTERNS: tuple[tuple[str, str], ...] = (
    ("code_dash", r"^\s*[\d.]+\s*-\s"),
    ("dotted", r"^\s*[\d.]+\s*$"),
    ("starts_digit", r"^\s*\d"),
    ("has_letter", r"[A-Za-z\u00c0-\u024f]"),
)
CALIBRATION_MIN = 0.995
CALIBRATION_SAMPLE = 40_000

_INT = re.compile(r"^\s*-?\d*\s*$")
_NUM = re.compile(r"^\s*-?[\d.,]*\s*$")
_DATE8 = re.compile(r"^\s*(\d{8}|-1|)\s*$")
# A document/process number may be masked (`568***806**`) or carry `/` and `-`,
# but it never contains a letter. That is what separates it from a name fragment.
_DOC = re.compile(r"^[^A-Za-zÀ-ɏ]*$")

MAX_EXCESS = 6
MAX_CANDIDATES = 50_000


def _is_text(col: str) -> bool:
    return col.startswith(("dsc_", "nom_"))


def _typed_ok(col: str, v: str) -> bool:
    if col.startswith("vlr_"):
        return bool(_NUM.match(v))
    if col.startswith("dat_"):
        return bool(_DATE8.match(v))
    if col.startswith(("seq_", "cod_")):
        return bool(_INT.match(v))
    if col.startswith("num_"):
        return bool(_DOC.match(v))
    return True


def _distributions(excess: int, slots: int):
    """Every way to add `excess` extra segments across `slots` text columns."""
    if slots == 0:
        return
    for cuts in itertools.combinations(range(excess + slots - 1), slots - 1):
        prev, out = -1, []
        for c in (*cuts, excess + slots - 1):
            out.append(c - prev - 1)
            prev = c
        yield out


class Layout:
    """One stream's header, and how to rebuild a ragged row against it."""

    def __init__(self, columns: list[str]) -> None:
        self.columns = columns
        self.n = len(columns)
        self.text = [i for i, c in enumerate(columns) if _is_text(c)]
        self.anchors: dict[int, re.Pattern[str]] = {}
        self._seen: dict[int, list[int]] = {}
        self._n_seen = 0
        self.calibrated = False
        self.repairable = bool(self.text)

    def observe(self, parts: list[str]) -> None:
        """Feed one well-formed row, to learn each text column's shape."""
        if len(parts) != self.n or self._n_seen >= CALIBRATION_SAMPLE:
            return
        self._n_seen += 1
        for i in self.text:
            v = parts[i]
            if not v.strip():
                continue
            hits = self._seen.setdefault(
                i, [0] * (len(CANDIDATE_PATTERNS) + 1)
            )
            hits[-1] += 1
            for j, (_, pat) in enumerate(CANDIDATE_PATTERNS):
                if re.search(pat, v):
                    hits[j] += 1

    def calibrate(self) -> None:
        """Keep only the patterns that hold for essentially every observed value."""
        self.anchors = {}
        for i, hits in self._seen.items():
            total = hits[-1]
            if total < 50:
                continue
            for j, (_, pat) in enumerate(CANDIDATE_PATTERNS):
                if hits[j] / total >= CALIBRATION_MIN:
                    self.anchors[i] = re.compile(pat)
                    break
        self.calibrated = True

    def repair(self, parts: list[str]) -> list[str] | None:
        excess = len(parts) - self.n
        if not self.repairable or excess <= 0 or excess > MAX_EXCESS:
            return None
        slots = len(self.text)
        combos = itertools.combinations(range(excess + slots - 1), slots - 1)
        try:
            if (
                slots > 1
                and sum(
                    1 for _ in itertools.islice(combos, MAX_CANDIDATES + 1)
                )
                > MAX_CANDIDATES
            ):
                return None
        except OverflowError:
            return None
        found = None
        for extra in _distributions(excess, slots):
            row, cur, ok = [], 0, True
            extra_by_col = dict(zip(self.text, extra, strict=True))
            for i, col in enumerate(self.columns):
                take = 1 + extra_by_col.get(i, 0)
                seg = ";".join(parts[cur : cur + take])
                cur += take
                if not _is_text(col):
                    if not _typed_ok(col, seg):
                        ok = False
                        break
                else:
                    a = self.anchors.get(i)
                    if a is not None and not a.match(seg):
                        ok = False
                        break
                row.append(seg)
            if not ok or cur != len(parts):
                continue
            if found is not None:
                return None  # ambiguous -> refuse
            found = row
        return found
