"""Read tables out of a Word document without a third-party dependency.

A ``.docx`` is a ZIP holding ``word/document.xml``; its tables are ordinary XML.
Reading them is exact, which is the whole reason this dataset is built from the
DOCX editions of Budget Paper No. 1 and the Final Budget Outcome rather than from
the PDF the same statements also ship as.

Four details of Word's XML cost real data if they are ignored. Each was found in
these documents, not anticipated:

1. **Non-breaking hyphens are elements, not characters.** The 2025-26 Budget
   writes ``1970-71`` as the run ``1970``, a ``<w:noBreakHyphen/>`` element, then
   ``71``. Concatenating only ``<w:t>`` nodes yields ``197071``, which matches no
   year pattern and silently drops the whole table.
2. **Merged cells shorten a row.** A cell spanning three grid columns is one
   ``<w:tc>`` with ``<w:gridSpan w:val="3"/>``. Unexpanded, header rows are
   narrower than data rows and every label lands left of the data it describes.
3. **Header spans and data spans must expand differently.** A spanned *header*
   cell describes every column it covers, so its text repeats. A spanned *data*
   cell holds one value, so repeating it invents observations -- the 2020-21 FBO
   emits total receipts as a spanned cell, and repeating it duplicated ``8,290``
   across a 54-year series. Two grids are therefore built per row.
4. **Captions are paragraphs, not table properties.** The caption is the last
   non-empty paragraph before the table and carries the number identifying which
   of the eleven historical tables this is.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass

W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
NS = {"w": W}

#: Matches "Table 11.4:", "Table 10.1:", "Table B.4:" and the 2019-20 FBO's
#: dotless "Table B1:". The separator may be a normal or non-breaking space.
CAPTION_RE = re.compile(
    r"Table\s*([0-9]{1,2}|B)\s*\.?\s*([0-9]{1,2})\s*:", re.I
)

#: "1970-71", "2029-30 (e)". The hyphen may already have been normalised from a
#: noBreakHyphen element, and en dashes appear in some editions.
# The dash class is deliberate: Treasury writes the year span with an ASCII
# hyphen, a Unicode hyphen, a non-breaking hyphen and an en dash in different
# editions, and all four must match.
YEAR_RE = re.compile(r"^\s*((?:19|20)[0-9]{2})\s*[-‐‑–]\s*([0-9]{2})\b")  # noqa: RUF001


@dataclass(frozen=True)
class Table:
    caption: str
    #: Table number as ``(prefix, number)``, e.g. ``("11", 4)`` or ``("B", 4)``.
    number: tuple[str, int] | None
    #: Grid-aligned rows. A spanned cell keeps its text in the first position it
    #: covers and blanks the rest, so no value is ever duplicated.
    rows: list[list[str]]
    #: The same rows with spanned text repeated across every column it covers.
    #: Correct for headers, wrong for data -- used only to build labels.
    spanned_rows: list[list[str]]

    @property
    def data_rows(self) -> list[list[str]]:
        """Rows whose first cell is a financial year."""
        return [row for row in self.rows if row and YEAR_RE.match(row[0])]

    @property
    def width(self) -> int:
        """Grid width, taken from the data rows because headers merge cells."""
        return max((len(row) for row in self.data_rows), default=0)

    def column_labels(self) -> list[str]:
        """One label per grid column, stacked from every header row.

        Fragments are joined with ``" | "`` because Word stores a wrapped header
        as several rows, so ``Net Future`` / ``Fund`` / ``earnings`` / ``$m`` are
        four header rows describing one column. Consecutive duplicates are
        collapsed, which is what a spanned group name produces.
        """
        width = self.width
        if not width:
            return []
        stacked: list[list[str]] = [[] for _ in range(width)]
        for faithful, spanned in zip(
            self.rows, self.spanned_rows, strict=False
        ):
            if faithful and YEAR_RE.match(faithful[0]):
                continue
            for index, value in enumerate(spanned[:width]):
                if value and (
                    not stacked[index] or stacked[index][-1] != value
                ):
                    stacked[index].append(value)
        return [" | ".join(parts) for parts in stacked]


def _text(element: ET.Element) -> str:
    """Flatten an element to text, restoring hyphens and tabs Word stores as tags."""
    parts: list[str] = []
    for node in element.iter():
        tag = node.tag.split("}")[-1]
        if tag == "t":
            parts.append(node.text or "")
        elif tag == "noBreakHyphen":
            parts.append("-")
        elif tag in ("tab", "br"):
            parts.append(" ")
    return re.sub(r"\s+", " ", "".join(parts)).strip()


def _expand_row(row: ET.Element) -> tuple[list[str], list[str]]:
    """Widen one row to grid positions, two ways: ``(faithful, spanned)``."""
    faithful: list[str] = []
    spanned: list[str] = []
    for cell in row.findall("w:tc", NS):
        text = _text(cell)
        span = 1
        properties = cell.find("w:tcPr", NS)
        if properties is not None:
            grid_span = properties.find("w:gridSpan", NS)
            if grid_span is not None:
                try:
                    span = max(1, int(grid_span.get(f"{{{W}}}val", "1")))
                except ValueError:
                    span = 1
        faithful.append(text)
        faithful.extend([""] * (span - 1))
        spanned.extend([text] * span)
    return faithful, spanned


#: How far back to look for a caption. Treasury splits the footnote markers of a
#: caption into their own paragraph, so the paragraph immediately before a table
#: is often just "(a)" or "(a)(b)" -- taking it verbatim loses the caption, and
#: with it the unit that tables 9 to 11 declare only in their caption.
_CAPTION_LOOKBACK = 4


def _pick_caption(preceding: list[str]) -> str:
    """The nearest real caption before a table, with its tail reattached.

    Word splits one visual caption line into several paragraphs: the 2021-22
    Budget writes Table 11.9's caption as "... by institutional sector" followed
    by a separate "($m)(a)". Returning only the paragraph that matches the table
    number loses the unit, which tables 9 to 11 declare nowhere else -- so
    everything from the caption paragraph to the table is rejoined.
    """
    window = preceding[-_CAPTION_LOOKBACK:]
    for offset in range(len(window) - 1, -1, -1):
        text = window[offset]
        if text.lower().startswith("table") and CAPTION_RE.search(text):
            return " ".join(window[offset:])
    return preceding[-1] if preceding else ""


def read_tables(path: str) -> list[Table]:
    """Every table in the document, in document order, with its caption."""
    with zipfile.ZipFile(path) as archive:
        root = ET.fromstring(archive.read("word/document.xml"))
    body = root.find("w:body", NS)
    if body is None:
        raise ValueError(f"{path}: document.xml has no body")

    tables: list[Table] = []
    preceding: list[str] = []
    for element in body:
        tag = element.tag.split("}")[-1]
        if tag == "p":
            text = _text(element)
            if text:
                preceding.append(text)
        elif tag == "tbl":
            expanded = [
                _expand_row(row) for row in element.findall("w:tr", NS)
            ]
            caption = _pick_caption(preceding)
            match = CAPTION_RE.search(caption)
            tables.append(
                Table(
                    caption=caption,
                    number=(match.group(1).upper(), int(match.group(2)))
                    if match
                    else None,
                    rows=[faithful for faithful, _ in expanded],
                    spanned_rows=[spanned for _, spanned in expanded],
                )
            )
            preceding = []
    return tables


def normalise_year(cell: str) -> tuple[str, bool] | None:
    """Parse a year cell into ``("1970-71", is_estimate)``.

    Treasury marks projected years with a trailing ``(e)`` and leaves outcomes
    unmarked. Reading that flag off the document is what makes the OUTCOME versus
    ESTIMATE split a recorded fact rather than an inference from the release date.
    """
    match = YEAR_RE.match(cell)
    if not match:
        return None
    financial_year = f"{match.group(1)}-{match.group(2)}"
    lowered = cell.lower()
    is_estimate = "(e)" in lowered or "(est" in lowered or "(p)" in lowered
    return financial_year, is_estimate


_NUMBER_RE = re.compile(r"^-?[0-9][0-9,]*(\.[0-9]+)?$")


def parse_number(cell: str) -> float | None:
    """Parse a table cell to a float, or None when it holds no number.

    Treasury writes negatives with the ASCII hyphen, the Unicode minus and the en
    dash; uses ``na`` and ``-`` for unavailable values; and footnotes some cells
    with a trailing letter in parentheses.
    """
    if not cell:
        return None
    # Treasury writes negatives with the Unicode minus, the en dash and the
    # non-breaking hyphen as well as the ASCII hyphen.
    text = cell.replace("−", "-").replace("–", "-").replace("‑", "-")  # noqa: RUF001
    text = re.sub(r"\([a-z]\)", "", text, flags=re.I).strip()
    text = text.replace(" ", "").replace("\xa0", "")
    if not text or text.lower() in {"na", "n/a", "-", "..", "nan", "nil"}:
        return None
    if not _NUMBER_RE.match(text):
        return None
    return float(text.replace(",", ""))
