"""Parse INE's own dictionaries into variable labels and value labels.

Two source dictionaries, both official and both from ``gs://bktdescargascenso2024``:

1. ``CPV2024.dicX`` - the Redatam XML dictionary shipped inside
   ``Microdatos_Redatam_Censo2024.zip``. This is the ONLY published artefact that
   carries value labels for the microdata. The Excel dictionary INE's own
   "Presentacion de microdatos" refers to was dropped when the microdata zip was
   re-uploaded on 2025-12-13; the current zip holds three parquet files and
   nothing else.

2. ``diccionario_variables_glosas_censo2024.xlsx`` - describes the 189 aggregate
   variables and the geographic identifiers of the manzana-entidad base.

IMPORTANT: the Redatam dictionary is used for variable labels and value LABELS
only. Its sentinel codes are Redatam recodes and do not occur in the parquet
microdata - see ``constants.SENTINEL_LABELS``.
"""

from __future__ import annotations

import re
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

from constants import REDATAM_DIR, SENTINEL_LABELS

REDATAM_DICTIONARY = REDATAM_DIR / "CPV2024.dicX"


def _text(element: ET.Element, tag: str) -> str | None:
    found = element.find(tag)
    return found.text if found is not None else None


def _clean(value: str | None) -> str:
    """Collapse whitespace, including the NBSP that INE's labels are littered with."""
    if value is None:
        return ""
    value = value.replace("\xa0", " ").replace("​", "")
    return re.sub(r"\s+", " ", value).strip()


def strip_question_number(label: str) -> str:
    """Drop the leading questionnaire number from a variable label.

    INE labels read ``21. ¿Cuántos años cumplidos tiene?``. The question number is
    already carried by the column name (``p21_...``), so the description keeps only
    the question text.
    """
    return re.sub(r"^\s*\d+[a-z]?\.\s*", "", label).strip()


def ascii_fold(value: str) -> str:
    """Strip accents. Used only for comparisons, never for stored values."""
    return "".join(
        c
        for c in unicodedata.normalize("NFD", value)
        if unicodedata.category(c) != "Mn"
    )


@dataclass
class Variable:
    name: str
    entity: str
    label: str
    value_labels: dict[str, str] = field(default_factory=dict)

    @property
    def description(self) -> str:
        return strip_question_number(self.label)

    @property
    def is_coded(self) -> bool:
        return bool(self.value_labels)


# ``cod_caenes`` is stored in the microdata as the ISIC Rev.4 section LETTER
# (A-U), but the Redatam dictionary keys the very same classification by ORDINAL
# NUMBER (1-21). The two label sets were compared item by item against the
# aggregate dictionary's ``n_caenes_<letter>`` descriptions and agree exactly, in
# order, so the letters are remapped onto the numeric codes. Without this, every
# CAENES value in the data is unlabelled - which is how the gap was found.
CAENES_LETTERS = "ABCDEFGHIJKLMNOPQRSTU"
CAENES_LETTER_TO_CODE = {
    letter: str(index + 1) for index, letter in enumerate(CAENES_LETTERS)
}


def _remap_caenes(variable: Variable) -> None:
    """Rekey CAENES value labels from ordinal numbers onto section letters."""
    remapped = {
        letter: variable.value_labels[code]
        for letter, code in CAENES_LETTER_TO_CODE.items()
        if code in variable.value_labels
    }
    if len(remapped) != len(CAENES_LETTERS):
        raise ValueError(
            "CAENES remap incomplete: expected labels for all 21 ISIC sections, "
            f"resolved {len(remapped)}. INE may have changed the classification."
        )
    # Keep the non-ordinal codes (999 'Respuesta no codificable') as published.
    for code, label in variable.value_labels.items():
        if code not in CAENES_LETTER_TO_CODE.values():
            remapped[code] = label
    variable.value_labels = remapped


def load_redatam_dictionary() -> dict[str, Variable]:
    """Return ``{lowercase variable name: Variable}`` from the Redatam dictionary."""
    if not REDATAM_DICTIONARY.exists():
        raise FileNotFoundError(
            f"Redatam dictionary not found at {REDATAM_DICTIONARY}. "
            f"Run download.py first."
        )

    root = ET.parse(REDATAM_DICTIONARY).getroot()
    variables: dict[str, Variable] = {}

    for entity in root.iter("entity"):
        entity_name = _clean(_text(entity, "name"))
        for element in entity.findall("variable"):
            name = _clean(_text(element, "name"))
            if not name:
                continue
            variable = Variable(
                name=name.lower(),
                entity=entity_name,
                label=_clean(_text(element, "label")),
            )
            value_labels = element.find("valueLabels")
            if value_labels is not None:
                for item in value_labels:
                    code = _clean(_text(item, "value"))
                    label = _clean(_text(item, "label"))
                    if code != "":
                        variable.value_labels[code] = label
            variables[variable.name] = variable

    if "cod_caenes" in variables:
        _remap_caenes(variables["cod_caenes"])

    return variables


def dictionary_rows(
    variables: dict[str, Variable],
    table_columns: dict[str, list[str]],
    observed_codes: dict[tuple[str, str], set[str]],
) -> list[dict[str, str]]:
    """Build the rows of the ``dicionario`` table.

    One row per (table, column, code). Codes actually present in the data but
    absent from INE's dictionary are emitted with the sentinel label when they are
    one of the known sentinels, and are otherwise reported by the caller - a code
    with no label is a defect to surface, never to paper over.
    """
    rows: list[dict[str, str]] = []
    for table, columns in table_columns.items():
        for column in columns:
            variable = variables.get(column)
            if variable is None or not variable.is_coded:
                continue
            # INE's own per-variable label always wins; the generic sentinel
            # labels only fill codes INE left unlabelled. Reversing this would
            # overwrite e.g. cod_caenes 999 "Respuesta no codificable".
            labels = dict(SENTINEL_LABELS)
            labels.update(variable.value_labels)
            for code in sorted(
                observed_codes.get((table, column), set()), key=_code_sort_key
            ):
                label = labels.get(code)
                if label is None:
                    continue
                # The dicionario table keeps BD's fixed PORTUGUESE schema even on a
                # Spanish-language dataset: the custom_dictionary_coverage test
                # macro binds to id_tabela/nome_coluna/chave/valor by name. Same
                # choice as cl_chilecompra_mercado_publico. Only the dicionario is
                # exempt from the "column names follow the data's language" rule.
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": code,
                        "cobertura_temporal": "",
                        "valor": label,
                    }
                )
    return rows


def unlabelled_codes(
    variables: dict[str, Variable],
    table_columns: dict[str, list[str]],
    observed_codes: dict[tuple[str, str], set[str]],
) -> dict[tuple[str, str], set[str]]:
    """Codes present in the data that neither INE nor the sentinel set explains."""
    gaps: dict[tuple[str, str], set[str]] = {}
    for table, columns in table_columns.items():
        for column in columns:
            variable = variables.get(column)
            if variable is None or not variable.is_coded:
                continue
            known = set(variable.value_labels) | set(SENTINEL_LABELS)
            missing = observed_codes.get((table, column), set()) - known
            if missing:
                gaps[(table, column)] = missing
    return gaps


def _code_sort_key(code: str) -> tuple[int, float, str]:
    """Sort numeric codes numerically, with letter codes after them."""
    try:
        return (0, float(code), "")
    except ValueError:
        return (1, 0.0, code)
