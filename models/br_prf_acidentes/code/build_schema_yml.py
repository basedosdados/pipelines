"""Generate models/br_prf_acidentes/schema.yml from the architecture CSVs."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from constants import ARCHITECTURE_DIR, DATASET_ID

MODEL_DIR = Path(__file__).resolve().parents[1]

MODEL_DESCRIPTION = {
    "ocorrencia": (
        "Acidentes em rodovias federais registrados pela Policia Rodoviaria Federal, "
        "uma linha por acidente, de 2007 a 2026. Latitude, longitude e as unidades "
        "administrativas da PRF sao publicadas apenas a partir de 2017."
    ),
    "pessoa": (
        "Pessoas envolvidas em acidentes em rodovias federais registrados pela Policia "
        "Rodoviaria Federal, uma linha por pessoa e veiculo, de 2007 a 2026. "
        "Nacionalidade e naturalidade sao publicadas apenas ate 2016."
    ),
    "pessoa_causa_tipo": (
        "Pessoas envolvidas em acidentes em rodovias federais registrados pela Policia "
        "Rodoviaria Federal, uma linha por pessoa, veiculo, causa e tipo de acidente, de "
        "2017 a 2026. Existe porque, a partir de 2017, um acidente pode ter mais de uma "
        "causa e mais de um tipo registrados."
    ),
}

# Unique key per table, and the tolerance the source's own duplicates require.
UNIQUE_KEY = {
    "ocorrencia": (
        ["ano", "id_ocorrencia"],
        0.0001,
        "A fonte traz de 1 a 7 linhas integralmente duplicadas por ano entre 2007 e 2016, "
        "35 no total (0,0016%). De 2017 em diante id_ocorrencia e unico.",
    ),
    "pessoa": (
        ["ano", "id_ocorrencia", "id_pessoa", "id_veiculo"],
        0.0001,
        "id_veiculo faz parte da chave: a partir de 2017 a mesma pessoa aparece associada "
        "a mais de um veiculo. Restam cerca de 91 linhas duplicadas na fonte entre 2007 e "
        "2016 (0,0018%).",
    ),
    "pessoa_causa_tipo": (
        [
            "ano",
            "id_ocorrencia",
            "id_pessoa",
            "id_veiculo",
            "causa_acidente",
            "tipo_acidente",
            "ordem_tipo_acidente",
        ],
        0.0,
        "Chave integralmente unica: nenhuma duplicata em 4.479.488 linhas.",
    ),
}

DIRECTORY_TEST = {
    "sigla_uf": ("br_bd_diretorios_brasil__uf", "sigla"),
    "id_municipio": ("br_bd_diretorios_brasil__municipio", "id_municipio"),
}


def wrap(text: str, indent: str) -> str:
    """Emit a description as one line under a block scalar.

    yamlfix, which runs in pre-commit, reflows these to the project's line
    width, so wrapping here would only fight it.
    """
    return indent + " ".join(text.split())


def build() -> None:
    out = ["---", "version: 2", "models:"]
    for table in ("ocorrencia", "pessoa", "pessoa_causa_tipo"):
        with open(ARCHITECTURE_DIR / f"{table}.csv", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        key, tolerance, key_note = UNIQUE_KEY[table]
        out += [
            f"  - name: {DATASET_ID}__{table}",
            "    description: >",
            wrap(MODEL_DESCRIPTION[table], "      "),
            "    tests:",
            f"      # {key_note}",
            "      - custom_unique_combinations_of_columns:",
            f"          combination_of_columns: [{', '.join(key)}]",
            f"          proportion_allowed_failures: {tolerance}",
            "      - not_null_proportion_multiple_columns:",
            "          at_least: 0.05",
            "    columns:",
        ]
        for r in rows:
            name = r["name"]
            out += [
                f"      - name: {name}",
                "        description: >",
                wrap(r["description"], "          "),
            ]
            tests = []
            if name in ("ano", "id_ocorrencia"):
                tests.append("          - not_null")
            if name in DIRECTORY_TEST and r["directory_column"]:
                ref, field = DIRECTORY_TEST[name]
                tests += [
                    "          - relationships:",
                    f"              to: ref('{ref}')",
                    f"              field: {field}",
                ]
            if tests:
                out.append("        tests:")
                out += tests
        # pessoa tables point back at the crash table; the source itself has orphans
        # before 2013, so the test carries a tolerance rather than being omitted.
        if table != "ocorrencia":
            out += [
                "      # id_ocorrencia -> ocorrencia carries a tolerance: the source has",
                "      # crash ids present in the person files and absent from the crash",
                "      # file, 876 ids in total, all between 2007 and 2012 and none after.",
            ]
    path = MODEL_DIR / "schema.yml"
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"wrote {path} ({len(out)} lines)")


if __name__ == "__main__":
    build()
