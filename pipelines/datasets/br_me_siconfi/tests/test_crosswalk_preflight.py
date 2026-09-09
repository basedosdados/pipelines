"""Regression tests for the crosswalk preflight.

The preflight reads account keys straight from the raw JSON so a crosswalk gap
fails a run in minutes instead of after the ~17h download (flow run 01a059bc,
2026-09-02). That speed is bought by *re-deriving* the join key outside the
builders, which is only safe while the derivation stays identical to theirs.
These tests pin that equivalence: every one of them fails if
``code/tables_final/`` changes how it builds the key and ``_COMP_SPEC`` /
``_account_keys`` are not updated to match.

The ``lstrip("0")`` case is not hypothetical — the first draft of the preflight
missed it and reported all 260 função codes of 2025 as gaps.
"""

from __future__ import annotations

import inspect
import json

import pandas as pd
import pytest

from pipelines.datasets.br_me_siconfi import utils

ANEXOS = {"DCA-Anexo I-C", "DCA-Anexo I-D", "DCA-Anexo I-E", "DCA-Anexo I-AB"}

# conta strings that exercise every branch of ``apply_conta_split``: the normal
# "CODE - NAME", a função code carrying leading zeros, a label with no code at
# all, the two mojibake dashes the API emits, and an empty value.
CONTA_SAMPLES = [
    "1.1.1.4.51.2.0 - Adicional ISS - Fundo Municipal de Combate à Pobreza",
    "01.031 - Ação Legislativa",
    "1.0.0.0.0.00.00 - Ativo",
    "RECEITAS (EXCETO INTRA-ORÇAMENTÁRIAS) (I)",
    "1.7.1.8.05.1.0 � Transferências de Recursos do SUS",
    "2.4.1.8.02.0.0 ¿ Transferências de Convênios",
    "  1.2.0.0.00.0.0   -   Receitas de Capital  ",
    "",
]


def _items(contas, anexo="DCA-Anexo I-C", coluna="Receitas Brutas Realizadas"):
    return [
        {"anexo": anexo, "coluna": coluna, "conta": c, "valor": 1.0}
        for c in contas
    ]


class TestKeyDerivationMatchesTheBuilders:
    """``_account_keys`` must split conta exactly as ``apply_conta_split`` does."""

    def test_split_agrees_with_apply_conta_split(self):
        shared = utils._shared()
        expected = shared.apply_conta_split(
            pd.DataFrame({"conta": CONTA_SAMPLES})
        )

        keys = utils._account_keys(
            _items(CONTA_SAMPLES), 2025, "municipio", ANEXOS
        )

        assert len(keys) == len(CONTA_SAMPLES)
        for (_, _, _, _, portaria, conta), (_, row) in zip(
            keys, expected.iterrows(), strict=True
        ):
            assert portaria == row["portaria"]
            assert conta == row["conta"]

    def test_estagio_is_the_apis_coluna(self):
        (key,) = utils._account_keys(
            _items(["1.0.0.0.0.00.00 - Ativo"], coluna="Deduções - FUNDEB"),
            2025,
            "uf",
            ANEXOS,
        )
        assert key[3] == "Deduções - FUNDEB"

    def test_anexo_gets_the_dca_prefix(self):
        # load_year_data re-prefixes any anexo the API sends bare.
        (key,) = utils._account_keys(
            [{"anexo": "Anexo I-C", "coluna": "x", "conta": "1 - y"}],
            2025,
            "brasil",
            ANEXOS,
        )
        assert key[2] == "DCA-Anexo I-C"

    def test_unchecked_anexos_are_dropped(self):
        # I-F/I-G/I-HI back no crosswalk; skipping them halves the scan.
        assert not utils._account_keys(
            _items(["1 - x"], anexo="DCA-Anexo I-F"), 2025, "uf", ANEXOS
        )

    @pytest.mark.parametrize(
        ("filename", "level"),
        [
            ("dca_2025_1.json", "brasil"),
            ("dca_2025_35.json", "uf"),
            ("dca_2025_3550308.json", "municipio"),
            ("municipio/dca_2025_3550308.json", "municipio"),
        ],
    )
    def test_level_comes_from_the_code_length(self, filename, level):
        # Same rule as load_year_data — the directory is not authoritative.
        assert utils._level_of_file(filename, 2025) == level

    def test_scan_of_a_download_tree_matches_a_direct_split(self, tmp_path):
        api_dir = tmp_path / "input" / "api"
        (api_dir / "municipio").mkdir(parents=True)
        (api_dir / "municipio" / "dca_2025_3550308.json").write_text(
            json.dumps({"data": {"items": _items(CONTA_SAMPLES)}}),
            encoding="utf-8",
        )

        scanned = utils.scan_keys_api_dir(
            str(api_dir), [2025], ["municipio"], ANEXOS
        )

        assert scanned == set(
            utils._account_keys(
                _items(CONTA_SAMPLES), 2025, "municipio", ANEXOS
            )
        )


class TestCompSpecTracksTheBuilders:
    """``_COMP_SPEC`` is a hand-mirrored copy of builder-local logic."""

    def test_every_crosswalk_backed_table_has_a_spec(self):
        # A new crosswalk-backed table must not silently skip the preflight.
        comp_files = {
            spec[2] for spec in utils._build_registry().values() if spec[2]
        }
        assert comp_files <= set(utils._COMP_SPEC)

    def test_targets_cover_every_crosswalk_backed_table(self):
        tables = utils.tables_for_levels(("brasil", "uf", "municipio"))
        builders = utils._build_registry()
        expected = {t for t in tables if builders[t][2]}

        targets = utils._crosswalk_targets(tables)

        assert {builders[t][2] for t in expected} == set(targets.values())

    def test_lstrip_zeros_marks_exactly_the_builders_that_do_it(self):
        """Only despesas_funcao strips leading zeros — assert that, from source.

        The API sends função code "01.031"; the crosswalk holds "1.031". Miss
        this and every função code reads as a gap.
        """
        import importlib

        utils._ensure_code_on_path()
        builders = utils._build_registry()
        for table, (_, _, comp_file) in builders.items():
            if not comp_file:
                continue
            source = inspect.getsource(
                importlib.import_module(f"tables_final.{table}")
            )
            strips = '["portaria"].str.lstrip("0")' in source
            assert strips == utils._COMP_SPEC[comp_file].lstrip_zeros, table


class TestGapDetection:
    """The gap check reproduces the builders' left join plus get_unmatched."""

    def test_a_key_absent_from_the_crosswalk_is_reported(self):
        gaps = utils.crosswalk_gaps(
            [
                (
                    2025,
                    "municipio",
                    "DCA-Anexo I-C",
                    "Receitas Brutas Realizadas",
                    "9.9.9.9.99.9.9",
                    "Conta Inventada",
                )
            ],
            ["municipio_receitas_orcamentarias"],
        )

        assert set(gaps) == {"receitas_orcamentarias"}
        assert gaps["receitas_orcamentarias"].to_dict("records") == [
            {
                "ano": "2025",
                "estagio": "Receitas Brutas Realizadas",
                "portaria": "9.9.9.9.99.9.9",
                "conta": "Conta Inventada",
            }
        ]

    def test_the_keys_closed_on_2026_09_02_now_resolve(self):
        # The five gaps that failed flow run 01a059bc after ~18h.
        closed = [
            (
                "Deduções - FUNDEB",
                "1.1.1.4.51.2.0",
                "Adicional ISS - Fundo Municipal de Combate à Pobreza",
            ),
            (
                "Deduções - FUNDEB",
                "1.3.9.0.00.0.0",
                "Demais Receitas Patrimoniais",
            ),
            (
                "Outras Deduções da Receita",
                "1.6.2.1.04.2.0",
                "Adicional sobre Tarifa Aeroportuária",
            ),
            (
                "Outras Deduções da Receita",
                "1.7.4.1.51.0.0",
                "Transferências de Convênios de Instituições Privadas para "
                "Programas de Educação",
            ),
            (
                "Receitas Brutas Realizadas",
                "1.3.4.6.04.0.0",
                "Contratos de Transição de Concessão Florestal",
            ),
        ]

        gaps = utils.crosswalk_gaps(
            [
                (2025, "municipio", "DCA-Anexo I-C", est, port, conta)
                for est, port, conta in closed
            ],
            ["municipio_receitas_orcamentarias"],
        )

        assert gaps == {}

    def test_funcao_codes_keep_their_leading_zero_in_the_source(self):
        # "01.031" from the API is "1.031" in despesas_funcao.xlsx.
        assert (
            utils.crosswalk_gaps(
                [
                    (
                        2025,
                        "municipio",
                        "DCA-Anexo I-E",
                        "Despesas Empenhadas",
                        "01.031",
                        "Ação Legislativa",
                    )
                ],
                ["municipio_despesas_funcao"],
            )
            == {}
        )

    def test_balanco_ignores_everything_but_the_year_end_snapshot(self):
        # municipio_balanco_patrimonial filters to estagio == "31/12/<ano>"
        # before merging, so an off-snapshot column is not a gap.
        key = (
            2025,
            "municipio",
            "DCA-Anexo I-AB",
            "31/03/2025",
            "9.9.9.9.99.9.9",
            "Conta Inventada",
        )

        assert (
            utils.crosswalk_gaps([key], ["municipio_balanco_patrimonial"])
            == {}
        )

        eoy = (*key[:3], "31/12/2025", *key[4:])
        assert utils.crosswalk_gaps([eoy], ["municipio_balanco_patrimonial"])

    def test_tables_without_a_crosswalk_check_nothing(self):
        assert (
            utils.crosswalk_gaps(
                [(2025, "uf", "DCA-Anexo I-F", "x", "1", "y")],
                ["uf_execucao_restos_pagar"],
            )
            == {}
        )


class TestFailureReport:
    """The operator has to hand-edit every key the message prints."""

    def test_no_gaps_does_not_raise(self):
        utils.raise_on_crosswalk_gaps({}, "nothing")

    def test_message_names_the_file_the_source_and_every_key(self):
        gaps = {
            "receitas_orcamentarias": pd.DataFrame(
                [
                    {
                        "ano": "2025",
                        "estagio": "e",
                        "portaria": "1",
                        "conta": "c",
                    }
                ]
            )
        }

        with pytest.raises(RuntimeError) as excinfo:
            utils.raise_on_crosswalk_gaps(gaps, "the previous run's archive")
        message = str(excinfo.value)

        assert "receitas_orcamentarias.xlsx" in message
        assert "1 unmatched key(s)" in message
        assert "the previous run's archive" in message
        assert "*_bd" in message

    def test_a_large_gap_list_is_truncated_with_a_count(self):
        n = utils._MAX_REPORTED_GAPS + 30
        gaps = {
            "despesas_orcamentarias": pd.DataFrame(
                {
                    "ano": ["2025"] * n,
                    "estagio": ["e"] * n,
                    "portaria": [str(i) for i in range(n)],
                    "conta": ["c"] * n,
                }
            )
        }

        with pytest.raises(RuntimeError, match="and 30 more"):
            utils.raise_on_crosswalk_gaps(gaps, "src")
