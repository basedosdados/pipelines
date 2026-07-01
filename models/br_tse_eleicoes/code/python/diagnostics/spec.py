"""Registry of builder functions, tables, year domains and raw file families.

This is the harness's single source of per-table truth. Everything here is
hand-curated from ``build.py``/``sub/*.py`` dispatch logic and reviewed by eye —
the AST extractor (tier 1) only extracts *mappings*; year domains and raw file
families come from this spec so they stay reviewable.
"""

from dataclasses import dataclass, field

EVEN_YEARS = list(range(1994, 2025, 2))
HIST_YEARS = [
    1945, 1947, 1950, 1954, 1955, 1958, 1960, 1962, 1965, 1966,
    1970, 1974, 1978, 1982, 1986, 1989, 1990,
]  # fmt: skip


# ---------------------------------------------------------------------------
# Raw file families -> where to find the official layout doc (leiame)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Family:
    """A TSE raw file family (one official layout per family x year)."""

    name: str
    # subdirectory patterns under TSE_DATA_DIR/input where an extracted
    # leiame.pdf may already exist ({ano} substituted)
    local_dirs: tuple[str, ...]
    # remote zip URL templates, tried in order ({ano}/{uf} substituted)
    url_templates: tuple[str, ...]
    # whether the remote zip is per-UF (use a small UF to fetch the leiame)
    per_uf: bool = False
    # regex selecting which data member's header row to use (zips that bundle
    # several different file types, e.g. prestacao receitas vs despesas)
    member_hint: str = ""
    # regex selecting the "TIPO DO ARQUIVO <name>" section in multi-section
    # historical leiames
    section_hint: str = ""


_CDN = "https://cdn.tse.jus.br/estatistica/sead/odsele"

FAMILIES: dict[str, Family] = {
    f.name: f
    for f in [
        Family(
            "consulta_cand",
            ("consulta_cand/consulta_cand_{ano}",),
            (f"{_CDN}/consulta_cand/consulta_cand_{{ano}}.zip",),
        ),
        Family(
            "consulta_cand_complementar",
            ("consulta_cand/consulta_cand_complementar_{ano}",),
            (
                f"{_CDN}/consulta_cand_complementar/consulta_cand_complementar_{{ano}}.zip",
                f"{_CDN}/consulta_cand/consulta_cand_complementar_{{ano}}.zip",
            ),
        ),
        Family(
            "consulta_coligacao",
            (
                "consulta_coligacao/consulta_coligacao_{ano}",
                "consulta_coligacao/consulta_legendas_{ano}",
                "consulta_coligacao/CONSULTA_LEGENDA_{ano}",
            ),
            (
                f"{_CDN}/consulta_coligacao/consulta_coligacao_{{ano}}.zip",
                f"{_CDN}/consulta_legendas/consulta_legendas_{{ano}}.zip",
            ),
        ),
        Family(
            "consulta_vagas",
            ("consulta_vagas/consulta_vagas_{ano}",),
            (f"{_CDN}/consulta_vagas/consulta_vagas_{{ano}}.zip",),
        ),
        Family(
            "bem_candidato",
            ("bem_candidato/bem_candidato_{ano}",),
            (f"{_CDN}/bem_candidato/bem_candidato_{{ano}}.zip",),
        ),
        # prestacao de contas: the zip name changes per generation but each
        # year resolves through the template list; receitas and despesas are
        # distinct layouts inside the same zip -> two families w/ member hints
        Family(
            "prestacao_receitas",
            (
                "prestacao_contas/prestacao_contas_{ano}",
                "prestacao_contas/prestacao_final_{ano}",
                "prestacao_contas/prestacao_contas_final_{ano}",
                "prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{ano}",
            ),
            (
                f"{_CDN}/prestacao_contas/prestacao_contas_{{ano}}.zip",
                f"{_CDN}/prestacao_contas/prestacao_final_{{ano}}.zip",
                f"{_CDN}/prestacao_contas/prestacao_contas_final_{{ano}}.zip",
                f"{_CDN}/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{{ano}}.zip",
            ),
            member_hint=(
                r"(?i)(receitas_candidatos_\d|ReceitaCandidato\.|ReceitasCandidatos\.)"
            ),
            section_hint=r"(?i)receitas?\s+(de\s+)?candidatos?",
        ),
        Family(
            "prestacao_despesas",
            (
                "prestacao_contas/prestacao_contas_{ano}",
                "prestacao_contas/prestacao_final_{ano}",
                "prestacao_contas/prestacao_contas_final_{ano}",
                "prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{ano}",
            ),
            (
                f"{_CDN}/prestacao_contas/prestacao_contas_{{ano}}.zip",
                f"{_CDN}/prestacao_contas/prestacao_final_{{ano}}.zip",
                f"{_CDN}/prestacao_contas/prestacao_contas_final_{{ano}}.zip",
                f"{_CDN}/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{{ano}}.zip",
            ),
            member_hint=(
                r"(?i)(despesas_contratadas_candidatos_\d"
                r"|despesas_candidatos_(prestacao_contas_final_)?\d"
                r"|DespesaCandidato\.|DespesasCandidatos\.)"
            ),
            section_hint=r"(?i)despesas?\s+(contratadas\s+)?(de\s+|pelos\s+)?candidatos?",
        ),
        Family(
            "votacao_candidato_munzona",
            ("votacao_candidato_munzona/votacao_candidato_munzona_{ano}",),
            (
                f"{_CDN}/votacao_candidato_munzona/votacao_candidato_munzona_{{ano}}.zip",
            ),
        ),
        Family(
            "votacao_partido_munzona",
            ("votacao_partido_munzona/votacao_partido_munzona_{ano}",),
            (
                f"{_CDN}/votacao_partido_munzona/votacao_partido_munzona_{{ano}}.zip",
            ),
        ),
        Family(
            "votacao_secao",
            ("votacao_secao/votacao_secao_{ano}_{uf}",),
            (f"{_CDN}/votacao_secao/votacao_secao_{{ano}}_{{uf}}.zip",),
            per_uf=True,
        ),
        Family(
            "detalhe_votacao_munzona",
            ("detalhe_votacao_munzona/detalhe_votacao_munzona_{ano}",),
            (
                f"{_CDN}/detalhe_votacao_munzona/detalhe_votacao_munzona_{{ano}}.zip",
            ),
        ),
        Family(
            "detalhe_votacao_secao",
            ("detalhe_votacao_secao/detalhe_votacao_secao_{ano}",),
            (
                f"{_CDN}/detalhe_votacao_secao/detalhe_votacao_secao_{{ano}}.zip",
                f"{_CDN}/detalhe_votacao_secao/detalhe_votacao_secao_{{ano}}_{{uf}}.zip",
            ),
        ),
        Family(
            "perfil_eleitorado",
            ("perfil_eleitorado/perfil_eleitorado_{ano}",),
            (f"{_CDN}/perfil_eleitorado/perfil_eleitorado_{{ano}}.zip",),
        ),
        Family(
            "perfil_eleitor_secao",
            (
                "perfil_eleitorado_secao/perfil_eleitor_secao_{ano}_{uf}",
                "perfil_eleitorado_secao",
            ),
            (
                f"{_CDN}/perfil_eleitor_secao/perfil_eleitor_secao_{{ano}}_{{uf}}.zip",
            ),
            per_uf=True,
        ),
        Family(
            "eleitorado_local_votacao",
            (
                "perfil_eleitorado_local_votacao/eleitorado_local_votacao_{ano}",
            ),
            (
                f"{_CDN}/eleitorado_locais_votacao/eleitorado_local_votacao_{{ano}}.zip",
                f"{_CDN}/eleitorado_local_votacao/eleitorado_local_votacao_{{ano}}.zip",
            ),
        ),
        Family(
            "votacao_candidato_uf",
            (
                "votacao_candidato_uf/VOTACAO_CANDIDATO_UF_{ano}",
                "votacao_candidato_uf/VOTACAO_CANDIDATO_{ano}",
            ),
            (f"{_CDN}/votacao_candidato_uf/votacao_candidato_uf_{{ano}}.zip",),
            section_hint=r"VOTACAO\s+CANDIDATO\s+UF",
        ),
        Family(
            "votacao_partido_uf",
            (
                "votacao_partido_uf/VOTACAO_PARTIDO_UF_{ano}",
                "votacao_partido_uf/VOTACAO_PARTIDO_{ano}",
            ),
            (f"{_CDN}/votacao_partido_uf/votacao_partido_uf_{{ano}}.zip",),
            section_hint=r"VOTACAO\s+PARTIDO\s+UF",
        ),
        Family(
            "detalhe_votacao_uf",
            ("detalhe_votacao_uf/DETALHE_VOTACAO_UF_{ano}",),
            (f"{_CDN}/detalhe_votacao_uf/detalhe_votacao_uf_{{ano}}.zip",),
            section_hint=r"DETALHE\s+VOTACAO\s+UF",
        ),
    ]
}


# ---------------------------------------------------------------------------
# Builder functions -> tables, year domains, families
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FuncSpec:
    """One builder function that contains raw-schema mapping site(s)."""

    module: str  # path relative to code/python, e.g. "sub/candidates.py"
    function: str
    tables: tuple[str, ...]  # published table(s) this function feeds
    years: tuple[int, ...]  # year domain the function is dispatched for
    family: str  # raw file family (key into FAMILIES)
    # columns created by assignment (df["x"] = ...) are auto-detected by the
    # extractor; columns added later in phase 2/3 are declared here
    phase_added_cols: tuple[str, ...] = field(default=())


def _f(module, function, tables, years, family, added=()):
    return FuncSpec(
        module=module,
        function=function,
        tables=tuple(tables),
        years=tuple(years),
        family=family,
        phase_added_cols=tuple(added),
    )


_CF = "sub/campaign_finance.py"

FUNC_SPECS: list[FuncSpec] = [
    _f(
        "sub/candidates.py",
        "_parse_schema",
        ["candidatos"],
        EVEN_YEARS,
        "consulta_cand",
        added=("id_municipio", "idade"),
    ),
    _f(
        "sub/candidates.py",
        "build_candidatos",
        ["candidatos"],
        [2024],
        "consulta_cand_complementar",
    ),
    _f(
        "sub/parties.py",
        "build_partidos",
        ["partidos"],
        [1990, *EVEN_YEARS],
        "consulta_coligacao",
        added=("id_municipio",),
    ),
    _f(
        "sub/vacancies.py",
        "build_vagas",
        ["vagas"],
        EVEN_YEARS,
        "consulta_vagas",
        added=("id_municipio",),
    ),
    _f(
        _CF,
        "build_bens",
        ["bens_candidato"],
        list(range(2006, 2025, 2)),
        "bem_candidato",
        added=("titulo_eleitoral_candidato",),
    ),
    # receitas_candidato — one function per TSE file generation
    _f(
        _CF,
        "_build_receitas_2002",
        ["receitas_candidato"],
        [2002],
        "prestacao_receitas",
    ),
    _f(
        _CF,
        "_build_receitas_2004",
        ["receitas_candidato"],
        [2004],
        "prestacao_receitas",
    ),
    _f(
        _CF,
        "_build_receitas_2006",
        ["receitas_candidato"],
        [2006],
        "prestacao_receitas",
    ),
    _f(
        _CF,
        "_build_receitas_2008",
        ["receitas_candidato"],
        [2008],
        "prestacao_receitas",
    ),
    _f(
        _CF,
        "_build_receitas_2010",
        ["receitas_candidato"],
        [2010],
        "prestacao_receitas",
    ),
    _f(
        _CF,
        "_build_receitas_2012",
        ["receitas_candidato"],
        [2012],
        "prestacao_receitas",
    ),
    _f(
        _CF,
        "_build_receitas_2014",
        ["receitas_candidato"],
        [2014],
        "prestacao_receitas",
    ),
    _f(
        _CF,
        "_build_receitas_2016",
        ["receitas_candidato"],
        [2016],
        "prestacao_receitas",
    ),
    _f(
        _CF,
        "_build_receitas_2018_plus",
        ["receitas_candidato"],
        [2018, 2020, 2022, 2024],
        "prestacao_receitas",
    ),
    # despesas_candidato
    _f(
        _CF,
        "_build_despesas_2002",
        ["despesas_candidato"],
        [2002],
        "prestacao_despesas",
    ),
    _f(
        _CF,
        "_build_despesas_2004",
        ["despesas_candidato"],
        [2004],
        "prestacao_despesas",
    ),
    _f(
        _CF,
        "_build_despesas_2006",
        ["despesas_candidato"],
        [2006],
        "prestacao_despesas",
    ),
    _f(
        _CF,
        "_build_despesas_2008",
        ["despesas_candidato"],
        [2008],
        "prestacao_despesas",
    ),
    _f(
        _CF,
        "_build_despesas_2010",
        ["despesas_candidato"],
        [2010],
        "prestacao_despesas",
    ),
    _f(
        _CF,
        "_build_despesas_2012",
        ["despesas_candidato"],
        [2012],
        "prestacao_despesas",
    ),
    _f(
        _CF,
        "_build_despesas_2014",
        ["despesas_candidato"],
        [2014],
        "prestacao_despesas",
    ),
    _f(
        _CF,
        "_build_despesas_2016",
        ["despesas_candidato"],
        [2016],
        "prestacao_despesas",
    ),
    _f(
        _CF,
        "_build_despesas_2018_plus",
        ["despesas_candidato"],
        [2018, 2020, 2022, 2024],
        "prestacao_despesas",
    ),
    _f(
        "sub/results_mun_zone.py",
        "_build_candidato",
        ["resultados_candidato_municipio_zona"],
        EVEN_YEARS,
        "votacao_candidato_munzona",
        added=("id_municipio", "titulo_eleitoral_candidato"),
    ),
    _f(
        "sub/results_mun_zone.py",
        "_build_partido",
        ["resultados_partido_municipio_zona"],
        EVEN_YEARS,
        "votacao_partido_munzona",
        added=("id_municipio",),
    ),
    _f(
        "sub/results_section.py",
        "build_resultados_secao",
        ["resultados_candidato_secao", "resultados_partido_secao"],
        EVEN_YEARS,
        "votacao_secao",
        added=("id_municipio", "titulo_eleitoral_candidato"),
    ),
    _f(
        "sub/results_state.py",
        "build_candidato",
        ["resultados_candidato_uf"],
        HIST_YEARS,
        "votacao_candidato_uf",
    ),
    _f(
        "sub/results_state.py",
        "build_partido",
        ["resultados_partido_uf"],
        [y for y in HIST_YEARS if y != 1989],
        "votacao_partido_uf",
    ),
    _f(
        "sub/voting_details_mun_zone.py",
        "build_detalhes_mun_zona",
        ["detalhes_votacao_municipio_zona"],
        EVEN_YEARS,
        "detalhe_votacao_munzona",
        added=("id_municipio",),
    ),
    _f(
        "sub/voting_details_section.py",
        "build_detalhes_secao",
        ["detalhes_votacao_secao"],
        EVEN_YEARS,
        "detalhe_votacao_secao",
        added=("id_municipio",),
    ),
    _f(
        "sub/voting_details_state.py",
        "build_voting_details_state",
        ["detalhes_votacao_uf"],
        HIST_YEARS,
        "detalhe_votacao_uf",
    ),
    _f(
        "sub/voter_profile_mun_zone.py",
        "build_perfil_mun_zona",
        ["perfil_eleitorado_municipio_zona"],
        EVEN_YEARS,
        "perfil_eleitorado",
        added=("id_municipio",),
    ),
    _f(
        "sub/voter_profile_section.py",
        "build_perfil_secao",
        ["perfil_eleitorado_secao"],
        list(range(2008, 2025, 2)),
        "perfil_eleitor_secao",
        added=("id_municipio",),
    ),
    _f(
        "sub/voter_profile_polling_place.py",
        "build_perfil_local_votacao",
        ["perfil_eleitorado_local_votacao"],
        list(range(2010, 2025, 2)),
        "eleitorado_local_votacao",
        added=("id_municipio",),
    ),
]

# Phase-3 tables (built from other tables by aggregation.py — no raw v-mapping;
# tier 1 only checks their output columns against the canonical schema)
AGGREGATION_TABLES = [
    "resultados_candidato",
    "resultados_candidato_municipio",
    "resultados_partido_municipio",
    "detalhes_votacao_municipio",
    "receitas_comite",
    "receitas_orgao_partidario",
]


# Per-(family, ano) preference regex when a zip carries several layout
# variants of the same file type and the builder reads a specific one
MEMBER_PREFER: dict[tuple[str, int], str] = {
    # builder reads receitas_candidatos_2012_brasil (28 cols), not the
    # per-UF files (22 cols, different layout)
    ("prestacao_receitas", 2012): r"(?i)brasil",
}

# Mapping sites that cannot be checked against the family's standard layout
# (they parse a different file variant); reported as UNVERIFIED, not FAIL
SUPPRESSED_SITES: dict[tuple[str, int], str] = {
    ("sub/campaign_finance.py", 680): (
        "2014 supplementary file (receitas_..._sup) has its own layout, "
        "different from the per-UF receitas files"
    ),
    ("sub/results_section.py", 71): (
        "legacy variant for votacao_secao_1998_BR / votacao_secao_2008_TO "
        "only; those specific zips are absent from the current TSE portal"
    ),
}


def funcs_for(table: str | None = None) -> list[FuncSpec]:
    if table is None or table == "all":
        return list(FUNC_SPECS)
    return [fs for fs in FUNC_SPECS if table in fs.tables]


def modules() -> list[str]:
    return sorted({fs.module for fs in FUNC_SPECS})
