"""Write the se_gu_vdem architecture CSVs.

Usage:
    python models/se_gu_vdem/code/build_architecture.py

The country_year_index columns are the 118 ``v2x*`` indices, in source order.
Their English descriptions are the codebook's variable names; Portuguese and
Spanish come from INDEX_NAMES below, and the script fails on any index without a
translation, so a new V-Dem release that adds an index must extend the table.

Reads ``$SE_GU_VDEM_DATA_DIR/input/{vdem,codebook}.parquet`` (see
``extract_rdata.R``).
"""

import csv
import os
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

DATA_DIR = Path(
    os.environ.get(
        "SE_GU_VDEM_DATA_DIR",
        Path.home() / "Library" / "Caches" / "se_gu_vdem_data",
    )
)
ARCH_DIR = Path(__file__).parent / "architecture"
COVERAGE = "1789(1)2025"
YEAR_DIRECTORY = "br_bd_diretorios_data_tempo.ano:ano"

FIELDS = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
    "description_en",
    "description_es",
    "observations_en",
    "observations_es",
]


def col(
    name: str,
    bq_type: str,
    pt: str,
    en: str,
    es: str,
    *,
    original: str = "",
    unit: str = "",
    directory: str = "",
    dictionary: bool = False,
    coverage: str = "",
    obs: tuple[str, str, str] = ("", "", ""),
) -> dict[str, str]:
    return {
        "name": name,
        "bigquery_type": bq_type,
        "description": pt,
        "temporal_coverage": coverage,
        "covered_by_dictionary": "yes" if dictionary else "no",
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs[0],
        "original_name": original or name,
        "description_en": en,
        "description_es": es,
        "observations_en": obs[1],
        "observations_es": obs[2],
    }


YEAR = col(
    "year",
    "INT64",
    "Ano de referência",
    "Reference year",
    "Año de referencia",
    unit="year",
    directory=YEAR_DIRECTORY,
    coverage=COVERAGE,
)
COUNTRY_ID = col(
    "country_id",
    "STRING",
    "Identificador numérico do país ou unidade política no V-Dem",
    "V-Dem numeric identifier of the country or political unit",
    "Identificador numérico del país o unidad política en V-Dem",
    obs=(
        "Inclui unidades históricas que não existem mais (ex.: Baviera); ver a tabela country",
        "Includes historical units that no longer exist (e.g. Bavaria); see the country table",
        "Incluye unidades históricas que ya no existen (p. ej., Baviera); ver la tabla country",
    ),
)

ESTIMATE_OBS = (
    "A escala depende da variável; ver scale na tabela indicator",
    "The scale depends on the variable; see scale in the indicator table",
    "La escala depende de la variable; ver scale en la tabla indicator",
)


def estimate(name: str, original: str, pt: str, en: str, es: str) -> dict:
    return col(
        name, "FLOAT64", pt, en, es, original=original, obs=ESTIMATE_OBS
    )


COUNTRY_YEAR = [
    YEAR,
    COUNTRY_ID,
    col(
        "variable_id",
        "STRING",
        "Identificador da variável no V-Dem",
        "V-Dem variable identifier",
        "Identificador de la variable en V-Dem",
        original="(column name without suffix)",
        obs=(
            "Descrita na tabela indicator",
            "Described in the indicator table",
            "Descrita en la tabla indicator",
        ),
    ),
    estimate(
        "value",
        "(no suffix)",
        "Valor da variável: estimativa pontual do modelo de medição para variáveis codificadas por especialistas, ou o valor observado",
        "Value of the variable: the measurement model's point estimate for expert-coded variables, or the observed value",
        "Valor de la variable: estimación puntual del modelo de medición para variables codificadas por expertos, o el valor observado",
    ),
    col(
        "value_text",
        "STRING",
        "Valor textual da variável, para as variáveis de nomes (partidos, chefes de Estado e de governo, instituições)",
        "Text value of the variable, for name variables (parties, heads of state and government, institutions)",
        "Valor textual de la variable, para las variables de nombres (partidos, jefes de Estado y de gobierno, instituciones)",
        original="(no suffix)",
    ),
    estimate(
        "code_low",
        "_codelow",
        "Limite inferior do intervalo de maior densidade posterior de 68% da estimativa",
        "Lower bound of the 68% highest posterior density interval of the estimate",
        "Límite inferior del intervalo de mayor densidad posterior del 68% de la estimación",
    ),
    estimate(
        "code_high",
        "_codehigh",
        "Limite superior do intervalo de maior densidade posterior de 68% da estimativa",
        "Upper bound of the 68% highest posterior density interval of the estimate",
        "Límite superior del intervalo de mayor densidad posterior del 68% de la estimación",
    ),
    estimate(
        "sd",
        "_sd",
        "Desvio padrão posterior da estimativa",
        "Posterior standard deviation of the estimate",
        "Desviación estándar posterior de la estimación",
    ),
    estimate(
        "osp",
        "_osp",
        "Estimativa na escala original da pergunta (linearizada)",
        "Estimate on the original scale of the question (linearized)",
        "Estimación en la escala original de la pregunta (linealizada)",
    ),
    estimate(
        "osp_code_low",
        "_osp_codelow",
        "Limite inferior do intervalo de 68% da estimativa na escala original",
        "Lower bound of the 68% interval of the original-scale estimate",
        "Límite inferior del intervalo del 68% de la estimación en la escala original",
    ),
    estimate(
        "osp_code_high",
        "_osp_codehigh",
        "Limite superior do intervalo de 68% da estimativa na escala original",
        "Upper bound of the 68% interval of the original-scale estimate",
        "Límite superior del intervalo del 68% de la estimación en la escala original",
    ),
    estimate(
        "osp_sd",
        "_osp_sd",
        "Desvio padrão posterior da estimativa na escala original",
        "Posterior standard deviation of the original-scale estimate",
        "Desviación estándar posterior de la estimación en la escala original",
    ),
    estimate(
        "ord",
        "_ord",
        "Estimativa na escala ordinal da pergunta (categoria mais provável)",
        "Estimate on the ordinal scale of the question (most likely category)",
        "Estimación en la escala ordinal de la pregunta (categoría más probable)",
    ),
    estimate(
        "ord_code_low",
        "_ord_codelow",
        "Limite inferior do intervalo de 68% da estimativa ordinal",
        "Lower bound of the 68% interval of the ordinal estimate",
        "Límite inferior del intervalo del 68% de la estimación ordinal",
    ),
    estimate(
        "ord_code_high",
        "_ord_codehigh",
        "Limite superior do intervalo de 68% da estimativa ordinal",
        "Upper bound of the 68% interval of the ordinal estimate",
        "Límite superior del intervalo del 68% de la estimación ordinal",
    ),
    estimate(
        "mean",
        "_mean",
        "Média simples das notas dos especialistas, sem o modelo de medição",
        "Simple mean of the experts' ratings, without the measurement model",
        "Media simple de las calificaciones de los expertos, sin el modelo de medición",
    ),
    col(
        "coder_count",
        "INT64",
        "Número de especialistas que codificaram a variável no país e ano",
        "Number of experts who coded the variable in the country and year",
        "Número de expertos que codificaron la variable en el país y año",
        original="_nr",
        unit="person",
    ),
]

COUNTRY = [
    COUNTRY_ID,
    col(
        "country_text_id",
        "STRING",
        "Código alfabético de três letras do país no V-Dem",
        "V-Dem three-letter alphabetic country code",
        "Código alfabético de tres letras del país en V-Dem",
        obs=(
            "Coincide com o ISO 3166-1 alfa-3 para a maioria dos países atuais, mas não para unidades históricas",
            "Matches ISO 3166-1 alpha-3 for most current countries, but not for historical units",
            "Coincide con ISO 3166-1 alfa-3 para la mayoría de los países actuales, pero no para unidades históricas",
        ),
    ),
    col(
        "country_name",
        "STRING",
        "Nome do país ou unidade política (em inglês)",
        "Name of the country or political unit",
        "Nombre del país o unidad política (en inglés)",
    ),
    col(
        "historical",
        "STRING",
        "Indica se a unidade foi codificada pelo projeto V-Dem histórico",
        "Whether the unit was coded by the Historical V-Dem project",
        "Indica si la unidad fue codificada por el proyecto V-Dem histórico",
        dictionary=True,
    ),
    *[
        col(
            name,
            "INT64",
            pt,
            en,
            es,
            original=original,
            unit="year",
        )
        for name, original, pt, en, es in [
            (
                "coding_start",
                "codingstart",
                "Primeiro ano codificado",
                "First coded year",
                "Primer año codificado",
            ),
            (
                "coding_end",
                "codingend",
                "Último ano codificado",
                "Last coded year",
                "Último año codificado",
            ),
            (
                "coding_start_contemporary",
                "codingstart_contemp",
                "Primeiro ano codificado pelo V-Dem contemporâneo",
                "First year coded by contemporary V-Dem",
                "Primer año codificado por el V-Dem contemporáneo",
            ),
            (
                "coding_end_contemporary",
                "codingend_contemp",
                "Último ano codificado pelo V-Dem contemporâneo",
                "Last year coded by contemporary V-Dem",
                "Último año codificado por el V-Dem contemporáneo",
            ),
            (
                "coding_start_historical",
                "codingstart_hist",
                "Primeiro ano codificado pelo V-Dem histórico",
                "First year coded by Historical V-Dem",
                "Primer año codificado por el V-Dem histórico",
            ),
            (
                "coding_end_historical",
                "codingend_hist",
                "Último ano codificado pelo V-Dem histórico",
                "Last year coded by Historical V-Dem",
                "Último año codificado por el V-Dem histórico",
            ),
            *[
                (
                    f"gap_{side}_{i}",
                    f"gap{side}{i}",
                    f"{'Primeiro' if side == 'start' else 'Último'} ano da {i}ª lacuna na codificação",
                    f"{'First' if side == 'start' else 'Last'} year of coding gap {i}",
                    f"{'Primer' if side == 'start' else 'Último'} año de la {i}ª laguna en la codificación",
                )
                for i in (1, 2, 3)
                for side in ("start", "end")
            ],
        ]
    ],
]

INDICATOR = [
    col(
        "variable_id",
        "STRING",
        "Identificador da variável no V-Dem",
        "V-Dem variable identifier",
        "Identificador de la variable en V-Dem",
    ),
    col(
        "codebook_tag",
        "STRING",
        "Identificador da entrada correspondente no livro de códigos do V-Dem",
        "Identifier of the matching entry in the V-Dem codebook",
        "Identificador de la entrada correspondiente en el libro de códigos de V-Dem",
        original="tag",
        obs=(
            "Difere de variable_id nas categorias de perguntas de múltipla escolha e nas versões ordinais dos índices; nulo quando o livro de códigos não tem entrada",
            "Differs from variable_id for the categories of multiple-selection questions and for ordinal versions of indices; null when the codebook has no entry",
            "Difiere de variable_id en las categorías de preguntas de selección múltiple y en las versiones ordinales de los índices; nulo cuando el libro de códigos no tiene entrada",
        ),
    ),
    col(
        "category",
        "STRING",
        "Categoria de resposta representada pela variável, quando ela é uma categoria de outra variável",
        "Response category the variable stands for, when it is a category of another variable",
        "Categoría de respuesta que representa la variable, cuando es una categoría de otra variable",
        obs=(
            "Número da opção em perguntas de múltipla escolha (ex.: 0) ou número de categorias de um índice ordinal (ex.: 3C)",
            "Option number in multiple-selection questions (e.g. 0) or number of categories of an ordinal index (e.g. 3C)",
            "Número de la opción en preguntas de selección múltiple (p. ej., 0) o número de categorías de un índice ordinal (p. ej., 3C)",
        ),
    ),
    col(
        "variants",
        "STRING",
        "Colunas da tabela country_year preenchidas para a variável, separadas por vírgula",
        "Columns of the country_year table filled for the variable, comma-separated",
        "Columnas de la tabla country_year completadas para la variable, separadas por coma",
        original="(derived)",
    ),
    col(
        "name",
        "STRING",
        "Nome da variável (em inglês)",
        "Name of the variable",
        "Nombre de la variable (en inglés)",
    ),
    col(
        "variable_type",
        "STRING",
        "Tipo da variável segundo o modo de codificação",
        "Type of the variable by how it is coded",
        "Tipo de la variable según el modo de codificación",
        original="vartype",
        dictionary=True,
    ),
    *[
        col(name, "STRING", pt, en, es, original=original)
        for name, original, pt, en, es in [
            (
                "question",
                "question",
                "Pergunta feita aos codificadores (em inglês)",
                "Question asked to the coders",
                "Pregunta hecha a los codificadores (en inglés)",
            ),
            (
                "clarification",
                "clarification",
                "Esclarecimentos sobre a pergunta (em inglês)",
                "Clarifications about the question",
                "Aclaraciones sobre la pregunta (en inglés)",
            ),
            (
                "responses",
                "responses",
                "Opções de resposta e seus códigos (em inglês)",
                "Response options and their codes",
                "Opciones de respuesta y sus códigos (en inglés)",
            ),
            (
                "scale",
                "scale",
                "Escala da variável (em inglês)",
                "Scale of the variable",
                "Escala de la variable (en inglés)",
            ),
            (
                "notes",
                "notes",
                "Notas do livro de códigos (em inglês)",
                "Codebook notes",
                "Notas del libro de códigos (en inglés)",
            ),
            (
                "cross_coder_aggregation",
                "crosscoder_aggregation",
                "Método de agregação entre codificadores (em inglês)",
                "Method of aggregation across coders",
                "Método de agregación entre codificadores (en inglés)",
            ),
            (
                "country_year_aggregation",
                "cy_aggregation",
                "Método de agregação de datas para país-ano (em inglês)",
                "Method of aggregation from dates to country-years",
                "Método de agregación de fechas a país-año (en inglés)",
            ),
            (
                "data_release",
                "datarelease",
                "Versões do V-Dem em que a variável foi publicada",
                "V-Dem versions in which the variable was released",
                "Versiones de V-Dem en que se publicó la variable",
            ),
            (
                "years",
                "years",
                "Anos cobertos pela variável",
                "Years covered by the variable",
                "Años cubiertos por la variable",
            ),
            (
                "convergence",
                "convergence",
                "Notas sobre a convergência do modelo de medição (em inglês)",
                "Notes on the convergence of the measurement model",
                "Notas sobre la convergencia del modelo de medición (en inglés)",
            ),
            (
                "project_manager",
                "projectmanager",
                "Gerente de projeto responsável pela variável",
                "Project manager responsible for the variable",
                "Gerente de proyecto responsable de la variable",
            ),
        ]
    ],
]

DICIONARIO = [
    col(
        "id_tabela",
        "STRING",
        "ID Tabela",
        "Table ID",
        "ID Tabla",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Nome da coluna",
        "Column name",
        "Nombre de la columna",
    ),
    col("chave", "STRING", "Chave", "Key", "Clave"),
    col(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal",
        "Temporal coverage",
        "Cobertura temporal",
    ),
    col("valor", "STRING", "Valor", "Value", "Valor"),
]

INDEX_PREFIX = [
    YEAR,
    COUNTRY_ID,
    col(
        "historical_date",
        "DATE",
        "Data de referência da observação",
        "Reference date of the observation",
        "Fecha de referencia de la observación",
        obs=(
            "31 de dezembro na maioria dos anos; pode ser outra data quando a unidade deixou de existir no ano",
            "31 December in most years; may be another date when the unit ceased to exist during the year",
            "31 de diciembre en la mayoría de los años; puede ser otra fecha cuando la unidad dejó de existir en el año",
        ),
    ),
    col(
        "project",
        "STRING",
        "Projeto V-Dem que codificou o país e ano",
        "V-Dem project that coded the country-year",
        "Proyecto V-Dem que codificó el país y año",
        dictionary=True,
    ),
    col(
        "historical_name",
        "STRING",
        "Nome histórico da unidade política no ano (em inglês)",
        "Historical name of the political unit in the year",
        "Nombre histórico de la unidad política en el año (en inglés)",
        original="histname",
    ),
    col(
        "gap_index",
        "STRING",
        "Identificador do período contínuo de codificação da unidade",
        "Identifier of the unit's continuous coding period",
        "Identificador del período continuo de codificación de la unidad",
    ),
    col(
        "cow_code",
        "STRING",
        "Código do país no Correlates of War (COW)",
        "Correlates of War (COW) country code",
        "Código del país en Correlates of War (COW)",
        original="COWcode",
    ),
]

# Indices stored as category codes rather than quantities.
CATEGORICAL_INDICES = {
    "v2x_regime",
    "v2x_regime_amb",
    "v2x_elecreg",
    "v2xex_elecreg",
    "v2xlg_elecreg",
    "v2xel_elecparl",
    "v2xlg_leginter",
    "v2xel_elecpres",
    "v2x_hosinter",
    "v2x_hosabort",
    "v2x_legabort",
}

# v2x index -> (Portuguese, Spanish) name. English comes from the codebook.
INDEX_NAMES = {
    "v2x_polyarchy": (
        "Índice de democracia eleitoral",
        "Índice de democracia electoral",
    ),
    "v2x_libdem": (
        "Índice de democracia liberal",
        "Índice de democracia liberal",
    ),
    "v2x_partipdem": (
        "Índice de democracia participativa",
        "Índice de democracia participativa",
    ),
    "v2x_delibdem": (
        "Índice de democracia deliberativa",
        "Índice de democracia deliberativa",
    ),
    "v2x_egaldem": (
        "Índice de democracia igualitária",
        "Índice de democracia igualitaria",
    ),
    "v2x_api": (
        "Índice aditivo de poliarquia",
        "Índice aditivo de poliarquía",
    ),
    "v2x_mpi": (
        "Índice multiplicativo de poliarquia",
        "Índice multiplicativo de poliarquía",
    ),
    "v2x_freexp_altinf": (
        "Índice de liberdade de expressão e fontes alternativas de informação",
        "Índice de libertad de expresión y fuentes alternativas de información",
    ),
    "v2x_frassoc_thick": (
        "Índice de liberdade de associação (versão ampla)",
        "Índice de libertad de asociación (versión amplia)",
    ),
    "v2x_suffr": (
        "Proporção da população com direito a voto",
        "Proporción de la población con derecho al voto",
    ),
    "v2xel_frefair": (
        "Índice de eleições limpas",
        "Índice de elecciones limpias",
    ),
    "v2x_elecoff": (
        "Índice de autoridades eleitas",
        "Índice de autoridades electas",
    ),
    "v2x_liberal": (
        "Índice do componente liberal",
        "Índice del componente liberal",
    ),
    "v2xcl_rol": (
        "Índice de igualdade perante a lei e liberdade individual",
        "Índice de igualdad ante la ley y libertad individual",
    ),
    "v2x_jucon": (
        "Índice de restrições judiciais ao Executivo",
        "Índice de restricciones judiciales al Ejecutivo",
    ),
    "v2xlg_legcon": (
        "Índice de restrições legislativas ao Executivo",
        "Índice de restricciones legislativas al Ejecutivo",
    ),
    "v2x_partip": (
        "Índice do componente participativo",
        "Índice del componente participativo",
    ),
    "v2x_cspart": (
        "Índice de participação da sociedade civil",
        "Índice de participación de la sociedad civil",
    ),
    "v2xdd_dd": (
        "Índice de voto popular direto",
        "Índice de voto popular directo",
    ),
    "v2xel_locelec": ("Índice de governo local", "Índice de gobierno local"),
    "v2xel_regelec": (
        "Índice de governo regional",
        "Índice de gobierno regional",
    ),
    "v2xdl_delib": (
        "Índice do componente deliberativo",
        "Índice del componente deliberativo",
    ),
    "v2x_egal": (
        "Índice do componente igualitário",
        "Índice del componente igualitario",
    ),
    "v2xeg_eqprotec": (
        "Índice de proteção igualitária",
        "Índice de protección igualitaria",
    ),
    "v2xeg_eqaccess": (
        "Índice de acesso igualitário",
        "Índice de acceso igualitario",
    ),
    "v2xeg_eqdr": (
        "Índice de distribuição igualitária de recursos",
        "Índice de distribución igualitaria de recursos",
    ),
    "v2x_regime": (
        "Regimes do mundo (medida RoW)",
        "Regímenes del mundo (medida RoW)",
    ),
    "v2x_regime_amb": (
        "Regimes do mundo (medida RoW) com categorias para casos ambíguos",
        "Regímenes del mundo (medida RoW) con categorías para casos ambiguos",
    ),
    "v2x_accountability": (
        "Índice de accountability",
        "Índice de rendición de cuentas",
    ),
    "v2x_veracc": (
        "Índice de accountability vertical",
        "Índice de rendición de cuentas vertical",
    ),
    "v2x_diagacc": (
        "Índice de accountability diagonal",
        "Índice de rendición de cuentas diagonal",
    ),
    "v2x_horacc": (
        "Índice de accountability horizontal",
        "Índice de rendición de cuentas horizontal",
    ),
    "v2x_ex_hereditary": (
        "Índice da dimensão hereditária",
        "Índice de la dimensión hereditaria",
    ),
    "v2x_ex_military": (
        "Índice da dimensão militar",
        "Índice de la dimensión militar",
    ),
    "v2x_ex_party": (
        "Índice da dimensão do partido governante",
        "Índice de la dimensión del partido gobernante",
    ),
    "v2x_ex_direlect": (
        "Índice da dimensão de eleição direta",
        "Índice de la dimensión de elección directa",
    ),
    "v2x_ex_confidence": (
        "Índice da dimensão de confiança",
        "Índice de la dimensión de confianza",
    ),
    "v2xnp_regcorr": ("Corrupção do regime", "Corrupción del régimen"),
    "v2x_neopat": (
        "Índice de governo neopatrimonial",
        "Índice de gobierno neopatrimonial",
    ),
    "v2xnp_pres": ("Índice de presidencialismo", "Índice de presidencialismo"),
    "v2xnp_client": ("Índice de clientelismo", "Índice de clientelismo"),
    "v2x_civlib": (
        "Índice de liberdades civis",
        "Índice de libertades civiles",
    ),
    "v2x_clpriv": (
        "Índice de liberdades civis privadas",
        "Índice de libertades civiles privadas",
    ),
    "v2x_clpol": (
        "Índice de liberdades civis políticas",
        "Índice de libertades civiles políticas",
    ),
    "v2x_clphy": ("Índice de violência física", "Índice de violencia física"),
    "v2xpe_exlsocgr": (
        "Índice de exclusão por grupo social",
        "Índice de exclusión por grupo social",
    ),
    "v2xpe_exlgender": (
        "Índice de exclusão por gênero",
        "Índice de exclusión por género",
    ),
    "v2xpe_exlecon": (
        "Exclusão por grupo socioeconômico",
        "Exclusión por grupo socioeconómico",
    ),
    "v2xpe_exlpol": (
        "Índice de exclusão por grupo político",
        "Índice de exclusión por grupo político",
    ),
    "v2xpe_exlgeo": (
        "Índice de exclusão por localização urbano-rural",
        "Índice de exclusión por ubicación urbano-rural",
    ),
    "v2x_corr": (
        "Índice de corrupção política",
        "Índice de corrupción política",
    ),
    "v2x_pubcorr": (
        "Índice de corrupção no setor público",
        "Índice de corrupción en el sector público",
    ),
    "v2x_execorr": (
        "Índice de corrupção no Executivo",
        "Índice de corrupción en el Ejecutivo",
    ),
    "v2x_gender": (
        "Índice de empoderamento político das mulheres",
        "Índice de empoderamiento político de las mujeres",
    ),
    "v2x_gencl": (
        "Índice de liberdades civis das mulheres",
        "Índice de libertades civiles de las mujeres",
    ),
    "v2x_gencs": (
        "Índice de participação das mulheres na sociedade civil",
        "Índice de participación de las mujeres en la sociedad civil",
    ),
    "v2x_genpp": (
        "Índice de participação política das mulheres",
        "Índice de participación política de las mujeres",
    ),
    "v2x_rule": ("Índice de Estado de direito", "Índice de Estado de derecho"),
    "v2xcl_acjst": ("Acesso à justiça", "Acceso a la justicia"),
    "v2xcl_prpty": ("Direitos de propriedade", "Derechos de propiedad"),
    "v2xdd_i_ci": (
        "Índice de iniciativa popular",
        "Índice de iniciativa popular",
    ),
    "v2xdd_i_rf": (
        "Índice de referendo popular",
        "Índice de referéndum popular",
    ),
    "v2xdd_i_or": (
        "Índice de referendo obrigatório",
        "Índice de referéndum obligatorio",
    ),
    "v2xdd_i_pl": ("Índice de plebiscito", "Índice de plebiscito"),
    "v2xdd_cic": (
        "Componente de iniciativa cidadã do índice de voto popular direto",
        "Componente de iniciativa ciudadana del índice de voto popular directo",
    ),
    "v2xdd_toc": (
        "Componente de cima para baixo do índice de voto popular direto",
        "Componente de arriba hacia abajo del índice de voto popular directo",
    ),
    "v2xcs_ccsi": (
        "Índice central da sociedade civil",
        "Índice central de la sociedad civil",
    ),
    "v2x_elecreg": (
        "Índice de regime eleitoral",
        "Índice de régimen electoral",
    ),
    "v2xex_elecreg": (
        "Índice de regime eleitoral do Executivo",
        "Índice de régimen electoral del Ejecutivo",
    ),
    "v2xlg_elecreg": (
        "Índice de regime eleitoral do Legislativo",
        "Índice de régimen electoral del Legislativo",
    ),
    "v2x_EDcomp_thick": (
        "Índice do componente eleitoral",
        "Índice del componente electoral",
    ),
    "v2x_freexp": (
        "Índice de liberdade de expressão",
        "Índice de libertad de expresión",
    ),
    "v2xme_altinf": (
        "Índice de fontes alternativas de informação",
        "Índice de fuentes alternativas de información",
    ),
    "v2xcl_dmove": (
        "Liberdade de circulação interna",
        "Libertad de circulación interna",
    ),
    "v2xcl_slave": (
        "Liberdade em relação ao trabalho forçado",
        "Libertad frente al trabajo forzoso",
    ),
    "v2xcl_disc": ("Liberdade de discussão", "Libertad de discusión"),
    "v2xel_elecparl": (
        "Eleição legislativa ou de assembleia constituinte",
        "Elección legislativa o de asamblea constituyente",
    ),
    "v2xlg_leginter": (
        "Legislativo fechado ou interrompido",
        "Legislativo cerrado o interrumpido",
    ),
    "v2xel_elecpres": ("Eleição presidencial", "Elección presidencial"),
    "v2x_hosinter": (
        "Chefe do Executivo deixou de ser eleito",
        "El jefe del Ejecutivo dejó de ser electo",
    ),
    "v2x_hosabort": (
        "Eleição presidencial interrompida",
        "Elección presidencial interrumpida",
    ),
    "v2x_legabort": (
        "Eleição legislativa ou de assembleia constituinte interrompida",
        "Elección legislativa o de asamblea constituyente interrumpida",
    ),
    "v2xex_elecleg": (
        "Legislativo eleito diretamente",
        "Legislativo electo directamente",
    ),
    "v2xps_party": (
        "Índice de institucionalização partidária",
        "Índice de institucionalización partidaria",
    ),
    "v2x_divparctrl": (
        "Índice de controle partidário dividido",
        "Índice de control partidario dividido",
    ),
    "v2x_feduni": (
        "Índice de divisão de poder",
        "Índice de división del poder",
    ),
    "v2xca_academ": (
        "Índice de liberdade acadêmica",
        "Índice de libertad académica",
    ),
    "v2x_electoral_integrity": (
        "Índice de integridade eleitoral",
        "Índice de integridad electoral",
    ),
    "v2x_polyarchy_stock": (
        "Estoque de democracia eleitoral",
        "Acervo de democracia electoral",
    ),
    "v2x_libdem_stock": (
        "Estoque de democracia liberal",
        "Acervo de democracia liberal",
    ),
    "v2x_partipdem_stock": (
        "Estoque de democracia participativa",
        "Acervo de democracia participativa",
    ),
    "v2x_delibdem_stock": (
        "Estoque de democracia deliberativa",
        "Acervo de democracia deliberativa",
    ),
    "v2x_egaldem_stock": (
        "Estoque de democracia igualitária",
        "Acervo de democracia igualitaria",
    ),
    "v2xpas_democracy": (
        "Índice de democracia do sistema partidário",
        "Índice de democracia del sistema de partidos",
    ),
    "v2xpas_democracy_government": (
        "Índice de democracia da coalizão de governo",
        "Índice de democracia de la coalición de gobierno",
    ),
    "v2xpas_democracy_opposition": (
        "Índice de democracia dos partidos de oposição",
        "Índice de democracia de los partidos de oposición",
    ),
    "v2xpas_religion": (
        "Índice de religião do sistema partidário",
        "Índice de religión del sistema de partidos",
    ),
    "v2xpas_religion_government": (
        "Índice de religião da coalizão de governo",
        "Índice de religión de la coalición de gobierno",
    ),
    "v2xpas_religion_opposition": (
        "Índice de religião dos partidos de oposição",
        "Índice de religión de los partidos de oposición",
    ),
    "v2xpas_exclusion": (
        "Índice de exclusão do sistema partidário",
        "Índice de exclusión del sistema de partidos",
    ),
    "v2xpas_exclusion_government": (
        "Índice de exclusão da coalizão de governo",
        "Índice de exclusión de la coalición de gobierno",
    ),
    "v2xpas_exclusion_opposition": (
        "Índice de exclusão dos partidos de oposição",
        "Índice de exclusión de los partidos de oposición",
    ),
    "v2xpas_economic": (
        "Índice esquerda-direita do sistema partidário",
        "Índice izquierda-derecha del sistema de partidos",
    ),
    "v2xpas_economic_government": (
        "Índice esquerda-direita da coalizão de governo",
        "Índice izquierda-derecha de la coalición de gobierno",
    ),
    "v2xpas_economic_opposition": (
        "Índice esquerda-direita dos partidos de oposição",
        "Índice izquierda-derecha de los partidos de oposición",
    ),
    "v2xed_ed_inpt": (
        "Potencial de doutrinação na educação",
        "Potencial de adoctrinamiento en la educación",
    ),
    "v2xed_ed_poed": (
        "Esforço de educação política no ensino",
        "Esfuerzo de educación política en la enseñanza",
    ),
    "v2xed_ed_inco": (
        "Coerência da doutrinação na educação",
        "Coherencia del adoctrinamiento en la educación",
    ),
    "v2xed_ed_cent": (
        "Centralização do sistema educacional",
        "Centralización del sistema educativo",
    ),
    "v2xed_ed_ctag": (
        "Controle sobre os agentes educacionais",
        "Control sobre los agentes educativos",
    ),
    "v2xed_ed_con": (
        "Conteúdo de doutrinação na educação",
        "Contenido de adoctrinamiento en la educación",
    ),
    "v2xed_ed_dmcon": (
        "Conteúdo de doutrinação democrática na educação",
        "Contenido de adoctrinamiento democrático en la educación",
    ),
    "v2xed_ed_ptcon": (
        "Conteúdo de doutrinação patriótica na educação",
        "Contenido de adoctrinamiento patriótico en la educación",
    ),
    "v2xed_ptcon": (
        "Conteúdo de doutrinação patriótica na educação e na mídia",
        "Contenido de adoctrinamiento patriótico en la educación y los medios",
    ),
    "v2xedvd_inpt": (
        "Potencial de doutrinação na educação e na mídia",
        "Potencial de adoctrinamiento en la educación y los medios",
    ),
    "v2xedvd_me_inco": (
        "Coerência (potencial) da doutrinação na mídia",
        "Coherencia (potencial) del adoctrinamiento en los medios",
    ),
    "v2xedvd_me_cent": (
        "Centralização do controle da mídia",
        "Centralización del control de los medios",
    ),
    "v2xedvd_me_ctag": (
        "Controle sobre os agentes da mídia",
        "Control sobre los agentes de los medios",
    ),
}


def index_columns(vdem_cols: list[str], codebook: pd.DataFrame) -> list[dict]:
    names = codebook.set_index("tag")["name"].str.strip()
    scale = codebook.set_index("tag")["scale"].fillna("")
    out = []
    for c in vdem_cols:
        if c not in INDEX_NAMES:
            raise KeyError(f"no translation for index {c!r}")
        pt, es = INDEX_NAMES[c]
        en = names[c]
        if c in CATEGORICAL_INDICES:
            out.append(col(c, "STRING", pt, en, es, dictionary=True))
            continue
        unit = "proportion" if c == "v2x_suffr" else "index"
        obs = ("", "", "")
        if scale[c].startswith("Interval") and "(0-1)" not in scale[c]:
            obs = (
                "Escala sem limites fixos (estimativa do modelo de medição); ver scale na tabela indicator",
                "Unbounded scale (measurement-model estimate); see scale in the indicator table",
                "Escala sin límites fijos (estimación del modelo de medición); ver scale en la tabla indicator",
            )
        out.append(col(c, "FLOAT64", pt, en, es, unit=unit, obs=obs))
    return out


def write(table: str, rows: list[dict]) -> None:
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    with open(
        ARCH_DIR / f"{table}.csv", "w", newline="", encoding="utf-8"
    ) as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print(f"{table}: {len(rows)} columns")


def main() -> None:
    vdem_cols = pq.read_schema(DATA_DIR / "input" / "vdem.parquet").names
    codebook = pd.read_parquet(DATA_DIR / "input" / "codebook.parquet")
    suffixes = ("_codelow", "_codehigh", "_sd", "_osp", "_ord", "_mean", "_nr")
    indices = [
        c
        for c in vdem_cols
        if c.startswith("v2x") and not c.endswith(suffixes)
    ]
    write("country_year", COUNTRY_YEAR)
    write(
        "country_year_index", INDEX_PREFIX + index_columns(indices, codebook)
    )
    write("country", COUNTRY)
    write("indicator", INDICATOR)
    write("dicionario", DICIONARIO)


if __name__ == "__main__":
    main()
