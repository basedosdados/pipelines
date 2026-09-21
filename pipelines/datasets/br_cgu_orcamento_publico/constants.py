"""Constants for the br_cgu_orcamento_publico recurring pipeline (Prefect 3).

Orçamento e execução da despesa do Governo Federal, publicados anualmente pelo
Portal da Transparência (CGU) como um ZIP por exercício.

Two things about this source drive the design:

* **Past exercises are restated.** The portal regenerates every year's file on
  the same schedule — on 2026-09-09 the 2024 file carried 142 IBAMA rows and
  R$ 2,16 bi autorizado against the 139 rows / R$ 2,05 bi that had been ingested
  earlier. So the pipeline re-downloads the whole history on every run and
  replaces the table (``dump_mode="overwrite"``), never appends.
* **The exercise year is not a useful freshness signal**, since it changes once
  a year while the contents change continuously. The poll uses the ZIP's
  ``Last-Modified`` header against ``Table.Update.latest`` instead.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the br_cgu_orcamento_publico pipeline."""

    DATASET_ID = "br_cgu_orcamento_publico"
    TABLE_ID = "orcamento"

    BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados/orcamento-despesa"

    # The portal answers a plain scripted GET, but sends 403 to some default
    # agents; keep a browser UA with a contact address.
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 rdahis@basedosdados.org"
    )

    # Earliest exercise the portal publishes. 2013 and earlier return 403.
    FIRST_YEAR = 2014

    # Source files are Latin-1, semicolon-separated, every field quoted.
    SOURCE_ENCODING = "latin-1"
    SOURCE_DELIMITER = ";"

    # Staging column names, in the order the existing external table expects.
    # These are the *raw-ish* names the dbt model renames to the published ones
    # (codigo_orgao_superior -> id_orgao_superior, and so on); staging mirrors
    # the source, the model does the renaming. ``ano_exercicio`` is absent
    # because it is the hive partition key, not a column in the file.
    STAGING_COLUMNS = [
        "codigo_orgao_superior",
        "nome_orgao_superior",
        "codigo_orgao_subordinado",
        "nome_orgao_subordinado",
        "codigo_unidade_orcamentaria",
        "nome_unidade_orcamentaria",
        "codigo_funcao",
        "nome_funcao",
        "codigo_subfuncao",
        "nome_subfuncao",
        "codigo_programa_orcamentario",
        "nome_programa_orcamentario",
        "codigo_acao",
        "nome_acao",
        "codigo_categoria_economica",
        "nome_categoria_economica",
        "codigo_grupo_de_despesa",
        "nome_grupo_de_despesa",
        "codigo_elemento_de_despesa",
        "nome_elemento_de_despesa",
        "orcamento_inicial",
        "orcamento_atualizado",
        "orcamento_empenhado",
        "orcamento_realizado",
        "porcentagem_realizado_orcamento",
    ]

    # Token each source header must start with once accent-folded, in file
    # order. Position 0 is the exercise. Validated on every run so a source
    # layout change fails loudly instead of silently mis-assigning columns.
    HEADER_TOKENS = [
        "exercicio",
        "codigo_orgao_superior",
        "nome_orgao_superior",
        "codigo_orgao_subordinado",
        "nome_orgao_subordinado",
        "codigo_unidade_orcamentaria",
        "nome_unidade_orcamentaria",
        "codigo_funcao",
        "nome_funcao",
        "codigo_subfuncao",
        "nome_subfuncao",
        "codigo_programa_orcamentario",
        "nome_programa_orcamentario",
        "codigo_acao",
        "nome_acao",
        "codigo_categoria_economica",
        "nome_categoria_economica",
        "codigo_grupo_de_despesa",
        "nome_grupo_de_despesa",
        "codigo_elemento_de_despesa",
        "nome_elemento_de_despesa",
        "orcamento_inicial",
        "orcamento_atualizado",
        "orcamento_empenhado",
        "orcamento_realizado",
        "realizado_do_orcamento",
    ]

    # Columns holding Brazilian-formatted numbers ("23929428,72", "86,16%").
    NUMERIC_COLUMNS = [
        "orcamento_inicial",
        "orcamento_atualizado",
        "orcamento_empenhado",
        "orcamento_realizado",
        "porcentagem_realizado_orcamento",
    ]

    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "br_cgu_orcamento_publico"
        / "code"
        / "architecture"
    )
