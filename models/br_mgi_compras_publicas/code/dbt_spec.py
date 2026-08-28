"""Per-table facts the dbt generator needs beyond the architecture CSVs.

Keys were chosen from the API's own identifier semantics and are asserted by
`dbt_utils.unique_combination_of_columns`; validate.py checks them against the
harvested data before the models are trusted.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DbtTable:
    #: columns forming the logical key
    key: list[str]
    #: partition column and its BigQuery type
    partition: str = "ano"
    #: inclusive first year and exclusive-ish last year for the partition range
    year_range: tuple[int, int] = (1990, 2031)
    #: scope expensive tests to recent partitions; see the note in build_dbt.py
    scope_tests: bool = False
    #: columns legitimately empty, excluded from the null-proportion test
    ignore_values: list[str] = field(default_factory=list)
    #: column ordering the dedup, newest kept. The API repeats records across
    #: pages -- on contratacao 1.19% of rows were byte-identical repeats -- so
    #: every model deduplicates on its key.
    dedup_order: str = ""
    #: one-line description used for the model and the backend
    description: str = ""


R_14133 = (2021, 2031)
R_ARP = (2023, 2032)
R_CONTRATO = (2010, 2031)
R_LEGADO = (1990, 2030)

TABLES: dict[str, DbtTable] = {
    "contratacao": DbtTable(
        key=["numero_controle_pncp"],
        year_range=R_14133,
        dedup_order="data_atualizacao_pncp",
        description=(
            "Contratações públicas realizadas sob a Lei 14.133/2021 e divulgadas no PNCP "
            "por meio do Compras.gov.br, de 2021 em diante. Uma linha por contratação, "
            "cobrindo os três níveis de governo"
        ),
    ),
    "contratacao_item": DbtTable(
        key=["id_compra_item"],
        dedup_order="data_atualizacao_pncp",
        year_range=R_14133,
        scope_tests=True,
        description=(
            "Itens das contratações realizadas sob a Lei 14.133/2021. Uma linha por item, "
            "com quantidade, valor estimado e, quando já apurado, o resultado"
        ),
    ),
    "contratacao_item_resultado": DbtTable(
        key=["id_compra_item", "sequencial_resultado"],
        dedup_order="data_atualizacao_pncp",
        year_range=R_14133,
        scope_tests=True,
        description=(
            "Resultados dos itens das contratações da Lei 14.133/2021. Uma linha por "
            "fornecedor classificado em cada item, com quantidade e valor homologados"
        ),
    ),
    "ata_registro_preco": DbtTable(
        key=["numero_controle_pncp_ata"],
        dedup_order="data_hora_atualizacao",
        year_range=R_ARP,
        description=(
            "Atas de registro de preços firmadas a partir de contratações da Lei "
            "14.133/2021. Uma linha por ata"
        ),
    ),
    "ata_registro_preco_item": DbtTable(
        key=[
            "numero_controle_pncp_ata",
            "numero_item",
            "classificacao_fornecedor",
        ],
        dedup_order="data_hora_atualizacao",
        year_range=R_ARP,
        description=(
            "Itens das atas de registro de preços, com o fornecedor registrado, o preço "
            "unitário e o limite de adesão. Uma linha por fornecedor classificado em cada item"
        ),
    ),
    "contrato": DbtTable(
        dedup_order="data_hora_inclusao",
        key=["codigo_orgao", "codigo_unidade_gestora", "numero_contrato"],
        year_range=R_CONTRATO,
        description=(
            "Contratos administrativos registrados no SIASG, de 2010 em diante. Uma linha "
            "por contrato, com vigência, fornecedor e valores"
        ),
    ),
    "contrato_item": DbtTable(
        dedup_order="data_hora_inclusao",
        key=[
            "codigo_orgao",
            "codigo_unidade_gestora",
            "numero_contrato",
            "numero_item",
        ],
        year_range=R_CONTRATO,
        description=(
            "Itens dos contratos administrativos registrados no SIASG. Uma linha por item "
            "contratado, com quantidade e valores unitário e total"
        ),
    ),
    "licitacao": DbtTable(
        dedup_order="data_alteracao",
        key=["id_compra"],
        year_range=R_LEGADO,
        description=(
            "Licitações realizadas sob a Lei 8.666/1993 e legislação anterior, de 1997 a "
            "2025. Uma linha por licitação, em todas as modalidades"
        ),
    ),
    "licitacao_pregao": DbtTable(
        dedup_order="data_alteracao",
        key=["id_compra"],
        year_range=R_LEGADO,
        description=(
            "Detalhamento processual dos pregões realizados sob a Lei 8.666/1993, incluindo "
            "portaria, situação e datas de encerramento e resultado. Uma linha por pregão"
        ),
    ),
    "licitacao_item": DbtTable(
        dedup_order="data_alteracao",
        key=["id_compra_item"],
        year_range=R_LEGADO,
        scope_tests=True,
        description=(
            "Itens das licitações realizadas sob a Lei 8.666/1993. Uma linha por item "
            "licitado, com quantidade, valor estimado e fornecedor vencedor"
        ),
    ),
    "licitacao_item_pregao": DbtTable(
        dedup_order="data_alteracao",
        key=["id_compra_item"],
        year_range=R_LEGADO,
        scope_tests=True,
        description=(
            "Resultado dos itens dos pregões da Lei 8.666/1993, com menor lance, valor "
            "negociado e valor homologado. Uma linha por item homologado"
        ),
    ),
    "compra_sem_licitacao": DbtTable(
        dedup_order="data_alteracao",
        key=["id_compra"],
        year_range=R_LEGADO,
        description=(
            "Dispensas e inexigibilidades de licitação sob a Lei 8.666/1993, de 1997 a "
            "2024. Uma linha por contratação direta, com fundamento legal e justificativa"
        ),
    ),
    "compra_sem_licitacao_item": DbtTable(
        dedup_order="data_alteracao",
        key=["id_compra_item"],
        year_range=R_LEGADO,
        scope_tests=True,
        description=(
            "Itens das dispensas e inexigibilidades de licitação sob a Lei 8.666/1993. "
            "Uma linha por item, com fornecedor vencedor e valor estimado"
        ),
    ),
    "orgao": DbtTable(
        key=["data_extracao", "codigo_orgao"],
        dedup_order="data_hora_movimento",
        partition="data_extracao",
        description=(
            "Cadastro dos órgãos registrados no SIASG, com hierarquia administrativa, "
            "esfera e poder. Uma linha por órgão em cada extração"
        ),
    ),
    "unidade_administrativa": DbtTable(
        key=["data_extracao", "codigo_uasg"],
        dedup_order="data_hora_movimento",
        partition="data_extracao",
        description=(
            "Cadastro das Unidades Administrativas de Serviços Gerais (UASG), as unidades "
            "que efetivamente compram. Uma linha por UASG em cada extração"
        ),
    ),
    "fornecedor": DbtTable(
        # cnpj and cpf are mutually exclusive and each is null for the other
        # kind of supplier, so the name is needed to separate the handful of
        # rows that share a masked cpf.
        key=["data_extracao", "cnpj", "cpf", "nome_razao_social"],
        # The registry carries no timestamp, but the repeats are byte-identical,
        # so any one of them is the right row to keep.
        dedup_order="cnpj",
        partition="data_extracao",
        description=(
            "Cadastro de fornecedores do SICAF, com atividade econômica, porte e natureza "
            "jurídica. Uma linha por fornecedor em cada extração"
        ),
    ),
    "catalogo_material": DbtTable(
        dedup_order="data_hora_atualizacao",
        key=["data_extracao", "codigo_item"],
        partition="data_extracao",
        description=(
            "Catálogo de Materiais (CATMAT), com a hierarquia de grupo, classe e padrão "
            "descritivo. Uma linha por item de material"
        ),
    ),
    "catalogo_servico": DbtTable(
        dedup_order="data_hora_atualizacao",
        key=["data_extracao", "codigo_servico"],
        partition="data_extracao",
        description=(
            "Catálogo de Serviços (CATSER), com a hierarquia de seção, divisão, grupo e "
            "classe. Uma linha por item de serviço"
        ),
    ),
    "dicionario": DbtTable(
        key=["id_tabela", "nome_coluna", "chave"],
        partition="",
        description=(
            "Dicionário das colunas codificadas do conjunto, relacionando cada chave ao "
            "seu significado"
        ),
    ),
}
