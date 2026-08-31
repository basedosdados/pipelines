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
    #: pages -- on contratacao 1.19% of rows were byte-identical repeats, on ata
    #: items 22% -- so every model deduplicates.
    #:
    #: The dedup partitions on the row's own content rather than on the key,
    #: excluding the volatile timestamps below. Partitioning on the key looks
    #: tidier but silently destroys data: `numero_controle_pncp_ata` is null for
    #: 15,908 ata item rows, and BigQuery groups every null into one partition,
    #: so `row_number() = 1` would have discarded 15,907 legitimate records.
    dedup_order: str = ""
    #: timestamp columns excluded from the dedup partition, so two recordings of
    #: the same logical row a second apart collapse to one
    dedup_exclude: list[str] = field(default_factory=list)
    #: collapse to the latest row per KEY rather than per distinct row content.
    #: Correct only where the endpoint revises a row in place and re-serves it in
    #: a later window: the same item is then fetched twice with genuinely
    #: different content ("Em andamento" with no supplier, then "Homologado"
    #: with one), and content-based dedup cannot collapse them -- keeping both
    #: double counts the item. Requires a key with no NULLs, or BigQuery groups
    #: every NULL into one partition and discards real rows.
    dedup_by_key: bool = False
    #: the key may be NULL. Rows with a null key fall back to partitioning on
    #: their own content, so identical repeats still collapse while genuinely
    #: distinct rows survive -- BigQuery would otherwise group every null into a
    #: single partition and keep exactly one of them.
    dedup_key_nullable: bool = False
    #: restrict the uniqueness test to rows matching this predicate. Used where
    #: part of the key is legitimately absent on a minority of rows.
    unique_where: str = ""
    #: share of duplicate keys the source genuinely contains. Above zero the
    #: model uses the relaxed uniqueness test, and the reason is stated in the
    #: table description.
    unique_tolerance: float = 0.0
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
        # Subrogation -- another body taking the procurement over -- happens in
        # 0.34% of contratacoes, so every subrogado column is empty for the rest.
        ignore_values=[
            "cnpj_orgao_subrogado",
            "nome_orgao_subrogado",
            "esfera_subrogado",
            "poder_subrogado",
            "codigo_unidade_subrogada",
            "nome_unidade_subrogada",
            "sigla_uf_subrogada",
            "id_municipio_subrogada",
            "nome_municipio_subrogada",
        ],
        description=(
            "Contratações públicas realizadas sob a Lei 14.133/2021 e divulgadas no PNCP "
            "por meio do Compras.gov.br, de 2021 em diante. Uma linha por contratação, "
            "cobrindo os três níveis de governo"
        ),
    ),
    "contratacao_item": DbtTable(
        # numero_controle_pncp + numero_item_pncp, not the SIASG id_compra_item:
        # one SIASG item id maps to several PNCP contratacoes, so it collides
        # (2.91% of rows against 2.66% for the PNCP pair). Neither column is ever
        # null, which is what makes dedup_by_key safe here.
        key=["numero_controle_pncp", "numero_item_pncp"],
        dedup_order="data_atualizacao_pncp",
        dedup_by_key=True,
        year_range=R_14133,
        scope_tests=True,
        ignore_values=["codigo_grupo", "nome_pdm", "codigo_pdm"],
        description=(
            "Itens das contratações realizadas sob a Lei 14.133/2021. Uma linha por item, "
            "com quantidade, valor estimado e, quando já apurado, o resultado"
        ),
    ),
    "contratacao_item_resultado": DbtTable(
        # id_compra_item collides across PNCP contratacoes on its own, but paired
        # with sequencial_resultado it is unique here -- verified, no tolerance.
        key=["id_compra_item", "sequencial_resultado"],
        dedup_order="data_atualizacao_pncp",
        year_range=R_14133,
        scope_tests=True,
        # Cancellation and tie-break fields only apply to the rare result that
        # was cancelled or decided on a tie-break: 98-99% empty by nature.
        ignore_values=[
            "id_amparo_legal_criterio_desempate",
            "data_cancelamento_pncp",
        ],
        description=(
            "Resultados dos itens das contratações da Lei 14.133/2021. Uma linha por "
            "fornecedor classificado em cada item, com quantidade e valor homologados"
        ),
    ),
    "ata_registro_preco": DbtTable(
        # numero_controle_pncp_ata is null for 2,799 atas (0.68%), so it cannot
        # be the key; this pair is the only candidate with no nulls at all.
        key=["id_compra", "numero_ata_registro_preco"],
        dedup_order="data_hora_atualizacao",
        dedup_exclude=["data_hora_inclusao", "data_hora_atualizacao"],
        # Same revision pattern as the item table, at small scale: all 55 of the
        # repeated keys differ in data_hora_atualizacao and 38 in
        # indicador_ata_excluida. Both key columns are non-null by construction,
        # so the collapse needs no fallback and the tolerance goes away.
        dedup_by_key=True,
        year_range=R_ARP,
        description=(
            "Atas de registro de preços firmadas a partir de contratações da Lei "
            "14.133/2021. Uma linha por ata, no estado mais recente informado pela fonte. "
            "A fonte não atribui número de controle no PNCP a 0,68% das atas, portanto a "
            "chave é o par id_compra e numero_ata_registro_preco"
        ),
    ),
    "ata_registro_preco_item": DbtTable(
        key=[
            "numero_controle_pncp_ata",
            "numero_item",
            "classificacao_fornecedor",
        ],
        dedup_order="data_hora_atualizacao",
        dedup_exclude=["data_hora_inclusao", "data_hora_atualizacao"],
        # The source revises an item and re-serves it: 91% of the repeated keys
        # differ only in indicador_item_excluido, the rest in fornecedor, price
        # or vigencia. Keeping both states double counts the item, so the newest
        # wins. numero_controle_pncp_ata is null for 15,908 rows, hence the
        # nullable fallback.
        dedup_by_key=True,
        dedup_key_nullable=True,
        year_range=R_ARP,
        description=(
            "Itens das atas de registro de preços, com o fornecedor registrado, o preço "
            "unitário e o limite de adesão. Uma linha por fornecedor classificado em cada "
            "item, no estado mais recente informado pela fonte"
        ),
    ),
    "contrato": DbtTable(
        dedup_order="data_hora_inclusao",
        # A unidade gestora reuses a contract number across procurements, so the
        # contract number alone is not a key: 2.94% of the three-column keys
        # repeat, and those repeats differ in objeto (95%), valor (92%) and
        # fornecedor (89%) -- genuinely different contracts, not revisions, so
        # they must not be collapsed. Adding id_compra identifies the
        # procurement and cuts the repeats to 0.84%; the 4,244 rows with no
        # id_compra are scoped out of the test rather than dropped. The residual
        # still differs in objeto and valor, so a small tolerance stands.
        key=[
            "codigo_orgao",
            "codigo_unidade_gestora",
            "numero_contrato",
            "id_compra",
        ],
        dedup_exclude=["data_hora_inclusao", "data_hora_exclusao"],
        unique_tolerance=0.01,
        unique_where="id_compra is not null",
        ignore_values=[
            "codigo_subcategoria",
            "subcategoria",
            "data_hora_exclusao",
        ],
        year_range=R_CONTRATO,
        description=(
            "Contratos administrativos registrados no SIASG, de 2010 em diante. Uma linha "
            "por contrato, com vigência, fornecedor e valores. Uma unidade gestora reutiliza "
            "o número do contrato entre contratações, de modo que a chave inclui a compra de "
            "origem; cerca de 0,3% das chaves ainda se repetem com conteúdo divergente na "
            "fonte, e o teste de unicidade admite essa proporção"
        ),
    ),
    "contrato_item": DbtTable(
        dedup_order="data_hora_inclusao",
        # Same revision pattern as the ata items: 90% of the repeated keys differ
        # only in indicador_item_excluido and 97% in the inclusion timestamp, so
        # the source re-serves an item after revising it and keeping both double
        # counts. id_compra is part of the key because a unidade gestora reuses a
        # contract number across procurements, as on the parent table; it is null
        # for 161,166 rows, hence the nullable fallback.
        key=[
            "codigo_orgao",
            "codigo_unidade_gestora",
            "numero_contrato",
            "id_compra",
            "numero_item",
        ],
        dedup_exclude=["data_hora_inclusao", "data_hora_exclusao_item"],
        dedup_by_key=True,
        dedup_key_nullable=True,
        year_range=R_CONTRATO,
        description=(
            "Itens dos contratos administrativos registrados no SIASG. Uma linha por item "
            "contratado, com quantidade e valores unitário e total, no estado mais recente "
            "informado pela fonte"
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
        # 91 of 5.2M keys repeat, 0.0017%. Unlike the item and ata tables these
        # are NOT revisions: only 10 of the 91 differ in data_alteracao, while
        # 17 differ in valor and 13 in objeto. Collapsing by key would pick one
        # of two genuinely different records on an ordering that is arbitrary
        # for 81 of them, so content dedup stands. The tolerance is set just
        # above what the source actually contains, so a regression trips it --
        # the previous 0.001 permitted 5,236 duplicates, sixty times the real
        # number, and would have stayed green while the problem grew.
        unique_tolerance=0.0001,
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
