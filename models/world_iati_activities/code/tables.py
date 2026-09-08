"""Per-table metadata for world_iati_activities, in one place.

``gen_dbt.py`` reads the Portuguese descriptions for schema.yml;
``register_metadata.py`` reads everything. Keeping them in one module is what
stops the dbt description and the backend description drifting apart.

Coverage years are measured, not assumed. IATI accepts whatever date a
publisher types, so the raw extremes are meaningless — ``activity`` alone spans
year 1016 to year 9999. Each range below is the span holding the central 99.9%
of that table's rows, computed from the cleaned parquet; the outliers are still
in the data and still queryable, they are simply not what the coverage claims.
Tables with no date of their own inherit the aid-flow range, 1983-2026.
"""

# entity slug -> the column that identifies that observation level. Passing the
# link is what stops the site rendering the level's columns as "Não informado".
OL_COLUMN = {
    "dataset": "registry_dataset_id",
    "project": "activity_id",
    "transaction": "transaction_id",
    "sector": "sector_code",
    "country": "recipient_country_code",
    "region": "recipient_region_code",
    "year": "year",
    "document": "document_link_id",
}

AID_FLOW_YEARS = (1983, 2026)

TABLES = {
    "registry_dataset": {
        "name_pt": "Conjuntos de dados do registro",
        "name_en": "Registry datasets",
        "name_es": "Conjuntos de datos del registro",
        "description_pt": (
            "Conjuntos de dados registrados no Registro IATI, com a licença "
            "declarada por cada organização publicadora. É a tabela de "
            "proveniência e de licenciamento à qual todas as demais se ligam. "
            "Inclui os conjuntos não comerciais, cujas linhas foram removidas "
            "das demais tabelas, e os conjuntos que o Bulk Data Service não "
            "conseguiu baixar (is_downloaded falso), que não têm linhas em "
            "nenhuma outra tabela"
        ),
        "description_en": (
            "Datasets registered in the IATI Registry, with the licence each "
            "publishing organisation declared. This is the provenance and "
            "licensing table every other table links to. It includes the "
            "non-commercial datasets, whose rows were removed from the other "
            "tables, and the datasets the Bulk Data Service could not download "
            "(is_downloaded false), which have no rows in any other table"
        ),
        "description_es": (
            "Conjuntos de datos registrados en el Registro IATI, con la "
            "licencia declarada por cada organización publicadora. Es la tabla "
            "de procedencia y de licenciamiento a la que se vinculan todas las "
            "demás. Incluye los conjuntos no comerciales, cuyas filas fueron "
            "eliminadas de las demás tablas, y los conjuntos que el Bulk Data "
            "Service no pudo descargar (is_downloaded falso), que no tienen "
            "filas en ninguna otra tabla"
        ),
        "entities": ["dataset"],
        "years": AID_FLOW_YEARS,
    },
    "activity": {
        "name_pt": "Atividades",
        "name_en": "Activities",
        "name_es": "Actividades",
        "description_pt": (
            "Atividades de cooperação e ajuda internacional publicadas no "
            "padrão IATI. Uma linha por atividade. É a tabela de topo: todas as "
            "demais, exceto organisation e registry_dataset, ligam-se a ela por "
            "activity_id"
        ),
        "description_en": (
            "Development cooperation and humanitarian aid activities published "
            "to the IATI standard. One row per activity. This is the top-level "
            "table: every other table except organisation and registry_dataset "
            "links to it through activity_id"
        ),
        "description_es": (
            "Actividades de cooperación y ayuda internacional publicadas en el "
            "estándar IATI. Una fila por actividad. Es la tabla principal: "
            "todas las demás, excepto organisation y registry_dataset, se "
            "vinculan a ella por activity_id"
        ),
        "entities": ["project"],
        "years": AID_FLOW_YEARS,
    },
    "transaction": {
        "name_pt": "Transações",
        "name_en": "Transactions",
        "name_es": "Transacciones",
        "description_pt": (
            "Transações financeiras declaradas em cada atividade, incluindo "
            "compromissos, desembolsos, gastos e reembolsos. Uma linha por "
            "transação. Particionada pelo ano de transaction_date"
        ),
        "description_en": (
            "Financial transactions declared on each activity, including "
            "commitments, disbursements, expenditure and refunds. One row per "
            "transaction. Partitioned by the year of transaction_date"
        ),
        "description_es": (
            "Transacciones financieras declaradas en cada actividad, incluidos "
            "compromisos, desembolsos, gastos y reembolsos. Una fila por "
            "transacción. Particionada por el año de transaction_date"
        ),
        "entities": ["transaction", "year"],
        "years": (1983, 2026),
    },
    "transaction_breakdown": {
        "name_pt": "Decomposição de transações",
        "name_en": "Transaction breakdown",
        "name_es": "Descomposición de transacciones",
        "description_pt": (
            "Decomposição de cada transação em partes proporcionais por setor e "
            "por destino geográfico, seguindo a metodologia do Country "
            "Development Finance Data. Uma linha por combinação de transação, "
            "setor, país e região. Particionada pelo ano de transaction_date. "
            "Não tem chave única: um publicador pode declarar o mesmo setor ou "
            "o mesmo receptor duas vezes na mesma atividade, e a decomposição "
            "então repete a combinação com valores distintos — 441.215 linhas, "
            "1,79% da tabela"
        ),
        "description_en": (
            "Each transaction split proportionally across sectors and "
            "geographic destinations, following the Country Development Finance "
            "Data methodology. One row per combination of transaction, sector, "
            "country and region. Partitioned by the year of transaction_date. "
            "It has no unique key: a publisher can declare the same sector or "
            "the same recipient twice on one activity, and the breakdown then "
            "repeats the combination with different values — 441,215 rows, "
            "1.79% of the table"
        ),
        "description_es": (
            "Descomposición de cada transacción en partes proporcionales por "
            "sector y por destino geográfico, siguiendo la metodología del "
            "Country Development Finance Data. Una fila por combinación de "
            "transacción, sector, país y región. Particionada por el año de "
            "transaction_date. No tiene clave única: un publicador puede "
            "declarar el mismo sector o el mismo receptor dos veces en la misma "
            "actividad, y la descomposición entonces repite la combinación con "
            "valores distintos — 441.215 filas, 1,79% de la tabla"
        ),
        "entities": ["transaction", "sector", "country", "year"],
        "years": (1985, 2026),
    },
    "transaction_sector": {
        "name_pt": "Setores das transações",
        "name_en": "Transaction sectors",
        "name_es": "Sectores de las transacciones",
        "description_pt": (
            "Setores declarados diretamente em cada transação, antes da "
            "decomposição proporcional. Uma linha por setor de cada transação"
        ),
        "description_en": (
            "Sectors declared directly on each transaction, before the "
            "proportional breakdown. One row per sector of each transaction"
        ),
        "description_es": (
            "Sectores declarados directamente en cada transacción, antes de la "
            "descomposición proporcional. Una fila por sector de cada "
            "transacción"
        ),
        "entities": ["transaction", "sector"],
        "years": AID_FLOW_YEARS,
    },
    "budget": {
        "name_pt": "Orçamentos",
        "name_en": "Budgets",
        "name_es": "Presupuestos",
        "description_pt": (
            "Orçamentos declarados em cada atividade, por período. Uma linha "
            "por período orçamentário. Particionada pelo ano de "
            "period_start_date, que se estende ao futuro porque os orçamentos "
            "são prospectivos"
        ),
        "description_en": (
            "Budgets declared on each activity, by period. One row per budget "
            "period. Partitioned by the year of period_start_date, which runs "
            "into the future because budgets are forward-looking"
        ),
        "description_es": (
            "Presupuestos declarados en cada actividad, por período. Una fila "
            "por período presupuestario. Particionada por el año de "
            "period_start_date, que se extiende al futuro porque los "
            "presupuestos son prospectivos"
        ),
        "entities": ["project", "year"],
        "years": (1976, 2050),
    },
    "planned_disbursement": {
        "name_pt": "Desembolsos planejados",
        "name_en": "Planned disbursements",
        "name_es": "Desembolsos planificados",
        "description_pt": (
            "Desembolsos planejados em cada atividade, por período. Uma linha "
            "por período. Particionada pelo ano de period_start_date"
        ),
        "description_en": (
            "Planned disbursements on each activity, by period. One row per "
            "period. Partitioned by the year of period_start_date"
        ),
        "description_es": (
            "Desembolsos planificados en cada actividad, por período. Una fila "
            "por período. Particionada por el año de period_start_date"
        ),
        "entities": ["project", "year"],
        "years": (1998, 2030),
    },
    "sector": {
        "name_pt": "Setores das atividades",
        "name_en": "Activity sectors",
        "name_es": "Sectores de las actividades",
        "description_pt": (
            "Setores atribuídos a cada atividade, com a parcela da atividade "
            "que cabe a cada um. Uma linha por setor de cada atividade"
        ),
        "description_en": (
            "Sectors attributed to each activity, with the share of the "
            "activity each accounts for. One row per sector of each activity"
        ),
        "description_es": (
            "Sectores atribuidos a cada actividad, con la parte de la actividad "
            "que corresponde a cada uno. Una fila por sector de cada actividad"
        ),
        "entities": ["project", "sector"],
        "years": AID_FLOW_YEARS,
    },
    "recipient_country": {
        "name_pt": "Países receptores",
        "name_en": "Recipient countries",
        "name_es": "Países receptores",
        "description_pt": (
            "Países receptores de cada atividade, com a parcela da atividade "
            "que cabe a cada um. Uma linha por país de cada atividade"
        ),
        "description_en": (
            "Recipient countries of each activity, with the share of the "
            "activity each accounts for. One row per country of each activity"
        ),
        "description_es": (
            "Países receptores de cada actividad, con la parte de la actividad "
            "que corresponde a cada uno. Una fila por país de cada actividad"
        ),
        "entities": ["project", "country"],
        "years": AID_FLOW_YEARS,
    },
    "recipient_region": {
        "name_pt": "Regiões receptoras",
        "name_en": "Recipient regions",
        "name_es": "Regiones receptoras",
        "description_pt": (
            "Regiões receptoras de cada atividade, com a parcela da atividade "
            "que cabe a cada uma. Uma linha por região de cada atividade"
        ),
        "description_en": (
            "Recipient regions of each activity, with the share of the activity "
            "each accounts for. One row per region of each activity"
        ),
        "description_es": (
            "Regiones receptoras de cada actividad, con la parte de la "
            "actividad que corresponde a cada una. Una fila por región de cada "
            "actividad"
        ),
        "entities": ["project", "region"],
        "years": AID_FLOW_YEARS,
    },
    "participating_org": {
        "name_pt": "Organizações participantes",
        "name_en": "Participating organisations",
        "name_es": "Organizaciones participantes",
        "description_pt": (
            "Organizações que participam de cada atividade e o papel de cada "
            "uma, entre financiador, responsável, extensor e executor. Uma "
            "linha por participação"
        ),
        "description_en": (
            "Organisations taking part in each activity and the role of each, "
            "among funding, accountable, extending and implementing. One row "
            "per participation"
        ),
        "description_es": (
            "Organizaciones que participan en cada actividad y el papel de cada "
            "una, entre financiador, responsable, extensor y ejecutor. Una fila "
            "por participación"
        ),
        "entities": ["project"],
        "years": AID_FLOW_YEARS,
    },
    "related_activity": {
        "name_pt": "Atividades relacionadas",
        "name_en": "Related activities",
        "name_es": "Actividades relacionadas",
        "description_pt": (
            "Ligações declaradas entre atividades, como atividade-mãe, filha, "
            "irmã e cofinanciada. Uma linha por ligação. A atividade "
            "referenciada pode não existir nesta base"
        ),
        "description_en": (
            "Declared links between activities, such as parent, child, sibling "
            "and co-funded activity. One row per link. The referenced activity "
            "may not exist in this database"
        ),
        "description_es": (
            "Vínculos declarados entre actividades, como actividad madre, hija, "
            "hermana y cofinanciada. Una fila por vínculo. La actividad "
            "referenciada puede no existir en esta base"
        ),
        "entities": ["project"],
        "years": AID_FLOW_YEARS,
    },
    "policy_marker": {
        "name_pt": "Marcadores de política",
        "name_en": "Policy markers",
        "name_es": "Marcadores de política",
        "description_pt": (
            "Marcadores de política atribuídos a cada atividade, como "
            "igualdade de gênero e mitigação climática, com o grau em que são "
            "objetivo da atividade. Uma linha por marcador de cada atividade"
        ),
        "description_en": (
            "Policy markers attributed to each activity, such as gender "
            "equality and climate mitigation, with the degree to which they are "
            "an objective of the activity. One row per marker of each activity"
        ),
        "description_es": (
            "Marcadores de política atribuidos a cada actividad, como igualdad "
            "de género y mitigación climática, con el grado en que son objetivo "
            "de la actividad. Una fila por marcador de cada actividad"
        ),
        "entities": ["project"],
        "years": AID_FLOW_YEARS,
    },
    "document_link": {
        "name_pt": "Documentos",
        "name_en": "Documents",
        "name_es": "Documentos",
        "description_pt": (
            "Documentos associados a cada atividade, com endereço, formato e "
            "título. Uma linha por documento. Os endereços são declarados pelo "
            "publicador e não são verificados"
        ),
        "description_en": (
            "Documents attached to each activity, with address, format and "
            "title. One row per document. The addresses are declared by the "
            "publisher and are not verified"
        ),
        "description_es": (
            "Documentos asociados a cada actividad, con dirección, formato y "
            "título. Una fila por documento. Las direcciones son declaradas por "
            "el publicador y no son verificadas"
        ),
        "entities": ["project", "document"],
        "years": AID_FLOW_YEARS,
    },
    "location": {
        "name_pt": "Locais",
        "name_en": "Locations",
        "name_es": "Lugares",
        "description_pt": (
            "Locais subnacionais associados a cada atividade, com coordenadas "
            "quando declaradas. Uma linha por local de cada atividade"
        ),
        "description_en": (
            "Sub-national locations attached to each activity, with coordinates "
            "where declared. One row per location of each activity"
        ),
        "description_es": (
            "Lugares subnacionales asociados a cada actividad, con coordenadas "
            "cuando se declaran. Una fila por lugar de cada actividad"
        ),
        "entities": ["project"],
        "years": AID_FLOW_YEARS,
    },
    "result": {
        "name_pt": "Resultados",
        "name_en": "Results",
        "name_es": "Resultados",
        "description_pt": (
            "Resultados declarados em cada atividade, entre produto, efeito e "
            "impacto. Uma linha por resultado"
        ),
        "description_en": (
            "Results declared on each activity, among output, outcome and "
            "impact. One row per result"
        ),
        "description_es": (
            "Resultados declarados en cada actividad, entre producto, efecto e "
            "impacto. Una fila por resultado"
        ),
        "entities": ["project"],
        "years": AID_FLOW_YEARS,
    },
    "result_indicator": {
        "name_pt": "Indicadores de resultado",
        "name_en": "Result indicators",
        "name_es": "Indicadores de resultado",
        "description_pt": "Indicadores de cada resultado. Uma linha por indicador",
        "description_en": "Indicators of each result. One row per indicator",
        "description_es": "Indicadores de cada resultado. Una fila por indicador",
        "entities": ["project"],
        "years": AID_FLOW_YEARS,
    },
    "result_indicator_period": {
        "name_pt": "Períodos dos indicadores",
        "name_en": "Indicator periods",
        "name_es": "Períodos de los indicadores",
        "description_pt": (
            "Períodos de medição de cada indicador, com meta e valor efetivo. "
            "Uma linha por período. Particionada pelo ano de period_start_date. "
            "5.600 linhas trazem 1900 como ano de início, um valor sentinela de "
            "publicadores, e ficam fora da cobertura declarada"
        ),
        "description_en": (
            "Measurement periods of each indicator, with target and actual "
            "value. One row per period. Partitioned by the year of "
            "period_start_date. 5,600 rows carry 1900 as their start year, a "
            "publisher sentinel, and fall outside the declared coverage"
        ),
        "description_es": (
            "Períodos de medición de cada indicador, con meta y valor efectivo. "
            "Una fila por período. Particionada por el año de "
            "period_start_date. 5.600 filas traen 1900 como año de inicio, un "
            "valor centinela de publicadores, y quedan fuera de la cobertura "
            "declarada"
        ),
        "entities": ["project", "year"],
        "years": (1990, 2031),
    },
    "organisation": {
        "name_pt": "Organizações",
        "name_en": "Organisations",
        "name_es": "Organizaciones",
        "description_pt": (
            "Organizações que publicam arquivos de organização no padrão IATI, "
            "distintos dos arquivos de atividade. Uma linha por organização"
        ),
        "description_en": (
            "Organisations that publish organisation files to the IATI "
            "standard, which are distinct from activity files. One row per "
            "organisation"
        ),
        "description_es": (
            "Organizaciones que publican archivos de organización en el "
            "estándar IATI, distintos de los archivos de actividad. Una fila "
            "por organización"
        ),
        "entities": ["dataset"],
        "years": AID_FLOW_YEARS,
    },
}
