"""English and Spanish renderings of every distinct column observation.

Keyed by the exact Portuguese text in the architecture CSVs. The backend stores
`observations` per language, and callers that pass only the bare `observations`
key leave EN and ES blank -- which is how 3,022 production columns ended up
Portuguese-only. Registering all three from the start avoids joining them.

`check_translations()` asserts the map covers every observation actually in use,
so adding a note to the architecture without translating it fails loudly rather
than silently shipping a PT-only column.
"""

from __future__ import annotations

OBSERVATIONS: dict[str, tuple[str, str]] = {
    "6 dispensa de licitação, 7 inexigibilidade de licitação": (
        "6 waiver of tender, 7 non-enforceability of tender",
        "6 dispensa de licitación, 7 inexigibilidad de licitación",
    ),
    "97,4% das contratações nunca são alteradas após a publicação": (
        "97.4% of procurements are never changed after publication",
        "97,4% de las contrataciones nunca se modifican tras la publicación",
    ),
    "A fonte não publica o código IBGE do município, apenas o nome, com grafia inconsistente": (
        "The source publishes no IBGE municipality code, only the name, spelled inconsistently",
        "La fuente no publica el código IBGE del municipio, solo el nombre, con grafía inconsistente",
    ),
    "A fonte publica o CPF parcialmente mascarado, no formato ***007562**": (
        "The source publishes the taxpayer number partly masked, as ***007562**",
        "La fuente publica el CPF parcialmente enmascarado, en el formato ***007562**",
    ),
    "A fonte publica o CPF parcialmente mascarado, no formato ***308103**": (
        "The source publishes the taxpayer number partly masked, as ***308103**",
        "La fuente publica el CPF parcialmente enmascarado, en el formato ***308103**",
    ),
    "A fonte publica o CPF parcialmente mascarado. O valor ESTRANGEIRO indica vencedor sem "
    "CPF brasileiro": (
        "The source publishes the taxpayer number partly masked. The value ESTRANGEIRO marks a "
        "winner with no Brazilian taxpayer number",
        "La fuente publica el CPF parcialmente enmascarado. El valor ESTRANGEIRO indica un "
        "ganador sin CPF brasileño",
    ),
    "A fonte publica o valor como 0 ou 1, não como booleano": (
        "The source publishes the value as 0 or 1, not as a boolean",
        "La fuente publica el valor como 0 o 1, no como booleano",
    ),
    "A fonte publica o valor como texto TRUE ou FALSE, não como booleano": (
        "The source publishes the value as the text TRUE or FALSE, not as a boolean",
        "La fuente publica el valor como texto TRUE o FALSE, no como booleano",
    ),
    "Chave primária da tabela": (
        "Primary key of the table",
        "Clave primaria de la tabla",
    ),
    "Chave primária da tabela. Junta com a tabela licitacao, embora pregões sem data de "
    "publicação não apareçam lá": (
        "Primary key of the table. Joins to licitacao, though reverse auctions with no "
        "publication date never appear there",
        "Clave primaria de la tabla. Se une a licitacao, aunque los pregones sin fecha de "
        "publicación no aparecen allí",
    ),
    "Chave primária da tabela. Junta com licitacao_item": (
        "Primary key of the table. Joins to licitacao_item",
        "Clave primaria de la tabla. Se une a licitacao_item",
    ),
    "Codificação do PNCP. O código 6 do SIASG corresponde ao 8 do PNCP, Dispensa": (
        "PNCP coding. SIASG code 6 corresponds to PNCP code 8, Dispensa",
        "Codificación del PNCP. El código 6 del SIASG corresponde al 8 del PNCP, Dispensa",
    ),
    "Codificação do SIASG, distinta da do PNCP. Apenas 3, 5, 6 e 7 possuem registros": (
        "SIASG coding, distinct from PNCP's. Only 3, 5, 6 and 7 carry any records",
        "Codificación del SIASG, distinta de la del PNCP. Solo 3, 5, 6 y 7 tienen registros",
    ),
    "Codificação do regime da Lei 8.666, distinta da usada nas tabelas de contratação sob a "
    "Lei 14.133": (
        "Coding of the Law 8,666 regime, distinct from the one used in the Law 14,133 "
        "procurement tables",
        "Codificación del régimen de la Ley 8.666, distinta de la usada en las tablas de "
        "contratación bajo la Ley 14.133",
    ),
    "Codificação interna do SIASG, distinta do código IBGE em id_municipio": (
        "SIASG-internal coding, distinct from the IBGE code in id_municipio",
        "Codificación interna del SIASG, distinta del código IBGE en id_municipio",
    ),
    "Codificação interna do SIASG, sem equivalência direta com o código IBGE. A UF pode ser "
    "obtida juntando com a tabela unidade_administrativa": (
        "SIASG-internal coding with no direct IBGE equivalent. The federative unit can be "
        "recovered by joining unidade_administrativa",
        "Codificación interna del SIASG, sin equivalencia directa con el código IBGE. La unidad "
        "federativa se obtiene uniendo con unidade_administrativa",
    ),
    "Coluna de particionamento, correspondente ao campo dt_ano_aviso da fonte": (
        "Partition column, matching the source's dt_ano_aviso field",
        "Columna de particionamiento, correspondiente al campo dt_ano_aviso de la fuente",
    ),
    "Coluna de particionamento, correspondente ao campo dt_ano_aviso_licitacao da fonte": (
        "Partition column, matching the source's dt_ano_aviso_licitacao field",
        "Columna de particionamiento, correspondiente al campo dt_ano_aviso_licitacao de la fuente",
    ),
    "Coluna de particionamento, derivada da data correspondente": (
        "Partition column, derived from the corresponding date",
        "Columna de particionamiento, derivada de la fecha correspondiente",
    ),
    "Coluna de particionamento, derivada de data_vigencia_inicial": (
        "Partition column, derived from data_vigencia_inicial",
        "Columna de particionamiento, derivada de data_vigencia_inicial",
    ),
    "Coluna de particionamento. A fonte não publica data para o item, portanto o ano é extraído "
    "dos quatro últimos dígitos de id_compra": (
        "Partition column. The source publishes no date for the item, so the year is taken from "
        "the last four digits of id_compra",
        "Columna de particionamiento. La fuente no publica fecha para el ítem, por lo que el año "
        "se extrae de los últimos cuatro dígitos de id_compra",
    ),
    "Coluna de particionamento. A fonte publica apenas o estado corrente do cadastro, sem "
    "histórico, portanto cada extração é uma fotografia": (
        "Partition column. The source publishes only the registry's current state, with no "
        "history, so each extraction is a snapshot",
        "Columna de particionamiento. La fuente publica solo el estado actual del registro, sin "
        "histórico, por lo que cada extracción es una fotografía",
    ),
    "Com codigo_orgao e codigo_unidade_gestora forma a chave primária": (
        "With codigo_orgao and codigo_unidade_gestora it forms the primary key",
        "Con codigo_orgao y codigo_unidade_gestora forma la clave primaria",
    ),
    "Com codigo_orgao, codigo_unidade_gestora e numero_contrato forma a chave primária": (
        "With codigo_orgao, codigo_unidade_gestora and numero_contrato it forms the primary key",
        "Con codigo_orgao, codigo_unidade_gestora y numero_contrato forma la clave primaria",
    ),
    "Com numero_controle_pncp_ata e classificacao_fornecedor forma a chave primária": (
        "With numero_controle_pncp_ata and classificacao_fornecedor it forms the primary key",
        "Con numero_controle_pncp_ata y classificacao_fornecedor forma la clave primaria",
    ),
    "Concatena código da UASG, modalidade e número da compra. NÃO é único: uma mesma chave do "
    "SIASG pode corresponder a dezenas de contratações distintas no PNCP, cada uma com seu "
    "número de controle e data de publicação. A chave primária da tabela é numero_controle_pncp": (
        "Concatenates the UASG code, modality and purchase number. NOT unique: one SIASG key can "
        "map to dozens of distinct PNCP procurements, each with its own control number and "
        "publication date. The table's primary key is numero_controle_pncp",
        "Concatena el código de la UASG, la modalidad y el número de la compra. NO es único: una "
        "misma clave del SIASG puede corresponder a decenas de contrataciones distintas en el "
        "PNCP, cada una con su número de control y fecha de publicación. La clave primaria de la "
        "tabla es numero_controle_pncp",
    ),
    "Concatena código da UASG, modalidade, número do aviso e ano. Chave primária da tabela": (
        "Concatenates the UASG code, modality, notice number and year. Primary key of the table",
        "Concatena el código de la UASG, la modalidad, el número del aviso y el año. Clave "
        "primaria de la tabla",
    ),
    "Concatena o PDM e os valores de cada característica do padrão descritivo": (
        "Concatenates the descriptive standard and the value of each of its characteristics",
        "Concatena el patrón descriptivo y los valores de cada una de sus características",
    ),
    "Contratações SRP geram atas de registro de preço, na tabela ata_registro_preco": (
        "SRP procurements produce price records, in the ata_registro_preco table",
        "Las contrataciones SRP generan actas de registro de precios, en la tabla "
        "ata_registro_preco",
    ),
    "D despesa, R receita, S sem ônus": (
        "D expenditure, R revenue, S no cost",
        "D gasto, R ingreso, S sin costo",
    ),
    "Durante a transição entre regimes algumas licitações da Lei 14.133 também aparecem no "
    "módulo legado": (
        "During the transition between regimes some Law 14,133 tenders also appear in the legacy "
        "module",
        "Durante la transición entre regímenes algunas licitaciones de la Ley 14.133 también "
        "aparecen en el módulo legado",
    ),
    "E Executivo, L Legislativo, J Judiciário, N não classificado": (
        "E Executive, L Legislative, J Judiciary, N unclassified",
        "E Ejecutivo, L Legislativo, J Judicial, N no clasificado",
    ),
    "F federal, E estadual, M municipal, D distrital, N não classificado": (
        "F federal, E state, M municipal, D district, N unclassified",
        "F federal, E estadual, M municipal, D distrital, N no clasificado",
    ),
    "Formato CNPJ-1-sequencial/ano. Chave primária da tabela e chave de junção com o conjunto "
    "br_pncp": (
        "Format CNPJ-1-sequential/year. Primary key of the table and the join key to the br_pncp "
        "dataset",
        "Formato CNPJ-1-secuencial/año. Clave primaria de la tabla y clave de unión con el "
        "conjunto br_pncp",
    ),
    "Idêntico a numero_controle_pncp em todas as amostras verificadas": (
        "Identical to numero_controle_pncp in every sample checked",
        "Idéntico a numero_controle_pncp en todas las muestras verificadas",
    ),
    "Integralmente vazia nas amostras verificadas em 2026-08": (
        "Entirely empty in the samples checked in 2026-08",
        "Completamente vacía en las muestras verificadas en 2026-08",
    ),
    "Integralmente vazia nas amostras verificadas em 2026-08. O nome da unidade está em "
    "nome_uasg": (
        "Entirely empty in the samples checked in 2026-08. The unit's name is in nome_uasg",
        "Completamente vacía en las muestras verificadas en 2026-08. El nombre de la unidad está "
        "en nome_uasg",
    ),
    "Integralmente vazia nas amostras verificadas em 2026-08. O vencedor está disponível na "
    "tabela licitacao_item": (
        "Entirely empty in the samples checked in 2026-08. The winner is available in the "
        "licitacao_item table",
        "Completamente vacía en las muestras verificadas en 2026-08. El ganador está disponible "
        "en la tabla licitacao_item",
    ),
    "Junta com a tabela ata_registro_preco": (
        "Joins to the ata_registro_preco table",
        "Se une a la tabla ata_registro_preco",
    ),
    "Junta com a tabela compra_sem_licitacao": (
        "Joins to the compra_sem_licitacao table",
        "Se une a la tabla compra_sem_licitacao",
    ),
    "Junta com a tabela contratacao": (
        "Joins to the contratacao table",
        "Se une a la tabla contratacao",
    ),
    "Junta com a tabela contratacao_item": (
        "Joins to the contratacao_item table",
        "Se une a la tabla contratacao_item",
    ),
    "Junta com a tabela licitacao": (
        "Joins to the licitacao table",
        "Se une a la tabla licitacao",
    ),
    "Junta com as tabelas licitacao e licitacao_pregao": (
        "Joins to the licitacao and licitacao_pregao tables",
        "Se une a las tablas licitacao y licitacao_pregao",
    ),
    "Junta com catalogo_material quando tipo_item é M e com catalogo_servico quando é S": (
        "Joins to catalogo_material when tipo_item is M and to catalogo_servico when it is S",
        "Se une a catalogo_material cuando tipo_item es M y a catalogo_servico cuando es S",
    ),
    "Lista separada por vírgula quando há mais de uma unidade": (
        "Comma-separated list when there is more than one unit",
        "Lista separada por comas cuando hay más de una unidad",
    ),
    "M material, S serviço": (
        "M material, S service",
        "M material, S servicio",
    ),
    "O Decreto 7.174/2010 regula a preferência para bens e serviços de informática. A fonte "
    "publica o valor como texto true ou false, não como booleano": (
        "Decree 7,174/2010 governs the preference for IT goods and services. The source "
        "publishes the value as the text true or false, not as a boolean",
        "El Decreto 7.174/2010 regula la preferencia para bienes y servicios informáticos. La "
        "fuente publica el valor como texto true o false, no como booleano",
    ),
    "O porte determina a elegibilidade aos benefícios da Lei Complementar 123/2006": (
        "Company size determines eligibility for the benefits of Complementary Law 123/2006",
        "El tamaño determina la elegibilidad para los beneficios de la Ley Complementaria 123/2006",
    ),
    "O valor 0 indica ausência de unidade espelho": (
        "The value 0 means there is no mirror unit",
        "El valor 0 indica ausencia de unidad espejo",
    ),
    "O valor 0 indica item que não integra grupo": (
        "The value 0 marks an item that belongs to no group",
        "El valor 0 indica un ítem que no integra ningún grupo",
    ),
    "O valor 0 indica item que não é material": (
        "The value 0 marks an item that is not a material",
        "El valor 0 indica un ítem que no es material",
    ),
    "O valor 0 indica órgão sem CNPJ próprio, como as agregações regionais": (
        "The value 0 marks a body with no taxpayer number of its own, such as the regional "
        "aggregations",
        "El valor 0 indica un órgano sin CNPJ propio, como las agregaciones regionales",
    ),
    "O valor EX identifica unidades sediadas no exterior": (
        "The value EX marks units based abroad",
        "El valor EX identifica unidades ubicadas en el exterior",
    ),
    "O valor EX identifica unidades sediadas no exterior, como embaixadas e consulados, e não "
    "corresponde a nenhuma unidade da federação": (
        "The value EX marks units based abroad, such as embassies and consulates, and "
        "corresponds to no federative unit",
        "El valor EX identifica unidades ubicadas en el exterior, como embajadas y consulados, y "
        "no corresponde a ninguna unidad federativa",
    ),
    "Os itens são preenchidos progressivamente: a mediana da defasagem entre inclusão e última "
    "atualização é de 78 dias": (
        "Items are filled in progressively: the median lag between inclusion and last update is "
        "78 days",
        "Los ítems se completan progresivamente: la mediana del desfase entre inclusión y última "
        "actualización es de 78 días",
    ),
    "PF pessoa física, PJ pessoa jurídica, PE pessoa estrangeira": (
        "PF individual, PJ legal entity, PE foreign person",
        "PF persona física, PJ persona jurídica, PE persona extranjera",
    ),
    "Preenchido apenas quando o item é material": (
        "Filled only when the item is a material",
        "Completado solo cuando el ítem es material",
    ),
    "Preenchido apenas quando o item é serviço": (
        "Filled only when the item is a service",
        "Completado solo cuando el ítem es servicio",
    ),
    "Preenchido para menos de 1% dos itens": (
        "Filled for fewer than 1% of items",
        "Completado para menos del 1% de los ítems",
    ),
    "Preenchido para menos de 4% dos itens, como codigo_subclasse": (
        "Filled for fewer than 4% of items, like codigo_subclasse",
        "Completado para menos del 4% de los ítems, como codigo_subclasse",
    ),
    "Publicada por extenso nesta tabela, ao contrário da coluna esfera de contratacao, que traz "
    "a sigla": (
        "Spelled out in this table, unlike contratacao's esfera column, which carries the code",
        "Escrita completa en esta tabla, a diferencia de la columna esfera de contratacao, que "
        "trae la sigla",
    ),
    "Publicado por extenso nesta tabela": (
        "Spelled out in this table",
        "Escrito completo en esta tabla",
    ),
    "SIORG é o Sistema de Informações Organizacionais do Governo Federal": (
        "SIORG is the federal government's organisational information system",
        "SIORG es el Sistema de Informaciones Organizacionales del Gobierno Federal",
    ),
    "SISG é o Sistema de Serviços Gerais, que reúne os órgãos da administração federal direta, "
    "autárquica e fundacional": (
        "SISG is the general services system, which covers the bodies of the direct, autarchic "
        "and foundational federal administration",
        "SISG es el Sistema de Servicios Generales, que reúne los órganos de la administración "
        "federal directa, autárquica y fundacional",
    ),
    "SISPP preço praticado, SISRP registro de preços": (
        "SISPP prevailing price, SISRP price registration",
        "SISPP precio practicado, SISRP registro de precios",
    ),
    "Sub-rogação ocorre em cerca de 0,1% das contratações": (
        "Subrogation occurs in about 0.1% of procurements",
        "La subrogación ocurre en cerca del 0,1% de las contrataciones",
    ),
    "Traz apenas o vencedor principal. A tabela contratacao_item_resultado lista todos os "
    "classificados": (
        "Carries only the main winner. The contratacao_item_resultado table lists every ranked "
        "supplier",
        "Trae solo al ganador principal. La tabla contratacao_item_resultado lista a todos los "
        "clasificados",
    ),
    "Um item pode registrar vários fornecedores classificados": (
        "One item may record several ranked suppliers",
        "Un ítem puede registrar varios proveedores clasificados",
    ),
    "Um item pode ter mais de um resultado, por exemplo em registro de preços com vários "
    "fornecedores classificados. Com id_compra_item forma a chave primária": (
        "An item may have more than one result, for instance in a price registration with "
        "several ranked suppliers. With id_compra_item it forms the primary key",
        "Un ítem puede tener más de un resultado, por ejemplo en un registro de precios con "
        "varios proveedores clasificados. Con id_compra_item forma la clave primaria",
    ),
    "Unidades estaduais e municipais aparecem no cadastro por adesão voluntária ao sistema "
    "federal": (
        "State and municipal units appear in the registry by voluntarily joining the federal "
        "system",
        "Las unidades estatales y municipales aparecen en el registro por adhesión voluntaria al "
        "sistema federal",
    ),
    "Vazio enquanto a contratação não tem resultado homologado": (
        "Empty while the procurement has no awarded result",
        "Vacío mientras la contratación no tiene resultado homologado",
    ),
    "Vazio para cerca de 80% dos contratos, que antecedem a obrigatoriedade de publicação no "
    "PNCP ou não foram enviados": (
        "Empty for about 80% of contracts, which predate the requirement to publish on PNCP or "
        "were never sent",
        "Vacío para cerca del 80% de los contratos, que anteceden a la obligatoriedad de "
        "publicación en el PNCP o no fueron enviados",
    ),
    "Vazio para fornecedores pessoa física, que são identificados em cpf": (
        "Empty for individual suppliers, who are identified in cpf",
        "Vacío para proveedores persona física, que se identifican en cpf",
    ),
}


def check_translations(observations: set[str]) -> list[str]:
    """Return any observation in use that has no EN/ES rendering."""
    return sorted(
        obs for obs in observations if obs and obs not in OBSERVATIONS
    )
