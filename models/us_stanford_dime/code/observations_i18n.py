"""Portuguese and Spanish renderings of the architecture's ``observations`` text.

Split from ``i18n.py`` only for size; the contract is identical — keyed by the
English string, so a note reused across tables is translated once.

``observations`` is per-language on the backend, and passing a bare
``observations`` key fills Portuguese while leaving English and Spanish blank.
That is how 3,022 production columns ended up Portuguese-only, so all three are
always passed here.
"""

from __future__ import annotations

# English -> (Portuguese, Spanish)
OBSERVATIONS: dict[str, tuple[str, str]] = {
    "Applies to committees only; these donors are excluded from the CFscore estimation": (
        "Aplica-se apenas a comitês; esses doadores são excluídos da estimação dos CFscores.",
        "Se aplica solo a comités; estos donantes se excluyen de la estimación de los CFscores.",
    ),
    "Assigned by DIME; populated for a minority of records": (
        "Atribuído pelo DIME; preenchido em uma minoria dos registros.",
        "Asignado por DIME; se completa en una minoría de los registros.",
    ),
    "Can be later than the cycle, as for senators fundraising during their first four years in office": (
        "Pode ser posterior ao ciclo, como no caso de senadores que arrecadam durante "
        "os quatro primeiros anos de mandato.",
        "Puede ser posterior al ciclo, como en el caso de senadores que recaudan durante "
        "los primeros cuatro años de mandato.",
    ),
    "Cleaned and standardized by DIME": (
        "Limpo e padronizado pelo DIME.",
        "Limpiado y estandarizado por DIME.",
    ),
    "Coded for federal congressional candidates only": (
        "Codificado apenas para candidatos ao Congresso federal.",
        "Codificado solo para candidatos al Congreso federal.",
    ),
    "Combines contributions, roll-call votes and other sources by multiple overimputation; new in version 4.0": (
        "Combina contribuições, votos nominais e outras fontes por sobreimputação "
        "múltipla; novidade da versão 4.0.",
        "Combina contribuciones, votaciones nominales y otras fuentes mediante "
        "sobreimputación múltiple; nuevo en la versión 4.0.",
    ),
    "Committees are listed as federal:committee or state:committee": (
        "Comitês aparecem como federal:committee ou state:committee.",
        "Los comités figuran como federal:committee o state:committee.",
    ),
    "Eleven-digit GEOID. Tract vintage follows the decennial boundaries in force for the cycle, so it is not linked to a single-vintage census tract directory": (
        "GEOID de onze dígitos. A safra do setor segue os limites decenais vigentes no "
        "ciclo, por isso não é vinculado a um diretório de setores censitários de safra única.",
        "GEOID de once dígitos. La vigencia del sector sigue los límites decenales vigentes "
        "en el ciclo, por lo que no se vincula a un directorio de sectores censales de una "
        "sola vigencia.",
    ),
    "Empty for committees and organizations": (
        "Vazio para comitês e organizações.",
        "Vacío para comités y organizaciones.",
    ),
    "Empty for state and local records": (
        "Vazio para registros estaduais e locais.",
        "Vacío para registros estatales y locales.",
    ),
    "Federal candidates take 'fd' as the state code, for example fd1980": (
        "Candidatos federais recebem 'fd' como código de estado, por exemplo fd1980.",
        "Los candidatos federales reciben 'fd' como código de estado, por ejemplo fd1980.",
    ),
    "Federal outcomes come from the FEC, state outcomes from NIMSP": (
        "Resultados federais vêm da FEC; resultados estaduais, do NIMSP.",
        "Los resultados federales provienen de la FEC; los estatales, de NIMSP.",
    ),
    "Five or nine digits depending on the filing": (
        "Cinco ou nove dígitos, conforme a declaração.",
        "Cinco o nueve dígitos, según la declaración.",
    ),
    "For example 1980_MS_S": (
        "Por exemplo, 1980_MS_S.",
        "Por ejemplo, 1980_MS_S.",
    ),
    "For example Jr., Sr., III": (
        "Por exemplo, Jr., Sr., III.",
        "Por ejemplo, Jr., Sr., III.",
    ),
    "For example Mr., Mrs., Dr., Esq.": (
        "Por exemplo, Mr., Mrs., Dr., Esq.",
        "Por ejemplo, Mr., Mrs., Dr., Esq.",
    ),
    "From voteview.com": ("De voteview.com.", "De voteview.com."),
    "From voteview.com, based on a joint scaling of the 1st to 117th Congresses": (
        "De voteview.com, com base em uma escala conjunta do 1º ao 117º Congresso.",
        "De voteview.com, con base en una escala conjunta del 1.º al 117.º Congreso.",
    ),
    "From voteview.com. House and Senate are scaled separately, so the two chambers are not directly comparable": (
        "De voteview.com. Câmara e Senado são escalados separadamente, portanto as duas "
        "casas não são diretamente comparáveis.",
        "De voteview.com. La Cámara y el Senado se escalan por separado, por lo que ambas "
        "cámaras no son directamente comparables.",
    ),
    "Geocoded with the Pelias geocoder": (
        "Geocodificado com o geocodificador Pelias.",
        "Geocodificado con el geocodificador Pelias.",
    ),
    "Geocoded with the Pelias geocoder; consistent per contributor_id across records with varying address completeness": (
        "Geocodificado com o geocodificador Pelias; consistente por contributor_id entre "
        "registros com graus distintos de completude do endereço.",
        "Geocodificado con el geocodificador Pelias; consistente por contributor_id entre "
        "registros con distintos grados de completitud de la dirección.",
    ),
    "Ideal point estimated from donations received; negative is liberal, positive is conservative. Constant across cycles for a recipient": (
        "Ponto ideal estimado a partir das doações recebidas; valores negativos indicam "
        "posição liberal e positivos, conservadora. Constante entre ciclos para um mesmo "
        "beneficiário.",
        "Punto ideal estimado a partir de las donaciones recibidas; los valores negativos "
        "indican posición liberal y los positivos, conservadora. Constante entre ciclos "
        "para un mismo receptor.",
    ),
    "Ideal point estimated from the contributor's giving; negative is liberal, positive is conservative. Repeated on every record of the same contributor": (
        "Ponto ideal estimado a partir das doações feitas pelo contribuinte; valores "
        "negativos indicam posição liberal e positivos, conservadora. Repetido em todos "
        "os registros do mesmo contribuinte.",
        "Punto ideal estimado a partir de las donaciones del contribuyente; los valores "
        "negativos indican posición liberal y los positivos, conservadora. Se repite en "
        "todos los registros del mismo contribuyente.",
    ),
    "Ideal point estimated from the donations the recipient received; negative is liberal, positive is conservative": (
        "Ponto ideal estimado a partir das doações recebidas pelo beneficiário; valores "
        "negativos indicam posição liberal e positivos, conservadora.",
        "Punto ideal estimado a partir de las donaciones recibidas por el receptor; los "
        "valores negativos indican posición liberal y los positivos, conservadora.",
    ),
    "Imputed by DIME from first-name gender ratios and gendered titles; not self-reported": (
        "Imputado pelo DIME a partir de razões de gênero por primeiro nome e de títulos "
        "com marcação de gênero; não é autodeclarado.",
        "Imputado por DIME a partir de proporciones de género por nombre de pila y de "
        "títulos con marca de género; no es autodeclarado.",
    ),
    "Imputed by DIME from first-name gender ratios reported by the U.S. Census and from gendered titles; not self-reported": (
        "Imputado pelo DIME a partir de razões de gênero por primeiro nome reportadas pelo "
        "Censo dos EUA e de títulos com marcação de gênero; não é autodeclarado.",
        "Imputado por DIME a partir de proporciones de género por nombre de pila reportadas "
        "por el Censo de EE. UU. y de títulos con marca de género; no es autodeclarado.",
    ),
    "Imputed for all candidates except those who served in Congress. Four rows carry the value with literal quote marks, an escaping artefact left in the source": (
        "Imputado para todos os candidatos, exceto os que serviram no Congresso. Quatro "
        "linhas trazem o valor entre aspas literais, artefato de escape presente na fonte.",
        "Imputado para todos los candidatos, salvo quienes sirvieron en el Congreso. Cuatro "
        "filas traen el valor entre comillas literales, un artefacto de escape presente en "
        "la fuente.",
    ),
    "Includes territories and foreign codes, so it is not linked to the state directory": (
        "Inclui territórios e códigos estrangeiros, por isso não é vinculado ao diretório "
        "de estados.",
        "Incluye territorios y códigos extranjeros, por lo que no se vincula al directorio "
        "de estados.",
    ),
    "Includes territories and foreign codes, so it is not restricted to the 50 states plus DC and is not linked to the state directory": (
        "Inclui territórios e códigos estrangeiros, portanto não se restringe aos 50 estados "
        "mais o DC e não é vinculado ao diretório de estados.",
        "Incluye territorios y códigos extranjeros, por lo que no se limita a los 50 estados "
        "más DC y no se vincula al directorio de estados.",
    ),
    "Joins to the contribution and contributor_cycle tables": (
        "Faz junção com as tabelas contribution e contributor_cycle.",
        "Se une con las tablas contribution y contributor_cycle.",
    ),
    "Joins to the contributor and contribution tables": (
        "Faz junção com as tabelas contributor e contribution.",
        "Se une con las tablas contributor y contribution.",
    ),
    "Joins to transaction_id in the contribution table": (
        "Faz junção com transaction_id na tabela contribution.",
        "Se une con transaction_id en la tabla contribution.",
    ),
    "Last name first; suffix and title removed": (
        "Sobrenome primeiro; sufixo e título removidos.",
        "Apellido primero; sufijo y título eliminados.",
    ),
    "Links a candidate to their own personal contributions. Partially populated: a missing value does not mean the candidate made no contributions": (
        "Liga um candidato às suas próprias contribuições pessoais. Preenchido "
        "parcialmente: um valor ausente não significa que o candidato não fez contribuições.",
        "Vincula a un candidato con sus propias contribuciones personales. Se completa "
        "parcialmente: un valor ausente no significa que el candidato no haya contribuido.",
    ),
    "Match against the recipient table for more detailed party codings": (
        "Cruze com a tabela recipient para codificações partidárias mais detalhadas.",
        "Cruce con la tabla recipient para codificaciones partidarias más detalladas.",
    ),
    "Measured at the most recent or concurrent presidential election": (
        "Medido na eleição presidencial mais recente ou concomitante.",
        "Medido en la elección presidencial más reciente o concurrente.",
    ),
    "Negative is liberal, positive is conservative. Donors who gave to a single recipient are assigned that recipient's ideal point": (
        "Valores negativos indicam posição liberal e positivos, conservadora. Doadores que "
        "contribuíram a um único beneficiário recebem o ponto ideal desse beneficiário.",
        "Los valores negativos indican posición liberal y los positivos, conservadora. A los "
        "donantes que contribuyeron a un solo receptor se les asigna el punto ideal de ese "
        "receptor.",
    ),
    "Nominal dollars": ("Dólares nominais.", "Dólares nominales."),
    "Nominal dollars, not adjusted for inflation. Negative values occur on refunds and corrections": (
        "Dólares nominais, sem ajuste pela inflação. Valores negativos ocorrem em "
        "reembolsos e correções.",
        "Dólares nominales, sin ajuste por inflación. Los valores negativos ocurren en "
        "reembolsos y correcciones.",
    ),
    "Nominal dollars. Reshaped from the amount.<cycle> columns of the source contributor file; cycles in which the donor gave nothing are not stored as rows": (
        "Dólares nominais. Reformatado a partir das colunas amount.<ciclo> do arquivo de "
        "contribuintes da fonte; ciclos em que o doador nada contribuiu não são armazenados "
        "como linhas.",
        "Dólares nominales. Reformateado a partir de las columnas amount.<ciclo> del archivo "
        "de contribuyentes de la fuente; los ciclos en que el donante no aportó nada no se "
        "almacenan como filas.",
    ),
    "Non-FEC codes 15S, 15L, 15PD, PF and PFR are assigned by DIME for state and local records": (
        "Os códigos 15S, 15L, 15PD, PF e PFR não pertencem à FEC e são atribuídos pelo DIME "
        "a registros estaduais e locais.",
        "Los códigos 15S, 15L, 15PD, PF y PFR no pertenecen a la FEC y son asignados por DIME "
        "a registros estatales y locales.",
    ),
    "Not linked to the state directory because it includes non-state codes": (
        "Não vinculado ao diretório de estados por incluir códigos que não são de estados.",
        "No se vincula al directorio de estados porque incluye códigos que no son de estados.",
    ),
    "Partition column": ("Coluna de partição.", "Columna de partición."),
    "Partition column. Cycles are even years; the 1980 cycle covers 1979-1980": (
        "Coluna de partição. Os ciclos são anos pares; o ciclo de 1980 cobre 1979-1980.",
        "Columna de partición. Los ciclos son años pares; el ciclo de 1980 cubre 1979-1980.",
    ),
    "Party switchers are assigned a new value after switching": (
        "Quem troca de partido recebe um novo valor após a troca.",
        "Quienes cambian de partido reciben un nuevo valor tras el cambio.",
    ),
    "Points at transaction_id; use it to drop duplicate entries": (
        "Aponta para transaction_id; use-o para descartar registros duplicados.",
        "Apunta a transaction_id; úselo para descartar registros duplicados.",
    ),
    "Populated for party switchers only": (
        "Preenchido apenas para quem trocou de partido.",
        "Se completa solo para quienes cambiaron de partido.",
    ),
    "Primary key within a cycle": (
        "Chave primária dentro de um ciclo.",
        "Clave primaria dentro de un ciclo.",
    ),
    "Re-estimated each cycle holding contributor scores constant": (
        "Reestimado a cada ciclo mantendo constantes os escores dos contribuintes.",
        "Reestimado en cada ciclo manteniendo constantes los puntajes de los contribuyentes.",
    ),
    "Readable labels of the form level:office, for example federal:house or state:lower. Offices prefixed 'local:' are not standardized, so the value space is open-ended (139 distinct values observed)": (
        "Rótulos legíveis na forma nível:cargo, por exemplo federal:house ou state:lower. "
        "Cargos com o prefixo 'local:' não são padronizados, de modo que o espaço de valores "
        "é aberto (139 valores distintos observados).",
        "Etiquetas legibles con la forma nivel:cargo, por ejemplo federal:house o state:lower. "
        "Los cargos con el prefijo 'local:' no están estandarizados, por lo que el espacio de "
        "valores es abierto (139 valores distintos observados).",
    ),
    "Readable phrases, not normalized at source: both 'Lost - General Election' and 'Lost-General' occur": (
        "Frases legíveis, não normalizadas na fonte: ocorrem tanto 'Lost - General Election' "
        "quanto 'Lost-General'.",
        "Frases legibles, no normalizadas en la fuente: aparecen tanto 'Lost - General "
        "Election' como 'Lost-General'.",
    ),
    "Roll-call score inferred from contributions by supervised machine learning (Bonica 2018)": (
        "Escore de votos nominais inferido das contribuições por aprendizado de máquina "
        "supervisionado (Bonica, 2018).",
        "Puntaje de votaciones nominales inferido de las contribuciones mediante aprendizaje "
        "automático supervisado (Bonica, 2018).",
    ),
    "Row identifier. Candidates who never served in Congress get identifiers derived from their FEC, NIMSP or state agency candidate identifier": (
        "Identificador de linha. Candidatos que nunca serviram no Congresso recebem "
        "identificadores derivados de seu identificador de candidato na FEC, no NIMSP ou no "
        "órgão estadual.",
        "Identificador de fila. Los candidatos que nunca sirvieron en el Congreso reciben "
        "identificadores derivados de su identificador de candidato en la FEC, en NIMSP o en "
        "el organismo estatal.",
    ),
    "See Bonica (2013)": ("Ver Bonica (2013).", "Véase Bonica (2013)."),
    "Spelled-out party names, not codes, and not normalized at source: DEMOCRATIC and DEMOCRAT both occur": (
        "Nomes de partido por extenso, não códigos, e não normalizados na fonte: ocorrem "
        "tanto DEMOCRATIC quanto DEMOCRAT.",
        "Nombres de partido escritos por extenso, no códigos, y no normalizados en la fuente: "
        "aparecen tanto DEMOCRATIC como DEMOCRAT.",
    ),
    "Stable across cycles, offices and levels of government; joins to the contribution table": (
        "Estável entre ciclos, cargos e níveis de governo; faz junção com a tabela contribution.",
        "Estable entre ciclos, cargos y niveles de gobierno; se une con la tabla contribution.",
    ),
    "Stable across cycles, offices and levels of government; joins to the recipient table": (
        "Estável entre ciclos, cargos e níveis de governo; faz junção com a tabela recipient.",
        "Estable entre ciclos, cargos y niveles de gobierno; se une con la tabla recipient.",
    ),
    "Stable across election cycles and levels of government; joins to the contributor table": (
        "Estável entre ciclos eleitorais e níveis de governo; faz junção com a tabela contributor.",
        "Estable entre ciclos electorales y niveles de gobierno; se une con la tabla contributor.",
    ),
    "Stored as 0 or 1 in the source": (
        "Armazenado como 0 ou 1 na fonte.",
        "Almacenado como 0 o 1 en la fuente.",
    ),
    "Stored as 0 or 1 in the source. Recipients with value 0 are absent from the smaller dime_recipients file": (
        "Armazenado como 0 ou 1 na fonte. Beneficiários com valor 0 não constam do arquivo "
        "menor dime_recipients.",
        "Almacenado como 0 o 1 en la fuente. Los receptores con valor 0 no figuran en el "
        "archivo menor dime_recipients.",
    ),
    "Stored as 0 or 1 in the source. Value 1 covers corporate and trade-affiliated committees and donors who gave to a single recipient, whose scores are less reliable": (
        "Armazenado como 0 ou 1 na fonte. O valor 1 abrange comitês ligados a empresas e a "
        "associações empresariais e doadores que contribuíram a um único beneficiário, cujos "
        "escores são menos confiáveis.",
        "Almacenado como 0 o 1 en la fuente. El valor 1 abarca comités vinculados a empresas "
        "y a asociaciones empresariales y donantes que aportaron a un solo receptor, cuyos "
        "puntajes son menos confiables.",
    ),
    "The number of observations behind the ideal point. Eight or more distinct recipients is typically enough for a reliable estimate": (
        "Número de observações por trás do ponto ideal. Oito ou mais beneficiários distintos "
        "costumam bastar para uma estimativa confiável.",
        "Número de observaciones detrás del punto ideal. Ocho o más receptores distintos "
        "suelen bastar para una estimación confiable.",
    ),
    "The source codebook mislabels this field as the first name": (
        "O codebook da fonte rotula este campo incorretamente como primeiro nome.",
        "El codebook de la fuente etiqueta erróneamente este campo como nombre de pila.",
    ),
    "The source uses '?' for an unrecorded outcome, which is kept rather than nulled": (
        "A fonte usa '?' para resultado não registrado, valor que é mantido em vez de "
        "convertido em nulo.",
        "La fuente usa '?' para un resultado no registrado, valor que se conserva en lugar "
        "de convertirse en nulo.",
    ),
    "Two-letter state abbreviation followed by the district number. District boundaries are those in force for the cycle, so the code is not comparable across redistricting cycles": (
        "Sigla de duas letras do estado seguida do número do distrito. Os limites distritais "
        "são os vigentes no ciclo, portanto o código não é comparável entre ciclos de "
        "redistritamento.",
        "Sigla de dos letras del estado seguida del número del distrito. Los límites "
        "distritales son los vigentes en el ciclo, por lo que el código no es comparable "
        "entre ciclos de redistritación.",
    ),
    "Two-letter state code followed by the district number; Senate candidates take 'S' followed by the year the seat is up": (
        "Sigla de duas letras do estado seguida do número do distrito; candidatos ao Senado "
        "recebem 'S' seguido do ano em que a cadeira está em disputa.",
        "Sigla de dos letras del estado seguida del número del distrito; los candidatos al "
        "Senado reciben 'S' seguido del año en que el escaño está en disputa.",
    ),
}
