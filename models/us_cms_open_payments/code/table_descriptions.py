"""Trilingual table descriptions, shared by the dbt schema and the backend."""

TABLE_DESCRIPTIONS = {
    "general": (
        "Pagamentos gerais -- transferências de valor de fabricantes de medicamentos e "
        "dispositivos médicos e de organizações de compras a médicos, profissionais não médicos "
        "e hospitais universitários que não estão ligadas a um estudo de pesquisa. Um registro "
        "por pagamento declarado, anos do programa 2016 a 2025.",
        "General payments -- transfers of value from drug and medical device manufacturers and "
        "group purchasing organizations to physicians, non-physician practitioners and teaching "
        "hospitals that are not tied to a research study. One row per reported payment, program "
        "years 2016 to 2025.",
        "Pagos generales -- transferencias de valor de fabricantes de medicamentos y dispositivos "
        "médicos y de organizaciones de compras a médicos, profesionales no médicos y hospitales "
        "universitarios que no están vinculadas a un estudio de investigación. Un registro por "
        "pago declarado, años del programa 2016 a 2025.",
    ),
    "general_legacy": (
        "Pagamentos gerais dos anos do programa 2013 a 2015, publicados pelo CMS em um esquema "
        "anterior: as colunas de identificação do beneficiário usam o prefixo Physician_ e os "
        "produtos associados aparecem em listas separadas de medicamentos e dispositivos, sem "
        "categoria terapêutica nem identificador de dispositivo.",
        "General payments for program years 2013 to 2015, published by CMS under an earlier "
        "schema: recipient identification columns use the Physician_ prefix and associated "
        "products appear as separate drug and device lists, with no therapeutic category and no "
        "device identifier.",
        "Pagos generales de los años del programa 2013 a 2015, publicados por el CMS bajo un "
        "esquema anterior: las columnas de identificación del beneficiario usan el prefijo "
        "Physician_ y los productos asociados aparecen en listas separadas de medicamentos y "
        "dispositivos, sin categoría terapéutica ni identificador de dispositivo.",
    ),
    "research": (
        "Pagamentos de pesquisa -- transferências de valor ligadas a um estudo, protocolo ou "
        "atividade de pesquisa. Um registro por pagamento declarado, anos do programa 2016 a "
        "2025. Os pesquisadores principais estão na tabela research_principal_investigator.",
        "Research payments -- transfers of value tied to a research study, protocol or activity. "
        "One row per reported payment, program years 2016 to 2025. Principal investigators are "
        "in the research_principal_investigator table.",
        "Pagos de investigación -- transferencias de valor vinculadas a un estudio, protocolo o "
        "actividad de investigación. Un registro por pago declarado, años del programa 2016 a "
        "2025. Los investigadores principales están en la tabla research_principal_investigator.",
    ),
    "research_legacy": (
        "Pagamentos de pesquisa dos anos do programa 2013 a 2015, no esquema anterior do CMS. "
        "Os pesquisadores principais estão na tabela research_principal_investigator.",
        "Research payments for program years 2013 to 2015, under the earlier CMS schema. "
        "Principal investigators are in the research_principal_investigator table.",
        "Pagos de investigación de los años del programa 2013 a 2015, bajo el esquema anterior "
        "del CMS. Los investigadores principales están en la tabla "
        "research_principal_investigator.",
    ),
    "research_principal_investigator": (
        "Pesquisadores principais associados a pagamentos de pesquisa, um registro por "
        "pesquisador. O CMS publica até cinco pesquisadores por pagamento em blocos repetidos de "
        "colunas; aqui cada bloco é uma linha, identificada por record_id e "
        "principal_investigator_number. Cobre 2013 a 2025: os anos anteriores a 2016 não "
        "informam tipo de beneficiário nem tipos e especialidades além do primeiro.",
        "Principal investigators associated with research payments, one row per investigator. "
        "CMS publishes up to five investigators per payment as repeated column blocks; here each "
        "block is a row, identified by record_id and principal_investigator_number. Covers 2013 "
        "to 2025: program years before 2016 carry no covered recipient type and no types or "
        "specialties beyond the first.",
        "Investigadores principales asociados a pagos de investigación, un registro por "
        "investigador. El CMS publica hasta cinco investigadores por pago en bloques repetidos "
        "de columnas; aquí cada bloque es una fila, identificada por record_id y "
        "principal_investigator_number. Cubre 2013 a 2025: los años anteriores a 2016 no "
        "informan tipo de beneficiario ni tipos y especialidades más allá del primero.",
    ),
    "ownership": (
        "Participações societárias e de investimento detidas por médicos ou seus familiares "
        "imediatos em fabricantes e organizações de compras, anos do programa 2013 a 2025. O "
        "identificador nacional de prestador só é informado a partir de 2015.",
        "Ownership and investment interests held by physicians or their immediate family members "
        "in manufacturers and group purchasing organizations, program years 2013 to 2025. The "
        "national provider identifier is only reported from 2015 onwards.",
        "Participaciones societarias y de inversión de médicos o sus familiares inmediatos en "
        "fabricantes y organizaciones de compras, años del programa 2013 a 2025. El "
        "identificador nacional de prestador solo se informa a partir de 2015.",
    ),
    "covered_recipient_profile": (
        "Perfis de médicos e profissionais não médicos com nome, endereço de prática, "
        "especialidade principal e taxonomia de prestador conforme a lista mestra de prestadores "
        "do CMS. O CMS publica o arquivo como instantâneo do ciclo corrente e nele inclui apenas "
        "quem tem ao menos um pagamento publicado nesse ciclo, de modo que a tabela cobre 2019 a "
        "2025 e não a série completa de pagamentos.",
        "Profiles of physicians and non-physician practitioners with name, practice address, "
        "primary specialty and provider taxonomy as listed in the CMS master provider list. CMS "
        "publishes the file as a snapshot of the current cycle and includes only those with at "
        "least one payment published in that cycle, so the table covers 2019 to 2025 rather than "
        "the full payment series.",
        "Perfiles de médicos y profesionales no médicos con nombre, dirección de práctica, "
        "especialidad principal y taxonomía de prestador según la lista maestra de prestadores del "
        "CMS. El CMS publica el archivo como instantánea del ciclo actual e incluye solo a quienes "
        "tienen al menos un pago publicado en ese ciclo, por lo que la tabla cubre 2019 a 2025 y no "
        "la serie completa de pagos.",
    ),
    "teaching_hospital_profile": (
        "Hospitais universitários elegíveis a receber pagamentos, com número de certificação do "
        "CMS, endereço e nomes alternativos.",
        "Teaching hospitals eligible to receive payments, with CMS certification number, address "
        "and alternate names.",
        "Hospitales universitarios elegibles para recibir pagos, con número de certificación del "
        "CMS, dirección y nombres alternativos.",
    ),
    "reporting_entity_profile": (
        "Fabricantes de medicamentos e dispositivos e organizações de compras que declaram "
        "pagamentos ao Open Payments, com identificador, sede e nomes alternativos.",
        "Drug and device manufacturers and group purchasing organizations that report payments "
        "to Open Payments, with identifier, home state and alternate names.",
        "Fabricantes de medicamentos y dispositivos y organizaciones de compras que declaran "
        "pagos a Open Payments, con identificador, sede y nombres alternativos.",
    ),
    "provider_profile_mapping": (
        "Correspondência entre perfis duplicados de um mesmo prestador, ligando cada perfil "
        "secundário ao perfil principal.",
        "Mapping between duplicate profiles of the same provider, linking each secondary profile "
        "to its primary profile.",
        "Correspondencia entre perfiles duplicados de un mismo prestador, vinculando cada perfil "
        "secundario al perfil principal.",
    ),
    "summary_by_recipient_nature": (
        "Totais anuais de pagamento por beneficiário e natureza do pagamento.",
        "Annual payment totals by recipient and nature of payment.",
        "Totales anuales de pago por beneficiario y naturaleza del pago.",
    ),
    "summary_by_recipient_entity": (
        "Totais anuais de pagamento por beneficiário, entidade declarante e categoria de pagamento.",
        "Annual payment totals by recipient, reporting entity and payment category.",
        "Totales anuales de pago por beneficiario, entidad declarante y categoría de pago.",
    ),
    "summary_by_entity_nature": (
        "Totais anuais de pagamento por entidade declarante e natureza do pagamento.",
        "Annual payment totals by reporting entity and nature of payment.",
        "Totales anuales de pago por entidad declarante y naturaleza del pago.",
    ),
    "summary_by_entity_recipient_nature": (
        "Totais anuais de pagamento por entidade declarante, beneficiário e natureza do pagamento.",
        "Annual payment totals by reporting entity, recipient and nature of payment.",
        "Totales anuales de pago por entidad declarante, beneficiario y naturaleza del pago.",
    ),
    "summary_state_by_nature": (
        "Totais, médias e medianas anuais de pagamento por estado, natureza do pagamento e tipo "
        "de beneficiário.",
        "Annual payment totals, means and medians by state, nature of payment and recipient type.",
        "Totales, medias y medianas anuales de pago por estado, naturaleza del pago y tipo de "
        "beneficiario.",
    ),
    "summary_national": (
        "Totais, médias e medianas anuais de pagamento no âmbito nacional, por categoria de "
        "pagamento e tipo de beneficiário.",
        "Annual national payment totals, means and medians by payment category and recipient type.",
        "Totales, medias y medianas anuales de pago a nivel nacional, por categoría de pago y "
        "tipo de beneficiario.",
    ),
    "summary_national_by_specialty": (
        "Totais, médias e medianas anuais de pagamento no âmbito nacional por especialidade do "
        "prestador, identificada pelo código de taxonomia.",
        "Annual national payment totals, means and medians by provider specialty, identified by "
        "taxonomy code.",
        "Totales, medias y medianas anuales de pago a nivel nacional por especialidad del "
        "prestador, identificada por el código de taxonomía.",
    ),
    "summary_state": (
        "Totais, médias e medianas anuais de pagamento por estado, categoria de pagamento e tipo "
        "de beneficiário.",
        "Annual payment totals, means and medians by state, payment category and recipient type.",
        "Totales, medias y medianas anuales de pago por estado, categoría de pago y tipo de "
        "beneficiario.",
    ),
    "summary_teaching_hospital": (
        "Totais anuais de pagamento por hospital universitário, separados entre pagamentos "
        "gerais e de pesquisa.",
        "Annual payment totals by teaching hospital, split between general and research payments.",
        "Totales anuales de pago por hospital universitario, separados entre pagos generales y "
        "de investigación.",
    ),
    "summary_reporting_entity": (
        "Totais anuais de pagamento por entidade declarante, separados por categoria de "
        "pagamento e tipo de beneficiário.",
        "Annual payment totals by reporting entity, split by payment category and recipient type.",
        "Totales anuales de pago por entidad declarante, separados por categoría de pago y tipo "
        "de beneficiario.",
    ),
    "summary_physician": (
        "Totais anuais de pagamento por perfil de médico ou profissional não médico, com os "
        "dados cadastrais do perfil.",
        "Annual payment totals by physician or non-physician practitioner profile, with the "
        "profile's registration details.",
        "Totales anuales de pago por perfil de médico o profesional no médico, con los datos de "
        "registro del perfil.",
    ),
    "summary_dashboard": (
        "Métricas agregadas do painel resumo publicado pelo CMS. A publicação original traz uma "
        "coluna por ano do programa; aqui cada par métrica-ano é uma linha.",
        "Aggregate metrics from the summary dashboard published by CMS. The original publication "
        "has one column per program year; here each metric-year pair is a row.",
        "Métricas agregadas del panel resumen publicado por el CMS. La publicación original trae "
        "una columna por año del programa; aquí cada par métrica-año es una fila.",
    ),
    "dicionario": (
        "Dicionário de códigos e rótulos das colunas categóricas das demais tabelas do conjunto.",
        "Dictionary of codes and labels for the categorical columns of the other tables in the "
        "dataset.",
        "Diccionario de códigos y etiquetas de las columnas categóricas de las demás tablas del "
        "conjunto.",
    ),
}
