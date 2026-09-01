"""Authoritative source -> target mapping for ``fr_meteofrance``.

Single source of truth for column names, BigQuery types, units and descriptions.
The architecture CSVs, the cleaning code and the dbt models are all generated
from here, so a rename happens in exactly one place.

Naming rule: **every** column is named in French, the language of the data —
including the temporal scaffolding (``annee``, ``mois``, ``date``, ``heure``),
which departs from ``fr_insee_sirene``, where those kept their Portuguese house
names. The one exception is ``dicionario``, whose column names
(``id_tabela``, ``nome_coluna``, ``chave``, ``valor``) are hard-coded in the
generic ``custom_dictionary_coverage`` test and cannot be renamed.

Descriptor: OBSERVATIONS_Descriptif_Technique_Données_SYNOP_OMM.pdf (2026-04-20).
Units are the source's own — temperature in kelvin, pressure in pascal — never
converted, only recorded.
"""

# (source column, target column, bigquery type, measurement unit, dictionary?, description PT)
SYNOP_COLUMNS = [
    (
        "pmer",
        "pression_mer",
        "INT64",
        "pascal",
        False,
        "Pressão atmosférica reduzida ao nível do mar",
    ),
    (
        "tend",
        "variation_pression_3h",
        "INT64",
        "pascal",
        False,
        "Variação da pressão atmosférica nas últimas 3 horas",
    ),
    (
        "cod_tend",
        "type_tendance_barometrique",
        "STRING",
        "",
        True,
        "Tipo de tendência barométrica (tabela de código OMM 0200)",
    ),
    (
        "dd",
        "direction_vent_moyen",
        "INT64",
        "degree",
        False,
        "Direção do vento médio em 10 minutos, em graus a partir do norte geográfico",
    ),
    (
        "ff",
        "vitesse_vent_moyen",
        "FLOAT64",
        "meter / second",
        False,
        "Velocidade do vento médio em 10 minutos",
    ),
    (
        "t",
        "temperature",
        "FLOAT64",
        "kelvin",
        False,
        "Temperatura do ar sob abrigo",
    ),
    (
        "td",
        "point_rosee",
        "FLOAT64",
        "kelvin",
        False,
        "Temperatura do ponto de orvalho",
    ),
    ("u", "humidite", "INT64", "percent", False, "Umidade relativa do ar"),
    (
        "vv",
        "visibilite_horizontale",
        "FLOAT64",
        "meter",
        False,
        "Visibilidade horizontal",
    ),
    (
        "ww",
        "temps_present",
        "STRING",
        "",
        True,
        "Tempo presente observado no momento da medição (tabela de código OMM 4677)",
    ),
    (
        "w1",
        "temps_passe_1",
        "STRING",
        "",
        True,
        "Tempo passado 1, observado no período anterior à medição (tabela de código OMM 4561)",
    ),
    (
        "w2",
        "temps_passe_2",
        "STRING",
        "",
        True,
        "Tempo passado 2, observado no período anterior à medição (tabela de código OMM 4561)",
    ),
    (
        "n",
        "nebulosite_totale",
        "FLOAT64",
        "percent",
        False,
        "Nebulosidade total, isto é, fração do céu coberta por nuvens",
    ),
    (
        "nbas",
        "nebulosite_etage_inferieur",
        "INT64",
        "",
        False,
        "Nebulosidade das nuvens do andar inferior",
    ),
    (
        "hbas",
        "hauteur_base_nuages_inferieurs",
        "INT64",
        "meter",
        False,
        "Altura da base das nuvens do andar inferior",
    ),
    (
        "cl",
        "type_nuages_etage_inferieur",
        "STRING",
        "",
        True,
        "Tipo das nuvens do andar inferior (tabela de código OMM 0513)",
    ),
    (
        "cm",
        "type_nuages_etage_moyen",
        "STRING",
        "",
        True,
        "Tipo das nuvens do andar médio (tabela de código OMM 0515)",
    ),
    (
        "ch",
        "type_nuages_etage_superieur",
        "STRING",
        "",
        True,
        "Tipo das nuvens do andar superior (tabela de código OMM 0509)",
    ),
    (
        "pres",
        "pression_station",
        "INT64",
        "pascal",
        False,
        "Pressão atmosférica medida no nível da estação",
    ),
    (
        "niv_bar",
        "niveau_barometrique",
        "INT64",
        "pascal",
        False,
        "Nível barométrico padrão ao qual o geopotencial se refere",
    ),
    (
        "geop",
        "geopotentiel",
        "INT64",
        "meter * meter / (second * second)",
        False,
        "Geopotencial da superfície isobárica de referência",
    ),
    (
        "tend24",
        "variation_pression_24h",
        "INT64",
        "pascal",
        False,
        "Variação da pressão atmosférica nas últimas 24 horas",
    ),
    (
        "tn12",
        "temperature_minimale_12h",
        "FLOAT64",
        "kelvin",
        False,
        "Temperatura mínima do ar nas últimas 12 horas",
    ),
    (
        "tn24",
        "temperature_minimale_24h",
        "FLOAT64",
        "kelvin",
        False,
        "Temperatura mínima do ar nas últimas 24 horas",
    ),
    (
        "tx12",
        "temperature_maximale_12h",
        "FLOAT64",
        "kelvin",
        False,
        "Temperatura máxima do ar nas últimas 12 horas",
    ),
    (
        "tx24",
        "temperature_maximale_24h",
        "FLOAT64",
        "kelvin",
        False,
        "Temperatura máxima do ar nas últimas 24 horas",
    ),
    (
        "tminsol",
        "temperature_minimale_sol_12h",
        "FLOAT64",
        "kelvin",
        False,
        "Temperatura mínima do solo nas últimas 12 horas",
    ),
    (
        "sw",
        "methode_mesure_tw",
        "STRING",
        "",
        True,
        "Método de obtenção da temperatura do termômetro úmido (tabela de código OMM 3855)",
    ),
    (
        "tw",
        "temperature_thermometre_mouille",
        "FLOAT64",
        "kelvin",
        False,
        "Temperatura do termômetro úmido",
    ),
    (
        "raf10",
        "rafale_10min",
        "FLOAT64",
        "meter / second",
        False,
        "Rajada máxima de vento nos últimos 10 minutos",
    ),
    (
        "rafper",
        "rafale_periode",
        "FLOAT64",
        "meter / second",
        False,
        "Rajada máxima de vento no período indicado em periode_mesure_rafale",
    ),
    (
        "per",
        "periode_mesure_rafale",
        "FLOAT64",
        "minute",
        False,
        "Período de medição da rajada registrada em rafale_periode",
    ),
    (
        "etat_sol",
        "etat_sol",
        "STRING",
        "",
        True,
        "Estado do solo sem neve (tabela de código OMM 0901)",
    ),
    (
        "ht_neige",
        "hauteur_neige",
        "FLOAT64",
        "meter",
        False,
        "Altura total da camada de neve, gelo ou outro depósito no solo",
    ),
    (
        "ssfrai",
        "hauteur_neige_fraiche",
        "FLOAT64",
        "meter",
        False,
        "Altura da neve fresca acumulada no período indicado em periode_mesure_neige_fraiche",
    ),
    (
        "perssfrai",
        "periode_mesure_neige_fraiche",
        "FLOAT64",
        "hour",
        False,
        "Período de medição da neve fresca, publicado pela fonte em décimos de hora",
    ),
    (
        "rr1",
        "precipitation_1h",
        "FLOAT64",
        "millimeter",
        False,
        "Precipitação acumulada na última hora",
    ),
    (
        "rr3",
        "precipitation_3h",
        "FLOAT64",
        "millimeter",
        False,
        "Precipitação acumulada nas últimas 3 horas",
    ),
    (
        "rr6",
        "precipitation_6h",
        "FLOAT64",
        "millimeter",
        False,
        "Precipitação acumulada nas últimas 6 horas",
    ),
    (
        "rr12",
        "precipitation_12h",
        "FLOAT64",
        "millimeter",
        False,
        "Precipitação acumulada nas últimas 12 horas",
    ),
    (
        "rr24",
        "precipitation_24h",
        "FLOAT64",
        "millimeter",
        False,
        "Precipitação acumulada nas últimas 24 horas",
    ),
    (
        "phenspe1",
        "phenomene_special_1",
        "STRING",
        "",
        False,
        "Fenômeno especial 1 observado na estação (tabela de código OMM 3778)",
    ),
    (
        "phenspe2",
        "phenomene_special_2",
        "STRING",
        "",
        False,
        "Fenômeno especial 2 observado na estação (tabela de código OMM 3778)",
    ),
    (
        "phenspe3",
        "phenomene_special_3",
        "STRING",
        "",
        False,
        "Fenômeno especial 3 observado na estação (tabela de código OMM 3778)",
    ),
    (
        "phenspe4",
        "phenomene_special_4",
        "STRING",
        "",
        False,
        "Fenômeno especial 4 observado na estação (tabela de código OMM 3778)",
    ),
    (
        "nnuage1",
        "nebulosite_couche_1",
        "INT64",
        "",
        False,
        "Nebulosidade da camada de nuvens 1",
    ),
    (
        "ctype1",
        "type_nuage_1",
        "STRING",
        "",
        True,
        "Tipo de nuvem da camada 1 (tabela de código OMM 0500)",
    ),
    (
        "hnuage1",
        "hauteur_base_nuage_1",
        "INT64",
        "meter",
        False,
        "Altura da base da camada de nuvens 1",
    ),
    (
        "nnuage2",
        "nebulosite_couche_2",
        "INT64",
        "",
        False,
        "Nebulosidade da camada de nuvens 2",
    ),
    (
        "ctype2",
        "type_nuage_2",
        "STRING",
        "",
        True,
        "Tipo de nuvem da camada 2 (tabela de código OMM 0500)",
    ),
    (
        "hnuage2",
        "hauteur_base_nuage_2",
        "INT64",
        "meter",
        False,
        "Altura da base da camada de nuvens 2",
    ),
    (
        "nnuage3",
        "nebulosite_couche_3",
        "INT64",
        "",
        False,
        "Nebulosidade da camada de nuvens 3",
    ),
    (
        "ctype3",
        "type_nuage_3",
        "STRING",
        "",
        True,
        "Tipo de nuvem da camada 3 (tabela de código OMM 0500)",
    ),
    (
        "hnuage3",
        "hauteur_base_nuage_3",
        "INT64",
        "meter",
        False,
        "Altura da base da camada de nuvens 3",
    ),
    (
        "nnuage4",
        "nebulosite_couche_4",
        "INT64",
        "",
        False,
        "Nebulosidade da camada de nuvens 4",
    ),
    (
        "ctype4",
        "type_nuage_4",
        "STRING",
        "",
        True,
        "Tipo de nuvem da camada 4 (tabela de código OMM 0500)",
    ),
    (
        "hnuage4",
        "hauteur_base_nuage_4",
        "INT64",
        "meter",
        False,
        "Altura da base da camada de nuvens 4",
    ),
]

# Leading columns of the `synop` table, ahead of SYNOP_COLUMNS.
SYNOP_LEADING = [
    (
        "annee",
        "INT64",
        "year",
        False,
        "Ano da observação, em tempo universal coordenado (UTC)",
    ),
    (
        "mois",
        "INT64",
        "month",
        False,
        "Mês da observação, em tempo universal coordenado (UTC)",
    ),
    (
        "date",
        "DATE",
        "",
        False,
        "Data da observação, em tempo universal coordenado (UTC)",
    ),
    (
        "heure",
        "TIME",
        "",
        False,
        "Hora da observação, em tempo universal coordenado (UTC)",
    ),
    (
        "indicatif_omm",
        "STRING",
        "",
        False,
        "Indicativo OMM da estação, de cinco dígitos",
    ),
    (
        "date_heure_traitement",
        "DATETIME",
        "",
        False,
        "Data e hora (UTC) da extração da medição das bases internas da Météo-France",
    ),
    (
        "date_heure_insertion",
        "DATETIME",
        "",
        False,
        "Data e hora (UTC) da inserção da medição do sensor nas bases internas da Météo-France",
    ),
]

STATION_SYNOP_COLUMNS = [
    (
        "indicatif_omm",
        "STRING",
        "",
        False,
        "Indicativo OMM da estação, de cinco dígitos",
    ),
    (
        "indicatif_wigos",
        "STRING",
        "",
        False,
        "Indicativo WIGOS da estação, no formato bloco-emissor-tipo-número",
    ),
    ("nom_station", "STRING", "", False, "Nome usual da estação"),
    (
        "latitude",
        "FLOAT64",
        "degree",
        False,
        "Latitude da estação, negativa ao sul do equador",
    ),
    (
        "longitude",
        "FLOAT64",
        "degree",
        False,
        "Longitude da estação, negativa a oeste de Greenwich",
    ),
    ("altitude", "FLOAT64", "meter", False, "Altitude da estação"),
    ("date_ouverture", "DATE", "", False, "Data de abertura da estação"),
    (
        "annee_debut_observation",
        "INT64",
        "year",
        False,
        "Primeiro ano em que a estação aparece no arquivo SYNOP",
    ),
    (
        "annee_fin_observation",
        "INT64",
        "year",
        False,
        "Último ano em que a estação aparece no arquivo SYNOP",
    ),
    (
        "geolocalisation",
        "GEOGRAPHY",
        "",
        False,
        "Ponto geográfico da estação, em WGS 84",
    ),
]

STATION_CLIMATOLOGIQUE_COLUMNS = [
    (
        "numero_poste",
        "STRING",
        "",
        False,
        "Número Météo-France do posto, de oito dígitos",
    ),
    ("nom_poste", "STRING", "", False, "Nome usual do posto"),
    (
        "id_departement",
        "STRING",
        "",
        False,
        "Código do departamento ou da coletividade ultramarina do posto",
    ),
    (
        "latitude",
        "FLOAT64",
        "degree",
        False,
        "Latitude do posto, negativa ao sul do equador",
    ),
    (
        "longitude",
        "FLOAT64",
        "degree",
        False,
        "Longitude do posto, negativa a oeste de Greenwich",
    ),
    (
        "altitude",
        "FLOAT64",
        "meter",
        False,
        "Altitude do posto, medida no pé do abrigo ou do pluviômetro",
    ),
    (
        "date_edition",
        "DATE",
        "",
        False,
        "Data de edição da ficha climatológica do posto",
    ),
    (
        "geolocalisation",
        "GEOGRAPHY",
        "",
        False,
        "Ponto geográfico do posto, em WGS 84",
    ),
]

NORMALE_COLUMNS = [
    (
        "numero_poste",
        "STRING",
        "",
        False,
        "Número Météo-France do posto, de oito dígitos",
    ),
    ("indicateur", "STRING", "", True, "Código do indicador climatológico"),
    (
        "periode",
        "STRING",
        "",
        True,
        "Período do indicador: mês de 01 a 12, ou annee para o valor anual",
    ),
    (
        "valeur",
        "FLOAT64",
        "",
        False,
        "Valor do indicador no período, na unidade registrada em unite",
    ),
    ("unite", "STRING", "", True, "Unidade de medida do valor"),
    (
        "libelle_indicateur",
        "STRING",
        "",
        False,
        "Rótulo do indicador tal como publicado na ficha climatológica",
    ),
    (
        "annee_debut_reference",
        "INT64",
        "year",
        False,
        "Primeiro ano do período de referência sobre o qual a normal foi calculada",
    ),
    (
        "annee_fin_reference",
        "INT64",
        "year",
        False,
        "Último ano do período de referência sobre o qual a normal foi calculada",
    ),
    (
        "date_debut_record",
        "DATE",
        "",
        False,
        "Primeira data do período sobre o qual o recorde foi estabelecido",
    ),
    (
        "date_fin_record",
        "DATE",
        "",
        False,
        "Última data do período sobre o qual o recorde foi estabelecido",
    ),
    (
        "jour_record",
        "INT64",
        "day",
        False,
        "Dia do mês em que o recorde foi observado",
    ),
    (
        "annee_record",
        "INT64",
        "year",
        False,
        "Ano em que o recorde foi observado",
    ),
]

DICIONARIO_COLUMNS = [
    ("id_tabela", "STRING", "", False, "Nome da tabela"),
    ("nome_coluna", "STRING", "", False, "Nome da coluna"),
    (
        "chave",
        "STRING",
        "",
        False,
        "Chave, isto é, o valor codificado na coluna",
    ),
    ("cobertura_temporal", "STRING", "", False, "Cobertura temporal da chave"),
    ("valor", "STRING", "", False, "Valor, isto é, o significado da chave"),
]

SYNOP_SOURCE_ORDER = [c[0] for c in SYNOP_COLUMNS]
SYNOP_TARGET_ORDER = [c[0] for c in SYNOP_LEADING] + [
    c[1] for c in SYNOP_COLUMNS
]
SYNOP_TYPES = dict(
    [(c[0], c[1]) for c in SYNOP_LEADING]
    + [(c[1], c[2]) for c in SYNOP_COLUMNS]
)

# Free-text `observations` for columns whose published encoding is wider than the
# code table cited in the technical descriptor. Verified against every value that
# occurs in the 1996-2026 archive.
OBSERVATIONS = {
    "temps_present": (
        "Os valores publicados seguem o descritor BUFR 0 20 003: 0 a 99 correspondem à "
        "tabela OMM 4677 (observação humana) e 100 ou mais à tabela OMM 4680 (estação "
        "automática). O arquivo 1996-2026 contém 121, 141 e 142."
    ),
    "temps_passe_1": (
        "Os valores publicados seguem o descritor BUFR 0 20 004: 0 a 9 correspondem à "
        "tabela OMM 4561 (observação humana) e 10 a 19 à tabela OMM 4531 (estação "
        "automática). O arquivo 1996-2026 contém 14."
    ),
    "temps_passe_2": (
        "Os valores publicados seguem o descritor BUFR 0 20 005: 0 a 9 correspondem à "
        "tabela OMM 4561 (observação humana) e 10 a 19 à tabela OMM 4531 (estação "
        "automática)."
    ),
    "type_nuages_etage_superieur": (
        "Os valores publicados seguem o descritor BUFR 0 20 012, no qual as nuvens do "
        "andar superior ocupam a faixa 10 a 19 e o valor 60 indica nuvens invisíveis."
    ),
    "type_nuages_etage_moyen": (
        "Os valores publicados seguem o descritor BUFR 0 20 012, no qual as nuvens do "
        "andar médio ocupam a faixa 20 a 29 e o valor 61 indica nuvens invisíveis."
    ),
    "type_nuages_etage_inferieur": (
        "Os valores publicados seguem o descritor BUFR 0 20 012, no qual as nuvens do "
        "andar inferior ocupam a faixa 30 a 39 e o valor 62 indica nuvens invisíveis."
    ),
    "etat_sol": (
        "Os valores publicados seguem o descritor BUFR 0 20 062: 0 a 9 correspondem à "
        "tabela OMM 0901 (solo sem neve) e 10 a 19 à tabela OMM 0975 (solo com neve ou gelo)."
    ),
    "type_tendance_barometrique": (
        "A tabela OMM 0200 define apenas os valores 0 a 8. O valor 10 aparece em 109 "
        "das 4.750.246 observações preenchidas e não tem significado definido na fonte."
    ),
    "phenomene_special_1": (
        "O descritor técnico cita a tabela OMM 3778, mas a fonte publica códigos compostos "
        "nacionais de até quatro dígitos, fora do domínio dessa tabela. Nenhuma tabela "
        "pública de valor para rótulo foi localizada, por isso a coluna não é coberta pelo "
        "dicionário."
    ),
    "periode_mesure_neige_fraiche": (
        "A fonte publica o período em décimos de hora, conforme o descritor técnico."
    ),
    "date_heure_traitement": (
        "Vazia nas observações anteriores a 2025, quando a fonte passou a publicar o campo."
    ),
    "date_heure_insertion": (
        "Vazia nas observações anteriores a 2025, quando a fonte passou a publicar o campo. "
        "Usada para desduplicar retransmissões da mesma medição."
    ),
    "methode_mesure_tw": (
        "Preenchida em apenas 8 das 5.379.359 observações do arquivo 1996-2026."
    ),
    "id_departement": (
        "Segue a codificação da Météo-France, que difere do Code officiel géographique do "
        "INSEE em dois pontos: a Córsega recebe 20, e não 2A ou 2B, e as coletividades de "
        "além-mar 984, 986, 987 e 988 não constam do diretório francês."
    ),
    "indicatif_omm": (
        "As estações do arquivo SYNOP passaram de 60 para 190 a partir de 2025, quando a "
        "Météo-France ampliou a rede publicada."
    ),
}
for _i in (2, 3, 4):
    OBSERVATIONS[f"phenomene_special_{_i}"] = OBSERVATIONS[
        "phenomene_special_1"
    ].replace("Fenômeno especial 1", f"Fenômeno especial {_i}")

_OCTA = (
    "Publicada em octas, isto é, oitavos de céu coberto, de 0 a 8. A unidade de medida "
    "fica em branco por ser adimensional e não constar do vocabulário da plataforma."
)
for _c in ["nebulosite_etage_inferieur"] + [
    f"nebulosite_couche_{_i}" for _i in range(1, 5)
]:
    OBSERVATIONS[_c] = _OCTA
