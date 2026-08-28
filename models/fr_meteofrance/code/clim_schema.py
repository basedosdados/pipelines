"""Schema map for the Météo-France *Données climatologiques de base* tables.

Two fact tables plus a station register, built from the daily (QUOT) and monthly
(MENS) per-département archives:

* ``quotidienne``  — poste x day, the RR-T-Vent and autres-parametres files joined
* ``mensuelle``    — poste x month
* ``poste``        — the station register, lifted out of the fact tables

Naming follows the rest of ``fr_meteofrance``: French throughout, including the
temporal scaffolding (``annee``, ``mois``, ``date``).

Most of this file is generated rather than hand-written. Météo-France documents
every column in its own descriptor files, and the great majority follow four
mechanical families:

    Q<X>        quality code of <X>
    NB<X>       count of present <X> values
    H<X>        clock time at which <X> occurred
    <X>DAT      day of the month on which <X> occurred

``PARAMS`` below carries only the base parameters, hand-authored; the families
are expanded by :func:`build`. That keeps roughly 300 columns' worth of metadata
consistent without 300 hand-written entries.
"""

# Base parameter -> (target name, bigquery type, measurement unit, PT, EN, ES).
#
# Units are the source's own, as everywhere in this dataset: tenths are NOT
# rescaled. Météo-France publishes "en mm et 1/10", i.e. millimetres with one
# decimal already applied in the value, so `millimeter` is correct.
#
# Occurrence flags (0/1) are STRING + dictionary-covered, per the house rule
# that booleans stored as codes are categorical, not quantities.
QUOT_PARAMS = {
    # --- precipitation, temperature, wind (RR-T-Vent) ---
    "RR": (
        "precipitation",
        "FLOAT64",
        "millimeter",
        "Precipitação acumulada em 24 horas, das 06h UTC do dia J às 06h UTC do dia J+1, atribuída ao dia J",
        "Precipitation accumulated over 24 hours, from 06:00 UTC on day J to 06:00 UTC on day J+1, assigned to day J",
        "Precipitación acumulada en 24 horas, de las 06h UTC del día J a las 06h UTC del día J+1, asignada al día J",
    ),
    "TN": (
        "temperature_minimale",
        "FLOAT64",
        "celsius_degree",
        "Temperatura mínima sob abrigo",
        "Minimum temperature in the shelter",
        "Temperatura mínima bajo abrigo",
    ),
    "TX": (
        "temperature_maximale",
        "FLOAT64",
        "celsius_degree",
        "Temperatura máxima sob abrigo",
        "Maximum temperature in the shelter",
        "Temperatura máxima bajo abrigo",
    ),
    "TM": (
        "temperature_moyenne",
        "FLOAT64",
        "celsius_degree",
        "Média diária das temperaturas horárias sob abrigo",
        "Daily mean of the hourly temperatures in the shelter",
        "Media diaria de las temperaturas horarias bajo abrigo",
    ),
    "TNTXM": (
        "temperature_moyenne_min_max",
        "FLOAT64",
        "celsius_degree",
        "Média diária entre a temperatura mínima e a máxima",
        "Daily mean of the minimum and maximum temperature",
        "Media diaria entre la temperatura mínima y la máxima",
    ),
    "TAMPLI": (
        "amplitude_thermique",
        "FLOAT64",
        "celsius_degree",
        "Amplitude térmica diária, isto é, a diferença entre a temperatura máxima e a mínima",
        "Daily temperature range, that is, the maximum minus the minimum temperature",
        "Amplitud térmica diaria, es decir, la diferencia entre la temperatura máxima y la mínima",
    ),
    "TNSOL": (
        "temperature_minimale_sol_10cm",
        "FLOAT64",
        "celsius_degree",
        "Temperatura mínima diária a 10 cm acima do solo",
        "Daily minimum temperature 10 cm above the ground",
        "Temperatura mínima diaria a 10 cm sobre el suelo",
    ),
    "TN50": (
        "temperature_minimale_sol_50cm",
        "FLOAT64",
        "celsius_degree",
        "Temperatura mínima diária a 50 cm acima do solo",
        "Daily minimum temperature 50 cm above the ground",
        "Temperatura mínima diaria a 50 cm sobre el suelo",
    ),
    "DG": (
        "duree_gel",
        "FLOAT64",
        "minute",
        "Duração de geada sob abrigo, isto é, o tempo com temperatura igual ou inferior a 0 °C",
        "Duration of frost in the shelter, that is, the time at or below 0 °C",
        "Duración de helada bajo abrigo, es decir, el tiempo con temperatura igual o inferior a 0 °C",
    ),
    "FFM": (
        "vitesse_vent_moyenne_10m",
        "FLOAT64",
        "meter / second",
        "Média diária da velocidade do vento medida em 10 minutos, a 10 m do solo",
        "Daily mean of the 10-minute mean wind speed, at 10 m",
        "Media diaria de la velocidad del viento medida en 10 minutos, a 10 m",
    ),
    "FF2M": (
        "vitesse_vent_moyenne_2m",
        "FLOAT64",
        "meter / second",
        "Média diária da velocidade do vento medida em 10 minutos, a 2 m do solo",
        "Daily mean of the 10-minute mean wind speed, at 2 m",
        "Media diaria de la velocidad del viento medida en 10 minutos, a 2 m",
    ),
    "FXY": (
        "vitesse_vent_maximale_moyennee_10m",
        "FLOAT64",
        "meter / second",
        "Máximo diário da velocidade máxima horária do vento medida em 10 minutos, a 10 m do solo",
        "Daily maximum of the hourly maximum 10-minute mean wind speed, at 10 m",
        "Máximo diario de la velocidad máxima horaria del viento medida en 10 minutos, a 10 m",
    ),
    "DXY": (
        "direction_vent_maximal_moyenne_10m",
        "INT64",
        "degree",
        "Direção do vento registrado em vitesse_vent_maximale_moyennee_10m, na rosa de 360 graus",
        "Direction of the wind recorded in vitesse_vent_maximale_moyennee_10m, on the 360-degree rose",
        "Dirección del viento registrado en vitesse_vent_maximale_moyennee_10m, en la rosa de 360 grados",
    ),
    "FXI": (
        "rafale_maximale_10m",
        "FLOAT64",
        "meter / second",
        "Máximo diário da rajada máxima horária do vento instantâneo, a 10 m do solo",
        "Daily maximum of the hourly maximum instantaneous wind gust, at 10 m",
        "Máximo diario de la racha máxima horaria del viento instantáneo, a 10 m",
    ),
    "DXI": (
        "direction_rafale_maximale_10m",
        "INT64",
        "degree",
        "Direção da rajada registrada em rafale_maximale_10m, na rosa de 360 graus",
        "Direction of the gust recorded in rafale_maximale_10m, on the 360-degree rose",
        "Dirección de la racha registrada en rafale_maximale_10m, en la rosa de 360 grados",
    ),
    "FXI2": (
        "rafale_maximale_2m",
        "FLOAT64",
        "meter / second",
        "Máximo diário da rajada máxima horária do vento instantâneo, a 2 m do solo",
        "Daily maximum of the hourly maximum instantaneous wind gust, at 2 m",
        "Máximo diario de la racha máxima horaria del viento instantáneo, a 2 m",
    ),
    "DXI2": (
        "direction_rafale_maximale_2m",
        "INT64",
        "degree",
        "Direção da rajada registrada em rafale_maximale_2m, na rosa de 360 graus",
        "Direction of the gust recorded in rafale_maximale_2m, on the 360-degree rose",
        "Dirección de la racha registrada en rafale_maximale_2m, en la rosa de 360 grados",
    ),
    "FXI3S": (
        "rafale_maximale_3s_10m",
        "FLOAT64",
        "meter / second",
        "Máximo diário da rajada máxima horária do vento medida em 3 segundos, a 10 m do solo",
        "Daily maximum of the hourly maximum 3-second mean wind gust, at 10 m",
        "Máximo diario de la racha máxima horaria del viento medida en 3 segundos, a 10 m",
    ),
    "DXI3S": (
        "direction_rafale_maximale_3s_10m",
        "INT64",
        "degree",
        "Direção da rajada registrada em rafale_maximale_3s_10m, na rosa de 360 graus",
        "Direction of the gust recorded in rafale_maximale_3s_10m, on the 360-degree rose",
        "Dirección de la racha registrada en rafale_maximale_3s_10m, en la rosa de 360 grados",
    ),
    "DRR": (
        "duree_precipitation",
        "FLOAT64",
        "minute",
        "Duração das precipitações",
        "Duration of the precipitation",
        "Duración de las precipitaciones",
    ),
    "STATUS_FXI3S": (
        "mode_obtention_rafale_3s",
        "STRING",
        "",
        "Modo de obtenção da rajada de 3 segundos: medida ou calculada por função de transferência",
        "How the 3-second gust was obtained: measured, or derived through a transfer function",
        "Modo de obtención de la racha de 3 segundos: medida o calculada por función de transferencia",
    ),
    "STATUS_DXI3S": (
        "mode_obtention_direction_rafale_3s",
        "STRING",
        "",
        "Modo de obtenção da direção da rajada de 3 segundos: medida ou assimilada à direção da rajada instantânea",
        "How the 3-second gust direction was obtained: measured, or taken from the instantaneous gust direction",
        "Modo de obtención de la dirección de la racha de 3 segundos: medida o asimilada a la dirección de la racha instantánea",
    ),
    # --- other parameters (autres-parametres) ---
    "DHUMEC": (
        "duree_humectation",
        "FLOAT64",
        "minute",
        "Duração de humectação, isto é, o tempo com a superfície molhada",
        "Duration of leaf wetness, that is, the time the surface stays wet",
        "Duración de humectación, es decir, el tiempo con la superficie mojada",
    ),
    "PMERM": (
        "pression_mer_moyenne",
        "FLOAT64",
        "hectopascal",
        "Média diária das pressões horárias reduzidas ao nível do mar",
        "Daily mean of the hourly pressures reduced to mean sea level",
        "Media diaria de las presiones horarias reducidas al nivel del mar",
    ),
    "PMERMIN": (
        "pression_mer_minimale",
        "FLOAT64",
        "hectopascal",
        "Mínimo diário das pressões horárias mínimas reduzidas ao nível do mar",
        "Daily minimum of the hourly minimum pressures reduced to mean sea level",
        "Mínimo diario de las presiones horarias mínimas reducidas al nivel del mar",
    ),
    "INST": (
        "duree_insolation",
        "FLOAT64",
        "minute",
        "Duração diária de insolação",
        "Daily sunshine duration",
        "Duración diaria de insolación",
    ),
    "GLOT": (
        "rayonnement_global",
        "FLOAT64",
        "joule_per_square_centimeter",
        "Radiação global diária",
        "Daily global radiation",
        "Radiación global diaria",
    ),
    "DIFT": (
        "rayonnement_diffus",
        "FLOAT64",
        "joule_per_square_centimeter",
        "Radiação difusa diária",
        "Daily diffuse radiation",
        "Radiación difusa diaria",
    ),
    "DIRT": (
        "rayonnement_direct",
        "FLOAT64",
        "joule_per_square_centimeter",
        "Radiação direta diária",
        "Daily direct radiation",
        "Radiación directa diaria",
    ),
    "INFRART": (
        "rayonnement_infrarouge",
        "FLOAT64",
        "joule_per_square_centimeter",
        "Soma das radiações infravermelhas horárias",
        "Sum of the hourly infrared radiation",
        "Suma de las radiaciones infrarrojas horarias",
    ),
    "UV": (
        "rayonnement_ultraviolet",
        "FLOAT64",
        "joule_per_square_centimeter",
        "Acumulado diário de radiação ultravioleta",
        "Daily cumulative ultraviolet radiation",
        "Acumulado diario de radiación ultravioleta",
    ),
    "UV_INDICEX": (
        "indice_uv_maximal",
        "FLOAT64",
        "",
        "Máximo dos índices UV horários",
        "Maximum of the hourly UV indices",
        "Máximo de los índices UV horarios",
    ),
    "SIGMA": (
        "fraction_insolation",
        "FLOAT64",
        "percent",
        "Fração de insolação em relação à duração do dia",
        "Sunshine fraction relative to day length",
        "Fracción de insolación respecto a la duración del día",
    ),
    "UN": (
        "humidite_minimale",
        "FLOAT64",
        "percent",
        "Mínimo diário das umidades relativas mínimas horárias",
        "Daily minimum of the hourly minimum relative humidities",
        "Mínimo diario de las humedades relativas mínimas horarias",
    ),
    "UX": (
        "humidite_maximale",
        "FLOAT64",
        "percent",
        "Máximo diário das umidades relativas máximas horárias",
        "Daily maximum of the hourly maximum relative humidities",
        "Máximo diario de las humedades relativas máximas horarias",
    ),
    "UM": (
        "humidite_moyenne",
        "FLOAT64",
        "percent",
        "Média diária das umidades relativas horárias",
        "Daily mean of the hourly relative humidities",
        "Media diaria de las humedades relativas horarias",
    ),
    "DHUMI40": (
        "duree_humidite_inf_40",
        "FLOAT64",
        "minute",
        "Duração com umidade relativa igual ou inferior a 40%",
        "Duration with relative humidity at or below 40%",
        "Duración con humedad relativa igual o inferior al 40%",
    ),
    "DHUMI80": (
        "duree_humidite_sup_80",
        "FLOAT64",
        "minute",
        "Duração com umidade relativa igual ou superior a 80%",
        "Duration with relative humidity at or above 80%",
        "Duración con humedad relativa igual o superior al 80%",
    ),
    "TSVM": (
        "tension_vapeur_moyenne",
        "FLOAT64",
        "hectopascal",
        "Tensão de vapor média",
        "Mean vapour pressure",
        "Tensión de vapor media",
    ),
    "ETPMON": (
        "evapotranspiration_penman_monteith",
        "FLOAT64",
        "millimeter",
        "Evapotranspiração potencial diária de Penman-Monteith",
        "Daily Penman-Monteith potential evapotranspiration",
        "Evapotranspiración potencial diaria de Penman-Monteith",
    ),
    "ETPGRILLE": (
        "evapotranspiration_point_grille",
        "FLOAT64",
        "millimeter",
        "Evapotranspiração potencial de Penman-Monteith calculada no ponto de grade mais próximo",
        "Penman-Monteith potential evapotranspiration computed at the nearest grid point",
        "Evapotranspiración potencial de Penman-Monteith calculada en el punto de rejilla más cercano",
    ),
    "HNEIGEF": (
        "hauteur_neige_fraiche",
        "FLOAT64",
        "centimeter",
        "Altura de neve fresca caída em 24 horas e ainda presente no solo às 06h UTC, atribuída ao dia J",
        "Depth of fresh snow fallen over 24 hours and still on the ground at 06:00 UTC, assigned to day J",
        "Altura de nieve fresca caída en 24 horas y aún presente en el suelo a las 06h UTC, asignada al día J",
    ),
    "NEIGETOTX": (
        "hauteur_neige_maximale",
        "FLOAT64",
        "centimeter",
        "Espessura máxima diária de neve, entre 01h e 24h UTC",
        "Daily maximum snow depth, between 01:00 and 24:00 UTC",
        "Espesor máximo diario de nieve, entre las 01h y las 24h UTC",
    ),
    "NEIGETOT06": (
        "hauteur_neige_06h",
        "FLOAT64",
        "centimeter",
        "Espessura total de neve no solo medida às 06h UTC",
        "Total snow depth on the ground measured at 06:00 UTC",
        "Espesor total de nieve en el suelo medido a las 06h UTC",
    ),
    "NB300": (
        "nebulosite_maximale_sous_300m",
        "INT64",
        "okta",
        "Nebulosidade máxima superior a 4 oitavos com base abaixo de 300 m",
        "Maximum cloud cover above 4 eighths with a base below 300 m",
        "Nubosidad máxima superior a 4 octavos con base por debajo de 300 m",
    ),
    "BA300": (
        "hauteur_base_sous_300m",
        "FLOAT64",
        "meter",
        "Altura mínima da base da nebulosidade registrada em nebulosite_maximale_sous_300m",
        "Minimum base height of the cloud recorded in nebulosite_maximale_sous_300m",
        "Altura mínima de la base de la nubosidad registrada en nebulosite_maximale_sous_300m",
    ),
    "TMERMIN": (
        "temperature_mer_minimale",
        "FLOAT64",
        "celsius_degree",
        "Temperatura mínima diária da água do mar",
        "Daily minimum sea water temperature",
        "Temperatura mínima diaria del agua del mar",
    ),
    "TMERMAX": (
        "temperature_mer_maximale",
        "FLOAT64",
        "celsius_degree",
        "Temperatura máxima diária da água do mar",
        "Daily maximum sea water temperature",
        "Temperatura máxima diaria del agua del mar",
    ),
}

# Occurrence flags: 0/1 codes, so STRING + dictionary-covered.
QUOT_FLAGS = {
    "NEIG": ("occurrence_neige", "neve", "snow", "nieve"),
    "BROU": ("occurrence_brouillard", "nevoeiro", "fog", "niebla"),
    "ORAG": ("occurrence_orage", "trovoada", "thunderstorm", "tormenta"),
    "GRESIL": (
        "occurrence_gresil",
        "granizo miúdo",
        "ice pellets",
        "granizo menudo",
    ),
    "GRELE": ("occurrence_grele", "granizo", "hail", "granizo"),
    "ROSEE": ("occurrence_rosee", "orvalho", "dew", "rocío"),
    "VERGLAS": (
        "occurrence_verglas",
        "gelo negro",
        "black ice",
        "hielo negro",
    ),
    "SOLNEIGE": (
        "occurrence_sol_enneige",
        "solo coberto de neve",
        "snow-covered ground",
        "suelo cubierto de nieve",
    ),
    "GELEE": (
        "occurrence_gelee_blanche",
        "geada branca",
        "hoar frost",
        "escarcha",
    ),
    "FUMEE": ("occurrence_fumee", "fumaça", "smoke", "humo"),
    "BRUME": ("occurrence_brume", "bruma", "mist", "bruma"),
    "ECLAIR": ("occurrence_eclair", "relâmpago", "lightning", "relámpago"),
}

# Columns dropped on purpose.
QUOT_DROP = {
    # Météo-France labels it "champ inutilisé"; 100% null in the sampled data.
    "ECOULEMENTM",
    "QECOULEMENTM",
}

# Station attributes lifted out of the fact tables into `poste`.
STATION_COLS = ["NOM_USUEL", "LAT", "LON", "ALTI"]
KEY_COLS = ["NUM_POSTE"]


# Monthly base parameters. `AB` = absolute extreme over the month, `_ME` =
# estimated, `M` on the tail = monthly mean. Units are the source's own.
MENS_PARAMS = {
    "RR": (
        "precipitation_totale",
        "FLOAT64",
        "millimeter",
        "Precipitação acumulada no mês",
        "Precipitation accumulated over the month",
        "Precipitación acumulada en el mes",
    ),
    "RR_ME": (
        "precipitation_totale_estimee",
        "FLOAT64",
        "millimeter",
        "Precipitação acumulada no mês, estimada",
        "Estimated precipitation accumulated over the month",
        "Precipitación acumulada en el mes, estimada",
    ),
    "RRAB": (
        "precipitation_quotidienne_maximale",
        "FLOAT64",
        "millimeter",
        "Precipitação máxima caída em 24 horas no mês",
        "Maximum precipitation fallen in 24 hours during the month",
        "Precipitación máxima caída en 24 horas en el mes",
    ),
    "PMERM": (
        "pression_mer_moyenne",
        "FLOAT64",
        "hectopascal",
        "Média mensal das pressões diárias médias reduzidas ao nível do mar",
        "Monthly mean of the daily mean pressures reduced to mean sea level",
        "Media mensual de las presiones diarias medias reducidas al nivel del mar",
    ),
    "PMERMINAB": (
        "pression_mer_minimale_absolue",
        "FLOAT64",
        "hectopascal",
        "Mínimo absoluto mensal das pressões diárias médias reduzidas ao nível do mar",
        "Monthly absolute minimum of the daily mean pressures reduced to mean sea level",
        "Mínimo absoluto mensual de las presiones diarias medias reducidas al nivel del mar",
    ),
    "TX": (
        "temperature_maximale_moyenne",
        "FLOAT64",
        "celsius_degree",
        "Média mensal das temperaturas máximas diárias",
        "Monthly mean of the daily maximum temperatures",
        "Media mensual de las temperaturas máximas diarias",
    ),
    "TX_ME": (
        "temperature_maximale_moyenne_estimee",
        "FLOAT64",
        "celsius_degree",
        "Média mensal estimada das temperaturas máximas diárias",
        "Estimated monthly mean of the daily maximum temperatures",
        "Media mensual estimada de las temperaturas máximas diarias",
    ),
    "TXAB": (
        "temperature_maximale_absolue",
        "FLOAT64",
        "celsius_degree",
        "Máximo absoluto mensal das temperaturas máximas diárias",
        "Monthly absolute maximum of the daily maximum temperatures",
        "Máximo absoluto mensual de las temperaturas máximas diarias",
    ),
    "TXMIN": (
        "temperature_maximale_la_plus_basse",
        "FLOAT64",
        "celsius_degree",
        "Mínimo mensal das temperaturas máximas diárias",
        "Monthly minimum of the daily maximum temperatures",
        "Mínimo mensual de las temperaturas máximas diarias",
    ),
    "TN": (
        "temperature_minimale_moyenne",
        "FLOAT64",
        "celsius_degree",
        "Média mensal das temperaturas mínimas diárias",
        "Monthly mean of the daily minimum temperatures",
        "Media mensual de las temperaturas mínimas diarias",
    ),
    "TN_ME": (
        "temperature_minimale_moyenne_estimee",
        "FLOAT64",
        "celsius_degree",
        "Média mensal estimada das temperaturas mínimas diárias",
        "Estimated monthly mean of the daily minimum temperatures",
        "Media mensual estimada de las temperaturas mínimas diarias",
    ),
    "TNAB": (
        "temperature_minimale_absolue",
        "FLOAT64",
        "celsius_degree",
        "Mínimo absoluto mensal das temperaturas mínimas diárias",
        "Monthly absolute minimum of the daily minimum temperatures",
        "Mínimo absoluto mensual de las temperaturas mínimas diarias",
    ),
    "TNMAX": (
        "temperature_minimale_la_plus_haute",
        "FLOAT64",
        "celsius_degree",
        "Máximo mensal das temperaturas mínimas diárias",
        "Monthly maximum of the daily minimum temperatures",
        "Máximo mensual de las temperaturas mínimas diarias",
    ),
    "TAMPLIM": (
        "amplitude_thermique_moyenne",
        "FLOAT64",
        "celsius_degree",
        "Média mensal das amplitudes térmicas diárias",
        "Monthly mean of the daily temperature ranges",
        "Media mensual de las amplitudes térmicas diarias",
    ),
    "TAMPLIAB": (
        "amplitude_thermique_absolue",
        "FLOAT64",
        "celsius_degree",
        "Máximo absoluto mensal das amplitudes térmicas diárias",
        "Monthly absolute maximum of the daily temperature ranges",
        "Máximo absoluto mensual de las amplitudes térmicas diarias",
    ),
    "TM": (
        "temperature_moyenne_min_max",
        "FLOAT64",
        "celsius_degree",
        "Média mensal das médias diárias entre a temperatura mínima e a máxima",
        "Monthly mean of the daily means of the minimum and maximum temperature",
        "Media mensual de las medias diarias entre la temperatura mínima y la máxima",
    ),
    "TMM": (
        "temperature_moyenne",
        "FLOAT64",
        "celsius_degree",
        "Média mensal das temperaturas médias diárias",
        "Monthly mean of the daily mean temperatures",
        "Media mensual de las temperaturas medias diarias",
    ),
    "TMMIN": (
        "temperature_moyenne_min_max_minimale",
        "FLOAT64",
        "celsius_degree",
        "Mínimo mensal das médias diárias entre a temperatura mínima e a máxima",
        "Monthly minimum of the daily means of the minimum and maximum temperature",
        "Mínimo mensual de las medias diarias entre la temperatura mínima y la máxima",
    ),
    "TMMAX": (
        "temperature_moyenne_min_max_maximale",
        "FLOAT64",
        "celsius_degree",
        "Máximo mensal das médias diárias entre a temperatura mínima e a máxima",
        "Monthly maximum of the daily means of the minimum and maximum temperature",
        "Máximo mensual de las medias diarias entre la temperatura mínima y la máxima",
    ),
    "UNAB": (
        "humidite_minimale_absolue",
        "FLOAT64",
        "percent",
        "Mínimo absoluto mensal das umidades relativas mínimas diárias",
        "Monthly absolute minimum of the daily minimum relative humidities",
        "Mínimo absoluto mensual de las humedades relativas mínimas diarias",
    ),
    "UXAB": (
        "humidite_maximale_absolue",
        "FLOAT64",
        "percent",
        "Máximo absoluto mensal das umidades relativas máximas diárias",
        "Monthly absolute maximum of the daily maximum relative humidities",
        "Máximo absoluto mensual de las humedades relativas máximas diarias",
    ),
    "UMM": (
        "humidite_moyenne",
        "FLOAT64",
        "percent",
        "Média mensal das umidades relativas médias diárias",
        "Monthly mean of the daily mean relative humidities",
        "Media mensual de las humedades relativas medias diarias",
    ),
    "TSVM": (
        "tension_vapeur_moyenne",
        "FLOAT64",
        "hectopascal",
        "Média mensal da tensão de vapor",
        "Monthly mean vapour pressure",
        "Media mensual de la tensión de vapor",
    ),
    "ETP": (
        "evapotranspiration_totale",
        "FLOAT64",
        "millimeter",
        "Soma das evapotranspirações potenciais decendiais de Penman-Monteith",
        "Sum of the ten-day Penman-Monteith potential evapotranspiration values",
        "Suma de las evapotranspiraciones potenciales decenales de Penman-Monteith",
    ),
    "FXIAB": (
        "rafale_maximale_absolue_10m",
        "FLOAT64",
        "meter / second",
        "Máximo absoluto mensal da rajada máxima diária do vento instantâneo, a 10 m do solo",
        "Monthly absolute maximum of the daily maximum instantaneous wind gust, at 10 m",
        "Máximo absoluto mensual de la racha máxima diaria del viento instantáneo, a 10 m",
    ),
    "DXIAB": (
        "direction_rafale_maximale_absolue_10m",
        "INT64",
        "degree",
        "Direção da rajada registrada em rafale_maximale_absolue_10m, na rosa de 360 graus",
        "Direction of the gust recorded in rafale_maximale_absolue_10m, on the 360-degree rose",
        "Dirección de la racha registrada en rafale_maximale_absolue_10m, en la rosa de 360 grados",
    ),
    "FXI3SAB": (
        "rafale_maximale_3s_absolue_10m",
        "FLOAT64",
        "meter / second",
        "Máximo absoluto mensal da rajada máxima diária do vento medida em 3 segundos, a 10 m do solo",
        "Monthly absolute maximum of the daily maximum 3-second mean wind gust, at 10 m",
        "Máximo absoluto mensual de la racha máxima diaria del viento medida en 3 segundos, a 10 m",
    ),
    "DXI3SAB": (
        "direction_rafale_maximale_3s_absolue_10m",
        "INT64",
        "degree",
        "Direção da rajada registrada em rafale_maximale_3s_absolue_10m, na rosa de 360 graus",
        "Direction of the gust recorded in rafale_maximale_3s_absolue_10m, on the 360-degree rose",
        "Dirección de la racha registrada en rafale_maximale_3s_absolue_10m, en la rosa de 360 grados",
    ),
    "FXYAB": (
        "vitesse_vent_maximale_moyennee_absolue_10m",
        "FLOAT64",
        "meter / second",
        "Máximo absoluto mensal da velocidade máxima diária do vento medida em 10 minutos, a 10 m do solo",
        "Monthly absolute maximum of the daily maximum 10-minute mean wind speed, at 10 m",
        "Máximo absoluto mensual de la velocidad máxima diaria del viento medida en 10 minutos, a 10 m",
    ),
    "DXYAB": (
        "direction_vent_maximal_moyenne_absolue_10m",
        "INT64",
        "degree",
        "Direção do vento registrado em vitesse_vent_maximale_moyennee_absolue_10m, na rosa de 360 graus",
        "Direction of the wind recorded in vitesse_vent_maximale_moyennee_absolue_10m, on the 360-degree rose",
        "Dirección del viento registrado en vitesse_vent_maximale_moyennee_absolue_10m, en la rosa de 360 grados",
    ),
    "FFM": (
        "vitesse_vent_moyenne_10m",
        "FLOAT64",
        "meter / second",
        "Média mensal da velocidade média diária do vento medida em 10 minutos, a 10 m do solo",
        "Monthly mean of the daily mean 10-minute mean wind speed, at 10 m",
        "Media mensual de la velocidad media diaria del viento medida en 10 minutos, a 10 m",
    ),
    "INST": (
        "duree_insolation_totale",
        "FLOAT64",
        "minute",
        "Soma mensal das durações diárias de insolação",
        "Monthly sum of the daily sunshine durations",
        "Suma mensual de las duraciones diarias de insolación",
    ),
    "GLOT": (
        "rayonnement_global_total",
        "FLOAT64",
        "joule_per_square_centimeter",
        "Soma mensal da radiação global diária",
        "Monthly sum of the daily global radiation",
        "Suma mensual de la radiación global diaria",
    ),
    "DIFT": (
        "rayonnement_diffus_total",
        "FLOAT64",
        "joule_per_square_centimeter",
        "Soma mensal da radiação difusa diária",
        "Monthly sum of the daily diffuse radiation",
        "Suma mensual de la radiación difusa diaria",
    ),
    "DIRT": (
        "rayonnement_direct_total",
        "FLOAT64",
        "joule_per_square_centimeter",
        "Soma mensal da radiação direta diária",
        "Monthly sum of the daily direct radiation",
        "Suma mensual de la radiación directa diaria",
    ),
    "HNEIGEFTOT": (
        "hauteur_neige_fraiche_totale",
        "FLOAT64",
        "centimeter",
        "Soma mensal da altura de neve fresca caída em 24 horas",
        "Monthly sum of the depth of fresh snow fallen over 24 hours",
        "Suma mensual de la altura de nieve fresca caída en 24 horas",
    ),
    "HNEIGEFAB": (
        "hauteur_neige_fraiche_maximale",
        "FLOAT64",
        "centimeter",
        "Máximo absoluto mensal da altura de neve fresca caída em 24 horas",
        "Monthly absolute maximum of the depth of fresh snow fallen over 24 hours",
        "Máximo absoluto mensual de la altura de nieve fresca caída en 24 horas",
    ),
    "NEIGETOTM": (
        "hauteur_neige_moyenne",
        "FLOAT64",
        "centimeter",
        "Média mensal da espessura total de neve medida diariamente às 06h UTC",
        "Monthly mean of the total snow depth measured daily at 06:00 UTC",
        "Media mensual del espesor total de nieve medido diariamente a las 06h UTC",
    ),
    "NEIGETOTAB": (
        "hauteur_neige_maximale",
        "FLOAT64",
        "centimeter",
        "Máximo absoluto mensal da espessura máxima diária de neve",
        "Monthly absolute maximum of the daily maximum snow depth",
        "Máximo absoluto mensual del espesor máximo diario de nieve",
    ),
}


# --- family expansion -------------------------------------------------------
# Météo-France's own descriptors carry a French sentence for every column. The
# four families below are mechanical, so their trilingual descriptions are
# templated from the base parameter they qualify rather than hand-written; only
# the ~99 base parameters above are authored by hand.

import json  # noqa: E402
import re  # noqa: E402
import unicodedata  # noqa: E402

DESCRIPTORS = "/tmp/mf_descriptors.json"


def slug(text: str) -> str:
    t = (
        text.lower()
        .replace("≥", " sup ")
        .replace("≤", " inf ")
        .replace("°c", "c")
    )
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode()
    t = re.sub(r"[^a-z0-9]+", "_", t).strip("_")
    return re.sub(r"_+", "_", t)


def _condition(french: str) -> str:
    """``nombre de jours avec RR ≥ 1.0 mm`` -> ``RR ≥ 1.0 mm``."""
    m = re.search(r"nombre de jours? (?:avec |de )?(.*)", french, re.I)
    return (m.group(1) if m else french).strip()


# Columns whose French description does not name a single parameter, so the
# family regexes cannot resolve them.
OVERRIDES = {
    # "nombre de valeurs présentes de hauteur de précipitation quotidienne" —
    # prose rather than a parameter token, so the family regex cannot match it.
    "NBRR": (
        "nombre_valeurs_precipitation",
        "INT64",
        "day",
        "Número de valores diários presentes de precipitação no mês",
        "Number of daily precipitation values present in the month",
        "Número de valores diarios presentes de precipitación en el mes",
    ),
    # "nombre de valeurs présentes du couple (TN, TX) quotidien" — the count of
    # days on which BOTH the daily minimum and maximum are present, which is
    # what temperature_moyenne_min_max is computed from.
    "NBTM": (
        "nombre_valeurs_temperature_moyenne_min_max",
        "INT64",
        "day",
        "Número de dias no mês em que a temperatura mínima e a máxima estão ambas presentes",
        "Number of days in the month on which both the minimum and the maximum temperature are present",
        "Número de días en el mes en que la temperatura mínima y la máxima están ambas presentes",
    ),
}


def _ref(name, known):
    """Target name for a referenced source parameter, whichever table defines it."""
    if name in known:
        return known[name][0]
    if name in QUOT_PARAMS:
        return QUOT_PARAMS[name][0]
    if name in MENS_PARAMS:
        return MENS_PARAMS[name][0]
    if name in QUOT_FLAGS:
        return QUOT_FLAGS[name][0]
    return slug(name)


RE_HEURE = re.compile(r"^heure de ([A-Z0-9_]+)", re.I)
RE_NBVAL = re.compile(
    r"nombre de valeurs pr.sentes (?:de|du) ([A-Z][A-Z0-9_]*) quotidien"
)
RE_JOUR = re.compile(r"^jour du ([A-Z][A-Z0-9_]*)")
RE_NBJOURS = re.compile(r"^nombre de jours?\b", re.I)


# "Nombre de jours avec ..." conditions that are prose rather than a formula.
# The formula ones ("RR >= 1.0 mm") are the source's parameter code and read the
# same in any language; these would otherwise leave raw French inside the
# Portuguese, English and Spanish descriptions.
def _needs_translation(condition):
    """True when a day-count condition contains prose rather than a comparison."""
    words = re.findall(r"[A-Za-zÀ-ÿ]{3,}", condition)
    return any(w.lower() not in _SYMBOLIC for w in words)


# Tokens that carry no language: parameter names and unit spellings.
_SYMBOLIC = {
    "sigma",
    "rrs",
    "neigetot",
    "hneigef",
    "fxi",
    "fxy",
    "tms",
    "tni",
    "tns",
    "txi",
    "txs",
    "mmm",
    "cms",
}

NBJ_CONDITIONS = {
    "nombre_jours_gelee": ("geada branca", "hoar frost", "escarcha"),
    "nombre_jours_sigma0": (
        "SIGMA = 0%, sendo SIGMA a fração de insolação em relação à duração do dia",
        "SIGMA = 0%, where SIGMA is the sunshine fraction relative to day length",
        "SIGMA = 0%, siendo SIGMA la fracción de insolación respecto a la duración "
        "del día",
    ),
    "nombre_jours_tms24": (
        "temperatura média diária ≥ 24°C",
        "daily mean temperature ≥ 24°C",
        "temperatura media diaria ≥ 24°C",
    ),
    "nombre_jours_neig": (
        "precipitação de neve",
        "snowfall",
        "precipitación de nieve",
    ),
    "nombre_jours_hneigef1": (
        "queda de neve em 24 horas superior a 1 cm",
        "snowfall over 24 hours above 1 cm",
        "nevada en 24 horas superior a 1 cm",
    ),
    "nombre_jours_hneigef5": (
        "queda de neve em 24 horas superior a 5 cm",
        "snowfall over 24 hours above 5 cm",
        "nevada en 24 horas superior a 5 cm",
    ),
    "nombre_jours_hneigef10": (
        "queda de neve em 24 horas superior a 10 cm",
        "snowfall over 24 hours above 10 cm",
        "nevada en 24 horas superior a 10 cm",
    ),
    "nombre_jours_solng": (
        "solo coberto de neve",
        "snow-covered ground",
        "suelo cubierto de nieve",
    ),
    "nombre_jours_neigetot1": (
        "espessura de neve superior a 1 cm",
        "snow depth above 1 cm",
        "espesor de nieve superior a 1 cm",
    ),
    "nombre_jours_neigetot10": (
        "espessura de neve superior a 10 cm",
        "snow depth above 10 cm",
        "espesor de nieve superior a 10 cm",
    ),
    "nombre_jours_neigetot30": (
        "espessura de neve superior a 30 cm",
        "snow depth above 30 cm",
        "espesor de nieve superior a 30 cm",
    ),
    "nombre_jours_grel": ("granizo", "hail", "granizo"),
    "nombre_jours_orag": ("trovoada", "thunderstorm", "tormenta"),
    "nombre_jours_brou": ("nevoeiro", "fog", "niebla"),
}


def _ref_daily(name, known):
    """Like :func:`_ref`, but the daily table wins — for the NB* counts."""
    if name in QUOT_PARAMS:
        return QUOT_PARAMS[name][0]
    return _ref(name, known)


def expand(cols, params, flags, descriptors):
    """Yield one schema row per source column, in source order.

    Runs :func:`_expand_body` twice. The first pass names every column; the
    second re-runs it with those names available, so a quality code resolves to
    the *derived* name of the column it qualifies. Without it ``QHTN`` becomes
    ``qualite_htn`` — the raw source token leaking into all three languages —
    rather than ``qualite_heure_temperature_minimale``.

    Returns tuples of
    ``(target, bigquery_type, unit, covered_by_dictionary, pt, en, es, original)``.
    """
    first = _expand_body(cols, params, flags, descriptors, {})
    return _expand_body(
        cols, params, flags, descriptors, {r[7]: r[0] for r in first}
    )


def _expand_body(cols, params, flags, descriptors, assigned):
    """One naming pass.

    Family membership is decided from Météo-France's own French description
    rather than from the column name. Name-munging is not enough: ``HXY`` is
    "heure de FXY" and ``NBUM`` is "nombre de valeurs présentes de UM
    quotidienne", so the referenced parameter is spelled differently from the
    prefix-stripped column.
    """
    known = dict(params)
    out = []
    for col in cols:
        if col in QUOT_DROP or col in STATION_COLS or col in KEY_COLS:
            continue
        if col in ("AAAAMMJJ", "AAAAMM"):
            continue
        fr = descriptors.get(col, "")

        if col in known:
            tgt, typ, unit, pt, en, es = known[col]
            out.append((tgt, typ, unit, False, pt, en, es, col))
            continue
        if col in flags:
            tgt, pt_w, en_w, es_w = flags[col]
            out.append(
                (
                    tgt,
                    "STRING",
                    "",
                    True,
                    f"Ocorrência de {pt_w} no dia, codificada como 0 ou 1",
                    f"Occurrence of {en_w} on the day, coded 0 or 1",
                    f"Ocurrencia de {es_w} en el día, codificada como 0 o 1",
                    col,
                )
            )
            continue
        if col.startswith("Q") and col[1:] in cols:
            # Resolve against names already assigned in this pass, so a quality
            # code on a derived column (heure_*) picks up the derived name
            # rather than falling through to the raw source token: QHTN is the
            # quality of heure_temperature_minimale, not of "htn".
            ref = assigned.get(col[1:]) or _ref(col[1:], known)
            out.append(
                (
                    f"qualite_{ref}",
                    "STRING",
                    "",
                    True,
                    f"Código de qualidade de {ref}",
                    f"Quality code of {ref}",
                    f"Código de calidad de {ref}",
                    col,
                )
            )
            continue

        m = RE_HEURE.match(fr)
        if m:
            ref = _ref(m.group(1), known)
            out.append(
                (
                    f"heure_{ref}",
                    "STRING",
                    "",
                    False,
                    f"Hora em que {ref} foi observado, no formato hhmm publicado pela fonte",
                    f"Time at which {ref} was observed, in the hhmm form the source publishes",
                    f"Hora en que se observó {ref}, en el formato hhmm publicado por la fuente",
                    col,
                )
            )
            continue

        m = RE_NBVAL.search(fr)
        if m:
            # These count DAILY values present in the month ("de TX
            # quotidienne"), so the reference resolves against the daily
            # parameter table first. Resolving against the monthly table would
            # name NBTX after the monthly mean rather than the daily maximum.
            ref = _ref_daily(m.group(1), known)
            out.append(
                (
                    f"nombre_valeurs_{ref}",
                    "INT64",
                    "day",
                    False,
                    f"Número de valores diários presentes de {ref} no mês",
                    f"Number of daily {ref} values present in the month",
                    f"Número de valores diarios presentes de {ref} en el mes",
                    col,
                )
            )
            continue

        if RE_NBJOURS.match(fr):
            target = f"nombre_jours_{slug(col.removeprefix('NBJ').removeprefix('NB'))}"
            raw = _condition(fr)
            if target in NBJ_CONDITIONS:
                pt, en, es = NBJ_CONDITIONS[target]
            else:
                # A condition that is a bare comparison ("TX >= 30°C") needs no
                # translation. Anything with prose words does, and falling back
                # would ship the French into all three languages -- so refuse.
                if _needs_translation(raw):
                    raise ValueError(
                        f"{col} -> {target}: prose condition {raw!r} has no "
                        "NBJ_CONDITIONS entry; add one rather than shipping the "
                        "French in every language."
                    )
                pt = en = es = raw
            out.append(
                (
                    target,
                    "INT64",
                    "day",
                    False,
                    f"Número de dias no mês com {pt}",
                    f"Number of days in the month with {en}",
                    f"Número de días en el mes con {es}",
                    col,
                )
            )
            continue

        m = RE_JOUR.match(fr)
        if m or col.endswith("DAT"):
            base = m.group(1) if m else col[:-3]
            ref = _ref(
                base if base in known else base.removesuffix("AB"), known
            )
            out.append(
                (
                    f"jour_{ref}",
                    "INT64",
                    "day",
                    False,
                    f"Dia do mês em que {ref} foi observado",
                    f"Day of the month on which {ref} was observed",
                    f"Día del mes en que se observó {ref}",
                    col,
                )
            )
            continue

        if col in OVERRIDES:
            tgt, typ, unit, pt, en, es = OVERRIDES[col]
            out.append((tgt, typ, unit, False, pt, en, es, col))
            continue

        out.append((slug(col), "STRING", "", False, fr, fr, fr, col))
    return out


def descriptors():
    with open(DESCRIPTORS, encoding="utf-8") as fh:
        d = json.load(fh)
    merged = {}
    for v in d.values():
        merged.update(v)
    return merged
