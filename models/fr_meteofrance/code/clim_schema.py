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
        "",
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
