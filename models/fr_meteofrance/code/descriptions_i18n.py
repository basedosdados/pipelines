"""English and Spanish column descriptions for ``fr_meteofrance``.

The Portuguese descriptions live in ``schema_map.py``, which is the canonical
schema. This module only carries the translations, keyed by target column name.
Columns that appear in more than one table share a description.
"""

EN_ES = {
    # --- shared scaffolding ---
    "annee": (
        "Year of the observation, in coordinated universal time (UTC)",
        "Año de la observación, en tiempo universal coordinado (UTC)",
    ),
    "mois": (
        "Month of the observation, in coordinated universal time (UTC)",
        "Mes de la observación, en tiempo universal coordinado (UTC)",
    ),
    "date": (
        "Date of the observation, in coordinated universal time (UTC)",
        "Fecha de la observación, en tiempo universal coordinado (UTC)",
    ),
    "heure": (
        "Time of the observation, in coordinated universal time (UTC)",
        "Hora de la observación, en tiempo universal coordinado (UTC)",
    ),
    "indicatif_omm": (
        "Five-digit WMO station identifier",
        "Identificador OMM de la estación, de cinco dígitos",
    ),
    "date_heure_traitement": (
        "Date and time (UTC) the measurement was extracted from Météo-France internal databases",
        "Fecha y hora (UTC) de la extracción de la medición de las bases internas de Météo-France",
    ),
    "date_heure_insertion": (
        "Date and time (UTC) the sensor measurement was inserted into Météo-France internal databases",
        "Fecha y hora (UTC) de la inserción de la medición del sensor en las bases internas de Météo-France",
    ),
    # --- synop parameters ---
    "pression_mer": (
        "Atmospheric pressure reduced to mean sea level",
        "Presión atmosférica reducida al nivel del mar",
    ),
    "variation_pression_3h": (
        "Change in atmospheric pressure over the last 3 hours",
        "Variación de la presión atmosférica en las últimas 3 horas",
    ),
    "type_tendance_barometrique": (
        "Characteristic of the pressure tendency (WMO code table 0200)",
        "Tipo de tendencia barométrica (tabla de código OMM 0200)",
    ),
    "direction_vent_moyen": (
        "Direction of the 10-minute mean wind, in degrees from true north",
        "Dirección del viento medio en 10 minutos, en grados desde el norte geográfico",
    ),
    "vitesse_vent_moyen": (
        "Speed of the 10-minute mean wind",
        "Velocidad del viento medio en 10 minutos",
    ),
    "temperature": (
        "Air temperature in the shelter",
        "Temperatura del aire bajo abrigo",
    ),
    "point_rosee": ("Dew point temperature", "Temperatura del punto de rocío"),
    "humidite": ("Relative humidity of the air", "Humedad relativa del aire"),
    "visibilite_horizontale": (
        "Horizontal visibility",
        "Visibilidad horizontal",
    ),
    "temps_present": (
        "Present weather observed at the time of measurement (WMO code table 4677)",
        "Tiempo presente observado en el momento de la medición (tabla de código OMM 4677)",
    ),
    "temps_passe_1": (
        "Past weather 1, observed in the period before the measurement (WMO code table 4561)",
        "Tiempo pasado 1, observado en el período anterior a la medición (tabla de código OMM 4561)",
    ),
    "temps_passe_2": (
        "Past weather 2, observed in the period before the measurement (WMO code table 4561)",
        "Tiempo pasado 2, observado en el período anterior a la medición (tabla de código OMM 4561)",
    ),
    "nebulosite_totale": (
        "Total cloud cover, that is, the fraction of the sky covered by clouds",
        "Nubosidad total, es decir, fracción del cielo cubierta por nubes",
    ),
    "nebulosite_etage_inferieur": (
        "Cloud cover of the low cloud layer",
        "Nubosidad de las nubes del piso inferior",
    ),
    "hauteur_base_nuages_inferieurs": (
        "Height of the base of the low clouds",
        "Altura de la base de las nubes del piso inferior",
    ),
    "type_nuages_etage_inferieur": (
        "Type of the low clouds (WMO code table 0513)",
        "Tipo de las nubes del piso inferior (tabla de código OMM 0513)",
    ),
    "type_nuages_etage_moyen": (
        "Type of the middle clouds (WMO code table 0515)",
        "Tipo de las nubes del piso medio (tabla de código OMM 0515)",
    ),
    "type_nuages_etage_superieur": (
        "Type of the high clouds (WMO code table 0509)",
        "Tipo de las nubes del piso superior (tabla de código OMM 0509)",
    ),
    "pression_station": (
        "Atmospheric pressure measured at station level",
        "Presión atmosférica medida al nivel de la estación",
    ),
    "niveau_barometrique": (
        "Standard barometric level the geopotential refers to",
        "Nivel barométrico estándar al que se refiere el geopotencial",
    ),
    "geopotentiel": (
        "Geopotential of the reference isobaric surface",
        "Geopotencial de la superficie isobárica de referencia",
    ),
    "variation_pression_24h": (
        "Change in atmospheric pressure over the last 24 hours",
        "Variación de la presión atmosférica en las últimas 24 horas",
    ),
    "temperature_minimale_12h": (
        "Minimum air temperature over the last 12 hours",
        "Temperatura mínima del aire en las últimas 12 horas",
    ),
    "temperature_minimale_24h": (
        "Minimum air temperature over the last 24 hours",
        "Temperatura mínima del aire en las últimas 24 horas",
    ),
    "temperature_maximale_12h": (
        "Maximum air temperature over the last 12 hours",
        "Temperatura máxima del aire en las últimas 12 horas",
    ),
    "temperature_maximale_24h": (
        "Maximum air temperature over the last 24 hours",
        "Temperatura máxima del aire en las últimas 24 horas",
    ),
    "temperature_minimale_sol_12h": (
        "Minimum ground temperature over the last 12 hours",
        "Temperatura mínima del suelo en las últimas 12 horas",
    ),
    "methode_mesure_tw": (
        "Method used to obtain the wet-bulb temperature (WMO code table 3855)",
        "Método de obtención de la temperatura del termómetro húmedo (tabla de código OMM 3855)",
    ),
    "temperature_thermometre_mouille": (
        "Wet-bulb temperature",
        "Temperatura del termómetro húmedo",
    ),
    "rafale_10min": (
        "Maximum wind gust over the last 10 minutes",
        "Racha máxima de viento en los últimos 10 minutos",
    ),
    "rafale_periode": (
        "Maximum wind gust over the period given in periode_mesure_rafale",
        "Racha máxima de viento en el período indicado en periode_mesure_rafale",
    ),
    "periode_mesure_rafale": (
        "Measurement period of the gust recorded in rafale_periode",
        "Período de medición de la racha registrada en rafale_periode",
    ),
    "etat_sol": (
        "State of the ground without snow (WMO code table 0901)",
        "Estado del suelo sin nieve (tabla de código OMM 0901)",
    ),
    "hauteur_neige": (
        "Total depth of snow, ice or other deposit on the ground",
        "Altura total de la capa de nieve, hielo u otro depósito en el suelo",
    ),
    "hauteur_neige_fraiche": (
        "Depth of fresh snow accumulated over the period given in periode_mesure_neige_fraiche",
        "Altura de la nieve fresca acumulada en el período indicado en periode_mesure_neige_fraiche",
    ),
    "periode_mesure_neige_fraiche": (
        "Measurement period of the fresh snow, published by the source in tenths of an hour",
        "Período de medición de la nieve fresca, publicado por la fuente en décimas de hora",
    ),
    "precipitation_1h": (
        "Precipitation accumulated over the last hour",
        "Precipitación acumulada en la última hora",
    ),
    "precipitation_3h": (
        "Precipitation accumulated over the last 3 hours",
        "Precipitación acumulada en las últimas 3 horas",
    ),
    "precipitation_6h": (
        "Precipitation accumulated over the last 6 hours",
        "Precipitación acumulada en las últimas 6 horas",
    ),
    "precipitation_12h": (
        "Precipitation accumulated over the last 12 hours",
        "Precipitación acumulada en las últimas 12 horas",
    ),
    "precipitation_24h": (
        "Precipitation accumulated over the last 24 hours",
        "Precipitación acumulada en las últimas 24 horas",
    ),
    "nom_station": (
        "Common name of the station",
        "Nombre usual de la estación",
    ),
    "indicatif_wigos": (
        "WIGOS station identifier, in block-issuer-type-number form",
        "Identificador WIGOS de la estación, en formato bloque-emisor-tipo-número",
    ),
    "latitude": (
        "Latitude of the station, negative south of the equator",
        "Latitud de la estación, negativa al sur del ecuador",
    ),
    "longitude": (
        "Longitude of the station, negative west of Greenwich",
        "Longitud de la estación, negativa al oeste de Greenwich",
    ),
    "altitude": (
        "Altitude of the station, measured at the foot of the shelter or rain gauge",
        "Altitud del puesto, medida al pie del abrigo o del pluviómetro",
    ),
    "date_ouverture": (
        "Date the station was opened",
        "Fecha de apertura de la estación",
    ),
    "annee_debut_observation": (
        "First year the station appears in the SYNOP archive",
        "Primer año en que la estación aparece en el archivo SYNOP",
    ),
    "annee_fin_observation": (
        "Last year the station appears in the SYNOP archive",
        "Último año en que la estación aparece en el archivo SYNOP",
    ),
    "geolocalisation": (
        "Geographic point of the station, in WGS 84",
        "Punto geográfico de la estación, en WGS 84",
    ),
    # --- normals ---
    "numero_poste": (
        "Eight-digit Météo-France station number",
        "Número Météo-France del puesto, de ocho dígitos",
    ),
    "nom_poste": ("Common name of the station", "Nombre usual del puesto"),
    "id_departement": (
        "Code of the department or overseas collectivity of the station",
        "Código del departamento o de la colectividad de ultramar del puesto",
    ),
    "date_edition": (
        "Date the station's climatological sheet was issued",
        "Fecha de edición de la ficha climatológica del puesto",
    ),
    "indicateur": (
        "Code of the climatological indicator",
        "Código del indicador climatológico",
    ),
    "periode": (
        "Period of the indicator: month from 01 to 12, or annee for the annual value",
        "Período del indicador: mes de 01 a 12, o annee para el valor anual",
    ),
    "valeur": (
        "Value of the indicator in the period, in the unit recorded in unite",
        "Valor del indicador en el período, en la unidad registrada en unite",
    ),
    "unite": (
        "Unit of measurement of the value",
        "Unidad de medida del valor",
    ),
    "libelle_indicateur": (
        "Label of the indicator as published in the climatological sheet",
        "Etiqueta del indicador tal como se publica en la ficha climatológica",
    ),
    "annee_debut_reference": (
        "First year of the reference period the normal was computed over",
        "Primer año del período de referencia sobre el que se calculó la normal",
    ),
    "annee_fin_reference": (
        "Last year of the reference period the normal was computed over",
        "Último año del período de referencia sobre el que se calculó la normal",
    ),
    "date_debut_record": (
        "First date of the period the record was established over",
        "Primera fecha del período sobre el que se estableció el récord",
    ),
    "date_fin_record": (
        "Last date of the period the record was established over",
        "Última fecha del período sobre el que se estableció el récord",
    ),
    "jour_record": (
        "Day of the month the record was observed",
        "Día del mes en que se observó el récord",
    ),
    "annee_record": (
        "Year the record was observed",
        "Año en que se observó el récord",
    ),
    # --- dicionario ---
    "id_tabela": ("Name of the table", "Nombre de la tabla"),
    "nome_coluna": ("Name of the column", "Nombre de la columna"),
    "chave": (
        "Key, that is, the coded value held in the column",
        "Clave, es decir, el valor codificado en la columna",
    ),
    "cobertura_temporal": (
        "Temporal coverage of the key",
        "Cobertura temporal de la clave",
    ),
    "valor": (
        "Value, that is, the meaning of the key",
        "Valor, es decir, el significado de la clave",
    ),
}


def _cloud_layer(n):
    return {
        f"nebulosite_couche_{n}": (
            f"Cloud cover of cloud layer {n}",
            f"Nubosidad de la capa de nubes {n}",
        ),
        f"type_nuage_{n}": (
            f"Cloud type of layer {n} (WMO code table 0500)",
            f"Tipo de nube de la capa {n} (tabla de código OMM 0500)",
        ),
        f"hauteur_base_nuage_{n}": (
            f"Height of the base of cloud layer {n}",
            f"Altura de la base de la capa de nubes {n}",
        ),
        f"phenomene_special_{n}": (
            f"Special phenomenon {n} observed at the station (WMO code table 3778)",
            f"Fenómeno especial {n} observado en la estación (tabla de código OMM 3778)",
        ),
    }


for _n in range(1, 5):
    EN_ES.update(_cloud_layer(_n))
