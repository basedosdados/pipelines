"""Build the ``fr_meteofrance`` dicionario table.

The SYNOP files publish WMO/BUFR code values, not labels. The tables below are the
BUFR descriptors the Météo-France export actually uses, which are wider than the
manual-observation tables cited in the technical descriptor:

* ``temps_present``      BUFR 0 20 003 (0-99 = WMO 4677 manned, 100+ = WMO 4680 automatic)
* ``temps_passe_1/2``    BUFR 0 20 004 / 0 20 005 (0-9 = WMO 4561, 10-19 = WMO 4531)
* ``type_nuages_*``      BUFR 0 20 012 (10-19 CH, 20-29 CM, 30-39 CL, 59-62 not visible)
* ``type_nuage_1..4``    BUFR 0 20 012 (0-9 cloud genus)
* ``etat_sol``           BUFR 0 20 062 (0-9 without snow = WMO 0901, 10-19 with snow = WMO 0975)
* ``type_tendance_...``  BUFR 0 10 063 (WMO 0200)
* ``methode_mesure_tw``  WMO 3855

Values are in French, the language of the data, as in ``fr_insee_sirene``.
Every key observed in the 1996-2026 archive is covered, so the dictionary-coverage
test passes without exceptions.
"""

import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

OUT = (
    Path(
        os.path.expanduser(
            os.environ.get(
                "MF_OUTPUT", "~/Downloads/fr_meteofrance_data/output"
            )
        )
    ).expanduser()
    / "dicionario"
)

rows = []


def add(tabela, colunas, pairs):
    for coluna in colunas if isinstance(colunas, (list, tuple)) else [colunas]:
        for chave, valor in pairs:
            rows.append((tabela, coluna, str(chave), "", valor))


# --- BUFR 0 10 063 — caractéristique de la tendance barométrique -------------
TENDANCE = [
    (
        "0",
        "En hausse, puis en baisse ; pression égale ou supérieure à celle d'il y a 3 heures",
    ),
    ("1", "En hausse, puis stationnaire, ou en hausse plus lente"),
    ("2", "En hausse régulière ou irrégulière"),
    (
        "3",
        "En baisse ou stationnaire, puis en hausse ; ou en hausse plus rapide",
    ),
    ("4", "Stationnaire ; pression égale à celle d'il y a 3 heures"),
    (
        "5",
        "En baisse, puis en hausse ; pression égale ou inférieure à celle d'il y a 3 heures",
    ),
    ("6", "En baisse, puis stationnaire, ou en baisse plus lente"),
    ("7", "En baisse régulière ou irrégulière"),
    (
        "8",
        "Stationnaire ou en hausse, puis en baisse ; ou en baisse plus rapide",
    ),
    ("10", "Valeur non définie dans la table de code"),
]

# --- BUFR 0 20 003 — temps présent, 0-99 (table OMM 4677) --------------------
WW_MANNED = {
    0: "Évolution des nuages non observée ou non observable",
    1: "Nuages se dissolvant ou devenant moins denses",
    2: "État du ciel dans l'ensemble sans changement",
    3: "Nuages se formant ou se développant",
    4: "Visibilité réduite par de la fumée",
    5: "Brume sèche",
    6: "Poussière en suspension dans l'air, non soulevée par le vent",
    7: "Poussière ou sable soulevé par le vent",
    8: "Tourbillon de poussière ou de sable bien développé",
    9: "Tempête de poussière ou de sable en vue ou à la station durant l'heure précédente",
    10: "Brume",
    11: "Bancs de brouillard peu épais au niveau du sol",
    12: "Brouillard mince plus ou moins continu au niveau du sol",
    13: "Éclairs visibles, tonnerre non entendu",
    14: "Précipitations en vue, n'atteignant pas le sol",
    15: "Précipitations en vue, atteignant le sol, loin de la station",
    16: "Précipitations en vue, atteignant le sol, près de la station",
    17: "Orage sans précipitations à la station",
    18: "Grains à la station ou en vue durant l'heure précédente",
    19: "Trombe à la station ou en vue durant l'heure précédente",
    20: "Bruine ou neige en grains durant l'heure précédente, non au moment de l'observation",
    21: "Pluie durant l'heure précédente, non au moment de l'observation",
    22: "Neige durant l'heure précédente, non au moment de l'observation",
    23: "Pluie et neige mêlées ou granules de glace durant l'heure précédente",
    24: "Bruine ou pluie se congelant durant l'heure précédente, non au moment de l'observation",
    25: "Averses de pluie durant l'heure précédente, non au moment de l'observation",
    26: "Averses de neige durant l'heure précédente, non au moment de l'observation",
    27: "Averses de grêle durant l'heure précédente, non au moment de l'observation",
    28: "Brouillard durant l'heure précédente, non au moment de l'observation",
    29: "Orage durant l'heure précédente, non au moment de l'observation",
    30: "Tempête de poussière ou de sable faible ou modérée, ayant diminué",
    31: "Tempête de poussière ou de sable faible ou modérée, sans changement",
    32: "Tempête de poussière ou de sable faible ou modérée, ayant commencé ou augmenté",
    33: "Forte tempête de poussière ou de sable, ayant diminué",
    34: "Forte tempête de poussière ou de sable, sans changement",
    35: "Forte tempête de poussière ou de sable, ayant commencé ou augmenté",
    36: "Chasse-neige faible ou modérée, basse",
    37: "Forte chasse-neige, basse",
    38: "Chasse-neige faible ou modérée, élevée",
    39: "Forte chasse-neige, élevée",
    40: "Brouillard à distance, non à la station, durant l'heure précédente",
    41: "Brouillard en bancs",
    42: "Brouillard, ciel visible, ayant diminué durant l'heure précédente",
    43: "Brouillard, ciel invisible, ayant diminué durant l'heure précédente",
    44: "Brouillard, ciel visible, sans changement durant l'heure précédente",
    45: "Brouillard, ciel invisible, sans changement durant l'heure précédente",
    46: "Brouillard, ciel visible, ayant commencé ou augmenté durant l'heure précédente",
    47: "Brouillard, ciel invisible, ayant commencé ou augmenté durant l'heure précédente",
    48: "Brouillard givrant, ciel visible",
    49: "Brouillard givrant, ciel invisible",
    50: "Bruine intermittente faible au moment de l'observation",
    51: "Bruine continue faible au moment de l'observation",
    52: "Bruine intermittente modérée au moment de l'observation",
    53: "Bruine continue modérée au moment de l'observation",
    54: "Bruine intermittente forte au moment de l'observation",
    55: "Bruine continue forte au moment de l'observation",
    56: "Bruine se congelant, faible",
    57: "Bruine se congelant, modérée ou forte",
    58: "Bruine et pluie, faibles",
    59: "Bruine et pluie, modérées ou fortes",
    60: "Pluie intermittente faible au moment de l'observation",
    61: "Pluie continue faible au moment de l'observation",
    62: "Pluie intermittente modérée au moment de l'observation",
    63: "Pluie continue modérée au moment de l'observation",
    64: "Pluie intermittente forte au moment de l'observation",
    65: "Pluie continue forte au moment de l'observation",
    66: "Pluie se congelant, faible",
    67: "Pluie se congelant, modérée ou forte",
    68: "Pluie ou bruine et neige mêlées, faibles",
    69: "Pluie ou bruine et neige mêlées, modérées ou fortes",
    70: "Chute intermittente de flocons de neige, faible",
    71: "Chute continue de flocons de neige, faible",
    72: "Chute intermittente de flocons de neige, modérée",
    73: "Chute continue de flocons de neige, modérée",
    74: "Chute intermittente de flocons de neige, forte",
    75: "Chute continue de flocons de neige, forte",
    76: "Poudrin de glace, avec ou sans brouillard",
    77: "Neige en grains, avec ou sans brouillard",
    78: "Étoiles de neige isolées, avec ou sans brouillard",
    79: "Granules de glace",
    80: "Averse de pluie faible",
    81: "Averse de pluie modérée ou forte",
    82: "Averse de pluie violente",
    83: "Averse de pluie et neige mêlées, faible",
    84: "Averse de pluie et neige mêlées, modérée ou forte",
    85: "Averse de neige faible",
    86: "Averse de neige modérée ou forte",
    87: "Averse de grésil ou de neige roulée, faible",
    88: "Averse de grésil ou de neige roulée, modérée ou forte",
    89: "Averse de grêle sans orage, faible",
    90: "Averse de grêle sans orage, modérée ou forte",
    91: "Pluie faible, orage durant l'heure précédente",
    92: "Pluie modérée ou forte, orage durant l'heure précédente",
    93: "Neige ou grêle faible, orage durant l'heure précédente",
    94: "Neige ou grêle modérée ou forte, orage durant l'heure précédente",
    95: "Orage faible ou modéré, sans grêle, avec pluie ou neige",
    96: "Orage faible ou modéré, avec grêle",
    97: "Orage fort, sans grêle, avec pluie ou neige",
    98: "Orage avec tempête de poussière ou de sable",
    99: "Orage fort, avec grêle",
}
# BUFR 0 20 003 — codes automatiques observés dans l'archive (table OMM 4680 + 100)
WW_AUTOMATIC = {
    121: "Précipitations durant l'heure précédente, non au moment de l'observation",
    141: "Précipitations faibles ou modérées",
    142: "Précipitations fortes",
}

# --- BUFR 0 20 004 / 0 20 005 — temps passé ---------------------------------
W_MANNED = {
    0: "Nuages couvrant au plus la moitié du ciel",
    1: "Nuages couvrant plus de la moitié du ciel pendant une partie de la période",
    2: "Nuages couvrant plus de la moitié du ciel pendant toute la période",
    3: "Tempête de sable, tempête de poussière ou chasse-neige",
    4: "Brouillard, brouillard givrant ou brume épaisse",
    5: "Bruine",
    6: "Pluie",
    7: "Neige, ou pluie et neige mêlées",
    8: "Averses",
    9: "Orage, avec ou sans précipitations",
}
W_AUTOMATIC = {
    10: "Aucun phénomène significatif observé",
    11: "Visibilité réduite",
    12: "Phénomène de chasse, visibilité réduite",
    13: "Brouillard",
    14: "Précipitations",
    15: "Bruine",
    16: "Pluie",
    17: "Neige ou granules de glace",
    18: "Averses ou précipitations intermittentes",
    19: "Orage",
}

# --- BUFR 0 20 012 — type de nuage ------------------------------------------
GENUS = {
    0: "Cirrus (Ci)",
    1: "Cirrocumulus (Cc)",
    2: "Cirrostratus (Cs)",
    3: "Altocumulus (Ac)",
    4: "Altostratus (As)",
    5: "Nimbostratus (Ns)",
    6: "Stratocumulus (Sc)",
    7: "Stratus (St)",
    8: "Cumulus (Cu)",
    9: "Cumulonimbus (Cb)",
}
CH_LABELS = {
    0: "Pas de nuages de l'étage supérieur",
    1: "Cirrus en filaments, non envahissants",
    2: "Cirrus denses, en plaques ou en faisceaux enchevêtrés",
    3: "Cirrus denses provenant souvent d'un cumulonimbus",
    4: "Cirrus en crochets ou en filaments, envahissant le ciel",
    5: "Cirrus et cirrostratus envahissants, à moins de 45° au-dessus de l'horizon",
    6: "Cirrus et cirrostratus envahissants, à plus de 45° au-dessus de l'horizon",
    7: "Voile de cirrostratus couvrant la totalité du ciel",
    8: "Cirrostratus non envahissant et ne couvrant pas la totalité du ciel",
    9: "Cirrocumulus prédominants",
}
CM_LABELS = {
    0: "Pas de nuages de l'étage moyen",
    1: "Altostratus translucide",
    2: "Altostratus opaque ou nimbostratus",
    3: "Altocumulus translucide à un seul niveau",
    4: "Altocumulus en bancs lenticulaires, changeant continuellement",
    5: "Altocumulus en bandes envahissant le ciel",
    6: "Altocumulus résultant de l'étalement de cumulus",
    7: "Altocumulus à plusieurs niveaux, ou avec altostratus ou nimbostratus",
    8: "Altocumulus en forme de petites tours ou de flocons",
    9: "Altocumulus d'un ciel chaotique, à plusieurs niveaux",
}
CL_LABELS = {
    0: "Pas de nuages de l'étage inférieur",
    1: "Cumulus humilis ou fractus, sans développement vertical",
    2: "Cumulus mediocris ou congestus, à fort développement vertical",
    3: "Cumulonimbus calvus, sans enclume ni sommet fibreux",
    4: "Stratocumulus résultant de l'étalement de cumulus",
    5: "Stratocumulus non issus de l'étalement de cumulus",
    6: "Stratus nebulosus ou fractus, en nappe plus ou moins continue",
    7: "Stratus fractus ou cumulus fractus de mauvais temps",
    8: "Cumulus et stratocumulus à bases situées à des niveaux différents",
    9: "Cumulonimbus capillatus, avec enclume ou sommet fibreux",
}
NOT_VISIBLE = {
    59: "Nuages invisibles en raison de l'obscurité, du brouillard ou d'un phénomène analogue",
    60: "Nuages de l'étage supérieur invisibles en raison de l'obscurité, du brouillard ou d'une couche de nuages plus bas",
    61: "Nuages de l'étage moyen invisibles en raison de l'obscurité, du brouillard ou d'une couche de nuages plus bas",
    62: "Nuages de l'étage inférieur invisibles en raison de l'obscurité, du brouillard ou d'un phénomène analogue",
}

# --- BUFR 0 20 062 — état du sol --------------------------------------------
ETAT_SOL = {
    0: "Surface du sol sèche, sans fissures ni poussière meuble",
    1: "Surface du sol humide",
    2: "Surface du sol mouillée, eau stagnante en petites ou grandes flaques",
    3: "Sol inondé",
    4: "Surface du sol gelée",
    5: "Verglas au sol",
    6: "Poussière ou sable meuble sec ne couvrant pas entièrement le sol",
    7: "Couche mince de poussière ou de sable meuble couvrant entièrement le sol",
    8: "Couche épaisse de poussière ou de sable meuble couvrant entièrement le sol",
    9: "Sol extrêmement sec, avec fissures",
    10: "Sol couvert de glace en majeure partie",
    11: "Neige compacte ou mouillée couvrant moins de la moitié du sol",
    12: "Neige compacte ou mouillée couvrant au moins la moitié du sol, sans le couvrir entièrement",
    13: "Couche uniforme de neige compacte ou mouillée couvrant entièrement le sol",
    14: "Couche non uniforme de neige compacte ou mouillée couvrant entièrement le sol",
    15: "Neige sèche poudreuse couvrant moins de la moitié du sol",
    16: "Neige sèche poudreuse couvrant au moins la moitié du sol, sans le couvrir entièrement",
    17: "Couche uniforme de neige sèche poudreuse couvrant entièrement le sol",
    18: "Couche non uniforme de neige sèche poudreuse couvrant entièrement le sol",
    19: "Neige couvrant entièrement le sol, congères élevées",
}

# --- OMM 3855 — méthode de mesure de la température du thermomètre mouillé ---
SW = {
    0: "Température du thermomètre mouillé mesurée, thermomètre ventilé",
    1: "Température du thermomètre mouillé mesurée, thermomètre non ventilé",
    2: "Température du thermomètre mouillé calculée",
}

CL_COLS = ["type_nuages_etage_inferieur"]
CM_COLS = ["type_nuages_etage_moyen"]
CH_COLS = ["type_nuages_etage_superieur"]
CTYPE_COLS = [f"type_nuage_{i}" for i in range(1, 5)]

add("synop", "type_tendance_barometrique", TENDANCE)
add(
    "synop",
    "temps_present",
    [(k, v) for k, v in sorted({**WW_MANNED, **WW_AUTOMATIC}.items())],
)
add(
    "synop",
    ["temps_passe_1", "temps_passe_2"],
    [(k, v) for k, v in sorted({**W_MANNED, **W_AUTOMATIC}.items())],
)
add(
    "synop",
    CH_COLS,
    [(10 + k, v) for k, v in sorted(CH_LABELS.items())]
    + [(60, NOT_VISIBLE[60])],
)
add(
    "synop",
    CM_COLS,
    [(20 + k, v) for k, v in sorted(CM_LABELS.items())]
    + [(61, NOT_VISIBLE[61])],
)
add(
    "synop",
    CL_COLS,
    [(30 + k, v) for k, v in sorted(CL_LABELS.items())]
    + [(62, NOT_VISIBLE[62])],
)
add(
    "synop",
    CTYPE_COLS,
    [(k, v) for k, v in sorted(GENUS.items())] + [(59, NOT_VISIBLE[59])],
)
add("synop", "etat_sol", [(k, v) for k, v in sorted(ETAT_SOL.items())])
add("synop", "methode_mesure_tw", [(k, v) for k, v in sorted(SW.items())])

# --- normale_climatologique -------------------------------------------------
PERIODES = [
    ("01", "Janvier"),
    ("02", "Février"),
    ("03", "Mars"),
    ("04", "Avril"),
    ("05", "Mai"),
    ("06", "Juin"),
    ("07", "Juillet"),
    ("08", "Août"),
    ("09", "Septembre"),
    ("10", "Octobre"),
    ("11", "Novembre"),
    ("12", "Décembre"),
    ("annee", "Année"),
]
UNITES = [
    ("celsius_degree", "Degré Celsius"),
    ("millimeter", "Millimètre"),
    ("meter_per_second", "Mètre par seconde"),
    ("day", "Jour"),
    ("hour", "Heure"),
    ("joule_per_square_centimeter", "Joule par centimètre carré"),
]
add("normale_climatologique", "periode", PERIODES)
add("normale_climatologique", "unite", UNITES)


def add_indicateurs():
    """Indicator labels are carried by the fiches themselves; reuse them verbatim."""
    import parse_ficheclim

    directory = (
        os.path.expanduser(
            os.environ.get("MF_INPUT", "~/Downloads/fr_meteofrance_data/input")
        )
        + "/ficheclim"
    )
    _stations, normals = parse_ficheclim.parse_all(directory)
    labels = {}
    for r in normals:
        labels.setdefault(r["indicateur"], r["libelle_indicateur"])
    add("normale_climatologique", "indicateur", sorted(labels.items()))


if __name__ == "__main__":
    import sys

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    add_indicateurs()

    df = pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )
    dups = df.duplicated(subset=["id_tabela", "nome_coluna", "chave"]).sum()
    assert dups == 0, f"{dups} duplicate dictionary keys"

    OUT.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(c, pa.string()) for c in df.columns])
    pq.write_table(
        pa.Table.from_pandas(df, schema=schema, preserve_index=False),
        OUT / "data.parquet",
        compression="snappy",
    )
    df.to_csv(
        Path(os.path.dirname(os.path.abspath(__file__))) / "dicionario.csv",
        index=False,
    )
    print(f"dicionario rows={len(df)} -> {OUT / 'data.parquet'}")
    print(df.groupby(["id_tabela", "nome_coluna"]).size().to_string())
