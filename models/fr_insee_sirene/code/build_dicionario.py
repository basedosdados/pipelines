"""Build the fr_insee_sirene `dicionario` table from the enumerated code lists.

Content mirrors the architecture dicionario sheet (163 entries). Output is a typed
all-STRING parquet: ~/Downloads/fr_insee_sirene_data/output/dicionario/data.parquet
(also writes a CSV copy next to this script for reference).
"""

import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

OUT = Path(
    os.path.expanduser("~/Downloads/fr_insee_sirene_data/output/dicionario")
)
OUT.mkdir(parents=True, exist_ok=True)

rows = []  # (id_tabela, nome_coluna, chave, cobertura_temporal, valor)


def add(tabela, coluna, pairs):
    for chave, valor in pairs:
        rows.append((tabela, coluna, chave, "", valor))


STATUT = [
    ("O", "Diffusible (donnée publique)"),
    ("P", "Diffusion partielle"),
    ("N", "Non diffusible"),
]
SEXE = [("M", "Masculin"), ("F", "Féminin")]
TRANCHE = [
    ("NN", "Unité non employeuse"),
    ("0", "0 salarié"),
    ("1", "1 ou 2 salariés"),
    ("2", "3 à 5 salariés"),
    ("3", "6 à 9 salariés"),
    ("11", "10 à 19 salariés"),
    ("12", "20 à 49 salariés"),
    ("21", "50 à 99 salariés"),
    ("22", "100 à 199 salariés"),
    ("31", "200 à 249 salariés"),
    ("32", "250 à 499 salariés"),
    ("41", "500 à 999 salariés"),
    ("42", "1000 à 1999 salariés"),
    ("51", "2000 à 4999 salariés"),
    ("52", "5000 à 9999 salariés"),
    ("53", "10000 salariés et plus"),
]
EMPLOYEUR = [("O", "Employeur"), ("N", "Non employeur")]
OUI_NON = [("O", "Oui"), ("N", "Non")]
CAT_ENTREPRISE = [
    ("PME", "Petite ou moyenne entreprise"),
    ("ETI", "Entreprise de taille intermédiaire"),
    ("GE", "Grande entreprise"),
]
NOMENCLATURE = [
    ("NAFRev2", "NAF révision 2 (2008)"),
    ("NAFRev1", "NAF révision 1 (2003)"),
    ("NAF1993", "NAF 1993"),
    ("NAP", "NAP 1973"),
]
TYPE_VOIE = [
    ("ALL", "Allée"),
    ("AV", "Avenue"),
    ("BD", "Boulevard"),
    ("CAR", "Carrefour"),
    ("CHE", "Chemin"),
    ("CHS", "Chaussée"),
    ("CITE", "Cité"),
    ("COR", "Corniche"),
    ("CRS", "Cours"),
    ("DOM", "Domaine"),
    ("DSC", "Descente"),
    ("ECA", "Écart"),
    ("ESP", "Esplanade"),
    ("FG", "Faubourg"),
    ("GR", "Grande Rue"),
    ("HAM", "Hameau"),
    ("HLE", "Halle"),
    ("IMP", "Impasse"),
    ("LD", "Lieu-dit"),
    ("LOT", "Lotissement"),
    ("MAR", "Marché"),
    ("MTE", "Montée"),
    ("PAS", "Passage"),
    ("PL", "Place"),
    ("PLN", "Plaine"),
    ("PLT", "Plateau"),
    ("PRO", "Promenade"),
    ("PRV", "Parvis"),
    ("QUA", "Quartier"),
    ("QUAI", "Quai"),
    ("RES", "Résidence"),
    ("RLE", "Ruelle"),
    ("ROC", "Rocade"),
    ("RPT", "Rond-point"),
    ("RTE", "Route"),
    ("RUE", "Rue"),
    ("SEN", "Sente - Sentier"),
    ("SQ", "Square"),
    ("TPL", "Terre-plein"),
    ("TRA", "Traverse"),
    ("VLA", "Villa"),
    ("VLGE", "Village"),
]
ETAT_UL = [("A", "Active"), ("C", "Cessée")]
ETAT_ET = [("A", "Actif"), ("F", "Fermé")]

add("unite_legale", "statut_diffusion", STATUT)
add("etablissement", "statut_diffusion", STATUT)
add("unite_legale", "sexe", SEXE)
add("unite_legale", "tranche_effectifs", TRANCHE)
add("etablissement", "tranche_effectifs", TRANCHE)
add("unite_legale", "etat_administratif", ETAT_UL)
add("etablissement", "etat_administratif", ETAT_ET)
add("unite_legale_historico", "etat_administratif", ETAT_UL)
add("etablissement_historico", "etat_administratif", ETAT_ET)
add("unite_legale", "caractere_employeur", EMPLOYEUR)
add("etablissement", "caractere_employeur", EMPLOYEUR)
add("unite_legale_historico", "caractere_employeur", EMPLOYEUR)
add("etablissement_historico", "caractere_employeur", EMPLOYEUR)
add("unite_legale", "categorie_entreprise", CAT_ENTREPRISE)
add("unite_legale", "economie_sociale_solidaire", OUI_NON)
add("unite_legale_historico", "economie_sociale_solidaire", OUI_NON)
add("unite_legale", "nomenclature_activite_principale", NOMENCLATURE)
add("etablissement", "nomenclature_activite_principale", NOMENCLATURE)
add("unite_legale_historico", "nomenclature_activite_principale", NOMENCLATURE)
add(
    "etablissement_historico", "nomenclature_activite_principale", NOMENCLATURE
)
add("etablissement", "type_voie", TYPE_VOIE)
add("etablissement", "type_voie_2", TYPE_VOIE)

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
assert len(df) == 163, f"expected 163 dicionario rows, got {len(df)}"

schema = pa.schema([(c, pa.string()) for c in df.columns])
table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
pq.write_table(table, OUT / "data.parquet", compression="snappy")
df.to_csv(Path(__file__).parent / "dicionario.csv", index=False)
print(f"wrote {len(df)} rows -> {OUT / 'data.parquet'}")
print(df.groupby("nome_coluna").size().to_string())
