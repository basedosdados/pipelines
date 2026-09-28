"""Source-format constants for br_prf_acidentes.

Every value here was measured from the 51 published archives on 2026-09-28, not
inferred from PRF documentation. See models/br_prf_acidentes/README.md for the
per-year inventory the maps below encode.
"""

from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------- paths

DATA_ROOT = Path(
    os.environ.get(
        "PRF_DATA_ROOT", Path.home() / "Downloads" / "br_prf_acidentes_data"
    )
)
INPUT_DIR = DATA_ROOT / "input"
OUTPUT_DIR = DATA_ROOT / "output"
ARCHITECTURE_DIR = Path(__file__).parent / "architecture"

DATASET_ID = "br_prf_acidentes"
YEARS = range(2007, 2027)

# ---------------------------------------------------------------- source shapes

# shape -> (table_slug, first_year, last_year)
SHAPES = {
    "ocorrencia": ("ocorrencia", 2007, 2026),
    "pessoa": ("pessoa", 2007, 2026),
    "pessoa_todas_causas": ("pessoa_causa_tipo", 2017, 2026),
}

# Google Drive file ids, scraped from the PRF open-data page. Download via
# https://drive.usercontent.google.com/download?id=<id>&export=download&confirm=t
DRIVE_IDS = {
    ("ocorrencia", 2007): "1EFpZF5F6cB0DOHd2Uxnj7X948WE69a8e",
    ("ocorrencia", 2008): "1_OSeHlyKJw8cIhMS_JzSg1RlYX8k6vSG",
    ("ocorrencia", 2009): "1qkVatg0pC_zosuBs0NCSgEXDJvBbnTYC",
    ("ocorrencia", 2010): "1_yU6FRh8M7USjiChQwyF20NtY48GTmEX",
    ("ocorrencia", 2011): "1HHhgLF-kSR6Gde2qOaTXL3T5ieD33hpG",
    ("ocorrencia", 2012): "18Yz2prqKSLthrMmW-73vrOiDmKTCL6xE",
    ("ocorrencia", 2013): "1p_7lw9RzkINfscYAZSmc-Z9Ci4ZPJyEr",
    ("ocorrencia", 2014): "1FpF5wTBsRDkEhLm3z2g8XDiXr9SO9Uk8",
    ("ocorrencia", 2015): "1DyqR5FFcwGsamSag-fGm13feQt0Y-3Da",
    ("ocorrencia", 2016): "16qooQl_ySoW61CrtsBbreBVNPYlEkoYm",
    ("ocorrencia", 2017): "1HPLWt5f_l4RIX3tKjI4tUXyZOev52W0N",
    ("ocorrencia", 2018): "1cM4IgGMIiR-u4gBIH5IEe3DcvBvUzedi",
    ("ocorrencia", 2019): "1pN3fn2wY34GH6cY-gKfbxRJJBFE0lb_l",
    ("ocorrencia", 2020): "1esu6IiH5TVTxFoedv6DBGDd01Gvi8785",
    ("ocorrencia", 2021): "12xH8LX9aN2gObR766YN3cMcuycwyCJDz",
    ("ocorrencia", 2022): "1PRQjuV5gOn_nn6UNvaJyVURDIfbSAK4-",
    ("ocorrencia", 2023): "1-WO3SfNrwwZ5_l7fRTiwBKRw7mi1-HUq",
    ("ocorrencia", 2024): "14lB0vqMFkaZj8HZ44b0njYgxs9nAN8KO",
    ("ocorrencia", 2025): "1-G3MdmHBt6CprDwcW99xxC4BZ2DU5ryR",
    ("ocorrencia", 2026): "1A3IirNm0AzRaSosA1IS94DOVmvKsn0Ol",
    ("pessoa", 2007): "1v2tz5wJYFlPBb-8ZIf-pchbSO-JT4YZG",
    ("pessoa", 2008): "1gabSVTay5faBo4r9VNksgxhKXuuxu71U",
    ("pessoa", 2009): "1gysMdwpeT3h4CUOGDkQMvFJzPp1Fn-bg",
    ("pessoa", 2010): "1Zg9SdeQa72Gu49ZhJosBuM7TkJcEYiaI",
    ("pessoa", 2011): "1XTcOujBbP_8AEYYrOfjxGxzqHcQYLNy2",
    ("pessoa", 2012): "1FYqtz2_QMvk0eOMhs9TOQHrmF4uOvv_W",
    ("pessoa", 2013): "1-zd3x9aTHD5cQH7SWGMDOQ-4-DuxVV1T",
    ("pessoa", 2014): "1qmCExfnS3nTYIZhDdJblJnn0jlTLWR3K",
    ("pessoa", 2015): "1oisqFB_DD3jAChYxBmiyQVR9WrGOwvQt",
    ("pessoa", 2016): "1zaBGNLk50krRVOxZmB-kfJl6zSOTdmMP",
    ("pessoa", 2017): "1mw2CerP_ZEZwREy4WYM_Kxd4rjUHQjbk",
    ("pessoa", 2018): "1Ja1dN8R_-Dw7sjQVo63UQHicO3jfyzIS",
    ("pessoa", 2019): "14r_9zudjjndgcnjRddzD5lvn9MX5VvZZ",
    ("pessoa", 2020): "1iQvs2D9a2XO9vIukrjEIIhtjzkNkgD7Z",
    ("pessoa", 2021): "15W_l07-6taOh8Ycn8uk2Er9PnSj0vq3f",
    ("pessoa", 2022): "1sd8YIqIHapYaxQeaa-6yeWMdVLTl6hgD",
    ("pessoa", 2023): "1-Yk6TV00CH3PixTkKmkoUJQsNiUc5xLm",
    ("pessoa", 2024): "14lVfqdoE2gxDliaKZu7K9Mx6847maPtl",
    ("pessoa", 2025): "1-Gp9S-ALO0D1nT8S_OKoC8xlW7BY8F82",
    ("pessoa", 2026): "1B_rvx1kPHuFowP5rs84ZO-57gN8yUs85",
    ("pessoa_todas_causas", 2017): "1Kv5mNgZvxtl0xwqsmDrxcaLY2KELxR-3",
    ("pessoa_todas_causas", 2018): "1J-012nSnIafOASNFvIYY_vDKKpM51w5_",
    ("pessoa_todas_causas", 2019): "1DAJYKVfkTcPhQodSmHp9rsG1Q8XJW-m3",
    ("pessoa_todas_causas", 2020): "1yQtVOsAlupPHQVVTmbJo0NR3XMzgHANO",
    ("pessoa_todas_causas", 2021): "1Gk3U6cMOZIevsDZHLi6J503xoCRS_lnI",
    ("pessoa_todas_causas", 2022): "1wskEgRC3ame7rncSDQ7qWhKsoKw1lohY",
    ("pessoa_todas_causas", 2023): "1-caam_dahYOf2eorq4mez04Om6DD5d_3",
    ("pessoa_todas_causas", 2024): "14qBOhrE1gioVtuXgxkCJ9kCA8YtUGXKA",
    ("pessoa_todas_causas", 2025): "1-PJGRbfSe7PVjU37A3wTCls_NRXyVGRD",
    ("pessoa_todas_causas", 2026): "1EsGox0UnBWSaM6mrkNSUlYUOecubLCsh",
}

# ---------------------------------------------------------------- per-file parse map

# Every one of the 51 files is latin-1; none is valid UTF-8 and none carries a BOM.
ENCODING = "latin-1"

# CSV field separator. `ocorrencia` is always ';'; `pessoa` switches at 2016.
SEPARATOR = {
    **{("ocorrencia", y): ";" for y in YEARS},
    **{("pessoa", y): "," for y in range(2007, 2016)},
    **{("pessoa", y): ";" for y in range(2016, 2027)},
    **{("pessoa_todas_causas", y): ";" for y in range(2017, 2027)},
}

# `data_inversa` format, keyed on (shape, year) because the two shapes DIVERGE
# for 2012-2015: ocorrencia is already ISO while pessoa is still DD/MM/YYYY.
# Each file is internally homogeneous -- 100% of its rows use one format.
DATE_FORMAT = {
    **{("ocorrencia", y): "%d/%m/%Y" for y in range(2007, 2012)},
    **{("ocorrencia", y): "%Y-%m-%d" for y in range(2012, 2016)},
    ("ocorrencia", 2016): "%d/%m/%y",
    **{("ocorrencia", y): "%Y-%m-%d" for y in range(2017, 2027)},
    **{("pessoa", y): "%d/%m/%Y" for y in range(2007, 2016)},
    ("pessoa", 2016): "%d/%m/%y",
    **{("pessoa", y): "%Y-%m-%d" for y in range(2017, 2027)},
    **{("pessoa_todas_causas", y): "%Y-%m-%d" for y in range(2017, 2027)},
}

# Decimal separator for km / latitude / longitude: '.' through 2015, ',' from 2016.
DECIMAL_COMMA_FROM = 2016

# Columns absent before the year given, so the cleaner emits NULL for earlier years.
COLUMN_FIRST_YEAR = {
    "latitude": 2017,
    "longitude": 2017,
    "regional": 2017,
    "delegacia": 2017,
    "uop": 2017,
}
# Columns dropped from the source in the year given (present only before it).
COLUMN_LAST_YEAR = {
    "nacionalidade": 2016,
    "naturalidade": 2016,
}

# Every token that means "no value" in any column of any year. The literal
# string '(null)' (2007-2015), the empty string (2016) and 'NA' (2017+) are all
# used, sometimes in the same column.
NULL_TOKENS = {"", "(null)", "NA", "na", "N/A", "null", "NULL", "nan"}

# ---------------------------------------------------------------- age

# PRF's published dictionary says -1 marks "could not collect". The data uses
# three different markers over time, and the 2017+ files changed to 0 without
# the dictionary being updated:
#   -1  2007-2015 (plus 1 stray row in 2017)
#   NA  2015-2016 in `pessoa`; 2017-2026 in `pessoa_causa_tipo`
#   0   2017-2026 in `pessoa` only
# Cross-checking `pessoa` against `pessoa_causa_tipo` on (id, pesid, id_veiculo)
# proves `pessoa.idade == 0` is the missing marker from 2017: for 2024, 13,503
# matched rows are 'NA' in the sister file against only 158 genuine zeros.
AGE_MISSING_TOKENS = {"-1", "NA"}
# `pessoa` only, and only from this year: 0 means missing, not "under one year".
AGE_ZERO_IS_MISSING = {"pessoa": 2017}
# Ages outside this range are data-entry corruption -- the source contains birth
# years (1916, 2023) and mangled numbers (913, 524) typed into the age field.
AGE_VALID_RANGE = (0, 120)

# `ano_fabricacao_veiculo` uses 0 and 1900 as missing markers.
VEHICLE_YEAR_MISSING = {"0", "1900"}
VEHICLE_YEAR_VALID_RANGE = (1900, 2027)

# ---------------------------------------------------------------- coordinates

# Brazil's bounding box, wide enough to include Fernando de Noronha (-3.8, -32.4)
# and the Trindade group. Used only to FLAG implausible points; raw values are kept.
BRAZIL_BBOX = {
    "lat_min": -34.0,
    "lat_max": 5.4,
    "lon_min": -74.1,
    "lon_max": -28.8,
}

# ---------------------------------------------------------------- harmonization

# Categorical vocabularies changed when PRF replaced the BR-Brasil system with
# the BAT system in 2017. Two classes of change, handled differently:
#
#   Class 1 -- the same concept relabelled (case, accent, hyphen, plural, or an
#   officially documented equivalence). These ARE collapsed, so a user can group
#   across all 20 years. Case/accent/punctuation variants collapse automatically
#   (see utils.build_case_variant_map); the semantic equivalences below are
#   listed explicitly because no string rule would find them.
#
#   Class 2 -- a genuine taxonomy change, where a category was split, merged or
#   replaced. These are NOT collapsed: doing so would invent or destroy
#   information. The published label is kept and the break is documented.
#   Affected: causa_acidente (11 categories through 2016 replaced by 90 from
#   2017, only 2 shared), tipo_acidente (Colisao lateral split into mesmo/oposto
#   sentido; objeto fixo/movel recoded twice), tipo_veiculo (Carroca + Charrete
#   merged into Carroca-charrete), tipo_envolvido and classificacao_acidente
#   (categories retired). causa_acidente and tipo_acidente are NOT comparable
#   across the 2016/2017 boundary.
#
# Canonical spelling follows the 2017+ vocabulary, which is the current one.
HARMONIZE = {
    "uso_solo": {
        # PRF dictionary, 2017+: "Urbano=Sim;Rural=Nao".
        "Sim": "Urbano",
        "Não": "Rural",
    },
    "sexo": {
        # 2016 alone uses single-letter codes. PRF dictionary: the value
        # "invalido" means the information could not be collected.
        "M": "Masculino",
        "F": "Feminino",
        "I": "Ignorado",
        "Inválido": "Ignorado",
    },
    "estado_fisico": {
        "Morto": "Óbito",
        "Ferido Grave": "Lesões Graves",
        "Ferido Leve": "Lesões Leves",
        "Ignorado": "Não Informado",
    },
    "dia_semana": {
        "Domingo": "domingo",
        "Segunda": "segunda-feira",
        "Terça": "terça-feira",
        "Quarta": "quarta-feira",
        "Quinta": "quinta-feira",
        "Sexta": "sexta-feira",
        "Sábado": "sábado",
    },
    "condicao_metereologica": {
        "Ignorada": "Ignorado",
    },
    "tipo_veiculo": {
        "Motocicletas": "Motocicleta",
        "Trator de esteiras": "Trator de esteira",
        "Bonde / Trem": "Trem-bonde",
    },
}

# Columns whose case/accent/punctuation variants collapse to the most recent
# spelling. Applied before HARMONIZE.
CASE_VARIANT_COLUMNS = [
    "dia_semana",
    "classificacao_acidente",
    "fase_dia",
    "sentido_via",
    "condicao_metereologica",
    "tipo_pista",
    "uso_solo",
    "tipo_envolvido",
    "estado_fisico",
    "sexo",
    "tipo_veiculo",
    "tipo_acidente",
    "causa_acidente",
    "causa_principal",
]

# `tracado_via` became multi-valued in 2017 (';'-delimited, 1,413 distinct
# combinations of 13 atoms). The published string is kept as-is; splitting it
# into a separate table is out of scope.
MULTIVALUED_COLUMNS = {"tracado_via"}

# Person-level binary flags. The same names in `ocorrencia` are COUNTS; here
# they are 0/1 indicators of this person's outcome, per PRF's dictionary
# ("Valor binario que identifica se o envolvido foi classificado como ileso").
PERSON_FLAG_COLUMNS = ["ilesos", "feridos_leves", "feridos_graves", "mortos"]

# ---------------------------------------------------------------- municipality

# PRF publishes the municipality NAME, never an IBGE code, in every year
# 2007-2026 (zero numeric values in 7,399,794 rows). Accents are present through
# 2016 and stripped from 2017, so matching must be accent-insensitive.
#
# Resolution chain against basedosdados.br_bd_diretorios_brasil.municipio
# (5,571 rows; no two municipalities in one UF share a normalized name):
#   1. (sigla_uf, name) with accents, case and punctuation stripped   2,456 pairs
#   2. same, additionally deleting spaces -- catches the apostrophe        +25
#      class, where PRF writes SAO MIGUEL DOESTE for Sao Miguel do Oeste
#   3. the override table below, for IBGE renames                         +18
#   4. name alone, where sigla_uf is null and the name is nationally unique +2
# Residual: 6 pairs / 16 rows (0.00022%), left NULL. See UNRESOLVABLE below.
#
# Every code here was verified against the directory, not recalled.
MUNICIPALITY_OVERRIDES = {
    ("SP", "EMBU"): "3515004",  # renamed Embu das Artes, 2011
    ("RJ", "PARATI"): "3303807",  # Paraty
    ("SC", "PICARRAS"): "4212809",  # renamed Balneario Picarras, 2004
    ("SC", "SAO MIGUEL DOESTE"): "4217204",  # Sao Miguel do Oeste
    ("PA", "SANTA IZABEL DO PARA"): "1506500",  # Santa Isabel do Para
    ("PE", "BELEM DE SAO FRANCISCO"): "2601607",  # Belem do Sao Francisco
    ("BA", "MUQUEM DO SAO FRANCISCO"): "2922250",  # Muquem de Sao Francisco
    ("RN", "ASSU"): "2400208",  # Acu
    ("CE", "ITAPAJE"): "2306306",  # Itapage
    ("PA", "ELDORADO DOS CARAJAS"): "1502954",  # Eldorado do Carajas
    ("PB", "SAO BENTO DE POMBAL"): "2513927",  # renamed Sao Bentinho
    ("PB", "SAO DOMINGOS DE POMBAL"): "2513968",  # renamed Sao Domingos
    ("RN", "AUGUSTO SEVERO"): "2401305",  # renamed Campo Grande, 2013
    ("PR", "VILA ALTA"): "4128625",  # renamed Alto Paraiso, 2010
    ("MT", "POXOREU"): "5107008",  # Poxoreo
    ("RO", "VILA NOVA DO MAMORE"): "1100338",  # Nova Mamore
    ("SC", "BARRA DO SUL"): "4202057",  # Balneario Barra do Sul
    ("PB", "SANTAREM"): "2513653",  # renamed Joca Claudino, 2010
}

# Left NULL on purpose. `PB / NOVA SERRANA` is a source error, not a matching
# failure: Nova Serrana is in MG, and the one affected crash (id 250110, 1 row in
# `ocorrencia` and 3 in `pessoa`) is recorded on BR-101 at km 39.4, which is
# consistent with the published PB, not with MG. The UF and the highway agree
# with each other, so the municipality name is the corrupt field; mapping it to
# the MG code would move a Paraiba crash to Minas Gerais.
UNRESOLVABLE_MUNICIPALITIES = {("PB", "NOVA SERRANA")}
