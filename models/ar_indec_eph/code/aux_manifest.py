"""Which INDEC document goes where, for the auxiliary-file bundles.

Per .claude/rules/auxiliary-files.md every document is a raw data source, a
per-table auxiliary file, or a link in the bundle README -- never two.

The EPH is unusable without its documentation: the microdata columns are
questionnaire codes (CH04, PP04D_COD, V5_01), the record layout changes wave by
wave, and the occupation, activity and geographic codes resolve only against
separate INDEC classifiers.

Split used here:

  record layouts     one per wave, in BOTH tables' bundles -- each file documents
                     the Hogar and the Personas sections together
  classifiers        individuo only: CNO (occupation), CAES/CIUO (activity) resolve
                     pp04d_cod, pp11d_cod, pp04b_cod, pp11b_cod, all individual
  geographic codes   individuo only: province and country tables resolve ch15_cod
                     and ch16_cod
  methodology        both: concepts, sampling errors, the 2016 revision annex,
                     the 2020 pandemic considerations, the questionnaire innovations
  dicionario         no bundle -- a derived table with nothing table-specific

Four record layouts are listed on datos.gob.ar under URLs that return INDEC's
HTML error page; working spellings were found by probing and are substituted here.
"""

# Not every document lives under the EPH directory: the 2016 revision annex is
# published under /ftp/cuadros/sociedad/. aux_documents.json therefore stores full
# URLs from the datos.gob.ar catalog, and build_auxiliary_files.py resolves a
# basename against that map rather than assuming the EPH path.

# Catalog URLs that 404 (as a 200 + HTML), mapped to a spelling that resolves.
BROKEN_URL_FIX = {
    "EPH_registro_t219.pdf": "EPH_registro_2t19.pdf",
    "EPH_registro_t319.pdf": "EPH_registro_3t19.pdf",
    "EPH_registro_t419.pdf": "EPH_registro_4t19.pdf",
    "EPH_registro_1t21.pdf": "EPH_registro_1T2021.pdf",
}

# Documents that resolve columns of the individual table only.
INDIVIDUO_ONLY = {
    "EPHcontinua_CNO2001_reducido_09.pdf": (
        "Clasificador Nacional de Ocupaciones (CNO 2001), version reducida. "
        "Resuelve pp04d_cod y pp11d_cod"
    ),
    "EPHcontinua_CAES_Mercosur_09.pdf": (
        "Clasificador de Actividades Economicas para Encuestas Sociodemograficas "
        "(CAES Mercosur). Resuelve pp04b_cod y pp11b_cod"
    ),
    "caes_mercosur_1.0.pdf": "Clasificador CAES Mercosur 1.0. Resuelve pp04b_caes y pp11b_caes",
    "CIUO-08.pdf": "Clasificacion Internacional Uniforme de Ocupaciones (CIUO-08)",
    "codigosprovincias_09.pdf": (
        "Codigos de provincia de INDEC. Resuelve ch15_cod y ch16_cod en las ondas "
        "que usan codificacion numerica"
    ),
    "codigospaises_09.pdf": (
        "Codigos de pais de INDEC. Resuelve ch15_cod y ch16_cod en las ondas que "
        "usan codificacion numerica"
    ),
}

# Methodological documents that belong in both tables' bundles.
SHARED_METHODOLOGY = {
    "EPH_Conceptos.pdf": "Conceptos y definiciones de la EPH",
    "EPH_errores_muestreo.pdf": "Errores de muestreo de la EPH",
    "anexo_informe_eph_23_08_16.pdf": (
        "Documento de revision, evaluacion y recuperacion de la EPH (agosto 2016). "
        "Fundamenta la advertencia oficial de INDEC sobre las series 2007-2015 y la "
        "no publicacion de 2015 Q3 a 2016 Q1"
    ),
    "EPH_consideraciones_metodologicas_2t20.pdf": (
        "Consideraciones metodologicas del segundo trimestre de 2020, cuando la "
        "pandemia obligo a relevar por telefono"
    ),
    "EPH_nota_metodologica_1_trim_2019.pdf": (
        "Nota metodologica del primer trimestre de 2019"
    ),
}


def is_record_layout(basename: str) -> bool:
    lowered = basename.lower()
    return lowered.startswith(
        ("eph_registro", "eph_disenoreg", "eph_diseno_reg")
    ) or (lowered.startswith("eph_estructura_bases"))
