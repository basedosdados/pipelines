"""The one cube that makes up world_oecd_socx, and how it is assembled.

SOCX Aggregated is a single SDMX data structure, ``DSD_SOCX_AGG``, exposed by the
full-database dataflow ``DF_SOCX_AGG``. The other dataflows the OECD publishes in
the same agency (``DF_PUB_OLD``, ``DF_PUB_FAM``, ``DF_PUB_DIS_SIC``,
``DF_PUB_PRV``, ``DF_NET_GDP``) are pre-filtered views over this cube, so they are
documentation rather than data.

Unlike the Education at a Glance cubes, ``DF_SOCX_AGG`` carries a real
``TIME_PERIOD`` dimension and a single live version (1.0): it is a proper annual
time series, not a set of vintage snapshots keyed on ``REF_PERIOD``. So no flow
versions are stacked. The vintage-as-version trap (an OECD dataflow republishing a
period under a new version) does not bite here, but ``source_flow_version`` is
still recorded so a future revision that bumps the version is visible in the data.

The dict shape mirrors ``world_oecd_education/code/tables.py`` so the generic
scripts (clean, gen_architecture, gen_dbt, gen_dicionario, verify, upload) run
unchanged, with one addition: ``agency``, because SOCX lives under a different
SDMX agency (``OECD.ELS.SPD``) than the education cubes.
"""

# slug -> cube definition
#   agency         SDMX agency the flow and DSD belong to
#   dsd            SDMX data structure definition id
#   dsd_version    the version whose dimension signature the table follows
#   flow           dataflow id (without the "DSD@" prefix) carrying the full cube
#   versions       flow versions to union, newest last (SOCX: just the one)
TABLES = {
    "expenditure": dict(
        agency="OECD.ELS.SPD",
        dsd="DSD_SOCX_AGG",
        dsd_version="1.0",
        flow="DSD_SOCX_AGG@DF_SOCX_AGG",
        versions=["1.0"],
        name_pt="Gasto social",
        name_en="Social expenditure",
        name_es="Gasto social",
        description_pt=(
            "Gasto social público e privado por área de política (velhice, sobreviventes, "
            "incapacidade, saúde, família, programas de mercado de trabalho, desemprego, "
            "habitação e outras), por origem do financiamento (público, privado obrigatório e "
            "privado voluntário) e natureza da prestação (em dinheiro ou em espécie), expresso "
            "como proporção do PIB, do gasto público total, em dólares por habitante em PPC e em "
            "moeda nacional. Base agregada do OECD Social Expenditure Database (SOCX)."
        ),
        description_en=(
            "Public and private social expenditure by policy area (old age, survivors, "
            "incapacity, health, family, active labour market programmes, unemployment, housing "
            "and other), by source of funding (public, mandatory private and voluntary private) "
            "and nature of the benefit (cash or in kind), expressed as a share of GDP, of total "
            "government expenditure, in US dollars per head at PPP and in national currency. "
            "Aggregated data of the OECD Social Expenditure Database (SOCX)."
        ),
        description_es=(
            "Gasto social público y privado por área de política (vejez, sobrevivientes, "
            "incapacidad, salud, familia, programas del mercado laboral, desempleo, vivienda y "
            "otras), por origen del financiamiento (público, privado obligatorio y privado "
            "voluntario) y naturaleza de la prestación (en efectivo o en especie), expresado como "
            "proporción del PIB, del gasto público total, en dólares por habitante en PPA y en "
            "moneda nacional. Base agregada del OECD Social Expenditure Database (SOCX)."
        ),
    ),
}
