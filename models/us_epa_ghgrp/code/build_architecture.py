"""Write the us_epa_ghgrp architecture CSVs and the trilingual columns JSON.

One CSV per table, in `architecture/`. The architecture is the single source of
truth for column order, BigQuery type and description; the cleaning transform
(`pipelines/datasets/us_epa_ghgrp/utils.py`), the dbt models and the metadata
step all read it back from here. `columns.json` carries the same columns with
PT/EN/ES descriptions for `bulk_upsert_columns`.

Run:  uv run python models/us_epa_ghgrp/code/build_architecture.py
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

ARCH_DIR = Path(__file__).parent / "architecture"

HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
    "description_en",
    "description_es",
]

DIR_YEAR = "br_bd_diretorios_data_tempo.ano:ano"
DIR_STATE = "br_bd_diretorios_us.state:id_state"
DIR_COUNTY = "br_bd_diretorios_us.county:id_county"
DIR_NAICS = "br_bd_diretorios_us.naics_2017:id_naics"

PARTITION = {"facility", "emission_subpart", "emission_sector"}


@dataclass
class Col:
    name: str
    bigquery_type: str
    pt: str
    en: str
    es: str
    dictionary: str = "no"
    directory: str = ""
    unit: str = ""
    observations: str = ""
    original_name: str = ""


def year() -> Col:
    return Col(
        "year",
        "INT64",
        "Ano de referência do relato",
        "Reporting year",
        "Año de referencia del reporte",
        directory=DIR_YEAR,
        unit="year",
        observations="Coluna de particionamento",
        original_name="year",
    )


def facility_id() -> Col:
    return Col(
        "facility_id",
        "STRING",
        "Identificador da instalação no GHGRP, estável entre anos mesmo quando o nome ou o endereço mudam",
        "GHGRP facility identifier, stable across years even when the name or address changes",
        "Identificador de la instalación en el GHGRP, estable entre años aunque cambien el nombre o la dirección",
        original_name="facility_id",
    )


def gas() -> Col:
    return Col(
        "gas",
        "STRING",
        "Código do gás de efeito estufa (CO2, CH4, N2O, SF6, NF3, HFC, PFC, HFE, BIOCO2, entre outros)",
        "Greenhouse gas code (CO2, CH4, N2O, SF6, NF3, HFC, PFC, HFE, BIOCO2, among others)",
        "Código del gas de efecto invernadero (CO2, CH4, N2O, SF6, NF3, HFC, PFC, HFE, BIOCO2, entre otros)",
        dictionary="yes",
        observations=(
            "BIOCO2 é o CO2 biogênico, reportado separadamente e não incluído nos totais "
            "da instalação publicados pela EPA"
        ),
        original_name="gas_id",
    )


def emission() -> Col:
    return Col(
        "emission",
        "FLOAT64",
        "Emissão reportada, em toneladas métricas de CO2 equivalente",
        "Reported emissions, in metric tons of CO2 equivalent",
        "Emisión reportada, en toneladas métricas de CO2 equivalente",
        unit="tonne_co2e",
        observations=(
            "Potenciais de aquecimento global (GWP) do IPCC AR4 a partir do ano de relato 2013; "
            "a EPA recalculou os anos anteriores com os mesmos GWP"
        ),
        original_name="co2e_emission",
    )


FACILITY = [
    year(),
    facility_id(),
    Col(
        "frs_id",
        "STRING",
        "Identificador da instalação no Facility Registry Service (FRS) da EPA",
        "EPA Facility Registry Service (FRS) identifier of the facility",
        "Identificador de la instalación en el Facility Registry Service (FRS) de la EPA",
        original_name="frs_id",
    ),
    Col(
        "state_id",
        "STRING",
        "Código FIPS de dois dígitos do estado da instalação",
        "Two-digit FIPS code of the facility's state",
        "Código FIPS de dos dígitos del estado de la instalación",
        directory=DIR_STATE,
        observations="Derivado da sigla do estado reportada (a fonte não publica o código FIPS do estado)",
        original_name="state",
    ),
    Col(
        "county_id",
        "STRING",
        "Código FIPS de cinco dígitos do condado da instalação",
        "Five-digit FIPS code of the facility's county",
        "Código FIPS de cinco dígitos del condado de la instalación",
        directory=DIR_COUNTY,
        observations=(
            "Nulo para reportantes de área ampla (bacias de petróleo e gás, gasodutos, "
            "distribuidoras). Condados legados de Connecticut e códigos extintos do Alasca "
            "não constam no diretório. Em cerca de 0,5% das linhas o condado está em outro "
            "estado que não o reportado (endereço corporativo); ambos são mantidos como reportados"
        ),
        original_name="county_fips",
    ),
    Col(
        "naics_id",
        "STRING",
        "Código NAICS primário de seis dígitos da instalação, conforme reportado",
        "Primary six-digit NAICS code of the facility, as reported",
        "Código NAICS primario de seis dígitos de la instalación, según lo reportado",
        directory=DIR_NAICS,
        observations=(
            "A edição do NAICS varia com o ano e o reportante: até 2017 os códigos seguem "
            "majoritariamente o NAICS 2007; de 2018 em diante, o NAICS 2017 ou 2022"
        ),
        original_name="naics_code",
    ),
    Col(
        "facility_name",
        "STRING",
        "Nome da instalação",
        "Facility name",
        "Nombre de la instalación",
        original_name="facility_name",
    ),
    Col(
        "parent_company",
        "STRING",
        "Empresa(s) controladora(s) reportada(s), com a participação acionária entre parênteses",
        "Reported parent company or companies, with the ownership share in parentheses",
        "Empresa(s) matriz reportada(s), con la participación accionaria entre paréntesis",
        original_name="parent_company",
    ),
    Col(
        "facility_type",
        "STRING",
        "Tipos de reportante da instalação, separados por vírgula: Direct Emitter, Supplier, CO2 Injection, Onshore Oil & Gas Production, Onshore Gathering & Boosting, Transmission Pipelines, LDC - Direct Emissions ou SF6 from Elec. Equip.",
        "Comma-separated reporter types of the facility: Direct Emitter, Supplier, CO2 Injection, Onshore Oil & Gas Production, Onshore Gathering & Boosting, Transmission Pipelines, LDC - Direct Emissions or SF6 from Elec. Equip.",
        "Tipos de reportante de la instalación, separados por coma: Direct Emitter, Supplier, CO2 Injection, Onshore Oil & Gas Production, Onshore Gathering & Boosting, Transmission Pipelines, LDC - Direct Emissions o SF6 from Elec. Equip.",
        observations="Nulo quando a instalação não reportou no ano (ver reporting_status)",
        original_name="facility_types",
    ),
    Col(
        "industry_type",
        "STRING",
        "Códigos dos subpartes e tipos de indústria sob os quais a instalação reportou, separados por vírgula (ex.: C,MM-REF,Y)",
        "Comma-separated subpart and industry-type codes the facility reported under (e.g. C,MM-REF,Y)",
        "Códigos de los subpartes y tipos de industria bajo los cuales la instalación reportó, separados por coma (p. ej. C,MM-REF,Y)",
        observations=(
            "As letras são os subpartes do 40 CFR Part 98 (ver o dicionário de subpart em "
            "emission_subpart); sufixos como -REF, -IMP, -LDC ou W-PROC detalham o tipo de indústria"
        ),
        original_name="reported_industry_types",
    ),
    Col(
        "reporting_status",
        "STRING",
        "Situação do relato: nulo quando a instalação reportou no ano; caso contrário, indica que deixou de reportar, com ou sem razão conhecida",
        "Reporting status: null when the facility reported for the year; otherwise flags that it stopped reporting, with or without a known reason",
        "Situación del reporte: nulo cuando la instalación reportó en el año; en caso contrario, indica que dejó de reportar, con o sin razón conocida",
        dictionary="yes",
        observations="Instalações que deixaram de reportar são mantidas pela fonte com seus últimos atributos e sem linhas de emissão no ano",
        original_name="reporting_status",
    ),
    Col(
        "state_abbreviation",
        "STRING",
        "Sigla de duas letras do estado da instalação",
        "Two-letter abbreviation of the facility's state",
        "Abreviatura de dos letras del estado de la instalación",
        original_name="state",
    ),
    Col(
        "county_name",
        "STRING",
        "Nome do condado da instalação, conforme reportado",
        "Name of the facility's county, as reported",
        "Nombre del condado de la instalación, según lo reportado",
        original_name="county",
    ),
    Col(
        "city",
        "STRING",
        "Cidade da instalação, conforme reportada",
        "City of the facility, as reported",
        "Ciudad de la instalación, según lo reportado",
        original_name="city",
    ),
    Col(
        "zip_code",
        "STRING",
        "Código postal (ZIP) da instalação",
        "ZIP code of the facility",
        "Código postal (ZIP) de la instalación",
        original_name="zip",
    ),
    Col(
        "address",
        "STRING",
        "Endereço da instalação",
        "Street address of the facility",
        "Dirección de la instalación",
        observations=(
            "Reportantes de bacias de petróleo e gás podem informar o endereço da sede "
            "corporativa, distante do local das emissões"
        ),
        original_name="address1",
    ),
    Col(
        "latitude",
        "FLOAT64",
        "Latitude da instalação, em graus decimais",
        "Latitude of the facility, in decimal degrees",
        "Latitud de la instalación, en grados decimales",
        unit="degree",
        original_name="latitude",
    ),
    Col(
        "longitude",
        "FLOAT64",
        "Longitude da instalação, em graus decimais",
        "Longitude of the facility, in decimal degrees",
        "Longitud de la instalación, en grados decimales",
        unit="degree",
        original_name="longitude",
    ),
    Col(
        "cems_used",
        "STRING",
        "Indica se a instalação utiliza monitoramento contínuo de emissões (CEMS): Y quando sim, nulo caso contrário",
        "Whether the facility uses continuous emissions monitoring (CEMS): Y when it does, null otherwise",
        "Indica si la instalación utiliza monitoreo continuo de emisiones (CEMS): Y cuando sí, nulo en caso contrario",
        dictionary="yes",
        original_name="cems_used",
    ),
    Col(
        "co2_captured",
        "STRING",
        "Indica se parte do CO2 é capturada no local e usada na fabricação de outros produtos, não sendo emitida pelas unidades de processo (reportado sob os subpartes G ou S): Y quando sim, nulo caso contrário",
        "Whether some CO2 is collected on site and used to manufacture other products, so it is not emitted from the process units (reported under subpart G or S): Y when it is, null otherwise",
        "Indica si parte del CO2 se captura en el sitio y se usa para fabricar otros productos, sin ser emitido por las unidades de proceso (reportado bajo los subpartes G o S): Y cuando sí, nulo en caso contrario",
        dictionary="yes",
        original_name="co2_captured",
    ),
    Col(
        "co2_supplied",
        "STRING",
        "Indica se parte do CO2 reportado como emissão sob os subpartes AA, G ou P é coletada e transferida para fora do local ou injetada (reportado sob o subparte PP): Y quando sim, nulo caso contrário",
        "Whether some CO2 reported as emissions under subpart AA, G or P is collected and transferred off site or injected (reported under subpart PP): Y when it is, null otherwise",
        "Indica si parte del CO2 reportado como emisión bajo los subpartes AA, G o P se recolecta y transfiere fuera del sitio o se inyecta (reportado bajo el subparte PP): Y cuando sí, nulo en caso contrario",
        dictionary="yes",
        original_name="emitted_co2_supplied",
    ),
]


def reporter_type(name: str, original: str) -> Col:
    return Col(
        name,
        "STRING",
        "Tipo de reportante ao qual a categoria pertence: E = emissor direto, S = fornecedor de combustíveis ou gases industriais, I = injeção de CO2",
        "Reporter type the category belongs to: E = direct emitter, S = supplier of fuels or industrial gases, I = CO2 injection",
        "Tipo de reportante al que pertenece la categoría: E = emisor directo, S = proveedor de combustibles o gases industriales, I = inyección de CO2",
        dictionary="yes",
        observations=(
            "Somente as categorias do tipo E são emissões diretas; as do tipo S são a quantidade "
            "de GEE associada aos produtos fornecidos e as do tipo I, o CO2 recebido para injeção "
            "ou sequestrado. Somar os três tipos conta a mesma tonelada mais de uma vez"
        ),
        original_name=original,
    )


EMISSION_SUBPART = [
    year(),
    facility_id(),
    Col(
        "subpart",
        "STRING",
        "Subparte do 40 CFR Part 98 sob o qual a emissão foi reportada, identificado pela letra (ex.: C, D, W, HH)",
        "Subpart of 40 CFR Part 98 the emissions were reported under, identified by its letter (e.g. C, D, W, HH)",
        "Subparte del 40 CFR Part 98 bajo el cual se reportó la emisión, identificado por su letra (p. ej. C, D, W, HH)",
        dictionary="yes",
        observations=(
            "Cada subparte corresponde a uma categoria de fonte (ex.: C = combustão estacionária, "
            "D = geração de eletricidade, W = sistemas de petróleo e gás natural)"
        ),
        original_name="sub_part_id",
    ),
    reporter_type("subpart_type", "subpart_type"),
    gas(),
    emission(),
]

EMISSION_SECTOR = [
    year(),
    facility_id(),
    Col(
        "sector",
        "STRING",
        "Código do setor industrial do FLIGHT ao qual a emissão é atribuída (ex.: POWERPLANTS, REFINERIES, WASTE)",
        "Code of the FLIGHT industry sector the emissions are attributed to (e.g. POWERPLANTS, REFINERIES, WASTE)",
        "Código del sector industrial del FLIGHT al que se atribuye la emisión (p. ej. POWERPLANTS, REFINERIES, WASTE)",
        dictionary="yes",
        observations=(
            "Setores são a classificação usada no painel FLIGHT da EPA; os totais por setor "
            "diferem dos totais por subparte porque a combustão estacionária (subparte C) é "
            "atribuída ao setor da instalação"
        ),
        original_name="sector_id",
    ),
    reporter_type("sector_type", "sector_type"),
    Col(
        "subsector",
        "STRING",
        "Código do subsetor dentro do setor (ex.: D, W1, C_FOOD, PRO)",
        "Code of the subsector within the sector (e.g. D, W1, C_FOOD, PRO)",
        "Código del subsector dentro del sector (p. ej. D, W1, C_FOOD, PRO)",
        dictionary="yes",
        observations="O mesmo código de subsetor pode ocorrer em mais de um setor (ex.: PRO, IMP, EXP para fornecedores)",
        original_name="subsector_id",
    ),
    gas(),
    emission(),
]

DICIONARIO = [
    Col(
        "id_tabela",
        "STRING",
        "Nome da tabela do conjunto us_epa_ghgrp que a entrada do dicionário descreve",
        "Slug of the us_epa_ghgrp table the dictionary entry describes",
        "Nombre de la tabla del conjunto us_epa_ghgrp que describe la entrada del diccionario",
        original_name="id_tabela",
    ),
    Col(
        "nome_coluna",
        "STRING",
        "Nome da coluna que a entrada do dicionário descreve",
        "Name of the column the dictionary entry describes",
        "Nombre de la columna que describe la entrada del diccionario",
        original_name="nome_coluna",
    ),
    Col(
        "chave",
        "STRING",
        "Código armazenado na coluna",
        "Code stored in the column",
        "Código almacenado en la columna",
        original_name="chave",
    ),
    Col(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal da entrada do dicionário",
        "Temporal coverage of the dictionary entry",
        "Cobertura temporal de la entrada del diccionario",
        original_name="cobertura_temporal",
    ),
    Col(
        "valor",
        "STRING",
        "Rótulo correspondente ao código",
        "Label the code stands for",
        "Etiqueta correspondiente al código",
        original_name="valor",
    ),
]

# Observations (PT in the Col objects) translated for the backend, which stores
# them per language. Keyed by the Portuguese text.
OBSERVATIONS_I18N: dict[str, tuple[str, str]] = {
    "Coluna de particionamento": (
        "Partition column",
        "Columna de particionamiento",
    ),
    (
        "BIOCO2 é o CO2 biogênico, reportado separadamente e não incluído nos totais "
        "da instalação publicados pela EPA"
    ): (
        "BIOCO2 is biogenic CO2, reported separately and not included in the facility "
        "totals EPA publishes",
        "BIOCO2 es el CO2 biogénico, reportado por separado y no incluido en los totales "
        "por instalación que publica la EPA",
    ),
    (
        "Potenciais de aquecimento global (GWP) do IPCC AR4 a partir do ano de relato 2013; "
        "a EPA recalculou os anos anteriores com os mesmos GWP"
    ): (
        "IPCC AR4 global warming potentials (GWP) from reporting year 2013 on; EPA "
        "recalculated earlier years with the same GWPs",
        "Potenciales de calentamiento global (GWP) del IPCC AR4 desde el año de reporte "
        "2013; la EPA recalculó los años anteriores con los mismos GWP",
    ),
    "Derivado da sigla do estado reportada (a fonte não publica o código FIPS do estado)": (
        "Derived from the reported state abbreviation (the source publishes no state FIPS code)",
        "Derivado de la abreviatura del estado reportada (la fuente no publica el código FIPS del estado)",
    ),
    (
        "Nulo para reportantes de área ampla (bacias de petróleo e gás, gasodutos, "
        "distribuidoras). Condados legados de Connecticut e códigos extintos do Alasca "
        "não constam no diretório. Em cerca de 0,5% das linhas o condado está em outro "
        "estado que não o reportado (endereço corporativo); ambos são mantidos como reportados"
    ): (
        "Null for wide-area reporters (oil and gas basins, pipelines, distribution "
        "companies). Legacy Connecticut counties and retired Alaska codes are absent from "
        "the directory. In about 0.5% of rows the county lies in a state other than the "
        "reported one (corporate address); both are kept as reported",
        "Nulo para reportantes de área amplia (cuencas de petróleo y gas, gasoductos, "
        "distribuidoras). Los condados heredados de Connecticut y los códigos extintos de "
        "Alaska no constan en el directorio. En cerca del 0,5% de las filas el condado está "
        "en un estado distinto del reportado (dirección corporativa); ambos se mantienen "
        "como se reportaron",
    ),
    (
        "A edição do NAICS varia com o ano e o reportante: até 2017 os códigos seguem "
        "majoritariamente o NAICS 2007; de 2018 em diante, o NAICS 2017 ou 2022"
    ): (
        "The NAICS edition varies with the year and the reporter: up to 2017 the codes "
        "mostly follow NAICS 2007; from 2018 on, NAICS 2017 or 2022",
        "La edición del NAICS varía con el año y el reportante: hasta 2017 los códigos "
        "siguen mayoritariamente el NAICS 2007; desde 2018, el NAICS 2017 o 2022",
    ),
    "Nulo quando a instalação não reportou no ano (ver reporting_status)": (
        "Null when the facility did not report for the year (see reporting_status)",
        "Nulo cuando la instalación no reportó en el año (ver reporting_status)",
    ),
    (
        "As letras são os subpartes do 40 CFR Part 98 (ver o dicionário de subpart em "
        "emission_subpart); sufixos como -REF, -IMP, -LDC ou W-PROC detalham o tipo de indústria"
    ): (
        "The letters are the 40 CFR Part 98 subparts (see the subpart dictionary of "
        "emission_subpart); suffixes such as -REF, -IMP, -LDC or W-PROC detail the industry type",
        "Las letras son los subpartes del 40 CFR Part 98 (ver el diccionario de subpart en "
        "emission_subpart); sufijos como -REF, -IMP, -LDC o W-PROC detallan el tipo de industria",
    ),
    (
        "Instalações que deixaram de reportar são mantidas pela fonte com seus últimos "
        "atributos e sem linhas de emissão no ano"
    ): (
        "Facilities that stopped reporting are carried by the source with their last "
        "attributes and no emission rows for the year",
        "Las instalaciones que dejaron de reportar se mantienen en la fuente con sus "
        "últimos atributos y sin filas de emisión en el año",
    ),
    (
        "Reportantes de bacias de petróleo e gás podem informar o endereço da sede "
        "corporativa, distante do local das emissões"
    ): (
        "Oil and gas basin reporters may give the corporate headquarters address, far "
        "from where the emissions occur",
        "Los reportantes de cuencas de petróleo y gas pueden informar la dirección de la "
        "sede corporativa, lejos del lugar de las emisiones",
    ),
    (
        "Cada subparte corresponde a uma categoria de fonte (ex.: C = combustão estacionária, "
        "D = geração de eletricidade, W = sistemas de petróleo e gás natural)"
    ): (
        "Each subpart is a source category (e.g. C = stationary combustion, D = electricity "
        "generation, W = petroleum and natural gas systems)",
        "Cada subparte corresponde a una categoría de fuente (p. ej. C = combustión "
        "estacionaria, D = generación de electricidad, W = sistemas de petróleo y gas natural)",
    ),
    (
        "Setores são a classificação usada no painel FLIGHT da EPA; os totais por setor "
        "diferem dos totais por subparte porque a combustão estacionária (subparte C) é "
        "atribuída ao setor da instalação"
    ): (
        "Sectors are the classification used in EPA's FLIGHT dashboard; sector totals "
        "differ from subpart totals because stationary combustion (subpart C) is attributed "
        "to the facility's sector",
        "Los sectores son la clasificación usada en el panel FLIGHT de la EPA; los totales "
        "por sector difieren de los totales por subparte porque la combustión estacionaria "
        "(subparte C) se atribuye al sector de la instalación",
    ),
    (
        "Somente as categorias do tipo E são emissões diretas; as do tipo S são a quantidade "
        "de GEE associada aos produtos fornecidos e as do tipo I, o CO2 recebido para injeção "
        "ou sequestrado. Somar os três tipos conta a mesma tonelada mais de uma vez"
    ): (
        "Only type E categories are direct emissions; type S is the GHG quantity associated "
        "with the products supplied and type I the CO2 received for injection or sequestered. "
        "Summing the three types counts the same tonne more than once",
        "Solo las categorías de tipo E son emisiones directas; el tipo S es la cantidad de GEI "
        "asociada a los productos suministrados y el tipo I, el CO2 recibido para inyección o "
        "secuestrado. Sumar los tres tipos cuenta la misma tonelada más de una vez",
    ),
    "O mesmo código de subsetor pode ocorrer em mais de um setor (ex.: PRO, IMP, EXP para fornecedores)": (
        "The same subsector code can occur in more than one sector (e.g. PRO, IMP, EXP for suppliers)",
        "El mismo código de subsector puede ocurrir en más de un sector (p. ej. PRO, IMP, EXP para proveedores)",
    ),
}


def _obs(c: Col) -> dict[str, str | None]:
    if not c.observations:
        return {
            "observations_pt": None,
            "observations_en": None,
            "observations_es": None,
        }
    en, es = OBSERVATIONS_I18N[c.observations]
    return {
        "observations_pt": c.observations,
        "observations_en": en,
        "observations_es": es,
    }


TABLES: dict[str, list[Col]] = {
    "dicionario": DICIONARIO,
    "facility": FACILITY,
    "emission_subpart": EMISSION_SUBPART,
    "emission_sector": EMISSION_SECTOR,
}


def main() -> None:
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    columns_json: dict[str, list[dict]] = {}
    for table, cols in TABLES.items():
        path = ARCH_DIR / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            # csv.writer defaults to CRLF; force LF so regenerating does not
            # reintroduce mixed line endings that pre-commit then has to fix.
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(HEADER)
            for c in cols:
                writer.writerow(
                    [
                        c.name,
                        c.bigquery_type,
                        c.pt,
                        "",
                        c.dictionary,
                        c.directory,
                        c.unit,
                        "no",
                        c.observations,
                        c.original_name,
                        c.en,
                        c.es,
                    ]
                )
        columns_json[table] = [
            {
                "name": c.name,
                "bigquery_type": c.bigquery_type,
                "description_pt": c.pt,
                "description_en": c.en,
                "description_es": c.es,
                "measurement_unit": c.unit or None,
                "covered_by_dictionary": c.dictionary == "yes",
                "directory_column": c.directory or None,
                "has_sensitive_data": False,
                **_obs(c),
                "is_partition": c.name == "year" and table in PARTITION,
            }
            for c in cols
        ]
        print(f"{table:20s} {len(cols):3d} columns -> {path.name}")

    json_path = ARCH_DIR / "columns.json"
    json_path.write_text(
        json.dumps(columns_json, ensure_ascii=False, indent=2) + "\n", "utf-8"
    )
    total = sum(len(v) for v in columns_json.values())
    print(f"\n{len(columns_json)} tables, {total} columns -> {json_path.name}")


if __name__ == "__main__":
    main()
