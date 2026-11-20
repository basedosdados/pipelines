"""Trilingual column labels for the us_treasury_usaspending architecture tables.

Source of truth for the English wording is the USAspending / DATA Act element
dictionary (https://api.usaspending.gov/api/v2/references/data_dictionary/),
condensed to a single line per column. Portuguese and Spanish are translations
of that wording.

Two families are templated rather than written out one by one:

* ``FLAGS`` — 84 recipient business-type indicators that all share the same
  sentence shape and differ only in the entity type being flagged.
* ``CODE_PAIRS`` — elements shipped by the source as a ``<x>_code`` /
  ``<x>_description`` (or ``<x>_code`` / ``<x>``) pair, where the two columns
  share one concept.

Everything else is in ``DESCRIPTIONS``.
"""

# --------------------------------------------------------------------------
# Recipient business-type flags. Stored as 't'/'f' in the source.
# label = (pt, en, es) noun phrase completing "the recipient is ...".
# --------------------------------------------------------------------------
FLAGS = {
    "alaskan_native_corporation_owned_firm": (
        "uma empresa de propriedade de uma corporação nativa do Alasca",
        "a firm owned by an Alaskan Native Corporation",
        "una empresa propiedad de una corporación nativa de Alaska",
    ),
    "american_indian_owned_business": (
        "uma empresa de propriedade de indígenas norte-americanos",
        "an American Indian owned business",
        "una empresa propiedad de indígenas estadounidenses",
    ),
    "indian_tribe_federally_recognized": (
        "uma tribo indígena reconhecida pelo governo federal",
        "a federally recognized Indian tribe",
        "una tribu indígena reconocida por el gobierno federal",
    ),
    "native_hawaiian_organization_owned_firm": (
        "uma empresa de propriedade de organização nativa havaiana",
        "a firm owned by a Native Hawaiian organization",
        "una empresa propiedad de una organización nativa hawaiana",
    ),
    "tribally_owned_firm": (
        "uma empresa de propriedade tribal",
        "a tribally owned firm",
        "una empresa de propiedad tribal",
    ),
    "veteran_owned_business": (
        "uma empresa de propriedade de veterano de guerra",
        "a veteran owned business",
        "una empresa propiedad de un veterano de guerra",
    ),
    "service_disabled_veteran_owned_business": (
        "uma empresa de propriedade de veterano com deficiência decorrente do serviço militar",
        "a service-disabled veteran owned business",
        "una empresa propiedad de un veterano con discapacidad derivada del servicio militar",
    ),
    "woman_owned_business": (
        "uma empresa de propriedade de mulher",
        "a woman owned business",
        "una empresa propiedad de una mujer",
    ),
    "women_owned_small_business": (
        "uma pequena empresa de propriedade de mulheres",
        "a women owned small business",
        "una pequeña empresa propiedad de mujeres",
    ),
    "economically_disadvantaged_women_owned_small_business": (
        "uma pequena empresa de propriedade de mulheres economicamente desfavorecidas",
        "an economically disadvantaged women owned small business",
        "una pequeña empresa propiedad de mujeres económicamente desfavorecidas",
    ),
    "joint_venture_women_owned_small_business": (
        "uma joint venture de pequenas empresas de propriedade de mulheres",
        "a joint venture of women owned small businesses",
        "una empresa conjunta de pequeñas empresas propiedad de mujeres",
    ),
    "joint_venture_economic_disadvantaged_women_owned_small_bus": (
        "uma joint venture de pequenas empresas de propriedade de mulheres economicamente desfavorecidas",
        "a joint venture of economically disadvantaged women owned small businesses",
        "una empresa conjunta de pequeñas empresas propiedad de mujeres económicamente desfavorecidas",
    ),
    "minority_owned_business": (
        "uma empresa de propriedade de minoria",
        "a minority owned business",
        "una empresa propiedad de una minoría",
    ),
    "subcontinent_asian_asian_indian_american_owned_business": (
        "uma empresa de propriedade de norte-americanos de origem sul-asiática",
        "a subcontinent Asian (Asian-Indian) American owned business",
        "una empresa propiedad de estadounidenses de origen surasiático",
    ),
    "asian_pacific_american_owned_business": (
        "uma empresa de propriedade de norte-americanos de origem asiático-pacífica",
        "an Asian-Pacific American owned business",
        "una empresa propiedad de estadounidenses de origen asiático-pacífico",
    ),
    "black_american_owned_business": (
        "uma empresa de propriedade de norte-americanos negros",
        "a Black American owned business",
        "una empresa propiedad de estadounidenses negros",
    ),
    "hispanic_american_owned_business": (
        "uma empresa de propriedade de norte-americanos hispânicos",
        "a Hispanic American owned business",
        "una empresa propiedad de estadounidenses hispanos",
    ),
    "native_american_owned_business": (
        "uma empresa de propriedade de norte-americanos nativos",
        "a Native American owned business",
        "una empresa propiedad de nativos estadounidenses",
    ),
    "other_minority_owned_business": (
        "uma empresa de propriedade de outra minoria",
        "a business owned by another minority group",
        "una empresa propiedad de otra minoría",
    ),
    "emerging_small_business": (
        "uma pequena empresa emergente",
        "an emerging small business",
        "una pequeña empresa emergente",
    ),
    "community_developed_corporation_owned_firm": (
        "uma empresa de propriedade de corporação de desenvolvimento comunitário",
        "a firm owned by a community development corporation",
        "una empresa propiedad de una corporación de desarrollo comunitario",
    ),
    "labor_surplus_area_firm": (
        "uma empresa sediada em área de excedente de mão de obra",
        "a firm located in a labor surplus area",
        "una empresa ubicada en un área con excedente de mano de obra",
    ),
    "us_federal_government": (
        "um órgão do governo federal dos Estados Unidos",
        "a United States federal government entity",
        "una entidad del gobierno federal de los Estados Unidos",
    ),
    "federally_funded_research_and_development_corp": (
        "um centro de pesquisa e desenvolvimento financiado pelo governo federal",
        "a federally funded research and development center",
        "un centro de investigación y desarrollo financiado por el gobierno federal",
    ),
    "federal_agency": (
        "uma agência federal",
        "a federal agency",
        "una agencia federal",
    ),
    "us_state_government": (
        "um governo estadual dos Estados Unidos",
        "a United States state government",
        "un gobierno estatal de los Estados Unidos",
    ),
    "us_local_government": (
        "um governo local dos Estados Unidos",
        "a United States local government",
        "un gobierno local de los Estados Unidos",
    ),
    "city_local_government": (
        "um governo municipal de cidade",
        "a city local government",
        "un gobierno local de ciudad",
    ),
    "county_local_government": (
        "um governo local de condado",
        "a county local government",
        "un gobierno local de condado",
    ),
    "inter_municipal_local_government": (
        "um governo local intermunicipal",
        "an inter-municipal local government",
        "un gobierno local intermunicipal",
    ),
    "local_government_owned": (
        "uma entidade de propriedade de governo local",
        "an entity owned by a local government",
        "una entidad propiedad de un gobierno local",
    ),
    "municipality_local_government": (
        "um governo local de município",
        "a municipality local government",
        "un gobierno local de municipio",
    ),
    "school_district_local_government": (
        "um governo local de distrito escolar",
        "a school district local government",
        "un gobierno local de distrito escolar",
    ),
    "township_local_government": (
        "um governo local de township",
        "a township local government",
        "un gobierno local de township",
    ),
    "us_tribal_government": (
        "um governo tribal dos Estados Unidos",
        "a United States tribal government",
        "un gobierno tribal de los Estados Unidos",
    ),
    "foreign_government": (
        "um governo estrangeiro",
        "a foreign government",
        "un gobierno extranjero",
    ),
    "corporate_entity_not_tax_exempt": (
        "uma pessoa jurídica não isenta de impostos",
        "a corporate entity that is not tax exempt",
        "una persona jurídica no exenta de impuestos",
    ),
    "corporate_entity_tax_exempt": (
        "uma pessoa jurídica isenta de impostos",
        "a tax exempt corporate entity",
        "una persona jurídica exenta de impuestos",
    ),
    "partnership_or_limited_liability_partnership": (
        "uma sociedade ou sociedade de responsabilidade limitada",
        "a partnership or limited liability partnership",
        "una sociedad o sociedad de responsabilidad limitada",
    ),
    "sole_proprietorship": (
        "uma empresa individual",
        "a sole proprietorship",
        "una empresa individual",
    ),
    "small_agricultural_cooperative": (
        "uma pequena cooperativa agrícola",
        "a small agricultural cooperative",
        "una pequeña cooperativa agrícola",
    ),
    "international_organization": (
        "uma organização internacional",
        "an international organization",
        "una organización internacional",
    ),
    "us_government_entity": (
        "uma entidade do governo dos Estados Unidos",
        "a United States government entity",
        "una entidad del gobierno de los Estados Unidos",
    ),
    "community_development_corporation": (
        "uma corporação de desenvolvimento comunitário",
        "a community development corporation",
        "una corporación de desarrollo comunitario",
    ),
    "domestic_shelter": (
        "um abrigo para vítimas de violência doméstica",
        "a domestic shelter",
        "un albergue para víctimas de violencia doméstica",
    ),
    "educational_institution": (
        "uma instituição de ensino",
        "an educational institution",
        "una institución educativa",
    ),
    "foundation": (
        "uma fundação",
        "a foundation",
        "una fundación",
    ),
    "manufacturer_of_goods": (
        "um fabricante de bens",
        "a manufacturer of goods",
        "un fabricante de bienes",
    ),
    "veterinary_hospital": (
        "um hospital veterinário",
        "a veterinary hospital",
        "un hospital veterinario",
    ),
    "hispanic_servicing_institution": (
        "uma instituição de ensino voltada à população hispânica",
        "a Hispanic-serving institution",
        "una institución de enseñanza al servicio de la población hispana",
    ),
    "receives_contracts": (
        "uma entidade que recebe contratos",
        "an entity that receives contracts",
        "una entidad que recibe contratos",
    ),
    "receives_financial_assistance": (
        "uma entidade que recebe assistência financeira",
        "an entity that receives financial assistance",
        "una entidad que recibe asistencia financiera",
    ),
    "receives_contracts_and_financial_assistance": (
        "uma entidade que recebe contratos e assistência financeira",
        "an entity that receives both contracts and financial assistance",
        "una entidad que recibe contratos y asistencia financiera",
    ),
    "airport_authority": (
        "uma autoridade aeroportuária",
        "an airport authority",
        "una autoridad aeroportuaria",
    ),
    "council_of_governments": (
        "um conselho de governos",
        "a council of governments",
        "un consejo de gobiernos",
    ),
    "housing_authorities_public_tribal": (
        "uma autoridade habitacional pública ou tribal",
        "a public or tribal housing authority",
        "una autoridad de vivienda pública o tribal",
    ),
    "interstate_entity": (
        "uma entidade interestadual",
        "an interstate entity",
        "una entidad interestatal",
    ),
    "planning_commission": (
        "uma comissão de planejamento",
        "a planning commission",
        "una comisión de planificación",
    ),
    "port_authority": (
        "uma autoridade portuária",
        "a port authority",
        "una autoridad portuaria",
    ),
    "transit_authority": (
        "uma autoridade de transporte público",
        "a transit authority",
        "una autoridad de transporte público",
    ),
    "subchapter_scorporation": (
        "uma sociedade do tipo S corporation",
        "a subchapter S corporation",
        "una sociedad del tipo S corporation",
    ),
    "limited_liability_corporation": (
        "uma sociedade de responsabilidade limitada",
        "a limited liability corporation",
        "una sociedad de responsabilidad limitada",
    ),
    "foreign_owned": (
        "uma entidade de propriedade estrangeira",
        "a foreign owned entity",
        "una entidad de propiedad extranjera",
    ),
    "for_profit_organization": (
        "uma organização com fins lucrativos",
        "a for-profit organization",
        "una organización con fines de lucro",
    ),
    "nonprofit_organization": (
        "uma organização sem fins lucrativos",
        "a nonprofit organization",
        "una organización sin fines de lucro",
    ),
    "other_not_for_profit_organization": (
        "outra organização sem fins lucrativos",
        "another type of not-for-profit organization",
        "otra organización sin fines de lucro",
    ),
    "the_ability_one_program": (
        "uma entidade participante do programa AbilityOne",
        "a participant in the AbilityOne program",
        "una entidad participante del programa AbilityOne",
    ),
    "private_university_or_college": (
        "uma universidade ou faculdade privada",
        "a private university or college",
        "una universidad o facultad privada",
    ),
    "state_controlled_institution_of_higher_learning": (
        "uma instituição de ensino superior controlada por governo estadual",
        "a state controlled institution of higher learning",
        "una institución de educación superior controlada por un gobierno estatal",
    ),
    "1862_land_grant_college": (
        "uma land-grant college criada pelo Morrill Act de 1862",
        "an 1862 Land Grant College",
        "una land-grant college creada por la Morrill Act de 1862",
    ),
    "1890_land_grant_college": (
        "uma land-grant college criada pelo Morrill Act de 1890",
        "an 1890 Land Grant College",
        "una land-grant college creada por la Morrill Act de 1890",
    ),
    "1994_land_grant_college": (
        "uma land-grant college tribal reconhecida em 1994",
        "a 1994 Land Grant College",
        "una land-grant college tribal reconocida en 1994",
    ),
    "minority_institution": (
        "uma instituição de ensino voltada a minorias",
        "a minority institution",
        "una institución educativa al servicio de minorías",
    ),
    "historically_black_college": (
        "uma universidade historicamente negra",
        "a historically Black college or university",
        "una universidad históricamente negra",
    ),
    "tribal_college": (
        "uma faculdade tribal",
        "a tribal college",
        "una universidad tribal",
    ),
    "alaskan_native_servicing_institution": (
        "uma instituição de ensino voltada a nativos do Alasca",
        "an Alaskan Native-serving institution",
        "una institución educativa al servicio de nativos de Alaska",
    ),
    "native_hawaiian_servicing_institution": (
        "uma instituição de ensino voltada a nativos havaianos",
        "a Native Hawaiian-serving institution",
        "una institución educativa al servicio de nativos hawaianos",
    ),
    "school_of_forestry": (
        "uma escola de engenharia florestal",
        "a school of forestry",
        "una escuela de ingeniería forestal",
    ),
    "veterinary_college": (
        "uma faculdade de medicina veterinária",
        "a veterinary college",
        "una facultad de medicina veterinaria",
    ),
    "dot_certified_disadvantage": (
        "uma empresa desfavorecida certificada pelo Departamento de Transportes",
        "a Department of Transportation certified disadvantaged business",
        "una empresa desfavorecida certificada por el Departamento de Transporte",
    ),
    "self_certified_small_disadvantaged_business": (
        "uma pequena empresa desfavorecida autodeclarada",
        "a self-certified small disadvantaged business",
        "una pequeña empresa desfavorecida autodeclarada",
    ),
    "c8a_program_participant": (
        "uma entidade participante do programa 8(a) da SBA",
        "a participant in the SBA 8(a) program",
        "una entidad participante del programa 8(a) de la SBA",
    ),
    "historically_underutilized_business_zone_hubzone_firm": (
        "uma empresa sediada em zona historicamente subutilizada (HUBZone)",
        "a firm located in a Historically Underutilized Business Zone (HUBZone)",
        "una empresa ubicada en una zona históricamente subutilizada (HUBZone)",
    ),
    "sba_certified_8a_joint_venture": (
        "uma joint venture 8(a) certificada pela SBA",
        "an SBA certified 8(a) joint venture",
        "una empresa conjunta 8(a) certificada por la SBA",
    ),
}

FLAG_TEMPLATE = (
    "Indica se o beneficiário é {}",
    "Indicates whether the recipient is {}",
    "Indica si el beneficiario es {}",
)

# Flags that describe the award rather than the recipient entity type.
FLAG_TEMPLATE_OVERRIDES = {
    "receives_contracts": FLAG_TEMPLATE,
    "receives_financial_assistance": FLAG_TEMPLATE,
    "receives_contracts_and_financial_assistance": FLAG_TEMPLATE,
}
