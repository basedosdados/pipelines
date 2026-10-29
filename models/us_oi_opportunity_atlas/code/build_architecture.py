"""Write the architecture CSVs for us_oi_opportunity_atlas.

Outcome columns are taken from the source headers, so each geography lists exactly
the outcomes it publishes (counties and commuting zones add the ACS-based
education and labour outcomes; tracts add the dollar-income predictions kfi and
kii). Definitions follow the Opportunity Atlas codebooks (tables 4-6) and the
Migration Patterns data dictionary.

Usage:
    python models/us_oi_opportunity_atlas/code/build_architecture.py

Needs the first line of each Atlas source file under
$US_OI_OPPORTUNITY_ATLAS_DATA_DIR/input (see clean_data.py).
"""

import csv
import io
import zipfile
from pathlib import Path

from clean_data import ATLAS, GROUP_RES, INPUT, OUTCOME_RE

ARCH_DIR = Path(__file__).resolve().parent / "architecture"
FIELDS = [
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
    "observations_en",
    "observations_es",
]

RANK_OBS = (
    "Posição percentil na escala de 0 a 1, relativa às outras crianças nascidas "
    "no mesmo ano"
)
RANK_OBS_EN = "Percentile rank on a 0 to 1 scale, relative to other children born in the same year"
RANK_OBS_ES = "Posición percentil en escala de 0 a 1, relativa a los demás niños nacidos en el mismo año"
ACS_OBS = "Medido na ACS ou no questionário amostral do Censo 2000"
ACS_OBS_EN = "Measured in the ACS or the 2000 Census long form"
ACS_OBS_ES = "Medido en la ACS o en el cuestionario ampliado del Censo 2000"

# code -> (pt, en, es, unit, observation kind). Order is the column order.
OUTCOMES: dict[str, tuple[str, str, str, str, str]] = {
    "kfr": (
        "Posição média na distribuição nacional de renda domiciliar (renda própria e do cônjuge), média de 2014-2015",
        "Mean rank in the national household income distribution (own and spouse's earnings), averaged over 2014-2015",
        "Posición media en la distribución nacional del ingreso del hogar (ingresos propios y del cónyuge), promedio de 2014-2015",
        "proportion",
        "rank",
    ),
    "kfr_top01": (
        "Probabilidade de chegar ao 1% do topo da distribuição nacional de renda domiciliar em 2014-2015",
        "Probability of reaching the top 1% of the national household income distribution in 2014-2015",
        "Probabilidad de alcanzar el 1% superior de la distribución nacional del ingreso del hogar en 2014-2015",
        "proportion",
        "",
    ),
    "kfr_top20": (
        "Probabilidade de chegar ao quintil superior da distribuição nacional de renda domiciliar em 2014-2015",
        "Probability of reaching the top quintile of the national household income distribution in 2014-2015",
        "Probabilidad de alcanzar el quintil superior de la distribución nacional del ingreso del hogar en 2014-2015",
        "proportion",
        "",
    ),
    "kfr_stycz": (
        "Posição média na distribuição nacional de renda domiciliar em 2014-2015, entre crianças que moram na adultez em uma das zonas de deslocamento da infância",
        "Mean rank in the national household income distribution in 2014-2015, among children who live in one of their childhood commuting zones in adulthood",
        "Posición media en la distribución nacional del ingreso del hogar en 2014-2015, entre los niños que viven en la adultez en una de sus zonas de desplazamiento de la infancia",
        "proportion",
        "rank",
    ),
    "kfr_imm": (
        "Posição média na distribuição nacional de renda domiciliar em 2014-2015, entre crianças cujas mães imigraram para os Estados Unidos",
        "Mean rank in the national household income distribution in 2014-2015, among children whose mothers immigrated to the United States",
        "Posición media en la distribución nacional del ingreso del hogar en 2014-2015, entre los niños cuyas madres inmigraron a Estados Unidos",
        "proportion",
        "rank_acs",
    ),
    "kfr_native": (
        "Posição média na distribuição nacional de renda domiciliar em 2014-2015, entre crianças cujas mães nasceram nos Estados Unidos",
        "Mean rank in the national household income distribution in 2014-2015, among children whose mothers were born in the United States",
        "Posición media en la distribución nacional del ingreso del hogar en 2014-2015, entre los niños cuyas madres nacieron en Estados Unidos",
        "proportion",
        "rank_acs",
    ),
    "kfr_24": (
        "Posição média na distribuição nacional de renda domiciliar aos 24 anos",
        "Mean rank in the national household income distribution at age 24",
        "Posición media en la distribución nacional del ingreso del hogar a los 24 años",
        "proportion",
        "rank",
    ),
    "kfr_26": (
        "Posição média na distribuição nacional de renda domiciliar aos 26 anos",
        "Mean rank in the national household income distribution at age 26",
        "Posición media en la distribución nacional del ingreso del hogar a los 26 años",
        "proportion",
        "rank",
    ),
    "kfr_29": (
        "Posição média na distribuição nacional de renda domiciliar aos 29 anos",
        "Mean rank in the national household income distribution at age 29",
        "Posición media en la distribución nacional del ingreso del hogar a los 29 años",
        "proportion",
        "rank",
    ),
    "kfi": (
        "Renda domiciliar média prevista (renda própria e do cônjuge), calculada a partir de kfr, kfr_top01 e kfr_top20",
        "Predicted mean household income (own and spouse's earnings), computed from kfr, kfr_top01 and kfr_top20",
        "Ingreso medio del hogar previsto (ingresos propios y del cónyuge), calculado a partir de kfr, kfr_top01 y kfr_top20",
        "usd",
        "dollar",
    ),
    "kir": (
        "Posição média na distribuição nacional de renda individual (apenas renda própria), média de 2014-2015",
        "Mean rank in the national individual income distribution (own earnings only), averaged over 2014-2015",
        "Posición media en la distribución nacional del ingreso individual (solo ingresos propios), promedio de 2014-2015",
        "proportion",
        "rank",
    ),
    "kir_top01": (
        "Probabilidade de chegar ao 1% do topo da distribuição nacional de renda individual em 2014-2015",
        "Probability of reaching the top 1% of the national individual income distribution in 2014-2015",
        "Probabilidad de alcanzar el 1% superior de la distribución nacional del ingreso individual en 2014-2015",
        "proportion",
        "",
    ),
    "kir_top20": (
        "Probabilidade de chegar ao quintil superior da distribuição nacional de renda individual em 2014-2015",
        "Probability of reaching the top quintile of the national individual income distribution in 2014-2015",
        "Probabilidad de alcanzar el quintil superior de la distribución nacional del ingreso individual en 2014-2015",
        "proportion",
        "",
    ),
    "kir_stycz": (
        "Posição média na distribuição nacional de renda individual em 2014-2015, entre crianças que moram na adultez em uma das zonas de deslocamento da infância",
        "Mean rank in the national individual income distribution in 2014-2015, among children who live in one of their childhood commuting zones in adulthood",
        "Posición media en la distribución nacional del ingreso individual en 2014-2015, entre los niños que viven en la adultez en una de sus zonas de desplazamiento de la infancia",
        "proportion",
        "rank",
    ),
    "kir_imm": (
        "Posição média na distribuição nacional de renda individual em 2014-2015, entre crianças cujas mães imigraram para os Estados Unidos",
        "Mean rank in the national individual income distribution in 2014-2015, among children whose mothers immigrated to the United States",
        "Posición media en la distribución nacional del ingreso individual en 2014-2015, entre los niños cuyas madres inmigraron a Estados Unidos",
        "proportion",
        "rank_acs",
    ),
    "kir_native": (
        "Posição média na distribuição nacional de renda individual em 2014-2015, entre crianças cujas mães nasceram nos Estados Unidos",
        "Mean rank in the national individual income distribution in 2014-2015, among children whose mothers were born in the United States",
        "Posición media en la distribución nacional del ingreso individual en 2014-2015, entre los niños cuyas madres nacieron en Estados Unidos",
        "proportion",
        "rank_acs",
    ),
    "kir_24": (
        "Posição média na distribuição nacional de renda individual aos 24 anos",
        "Mean rank in the national individual income distribution at age 24",
        "Posición media en la distribución nacional del ingreso individual a los 24 años",
        "proportion",
        "rank",
    ),
    "kir_26": (
        "Posição média na distribuição nacional de renda individual aos 26 anos",
        "Mean rank in the national individual income distribution at age 26",
        "Posición media en la distribución nacional del ingreso individual a los 26 años",
        "proportion",
        "rank",
    ),
    "kir_29": (
        "Posição média na distribuição nacional de renda individual aos 29 anos",
        "Mean rank in the national individual income distribution at age 29",
        "Posición media en la distribución nacional del ingreso individual a los 29 años",
        "proportion",
        "rank",
    ),
    "kii": (
        "Renda individual média prevista (apenas renda própria), calculada a partir de kir, kir_top01 e kir_top20",
        "Predicted mean individual income (own earnings only), computed from kir, kir_top01 and kir_top20",
        "Ingreso individual medio previsto (solo ingresos propios), calculado a partir de kir, kir_top01 y kir_top20",
        "usd",
        "dollar",
    ),
    "spouse_rk": (
        "Posição média de renda individual em 2014-2015 do cônjuge, entre crianças casadas em 2015",
        "Mean individual income rank in 2014-2015 of the spouse, among children married in 2015",
        "Posición media del ingreso individual en 2014-2015 del cónyuge, entre los niños casados en 2015",
        "proportion",
        "rank",
    ),
    "wgflx_rk": (
        "Posição do salário-hora, dentro do ano de nascimento e do ano da ACS, entre crianças que responderam à ACS com 30 anos ou mais",
        "Hourly wage rank, within birth year and ACS year, among children who received the ACS at age 30 or older",
        "Posición del salario por hora, dentro del año de nacimiento y del año de la ACS, entre los niños que respondieron la ACS con 30 años o más",
        "proportion",
        "",
    ),
    "hours_wk": (
        "Média de horas semanais trabalhadas nos 12 meses anteriores à ACS, entre crianças que responderam à ACS com 30 anos ou mais (zero para quem não trabalha)",
        "Mean weekly hours worked in the 12 months before the ACS, among children who received the ACS at age 30 or older (zero for those not working)",
        "Promedio de horas semanales trabajadas en los 12 meses previos a la ACS, entre los niños que respondieron la ACS con 30 años o más (cero para quienes no trabajan)",
        "hour",
        "",
    ),
    "pos_hours": (
        "Fração com horas trabalhadas positivas nos 12 meses anteriores à ACS, entre crianças que responderam à ACS com 30 anos ou mais",
        "Fraction with positive hours worked in the 12 months before the ACS, among children who received the ACS at age 30 or older",
        "Fracción con horas trabajadas positivas en los 12 meses previos a la ACS, entre los niños que respondieron la ACS con 30 años o más",
        "proportion",
        "",
    ),
    "working": (
        "Fração com rendimentos positivos no formulário W-2 em 2015",
        "Fraction with positive W-2 earnings in 2015",
        "Fracción con ingresos positivos en el formulario W-2 en 2015",
        "proportion",
        "",
    ),
    "work_24": (
        "Fração com rendimentos positivos no formulário W-2 aos 24 anos",
        "Fraction with positive W-2 earnings at age 24",
        "Fracción con ingresos positivos en el formulario W-2 a los 24 años",
        "proportion",
        "",
    ),
    "work_26": (
        "Fração com rendimentos positivos no formulário W-2 aos 26 anos",
        "Fraction with positive W-2 earnings at age 26",
        "Fracción con ingresos positivos en el formulario W-2 a los 26 años",
        "proportion",
        "",
    ),
    "work_29": (
        "Fração com rendimentos positivos no formulário W-2 aos 29 anos",
        "Fraction with positive W-2 earnings at age 29",
        "Fracción con ingresos positivos en el formulario W-2 a los 29 años",
        "proportion",
        "",
    ),
    "work_32": (
        "Fração com rendimentos positivos no formulário W-2 aos 32 anos",
        "Fraction with positive W-2 earnings at age 32",
        "Fracción con ingresos positivos en el formulario W-2 a los 32 años",
        "proportion",
        "",
    ),
    "proginc": (
        "Fração que recebe renda de assistência pública, entre crianças que responderam à ACS com 30 anos ou mais",
        "Fraction receiving public assistance income, among children who received the ACS at age 30 or older",
        "Fracción que recibe ingresos de asistencia pública, entre los niños que respondieron la ACS con 30 años o más",
        "proportion",
        "",
    ),
    "hs": (
        "Fração que concluiu o ensino médio ou obteve o GED, entre crianças que responderam à ACS ou ao questionário amostral do Censo 2000 com 19 anos ou mais",
        "Fraction who completed high school or obtained a GED, among children who received the ACS or the 2000 Census long form at age 19 or older",
        "Fracción que terminó la secundaria u obtuvo el GED, entre los niños que respondieron la ACS o el cuestionario ampliado del Censo 2000 con 19 años o más",
        "proportion",
        "",
    ),
    "somecoll": (
        "Fração com alguma experiência de ensino superior, entre crianças que responderam à ACS ou ao questionário amostral do Censo 2000 com 25 anos ou mais",
        "Fraction with at least some college, among children who received the ACS or the 2000 Census long form at age 25 or older",
        "Fracción con al menos algo de educación superior, entre los niños que respondieron la ACS o el cuestionario ampliado del Censo 2000 con 25 años o más",
        "proportion",
        "",
    ),
    "comcoll": (
        "Fração com pelo menos um diploma de community college, entre crianças que responderam à ACS ou ao questionário amostral do Censo 2000 com 25 anos ou mais",
        "Fraction with at least a community college degree, among children who received the ACS or the 2000 Census long form at age 25 or older",
        "Fracción con al menos un título de community college, entre los niños que respondieron la ACS o el cuestionario ampliado del Censo 2000 con 25 años o más",
        "proportion",
        "",
    ),
    "coll": (
        "Fração com diploma de graduação de quatro anos, entre crianças que responderam à ACS ou ao questionário amostral do Censo 2000 com 25 anos ou mais",
        "Fraction with a four-year college degree, among children who received the ACS or the 2000 Census long form at age 25 or older",
        "Fracción con un título universitario de cuatro años, entre los niños que respondieron la ACS o el cuestionario ampliado del Censo 2000 con 25 años o más",
        "proportion",
        "",
    ),
    "grad": (
        "Fração com diploma de pós-graduação, entre crianças que responderam à ACS ou ao questionário amostral do Censo 2000 com 30 anos ou mais",
        "Fraction with a graduate degree, among children who received the ACS or the 2000 Census long form at age 30 or older",
        "Fracción con un título de posgrado, entre los niños que respondieron la ACS o el cuestionario ampliado del Censo 2000 con 30 años o más",
        "proportion",
        "",
    ),
    "jail": (
        "Fração encarcerada em 1º de abril de 2010 (prisão federal ou estadual, cadeia local, centro de detenção, instituição correcional residencial, militar ou juvenil)",
        "Fraction incarcerated on April 1, 2010 (federal or state prison, local jail, detention center, residential, military or juvenile correctional facility)",
        "Fracción encarcelada el 1 de abril de 2010 (prisión federal o estatal, cárcel local, centro de detención, establecimiento correccional residencial, militar o juvenil)",
        "proportion",
        "",
    ),
    "teenbrth": (
        "Fração das mulheres que declararam como dependente um filho nascido quando elas tinham entre 13 e 19 anos",
        "Fraction of women who ever claimed as a dependent a child born when they were aged 13 to 19",
        "Fracción de mujeres que alguna vez declararon como dependiente a un hijo nacido cuando tenían entre 13 y 19 años",
        "proportion",
        "",
    ),
    "married": (
        "Fração que declara imposto de renda como casada (conjunta ou separadamente) em 2015",
        "Fraction filing their 2015 federal income tax return as married (jointly or separately)",
        "Fracción que presenta la declaración federal de impuestos de 2015 como casada (conjunta o separadamente)",
        "proportion",
        "",
    ),
    "marr_24": (
        "Fração que declara imposto de renda como casada aos 24 anos",
        "Fraction filing their federal income tax return as married at age 24",
        "Fracción que presenta la declaración federal de impuestos como casada a los 24 años",
        "proportion",
        "",
    ),
    "marr_26": (
        "Fração que declara imposto de renda como casada aos 26 anos",
        "Fraction filing their federal income tax return as married at age 26",
        "Fracción que presenta la declaración federal de impuestos como casada a los 26 años",
        "proportion",
        "",
    ),
    "marr_29": (
        "Fração que declara imposto de renda como casada aos 29 anos",
        "Fraction filing their federal income tax return as married at age 29",
        "Fracción que presenta la declaración federal de impuestos como casada a los 29 años",
        "proportion",
        "",
    ),
    "marr_32": (
        "Fração que declara imposto de renda como casada aos 32 anos",
        "Fraction filing their federal income tax return as married at age 32",
        "Fracción que presenta la declaración federal de impuestos como casada a los 32 años",
        "proportion",
        "",
    ),
    "has_dad": (
        "Fração com um declarante do sexo masculino no ano da vinculação aos pais",
        "Fraction with a male claimer in the year they are linked to parents",
        "Fracción con un declarante masculino en el año de vinculación a los padres",
        "proportion",
        "",
    ),
    "has_mom": (
        "Fração com uma declarante do sexo feminino no ano da vinculação aos pais",
        "Fraction with a female claimer in the year they are linked to parents",
        "Fracción con una declarante femenina en el año de vinculación a los padres",
        "proportion",
        "",
    ),
    "two_par": (
        "Fração declarada como dependente por duas pessoas no ano da vinculação aos pais",
        "Fraction claimed by two people in the year they are linked to parents",
        "Fracción declarada como dependiente por dos personas en el año de vinculación a los padres",
        "proportion",
        "",
    ),
    "lpov_nbh": (
        "Fração que mora na adultez em um setor censitário com taxa de pobreza abaixo de 10% no Censo 2000",
        "Fraction living in adulthood in a tract with a poverty rate below 10% in the 2000 Census",
        "Fracción que vive en la adultez en un sector censal con tasa de pobreza inferior al 10% en el Censo 2000",
        "proportion",
        "",
    ),
    "staycz": (
        "Fração que mora na adultez em uma das zonas de deslocamento da infância",
        "Fraction living in adulthood in one of their childhood commuting zones",
        "Fracción que vive en la adultez en una de sus zonas de desplazamiento de la infancia",
        "proportion",
        "",
    ),
    "staytract": (
        "Fração que mora na adultez em um dos setores censitários da infância",
        "Fraction living in adulthood in one of their childhood census tracts",
        "Fracción que vive en la adultez en uno de sus sectores censales de la infancia",
        "proportion",
        "",
    ),
    "stayhome": (
        "Fração que mora no mesmo endereço dos pais em 2015",
        "Fraction living at the same address as their parents in 2015",
        "Fracción que vive en la misma dirección que sus padres en 2015",
        "proportion",
        "",
    ),
}

OBS = {
    "rank": (RANK_OBS, RANK_OBS_EN, RANK_OBS_ES),
    "rank_acs": (
        f"{RANK_OBS}. {ACS_OBS}",
        f"{RANK_OBS_EN}. {ACS_OBS_EN}",
        f"{RANK_OBS_ES}. {ACS_OBS_ES}",
    ),
    "dollar": (
        "Publicado apenas nos percentis 25 e 75 de renda dos pais",
        "Published at the 25th and 75th parental income percentiles only",
        "Publicado solo en los percentiles 25 y 75 del ingreso de los padres",
    ),
    "": ("", "", ""),
}
SE_OBS = (
    "Publicado apenas nos percentis 25 e 75 e na média; inclui o ruído adicionado para proteger a privacidade",
    "Published at the 25th and 75th percentiles and the mean only; includes the noise added to protect privacy",
    "Publicado solo en los percentiles 25 y 75 y en la media; incluye el ruido añadido para proteger la privacidad",
)


def row(name, bq_type, pt, en, es, obs=("", "", ""), **kw) -> dict[str, str]:
    r = dict.fromkeys(FIELDS, "")
    r.update(
        observations=obs[0], observations_en=obs[1], observations_es=obs[2]
    )
    r.update(
        name=name,
        bigquery_type=bq_type,
        description=pt,
        description_en=en,
        description_es=es,
        covered_by_dictionary="no",
        has_sensitive_data="no",
        original_name=kw.pop("original_name", name),
    )
    r.update(kw)
    return r


STATE = row(
    "state_id", "STRING",
    "Código FIPS de dois dígitos do estado (2010)",
    "Two-digit state FIPS code (2010)",
    "Código FIPS de dos dígitos del estado (2010)",
    directory_column="br_bd_diretorios_us.state:id_state",
    original_name="state",
)  # fmt: skip
COUNTY = row(
    "county_id", "STRING",
    "Código FIPS de cinco dígitos do condado (2010), estado mais condado",
    "Five-digit county FIPS code (2010), state plus county",
    "Código FIPS de cinco dígitos del condado (2010), estado más condado",
    directory_column="br_bd_diretorios_us.county:id_county",
    obs=(
        "Códigos de 2010; condados extintos depois de 2010 (Connecticut, 46113, 02270, 02261, 51515) não estão no diretório",
        "2010 codes; counties retired after 2010 (Connecticut, 46113, 02270, 02261, 51515) are not in the directory",
        "Códigos de 2010; los condados extinguidos después de 2010 (Connecticut, 46113, 02270, 02261, 51515) no están en el directorio",
    ),
    original_name="state, county",
)  # fmt: skip
TRACT = row(
    "tract_id", "STRING",
    "Código FIPS de onze dígitos do setor censitário (2010), estado, condado e setor",
    "Eleven-digit census tract FIPS code (2010), state, county and tract",
    "Código FIPS de once dígitos del sector censal (2010), estado, condado y sector",
    obs=(
        "Setores de 2010, sem diretório (o diretório br_bd_diretorios_us usa setores de 2020); a Opportunity Insights publica uma tabela de correspondência 2010-2020",
        "2010 tracts, with no directory (br_bd_diretorios_us uses 2020 tracts); Opportunity Insights publishes a 2010-2020 crosswalk",
        "Sectores de 2010, sin directorio (br_bd_diretorios_us usa sectores de 2020); Opportunity Insights publica una tabla de correspondencia 2010-2020",
    ),
    original_name="state, county, tract",
)  # fmt: skip
CZ = row(
    "commuting_zone_id", "STRING",
    "Identificador da zona de deslocamento (definição de 1990)",
    "Commuting zone identifier (1990 definition)",
    "Identificador de la zona de desplazamiento (definición de 1990)",
    original_name="cz",
)  # fmt: skip
CZ_NAME = row(
    "commuting_zone_name", "STRING",
    "Nome da zona de deslocamento",
    "Commuting zone name",
    "Nombre de la zona de desplazamiento",
    original_name="czname",
)  # fmt: skip
RACE = row(
    "race", "STRING",
    "Raça ou etnia da criança: pooled (todas), white, black, hispanic, asian, native_american ou other",
    "Child's race or ethnicity: pooled (all), white, black, hispanic, asian, native_american or other",
    "Raza o etnia del niño: pooled (todas), white, black, hispanic, asian, native_american u other",
    original_name="[race] in column names",
)  # fmt: skip
GENDER = row(
    "gender", "STRING",
    "Sexo da criança: pooled (ambos), male ou female",
    "Child's gender: pooled (both), male or female",
    "Sexo del niño: pooled (ambos), male o female",
    original_name="[gender] in column names",
)  # fmt: skip
PCTILE = row(
    "parent_income_percentile", "STRING",
    "Percentil da renda domiciliar dos pais na distribuição nacional em que a previsão é avaliada (1, 10, 25, 50, 75 ou 100), ou mean para a média sem condicionar na renda dos pais",
    "Percentile of parental household income in the national distribution at which the prediction is evaluated (1, 10, 25, 50, 75 or 100), or mean for the average not conditioning on parental income",
    "Percentil del ingreso del hogar de los padres en la distribución nacional en que se evalúa la predicción (1, 10, 25, 50, 75 o 100), o mean para el promedio sin condicionar en el ingreso de los padres",
    obs=(
        "O percentil 10 é publicado pela fonte, mas não consta dos livros de códigos",
        "The 10th percentile is published by the source but not listed in its codebooks",
        "El percentil 10 es publicado por la fuente, pero no figura en sus libros de códigos",
    ),
    original_name="p[pctile] or mean in column names",
)  # fmt: skip


def outcome_rows(outcomes: list[str]) -> list[dict]:
    rows = []
    for o in outcomes:
        pt, en, es, unit, kind = OUTCOMES[o]
        obs = OBS[kind]
        rows.append(
            row(o, "FLOAT64", pt, en, es, obs=obs, measurement_unit=unit,
                original_name=f"{o}_[race]_[gender]_[pctile]")
        )  # fmt: skip
        rows.append(
            row(
                f"{o}_se", "FLOAT64",
                f"Erro-padrão de {o}", f"Standard error of {o}", f"Error estándar de {o}",
                measurement_unit=unit, obs=SE_OBS,
                original_name=f"{o}_[race]_[gender]_[pctile]_se",
            )
        )  # fmt: skip
    return rows


def sample_rows(outcomes: list[str]) -> list[dict]:
    rows = [
        row("parent_rank_mean", "FLOAT64",
            "Posição média da renda domiciliar dos pais, relativa aos pais de crianças da mesma coorte (média de 1994-1995 e 1998-2000)",
            "Mean household income rank of the parents, relative to other parents of children in the same birth cohort (averaged over 1994-1995 and 1998-2000)",
            "Posición media del ingreso del hogar de los padres, relativa a otros padres de niños de la misma cohorte (promedio de 1994-1995 y 1998-2000)",
            measurement_unit="proportion", original_name="par_rank_[race]_[gender]_mean"),
        row("parent_rank_mean_se", "FLOAT64",
            "Erro-padrão de parent_rank_mean", "Standard error of parent_rank_mean",
            "Error estándar de parent_rank_mean",
            measurement_unit="proportion", original_name="par_rank_[race]_[gender]_mean_se"),
        row("child_count_2000", "INT64",
            "Número de crianças com menos de 18 anos no local em 2000 (Censo decenal de 2000)",
            "Number of children under 18 in the place in 2000 (2000 Decennial Census)",
            "Número de niños menores de 18 años en el lugar en 2000 (Censo decenal de 2000)",
            measurement_unit="person", original_name="kid_[race]_[gender]_n"),
        row("child_below_median_count_2000", "FLOAT64",
            "Número estimado de crianças com menos de 18 anos no local em 2000 cujos pais têm renda abaixo da mediana nacional (child_count_2000 vezes share_below_median)",
            "Estimated number of children under 18 in the place in 2000 whose parents' income is below the national median (child_count_2000 times share_below_median)",
            "Número estimado de niños menores de 18 años en el lugar en 2000 cuyos padres tienen ingresos por debajo de la mediana nacional (child_count_2000 por share_below_median)",
            measurement_unit="person",
            obs=("Peso populacional preferido pela fonte para comparar locais; não é inteiro por ser um produto",
                 "The source's preferred population weight for comparing places; not an integer because it is a product",
                 "Peso poblacional preferido por la fuente para comparar lugares; no es entero por ser un producto"),
            original_name="kid_[race]_[gender]_blw_p50_n"),
        row("share_below_median", "FLOAT64",
            "Fração de crianças cujos pais têm renda abaixo da mediana nacional dos pais da mesma coorte, ponderada pelos anos de infância passados no local",
            "Fraction of children whose parents' income is below the national median for parents in the same birth cohort, weighted by childhood years spent in the place",
            "Fracción de niños cuyos padres tienen ingresos por debajo de la mediana nacional de los padres de la misma cohorte, ponderada por los años de infancia pasados en el lugar",
            measurement_unit="proportion", original_name="frac_below_median_[race]_[gender]"),
        row("share_childhood_years", "FLOAT64",
            "Fração média dos anos possíveis de infância (até 23 anos) passados no local, entre crianças que passaram algum tempo nele",
            "Mean fraction of possible childhood years (up to age 23) spent in the place, among children who spent any time there",
            "Fracción media de los años posibles de infancia (hasta los 23 años) pasados en el lugar, entre los niños que pasaron algún tiempo en él",
            measurement_unit="proportion", original_name="frac_years_xw_[race]_[gender]"),
    ]  # fmt: skip
    for o in outcomes:
        rows.append(
            row(
                f"{o}_n", "INT64",
                f"Número de crianças com dados não ausentes usados para estimar {o}",
                f"Number of children with non-missing data used to estimate {o}",
                f"Número de niños con datos no faltantes usados para estimar {o}",
                measurement_unit="person",
                original_name=f"{o}_[race]_[gender]_n",
            )
        )  # fmt: skip
    return rows


def header_outcomes(geo: str) -> tuple[list[str], list[str]]:
    """Return (outcomes with estimates, outcomes with sample sizes), in OUTCOMES order."""
    path = INPUT / ATLAS[geo]["file"]
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as z:
            name = next(n for n in z.namelist() if n.endswith(".csv"))
            with z.open(name) as f:
                line = io.TextIOWrapper(f, encoding="utf-8").readline()
    else:
        with open(path, encoding="utf-8") as f:
            line = f.readline()
    est, n = set(), set()
    for c in next(csv.reader([line.strip()])):
        if any(rx.match(c) for rx in GROUP_RES.values()):
            continue
        m = OUTCOME_RE.match(c)
        if m:
            (n if m["stat"] == "n" else est).add(m["outcome"])
    unknown = (est | n) - set(OUTCOMES)
    if unknown:
        raise KeyError(
            f"{geo}: outcomes without a definition: {sorted(unknown)}"
        )
    return [o for o in OUTCOMES if o in est], [o for o in OUTCOMES if o in n]


def migration_rows() -> list[dict]:
    side = {
        "origin": ("de origem (onde a pessoa morava aos 16 anos)", "of origin (where the person lived at age 16)", "de origen (donde la persona vivía a los 16 años)", "o"),
        "destination": ("de destino (onde a pessoa mora aos 26 anos)", "of destination (where the person lives at age 26)", "de destino (donde la persona vive a los 26 años)", "d"),
    }  # fmt: skip
    rows = []
    for s, (pt, en, es, p) in side.items():
        rows += [
            row(f"{s}_commuting_zone_id", "STRING",
                f"Identificador da zona de deslocamento {pt} (definição de 1990)",
                f"Identifier of the commuting zone {en} (1990 definition)",
                f"Identificador de la zona de desplazamiento {es} (definición de 1990)",
                original_name=f"{p}_cz"),
            row(f"{s}_commuting_zone_name", "STRING",
                f"Nome da zona de deslocamento {pt.split(' (')[0]}",
                f"Name of the commuting zone {en.split(' (')[0]}",
                f"Nombre de la zona de desplazamiento {es.split(' (')[0]}",
                original_name=f"{p}_cz_name"),
            row(f"{s}_state_name", "STRING",
                f"Nome do estado da zona de deslocamento {pt.split(' (')[0]}",
                f"Name of the state of the commuting zone {en.split(' (')[0]}",
                f"Nombre del estado de la zona de desplazamiento {es.split(' (')[0]}",
                original_name=f"{p}_state_name"),
        ]  # fmt: skip
    rows += [
        row("race", "STRING",
            "Raça ou etnia: pooled (todas), asian, black, hispanic, white ou other",
            "Race or ethnicity: pooled (all), asian, black, hispanic, white or other",
            "Raza o etnia: pooled (todas), asian, black, hispanic, white u other",
            obs=("other inclui também pessoas não vinculadas ao Censo decenal ou à ACS",
                 "other also includes people not linked to the Decennial Census or the ACS",
                 "other incluye también a personas no vinculadas al Censo decenal ni a la ACS"),
            original_name="pool"),
        row("parent_income_quintile", "STRING",
            "Quintil da renda dos pais na distribuição nacional, de 1 (mais pobre) a 5 (mais rico), ou pooled (todos)",
            "Parental income quintile in the national distribution, from 1 (poorest) to 5 (richest), or pooled (all)",
            "Quintil del ingreso de los padres en la distribución nacional, de 1 (más pobre) a 5 (más rico), o pooled (todos)",
            obs=("Renda familiar média de 5 anos quando a criança tinha de 14 a 18 anos, do formulário 1040 do pai ou mãe que a declara como dependente",
                 "Five-year average of family income when the child was aged 14 to 18, from the Form 1040 of the parent who claims the child as a dependent",
                 "Promedio de 5 años del ingreso familiar cuando el niño tenía de 14 a 18 años, del formulario 1040 del padre o madre que lo declara como dependiente"),
            original_name="pool"),
        row("individuals", "INT64",
            "Número de pessoas da zona de origem que moram na zona de destino",
            "Number of people from the origin commuting zone living in the destination commuting zone",
            "Número de personas de la zona de origen que viven en la zona de destino",
            measurement_unit="person",
            obs=("Ruído adicionado para privacidade diferencial; algumas células são negativas",
                 "Noise added for differential privacy; some cells are negative",
                 "Ruido añadido para privacidad diferencial; algunas celdas son negativas"),
            original_name="n"),
        row("origin_individuals", "INT64",
            "Número total de pessoas da zona de origem (soma de individuals entre destinos)",
            "Total number of people from the origin commuting zone (individuals summed over destinations)",
            "Número total de personas de la zona de origen (individuals sumado entre destinos)",
            measurement_unit="person", original_name="n_tot_o"),
        row("destination_individuals", "INT64",
            "Número total de pessoas que moram na zona de destino (soma de individuals entre origens)",
            "Total number of people living in the destination commuting zone (individuals summed over origins)",
            "Número total de personas que viven en la zona de destino (individuals sumado entre orígenes)",
            measurement_unit="person", original_name="n_tot_d"),
        row("probability_destination_given_origin", "FLOAT64",
            "Probabilidade de morar no destino dado ter crescido na origem, Pr{D|O} = individuals / origin_individuals",
            "Probability of living in the destination given growing up in the origin, Pr{D|O} = individuals / origin_individuals",
            "Probabilidad de vivir en el destino dado haber crecido en el origen, Pr{D|O} = individuals / origin_individuals",
            measurement_unit="proportion", original_name="pr_d_o"),
        row("probability_origin_given_destination", "FLOAT64",
            "Probabilidade de ter crescido na origem dado morar no destino, Pr{O|D} = individuals / destination_individuals",
            "Probability of having grown up in the origin given living in the destination, Pr{O|D} = individuals / destination_individuals",
            "Probabilidad de haber crecido en el origen dado vivir en el destino, Pr{O|D} = individuals / destination_individuals",
            measurement_unit="proportion", original_name="pr_o_d"),
    ]  # fmt: skip
    return rows


def write(table: str, rows: list[dict]) -> None:
    ARCH_DIR.mkdir(exist_ok=True)
    with open(
        ARCH_DIR / f"{table}.csv", "w", newline="", encoding="utf-8"
    ) as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print(f"{table}: {len(rows)} columns")


def main() -> None:
    keys = {
        "tract": [STATE, COUNTY, TRACT, CZ],
        "county": [STATE, COUNTY, CZ],
        "commuting_zone": [CZ, CZ_NAME],
    }
    for geo in ATLAS:
        est, n = header_outcomes(geo)
        write(
            f"{geo}_outcome",
            keys[geo] + [RACE, GENDER, PCTILE] + outcome_rows(est),
        )
        write(f"{geo}_sample", keys[geo] + [RACE, GENDER] + sample_rows(n))
    write("commuting_zone_migration", migration_rows())


if __name__ == "__main__":
    main()
