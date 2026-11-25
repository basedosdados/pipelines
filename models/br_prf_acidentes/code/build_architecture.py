"""Emit the architecture CSVs and the trilingual columns JSON for br_prf_acidentes.

Writes, for each of the three tables:
  code/architecture/<table>.csv   -- the repo source of truth (PT descriptions)
  code/architecture/<table>_columns.json -- PT/EN/ES, for bulk_upsert_columns

The Google Sheets copy is produced from these CSVs; `upload_columns_from_sheet`
needs a Sheet, but `bulk_upsert_columns` takes the JSON directly and needs none.
"""

from __future__ import annotations

import csv
import json

from models.br_prf_acidentes.code.clean_data import TABLE_COLUMNS
from models.br_prf_acidentes.code.constants import ARCHITECTURE_DIR

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
]

COV_2017 = "2017(1)2026"

# name -> (type, pt, en, es, unit, directory, original_name, coverage, observations)
D: dict[str, tuple] = {}


def col(
    name,
    typ,
    pt,
    en,
    es,
    unit="",
    directory="",
    original="",
    coverage="",
    obs="",
):
    D[name] = (
        typ,
        pt,
        en,
        es,
        unit,
        directory,
        original or name,
        coverage,
        obs,
    )


# ---------------------------------------------------------------- shared

col(
    "ano",
    "INT64",
    "Ano de referência do acidente",
    "Reference year of the crash",
    "Año de referencia del accidente",
    unit="year",
    directory="br_bd_diretorios_data_tempo.ano:ano",
    original="ano / data_inversa",
    obs="Coluna de particionamento. Presente na fonte apenas em ocorrencia 2007-2015; "
    "nos demais arquivos é derivada de data_inversa.",
)
col(
    "data",
    "DATE",
    "Data em que o acidente ocorreu",
    "Date on which the crash occurred",
    "Fecha en que ocurrió el accidente",
    original="data_inversa",
    obs="A fonte publica quatro formatos ao longo da série, e os dois recortes divergem "
    "entre si em 2012-2015: DD/MM/YYYY (ocorrencia 2007-2011; pessoa 2007-2015), "
    "YYYY-MM-DD (ocorrencia 2012-2015 e todos os arquivos de 2017 em diante) e "
    "DD/MM/YY (2016). Cada arquivo usa um único formato em 100% das linhas.",
)
col(
    "horario",
    "TIME",
    "Hora em que o acidente ocorreu",
    "Time at which the crash occurred",
    "Hora en que ocurrió el accidente",
    original="horario",
)
col(
    "dia_semana",
    "STRING",
    "Dia da semana em que o acidente ocorreu",
    "Day of the week on which the crash occurred",
    "Día de la semana en que ocurrió el accidente",
    obs="Padronizado para a grafia usada de 2017 em diante (minúscula e sufixo -feira): "
    "Sexta passa a sexta-feira, Sábado a sábado, e assim por diante.",
)
col(
    "sigla_uf",
    "STRING",
    "Sigla da unidade da federação onde o acidente ocorreu",
    "Abbreviation of the federative unit where the crash occurred",
    "Sigla de la unidad federativa donde ocurrió el accidente",
    directory="br_bd_diretorios_brasil.uf:sigla",
    original="uf",
)
col(
    "id_municipio",
    "STRING",
    "Identificador IBGE de 7 dígitos do município onde o acidente ocorreu",
    "Seven-digit IBGE identifier of the municipality where the crash occurred",
    "Identificador IBGE de 7 dígitos del municipio donde ocurrió el accidente",
    directory="br_bd_diretorios_brasil.municipio:id_municipio",
    original="municipio",
    obs="A fonte publica o NOME do município, nunca o código, em todos os anos. O código "
    "foi resolvido contra o diretório de municípios por correspondência insensível a "
    "acentos, caixa e pontuação, complementada por uma lista de 18 renomeações do IBGE "
    "(por exemplo Embu, hoje Embu das Artes). Restam 16 linhas sem código em 11.879.282: "
    "12 em que a fonte não traz município e 4 de um único acidente cujo nome de município "
    "é incompatível com a UF e a BR publicadas.",
)
col(
    "id_ocorrencia",
    "STRING",
    "Identificador do acidente atribuído pela PRF",
    "Crash identifier assigned by the Federal Highway Police",
    "Identificador del accidente asignado por la Policía Federal de Carreteras",
    original="id",
    obs="Único dentro de cada ano de 2017 em diante. Entre 2007 e 2016 há de 1 a 7 linhas "
    "integralmente duplicadas por ano (35 no total, 0,0016%).",
)
col(
    "br",
    "STRING",
    "Número da rodovia federal onde o acidente ocorreu",
    "Number of the federal highway where the crash occurred",
    "Número de la carretera federal donde ocurrió el accidente",
    obs="Rótulo de rota, não quantidade: soma e média não têm significado, por isso STRING.",
)
col(
    "km",
    "FLOAT64",
    "Marco quilométrico da rodovia onde o acidente ocorreu",
    "Kilometre marker on the highway where the crash occurred",
    "Marcador kilométrico de la carretera donde ocurrió el accidente",
    unit="kilometer",
    obs="Separador decimal ponto até 2015 e vírgula de 2016 em diante na fonte; "
    "normalizado aqui. Valor mínimo publicado de 0,1 km.",
)
col(
    "latitude",
    "FLOAT64",
    "Latitude do local do acidente, em graus decimais",
    "Latitude of the crash location, in decimal degrees",
    "Latitud del lugar del accidente, en grados decimales",
    coverage=COV_2017,
    obs="Publicada apenas de 2017 em diante; nula nos anos anteriores. Valores brutos "
    "preservados, exceto os que não podem ser uma coordenada: a fonte de 2017 traz "
    "latitudes como -1033382874, que é -10,33382874 sem o separador decimal, e esses "
    "valores foram convertidos em nulo (5 latitudes e 33 longitudes em ocorrencia, todas "
    "em 2017). Validação contra br_geobr_mapas: de 681.393 pontos em ocorrencia, 1.188 "
    "(0,17%) caem fora de todos os polígonos de unidade da federação, dos quais 7 são "
    "pares 0,0, e 47.573 (6,98%) caem fora do município ao qual o registro é atribuído, "
    "o que é esperado porque id_municipio vem do campo administrativo da PRF e não da "
    "coordenada.",
)
col(
    "longitude",
    "FLOAT64",
    "Longitude do local do acidente, em graus decimais",
    "Longitude of the crash location, in decimal degrees",
    "Longitud del lugar del accidente, en grados decimales",
    coverage=COV_2017,
    obs="Publicada apenas de 2017 em diante; nula nos anos anteriores. Valores brutos "
    "preservados, exceto os que não podem ser uma coordenada (fora do intervalo de -180 a "
    "180 graus), convertidos em nulo. Ver as observações de latitude.",
)
col(
    "causa_acidente",
    "STRING",
    "Causa presumida do acidente registrada pelo policial",
    "Presumed cause of the crash as recorded by the officer",
    "Causa presunta del accidente registrada por el agente",
    obs="A taxonomia foi integralmente substituída em 2017: 11 causas até 2016 contra 90 "
    "de 2017 em diante, com apenas 2 rótulos em comum. NÃO é comparável entre os dois "
    "períodos, e por isso os rótulos publicados foram mantidos em vez de unificados.",
)
col(
    "tipo_acidente",
    "STRING",
    "Tipo de acidente",
    "Type of crash",
    "Tipo de accidente",
    obs="Categorias foram divididas e recodificadas em 2017: Colisão lateral passa a "
    "distinguir mesmo sentido de sentido oposto, e Colisão com objeto fixo/móvel foi "
    "recodificada duas vezes. Rótulos publicados mantidos; não comparável entre períodos.",
)
col(
    "classificacao_acidente",
    "STRING",
    "Classificação do acidente quanto à gravidade",
    "Classification of the crash by severity",
    "Clasificación del accidente según su gravedad",
    obs="A categoria Ignorado aparece somente até 2016.",
)
col(
    "fase_dia",
    "STRING",
    "Fase do dia no momento do acidente",
    "Phase of the day at the moment of the crash",
    "Fase del día en el momento del accidente",
    obs="Grafia padronizada: Plena noite passa a Plena Noite.",
)
col(
    "sentido_via",
    "STRING",
    "Sentido da via considerando o ponto de colisão",
    "Direction of the carriageway at the point of collision",
    "Sentido de la vía considerando el punto de colisión",
    obs="A categoria Não Informado aparece somente de 2017 em diante.",
)
col(
    "condicao_metereologica",
    "STRING",
    "Condição meteorológica no momento do acidente",
    "Weather condition at the moment of the crash",
    "Condición meteorológica en el momento del accidente",
    obs="Grafia padronizada: Ceu Claro passa a Céu Claro, Ignorada a Ignorado e "
    "Nevoeiro/neblina a Nevoeiro/Neblina. A categoria Garoa/Chuvisco só existe de 2017 "
    "em diante.",
)
col(
    "tipo_pista",
    "STRING",
    "Tipo da pista quanto ao número de faixas",
    "Type of carriageway by number of lanes",
    "Tipo de calzada según el número de carriles",
    obs="Valores: Simples, Dupla e Múltipla.",
)
col(
    "tracado_via",
    "STRING",
    "Traçado da via no local do acidente",
    "Alignment of the road at the crash location",
    "Trazado de la vía en el lugar del accidente",
    obs="Passou a ser multivalorado em 2017: a fonte concatena com ponto e vírgula os "
    "atributos aplicáveis (por exemplo Aclive;Curva;Em Obras), resultando em 1.413 "
    "combinações de 13 atributos. Até 2016 assumia um único valor entre Reta, Curva e "
    "Cruzamento. A cadeia publicada foi preservada sem divisão.",
)
col(
    "uso_solo",
    "STRING",
    "Característica do local do acidente quanto à ocupação do solo",
    "Land use at the crash location",
    "Uso del suelo en el lugar del accidente",
    obs="De 2017 em diante a fonte publica Sim e Não, que o dicionário da PRF define como "
    "Urbano=Sim e Rural=Não; aqui foram convertidos para Urbano e Rural, a grafia usada "
    "até 2016, de modo que a coluna seja comparável ao longo de toda a série.",
)
col(
    "regional",
    "STRING",
    "Superintendência regional da PRF responsável pelo trecho",
    "Regional superintendency of the Federal Highway Police responsible for the stretch",
    "Superintendencia regional de la Policía Federal de Carreteras responsable del tramo",
    coverage=COV_2017,
    obs="Publicada apenas de 2017 em diante.",
)
col(
    "delegacia",
    "STRING",
    "Delegacia da PRF responsável pelo trecho",
    "Federal Highway Police precinct responsible for the stretch",
    "Delegación de la Policía Federal de Carreteras responsable del tramo",
    coverage=COV_2017,
    obs="Publicada apenas de 2017 em diante.",
)
col(
    "uop",
    "STRING",
    "Unidade operacional da PRF em cuja circunscrição o acidente ocorreu",
    "Federal Highway Police operational unit whose area covers the crash",
    "Unidad operativa de la Policía Federal de Carreteras en cuya jurisdicción ocurrió",
    coverage=COV_2017,
    obs="Publicada apenas de 2017 em diante.",
)

# ---------------------------------------------------------------- ocorrencia only

_COUNTS = [
    (
        "quantidade_pessoas",
        "pessoas",
        "Número de pessoas envolvidas no acidente",
        "Number of people involved in the crash",
        "Número de personas involucradas en el accidente",
    ),
    (
        "quantidade_mortos",
        "mortos",
        "Número de pessoas mortas no acidente",
        "Number of people killed in the crash",
        "Número de personas muertas en el accidente",
    ),
    (
        "quantidade_feridos_leves",
        "feridos_leves",
        "Número de pessoas com lesões leves no acidente",
        "Number of people with slight injuries in the crash",
        "Número de personas con lesiones leves en el accidente",
    ),
    (
        "quantidade_feridos_graves",
        "feridos_graves",
        "Número de pessoas com lesões graves no acidente",
        "Number of people with serious injuries in the crash",
        "Número de personas con lesiones graves en el accidente",
    ),
    (
        "quantidade_ilesos",
        "ilesos",
        "Número de pessoas ilesas no acidente",
        "Number of uninjured people in the crash",
        "Número de personas ilesas en el accidente",
    ),
    (
        "quantidade_ignorados",
        "ignorados",
        "Número de pessoas cujo estado físico não foi informado no acidente",
        "Number of people whose physical condition was not reported in the crash",
        "Número de personas cuyo estado físico no fue informado en el accidente",
    ),
    (
        "quantidade_feridos",
        "feridos",
        "Número total de pessoas feridas no acidente, somando lesões leves e graves",
        "Total number of people injured in the crash, slight and serious combined",
        "Número total de personas heridas en el accidente, sumando lesiones leves y graves",
    ),
]
for name, original, pt, en, es in _COUNTS:
    col(name, "INT64", pt, en, es, unit="person", original=original)

col(
    "quantidade_veiculos",
    "INT64",
    "Número de veículos envolvidos no acidente",
    "Number of vehicles involved in the crash",
    "Número de vehículos involucrados en el accidente",
    original="veiculos",
    obs="A unidade de medida é veículos, que ainda não existe no vocabulário de unidades "
    "do back-end; por isso measurement_unit está vazio.",
)

# ---------------------------------------------------------------- person tables

col(
    "id_veiculo",
    "STRING",
    "Identificador do veículo envolvido, atribuído pela PRF",
    "Identifier of the vehicle involved, assigned by the Federal Highway Police",
    "Identificador del vehículo involucrado, asignado por la Policía Federal de Carreteras",
    original="id_veiculo",
)
col(
    "id_pessoa",
    "STRING",
    "Identificador da pessoa envolvida, atribuído pela PRF",
    "Identifier of the person involved, assigned by the Federal Highway Police",
    "Identificador de la persona involucrada, asignado por la Policía Federal de Carreteras",
    original="pesid",
    obs="A chave única da tabela é id_ocorrencia, id_pessoa e id_veiculo. De 2017 em diante "
    "a mesma pessoa aparece associada a mais de um veículo, de modo que id_ocorrencia "
    "com id_pessoa não identifica a linha: em 2024 sobram 6.313 duplicatas sem "
    "id_veiculo e nenhuma com.",
)
col(
    "tipo_veiculo",
    "STRING",
    "Tipo do veículo envolvido",
    "Type of vehicle involved",
    "Tipo de vehículo involucrado",
    obs="Grafia padronizada onde a mudança foi apenas de forma (Motocicletas passa a "
    "Motocicleta, Semi-Reboque a Semireboque, Microônibus a Micro-ônibus). Mudanças "
    "reais de taxonomia foram preservadas: Carroça e Charrete, separadas até 2016, "
    "aparecem como Carroça-charrete de 2017 em diante.",
)
col(
    "marca",
    "STRING",
    "Marca e modelo do veículo, como registrados pela PRF",
    "Make and model of the vehicle, as recorded by the Federal Highway Police",
    "Marca y modelo del vehículo, tal como los registró la Policía Federal de Carreteras",
    obs="Texto livre e não padronizado, com 7.368 valores distintos em 2024 e entradas "
    "inválidas na fonte. Preservado sem tratamento.",
)
col(
    "ano_fabricacao_veiculo",
    "INT64",
    "Ano de fabricação do veículo envolvido",
    "Year of manufacture of the vehicle involved",
    "Año de fabricación del vehículo involucrado",
    unit="year",
    original="ano_fabricacao_veiculo",
    obs="Os valores 0 e 1900 marcam ausência de informação na fonte e foram convertidos "
    "em nulo.",
)
col(
    "tipo_envolvido",
    "STRING",
    "Tipo de envolvimento da pessoa no acidente",
    "Type of the person's involvement in the crash",
    "Tipo de participación de la persona en el accidente",
    obs="As categorias Autor, Vítima e Ciclista aparecem apenas até 2016; Testemunha "
    "apenas de 2017 em diante.",
)
col(
    "estado_fisico",
    "STRING",
    "Estado físico da pessoa envolvida, conforme a gravidade das lesões",
    "Physical condition of the person involved, by severity of injuries",
    "Estado físico de la persona involucrada, según la gravedad de las lesiones",
    obs="Padronizado para o vocabulário de 2017 em diante: Morto passa a Óbito, Ferido "
    "Grave a Lesões Graves, Ferido Leve a Lesões Leves e Ignorado a Não Informado.",
)
col(
    "idade",
    "INT64",
    "Idade da pessoa envolvida, em anos completos",
    "Age of the person involved, in completed years",
    "Edad de la persona involucrada, en años cumplidos",
    unit="year",
    obs="A fonte usa três marcadores de ausência ao longo da série, todos convertidos em "
    "nulo: -1 (2007-2015), NA (pessoa 2015-2016 e pessoa_causa_tipo 2017-2026) e 0 "
    "(pessoa, 2017-2026). Em pessoa, de 2017 em diante, 0 é marcador de ausência e não "
    "recém-nascido: o cruzamento com pessoa_causa_tipo por id_ocorrencia, id_pessoa e "
    "id_veiculo mostra que em 2024 13.503 dessas linhas trazem NA no arquivo irmão "
    "contra apenas 158 zeros genuínos, de modo que a distinção entre criança de menos "
    "de um ano e ausência de informação foi destruída na própria fonte. Até 2016 o zero "
    "é preservado como idade real. Valores fora do intervalo de 0 a 120 anos são erros "
    "de digitação (a fonte contém anos de nascimento e números truncados no campo de "
    "idade) e foram convertidos em nulo.",
)
col(
    "sexo",
    "STRING",
    "Sexo da pessoa envolvida",
    "Sex of the person involved",
    "Sexo de la persona involucrada",
    obs="Padronizado: em 2016 a fonte usa os códigos M, F e I, convertidos para Masculino, "
    "Feminino e Ignorado. O valor Inválido, usado até 2015, indica que a informação não "
    "pôde ser coletada conforme o dicionário da PRF, e foi unificado em Ignorado.",
)

_FLAGS = [
    ("indicador_ileso", "ilesos", "ileso", "uninjured", "ileso"),
    (
        "indicador_ferido_leve",
        "feridos_leves",
        "com lesões leves",
        "slightly injured",
        "con lesiones leves",
    ),
    (
        "indicador_ferido_grave",
        "feridos_graves",
        "com lesões graves",
        "seriously injured",
        "con lesiones graves",
    ),
    ("indicador_morto", "mortos", "morta", "killed", "muerta"),
]
for name, original, pt_adj, en_adj, es_adj in _FLAGS:
    col(
        name,
        "STRING",
        f"Indicador de que a pessoa envolvida foi classificada como {pt_adj}",
        f"Indicator that the person involved was classified as {en_adj}",
        f"Indicador de que la persona involucrada fue clasificada como {es_adj}",
        original=original,
        coverage=COV_2017,
        obs="Indicador binário (0 ou 1) no nível da pessoa, publicado apenas de 2017 em "
        "diante. Não confundir com a coluna de mesmo nome na fonte da tabela "
        "ocorrencia, que é uma contagem de pessoas. Mantido como STRING porque valores "
        "0 e 1 são códigos, não quantidades.",
    )

col(
    "nacionalidade",
    "STRING",
    "Nacionalidade da pessoa envolvida",
    "Nationality of the person involved",
    "Nacionalidad de la persona involucrada",
    coverage="2007(1)2016",
    obs="Publicada apenas até 2016; nula de 2017 em diante.",
)
col(
    "naturalidade",
    "STRING",
    "Município de naturalidade da pessoa envolvida",
    "Municipality of birth of the person involved",
    "Municipio de nacimiento de la persona involucrada",
    coverage="2007(1)2016",
    obs="Publicada apenas até 2016, como nome de município e sem unidade da federação, "
    "razão pela qual não foi resolvida para código do IBGE.",
)

# ---------------------------------------------------------------- causa_tipo only

col(
    "causa_principal",
    "STRING",
    "Indicador de que a causa registrada na linha é a causa principal do acidente",
    "Indicator that the cause recorded on the row is the crash's main cause",
    "Indicador de que la causa registrada en la fila es la causa principal del accidente",
    coverage=COV_2017,
    obs="Valores Sim e Não. Existe porque, de 2017 em diante, um acidente pode ter mais de "
    "uma causa registrada.",
)
col(
    "ordem_tipo_acidente",
    "STRING",
    "Ordem do tipo de acidente dentro da ocorrência",
    "Order of the crash type within the occurrence",
    "Orden del tipo de accidente dentro de la ocurrencia",
    coverage=COV_2017,
    obs="Número de sequência, não quantidade: soma e média não têm significado, por isso "
    "STRING.",
)

# ---------------------------------------------------------------- emit

SENSITIVE = {"idade", "sexo", "nacionalidade", "naturalidade"}


def build() -> None:
    ARCHITECTURE_DIR.mkdir(parents=True, exist_ok=True)
    for table, columns in TABLE_COLUMNS.items():
        rows, cols_json = [], []
        for name in columns:
            if name not in D:
                raise KeyError(
                    f"{table}: no description defined for column {name!r}"
                )
            typ, pt, en, es, unit, directory, original, coverage, obs = D[name]
            rows.append(
                {
                    "name": name,
                    "bigquery_type": typ,
                    "description": pt,
                    "temporal_coverage": coverage,
                    "covered_by_dictionary": "no",
                    "directory_column": directory,
                    "measurement_unit": unit,
                    "has_sensitive_data": "yes" if name in SENSITIVE else "no",
                    "observations": obs,
                    "original_name": original,
                }
            )
            cols_json.append(
                {
                    "name": name,
                    "bigquery_type": typ,
                    "description_pt": pt,
                    "description_en": en,
                    "description_es": es,
                    "temporal_coverage": coverage,
                    "covered_by_dictionary": False,
                    "directory_column": directory,
                    "measurement_unit": unit,
                    "has_sensitive_data": name in SENSITIVE,
                    "observations": obs,
                    "is_partition": name == "ano",
                    "is_primary_key": False,
                }
            )
        csv_path = ARCHITECTURE_DIR / f"{table}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=FIELDS, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        json_path = ARCHITECTURE_DIR / f"{table}_columns.json"
        json_path.write_text(
            json.dumps(cols_json, ensure_ascii=False, indent=1)
        )
        print(
            f"{table:20} {len(rows):>3} columns -> {csv_path.name}, {json_path.name}"
        )


if __name__ == "__main__":
    build()
