"""Portuguese and Spanish renderings of the architecture's English text.

The DIME source and its codebook are in English, so ``architecture.py`` holds
the English wording and this module carries the other two languages required by
the house style. Translations are keyed by the English string rather than by
column, so a description reused across tables is translated once and cannot
drift between them.

``missing()`` reports any architecture string that has no translation yet, and
``gen_metadata.py`` refuses to build a payload while anything is missing —
silently shipping an English description as Portuguese is the failure mode this
guards against.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import architecture as arch

# English -> (Portuguese, Spanish)
DESCRIPTIONS: dict[str, tuple[str, str]] = {
    # -- shared / temporal -------------------------------------------------
    "Two-year election cycle during which the contribution was recorded": (
        "Ciclo eleitoral bienal durante o qual a contribuição foi registrada",
        "Ciclo electoral bienal durante el cual se registró la contribución",
    ),
    "Two-year election cycle the entry refers to": (
        "Ciclo eleitoral bienal a que se refere o registro",
        "Ciclo electoral bienal al que se refiere el registro",
    ),
    "Two-year election cycle the amount refers to": (
        "Ciclo eleitoral bienal a que se refere o valor",
        "Ciclo electoral bienal al que se refiere el monto",
    ),
    # -- contribution identifiers -----------------------------------------
    "Unique identifier of the contribution record, assigned by DIME": (
        "Identificador único do registro de contribuição, atribuído pelo DIME",
        "Identificador único del registro de contribución, asignado por DIME",
    ),
    "Unique identifier of the contributor, assigned by DIME entity resolution": (
        "Identificador único do contribuinte, atribuído pela resolução de "
        "entidades do DIME",
        "Identificador único del contribuyente, asignado por la resolución de "
        "entidades de DIME",
    ),
    "Unique identifier of the recipient candidate or committee, assigned by DIME": (
        "Identificador único do candidato ou comitê beneficiário, atribuído pelo DIME",
        "Identificador único del candidato o comité receptor, asignado por DIME",
    ),
    "Unique identifier of the candidate or committee, assigned by DIME": (
        "Identificador único do candidato ou comitê, atribuído pelo DIME",
        "Identificador único del candidato o comité, asignado por DIME",
    ),
    "Original transaction identifier from the FEC electronic filing": (
        "Identificador original da transação na declaração eletrônica da FEC",
        "Identificador original de la transacción en la declaración electrónica de la FEC",
    ),
    "Original committee identifier from the FEC electronic filing": (
        "Identificador original do comitê na declaração eletrônica da FEC",
        "Identificador original del comité en la declaración electrónica de la FEC",
    ),
    "Identifier of an earlier record in the database that this record repeats": (
        "Identificador de um registro anterior da base que este registro repete",
        "Identificador de un registro anterior de la base que este registro repite",
    ),
    # -- contribution measures --------------------------------------------
    "Date on which the contribution was transacted": (
        "Data em que a contribuição foi transacionada",
        "Fecha en que se realizó la contribución",
    ),
    "Dollar amount of the contribution": (
        "Valor da contribuição em dólares",
        "Monto de la contribución en dólares",
    ),
    "FEC code for the type of transaction": (
        "Código da FEC para o tipo de transação",
        "Código de la FEC para el tipo de transacción",
    ),
    "Type of election the contribution was made for": (
        "Tipo de eleição para a qual a contribuição foi feita",
        "Tipo de elección para la cual se realizó la contribución",
    ),
    "Elected office sought by the recipient": (
        "Cargo eletivo pretendido pelo beneficiário",
        "Cargo electivo al que aspira el receptor",
    ),
    "Office sought by the candidate": (
        "Cargo pretendido pelo candidato",
        "Cargo al que aspira el candidato",
    ),
    # -- contributor attributes -------------------------------------------
    "Full name of the contributor, cleaned and standardized by DIME": (
        "Nome completo do contribuinte, limpo e padronizado pelo DIME",
        "Nombre completo del contribuyente, limpiado y estandarizado por DIME",
    ),
    "Last name of the contributor": (
        "Sobrenome do contribuinte",
        "Apellido del contribuyente",
    ),
    "First name of the contributor": (
        "Primeiro nome do contribuinte",
        "Nombre de pila del contribuyente",
    ),
    "Middle name or initial of the contributor": (
        "Nome do meio ou inicial do contribuinte",
        "Segundo nombre o inicial del contribuyente",
    ),
    "Name suffix of the contributor": (
        "Sufixo do nome do contribuinte",
        "Sufijo del nombre del contribuyente",
    ),
    "Title of the contributor": (
        "Título do contribuinte",
        "Título del contribuyente",
    ),
    "First name, middle name, suffix and title of the contributor concatenated": (
        "Primeiro nome, nome do meio, sufixo e título do contribuinte concatenados",
        "Nombre de pila, segundo nombre, sufijo y título del contribuyente concatenados",
    ),
    "Whether the contributor is an individual or an organization": (
        "Se o contribuinte é pessoa física ou organização",
        "Si el contribuyente es una persona física o una organización",
    ),
    "Gender of the contributor": (
        "Gênero do contribuinte",
        "Género del contribuyente",
    ),
    "Street address reported by the contributor": (
        "Endereço declarado pelo contribuinte",
        "Dirección declarada por el contribuyente",
    ),
    "City reported by the contributor": (
        "Cidade declarada pelo contribuinte",
        "Ciudad declarada por el contribuyente",
    ),
    "Two-letter state abbreviation reported by the contributor": (
        "Sigla de duas letras do estado declarado pelo contribuinte",
        "Sigla de dos letras del estado declarado por el contribuyente",
    ),
    "ZIP code reported by the contributor": (
        "Código postal declarado pelo contribuinte",
        "Código postal declarado por el contribuyente",
    ),
    "Congressional district the contributor's geocode falls in": (
        "Distrito congressional em que recai a geocodificação do contribuinte",
        "Distrito congresional en el que recae la geocodificación del contribuyente",
    ),
    "Census tract the contributor's geocode falls in": (
        "Setor censitário em que recai a geocodificação do contribuinte",
        "Sector censal en el que recae la geocodificación del contribuyente",
    ),
    "Latitude of the contributor's geocoded address": (
        "Latitude do endereço geocodificado do contribuinte",
        "Latitud de la dirección geocodificada del contribuyente",
    ),
    "Longitude of the contributor's geocoded address": (
        "Longitude do endereço geocodificado do contribuinte",
        "Longitud de la dirección geocodificada del contribuyente",
    ),
    "Confidence in the accuracy of the geocoded coordinates, from 0 to 1": (
        "Confiança na acurácia das coordenadas geocodificadas, de 0 a 1",
        "Confianza en la exactitud de las coordenadas geocodificadas, de 0 a 1",
    ),
    "Occupation reported by the contributor": (
        "Ocupação declarada pelo contribuinte",
        "Ocupación declarada por el contribuyente",
    ),
    "Employer reported by the contributor": (
        "Empregador declarado pelo contribuinte",
        "Empleador declarado por el contribuyente",
    ),
    "Industry category the reported occupation was mapped to": (
        "Categoria setorial à qual a ocupação declarada foi associada",
        "Categoría sectorial a la que se asoció la ocupación declarada",
    ),
    "Whether the contributing committee is a corporation or trade organization": (
        "Se o comitê contribuinte é uma empresa ou associação empresarial",
        "Si el comité contribuyente es una empresa o asociación empresarial",
    ),
    "Whether the contributor is a corporation or trade organization": (
        "Se o contribuinte é uma empresa ou associação empresarial",
        "Si el contribuyente es una empresa o asociación empresarial",
    ),
    # -- recipient side on the contribution table --------------------------
    "Name of the recipient candidate or committee": (
        "Nome do candidato ou comitê beneficiário",
        "Nombre del candidato o comité receptor",
    ),
    "Party of the recipient": (
        "Partido do beneficiário",
        "Partido del receptor",
    ),
    "Whether the recipient is a candidate or a committee": (
        "Se o beneficiário é um candidato ou um comitê",
        "Si el receptor es un candidato o un comité",
    ),
    "Two-letter state abbreviation of the recipient": (
        "Sigla de duas letras do estado do beneficiário",
        "Sigla de dos letras del estado del receptor",
    ),
    "Common-space CFscore of the contributor": (
        "CFscore do contribuinte no espaço comum",
        "CFscore del contribuyente en el espacio común",
    ),
    "Common-space CFscore of the recipient": (
        "CFscore do beneficiário no espaço comum",
        "CFscore del receptor en el espacio común",
    ),
    "Whether the record was excluded from the CFscore estimation": (
        "Se o registro foi excluído da estimação dos CFscores",
        "Si el registro fue excluido de la estimación de los CFscores",
    ),
    "Memo field from the FEC electronic filing": (
        "Campo de memorando da declaração eletrônica da FEC",
        "Campo de memorando de la declaración electrónica de la FEC",
    ),
    "Auxiliary memo field from the FEC electronic filing": (
        "Campo auxiliar de memorando da declaração eletrônica da FEC",
        "Campo auxiliar de memorando de la declaración electrónica de la FEC",
    ),
    "Original recipient name from the FEC electronic filing": (
        "Nome original do beneficiário na declaração eletrônica da FEC",
        "Nombre original del receptor en la declaración electrónica de la FEC",
    ),
    "Form type of the FEC electronic filing the record came from": (
        "Tipo de formulário da declaração eletrônica da FEC de que veio o registro",
        "Tipo de formulario de la declaración electrónica de la FEC del que proviene el registro",
    ),
    # -- recipient table ---------------------------------------------------
    "Identifier of the same person in the contributor database": (
        "Identificador da mesma pessoa na base de contribuintes",
        "Identificador de la misma persona en la base de contribuyentes",
    ),
    "Election cycle prefixed by the two-letter state code": (
        "Ciclo eleitoral precedido da sigla de duas letras do estado",
        "Ciclo electoral precedido de la sigla de dos letras del estado",
    ),
    "Adjusted ICPSR legislator identifier with the election cycle appended": (
        "Identificador ICPSR ajustado do legislador, com o ciclo eleitoral anexado",
        "Identificador ICPSR ajustado del legislador, con el ciclo electoral añadido",
    ),
    "Adjusted ICPSR legislator identifier without the cycle suffix": (
        "Identificador ICPSR ajustado do legislador, sem o sufixo de ciclo",
        "Identificador ICPSR ajustado del legislador, sin el sufijo de ciclo",
    ),
    "Candidate identifier assigned by the FEC or the state reporting agency": (
        "Identificador do candidato atribuído pela FEC ou pelo órgão estadual declarante",
        "Identificador del candidato asignado por la FEC o por el organismo estatal declarante",
    ),
    "Committee identifier assigned by the FEC or the state reporting agency": (
        "Identificador do comitê atribuído pela FEC ou pelo órgão estadual declarante",
        "Identificador del comité asignado por la FEC o por el organismo estatal declarante",
    ),
    "Candidate identifier assigned by CRP/NIMSP": (
        "Identificador do candidato atribuído pelo CRP/NIMSP",
        "Identificador del candidato asignado por CRP/NIMSP",
    ),
    "ICPSR identifier held before switching parties": (
        "Identificador ICPSR anterior à troca de partido",
        "Identificador ICPSR anterior al cambio de partido",
    ),
    "ICPSR identifier held after switching parties": (
        "Identificador ICPSR posterior à troca de partido",
        "Identificador ICPSR posterior al cambio de partido",
    ),
    "Year of the election the campaign targets, as listed by the FEC": (
        "Ano da eleição visada pela campanha, conforme registrado pela FEC",
        "Año de la elección objetivo de la campaña, según lo registrado por la FEC",
    ),
    "Name of the candidate or committee": (
        "Nome do candidato ou comitê",
        "Nombre del candidato o comité",
    ),
    "Last name of the candidate": (
        "Sobrenome do candidato",
        "Apellido del candidato",
    ),
    "First name of the candidate": (
        "Primeiro nome do candidato",
        "Nombre de pila del candidato",
    ),
    "Middle name of the candidate": (
        "Nome do meio do candidato",
        "Segundo nombre del candidato",
    ),
    "First name, middle name, suffix and title of the candidate concatenated": (
        "Primeiro nome, nome do meio, sufixo e título do candidato concatenados",
        "Nombre de pila, segundo nombre, sufijo y título del candidato concatenados",
    ),
    "Title of the candidate": ("Título do candidato", "Título del candidato"),
    "Name suffix of the candidate": (
        "Sufixo do nome do candidato",
        "Sufijo del nombre del candidato",
    ),
    "Gender of the candidate": ("Gênero do candidato", "Género del candidato"),
    "Party of the candidate or committee": (
        "Partido do candidato ou comitê",
        "Partido del candidato o comité",
    ),
    "Party held before a party switch": (
        "Partido anterior à troca de legenda",
        "Partido anterior al cambio de afiliación",
    ),
    "Two-letter state abbreviation of the candidate or committee": (
        "Sigla de duas letras do estado do candidato ou comitê",
        "Sigla de dos letras del estado del candidato o comité",
    ),
    "District code of the seat sought": (
        "Código do distrito da cadeira pretendida",
        "Código del distrito del escaño al que se aspira",
    ),
    "District and cycle the candidate contested": (
        "Distrito e ciclo em que o candidato concorreu",
        "Distrito y ciclo en que el candidato compitió",
    ),
    "Incumbency status of the candidate": (
        "Situação do candidato quanto à titularidade do cargo",
        "Situación del candidato respecto a la titularidad del cargo",
    ),
    "Cycle-specific CFscore of the recipient": (
        "CFscore do beneficiário específico do ciclo",
        "CFscore del receptor específico del ciclo",
    ),
    "CFscore of the recipient estimated from their own personal donations": (
        "CFscore do beneficiário estimado a partir de suas próprias doações pessoais",
        "CFscore del receptor estimado a partir de sus propias donaciones personales",
    ),
    "DW-DIME score of the recipient": (
        "Escore DW-DIME do beneficiário",
        "Puntaje DW-DIME del receptor",
    ),
    "Composite ideological score of the recipient": (
        "Escore ideológico composto do beneficiário",
        "Puntaje ideológico compuesto del receptor",
    ),
    "First-dimension common-space DW-NOMINATE score": (
        "Escore DW-NOMINATE de primeira dimensão no espaço comum",
        "Puntaje DW-NOMINATE de primera dimensión en el espacio común",
    ),
    "Second-dimension common-space DW-NOMINATE score": (
        "Escore DW-NOMINATE de segunda dimensão no espaço comum",
        "Puntaje DW-NOMINATE de segunda dimensión en el espacio común",
    ),
    "First-dimension Nokken-Poole period-specific DW-NOMINATE score": (
        "Escore DW-NOMINATE de primeira dimensão de Nokken-Poole, específico do período",
        "Puntaje DW-NOMINATE de primera dimensión de Nokken-Poole, específico del período",
    ),
    "Second-dimension Nokken-Poole period-specific DW-NOMINATE score": (
        "Escore DW-NOMINATE de segunda dimensão de Nokken-Poole, específico do período",
        "Puntaje DW-NOMINATE de segunda dimensión de Nokken-Poole, específico del período",
    ),
    "Ideal point of the recipient from an IRT count model applied to PAC data": (
        "Ponto ideal do beneficiário obtido de um modelo IRT de contagem aplicado a "
        "dados de PACs",
        "Punto ideal del receptor obtenido de un modelo IRT de conteo aplicado a "
        "datos de PACs",
    ),
    "Number of distinct donors who gave to the recipient during the cycle": (
        "Número de doadores distintos que contribuíram ao beneficiário no ciclo",
        "Número de donantes distintos que contribuyeron al receptor en el ciclo",
    ),
    "Number of distinct donors who gave to the recipient over their whole career": (
        "Número de doadores distintos que contribuíram ao beneficiário ao longo de "
        "toda a sua carreira",
        "Número de donantes distintos que contribuyeron al receptor a lo largo de "
        "toda su carrera",
    ),
    "Total contributions raised during the cycle": (
        "Total de contribuições arrecadadas no ciclo",
        "Total de contribuciones recaudadas en el ciclo",
    ),
    "Total campaign disbursements during the cycle": (
        "Total de desembolsos de campanha no ciclo",
        "Total de desembolsos de campaña en el ciclo",
    ),
    "Total itemized contributions from individuals raised during the cycle": (
        "Total de contribuições individuais discriminadas arrecadadas no ciclo",
        "Total de contribuciones individuales desglosadas recaudadas en el ciclo",
    ),
    "Total unitemized contributions from individuals raised during the cycle": (
        "Total de contribuições individuais não discriminadas arrecadadas no ciclo",
        "Total de contribuciones individuales no desglosadas recaudadas en el ciclo",
    ),
    "Total raised from PACs and other committees during the cycle": (
        "Total arrecadado junto a PACs e outros comitês no ciclo",
        "Total recaudado de PACs y otros comités en el ciclo",
    ),
    "Total raised from party committees during the cycle": (
        "Total arrecadado junto a comitês partidários no ciclo",
        "Total recaudado de comités partidarios en el ciclo",
    ),
    "Total raised from the candidate's own contributions during the cycle": (
        "Total arrecadado a partir de contribuições do próprio candidato no ciclo",
        "Total recaudado a partir de contribuciones del propio candidato en el ciclo",
    ),
    "Total independent expenditures made to support the candidate": (
        "Total de gastos independentes realizados em apoio ao candidato",
        "Total de gastos independientes realizados en apoyo al candidato",
    ),
    "Total independent expenditures made to oppose the candidate": (
        "Total de gastos independentes realizados em oposição ao candidato",
        "Total de gastos independientes realizados en oposición al candidato",
    ),
    "Vote share won in the primary election, as reported by the FEC": (
        "Percentual de votos obtido na eleição primária, conforme reportado pela FEC",
        "Porcentaje de votos obtenido en la elección primaria, según lo reportado por la FEC",
    ),
    "Outcome of the primary election": (
        "Resultado da eleição primária",
        "Resultado de la elección primaria",
    ),
    "Vote share won in the general election, as reported by the FEC": (
        "Percentual de votos obtido na eleição geral, conforme reportado pela FEC",
        "Porcentaje de votos obtenido en la elección general, según lo reportado por la FEC",
    ),
    "Outcome of the general election": (
        "Resultado da eleição geral",
        "Resultado de la elección general",
    ),
    "Outcome of the special election, as coded by the FEC": (
        "Resultado da eleição especial, conforme codificado pela FEC",
        "Resultado de la elección especial, según la codificación de la FEC",
    ),
    "Outcome of the run-off election, as coded by the FEC": (
        "Resultado do segundo turno, conforme codificado pela FEC",
        "Resultado de la segunda vuelta, según la codificación de la FEC",
    ),
    "Two-party vote share won by the Democratic presidential nominee in the district": (
        "Percentual de votos entre os dois principais partidos obtido pelo candidato "
        "democrata à presidência no distrito",
        "Porcentaje de votos entre los dos principales partidos obtenido por el "
        "candidato demócrata a la presidencia en el distrito",
    ),
    "Status of the candidate's campaign as assigned by the FEC": (
        "Situação da campanha do candidato conforme atribuída pela FEC",
        "Situación de la campaña del candidato según la asignación de la FEC",
    ),
    "FEC interest group category of the committee": (
        "Categoria de grupo de interesse do comitê segundo a FEC",
        "Categoría de grupo de interés del comité según la FEC",
    ),
    "FEC code for the type of committee": (
        "Código da FEC para o tipo de comitê",
        "Código de la FEC para el tipo de comité",
    ),
    "Whether the recipient met the requirements for inclusion in the CFscore estimation": (
        "Se o beneficiário atendeu aos requisitos de inclusão na estimação dos CFscores",
        "Si el receptor cumplió los requisitos de inclusión en la estimación de los CFscores",
    ),
    "Party name assigned by NIMSP": (
        "Nome do partido atribuído pelo NIMSP",
        "Nombre del partido asignado por NIMSP",
    ),
    "Incumbency status assigned by NIMSP": (
        "Situação de titularidade do cargo atribuída pelo NIMSP",
        "Situación de titularidad del cargo asignada por NIMSP",
    ),
    "District number assigned by NIMSP": (
        "Número do distrito atribuído pelo NIMSP",
        "Número del distrito asignado por NIMSP",
    ),
    "State office sought, as coded by NIMSP": (
        "Cargo estadual pretendido, conforme codificado pelo NIMSP",
        "Cargo estatal al que se aspira, según la codificación de NIMSP",
    ),
    "Election outcome as recorded by NIMSP": (
        "Resultado eleitoral conforme registrado pelo NIMSP",
        "Resultado electoral según lo registrado por NIMSP",
    ),
    # -- contributor table -------------------------------------------------
    "Whether the contributor was projected onto the space rather than estimated within it": (
        "Se o contribuinte foi projetado no espaço em vez de estimado dentro dele",
        "Si el contribuyente fue proyectado en el espacio en lugar de estimado dentro de él",
    ),
    "Number of distinct recipients in the scaling that received contributions from the donor": (
        "Número de beneficiários distintos incluídos na escala que receberam "
        "contribuições do doador",
        "Número de receptores distintos incluidos en la escala que recibieron "
        "contribuciones del donante",
    ),
    "First election cycle in which the donor was recorded as active": (
        "Primeiro ciclo eleitoral em que o doador foi registrado como ativo",
        "Primer ciclo electoral en que el donante fue registrado como activo",
    ),
    "Last election cycle in which the donor was recorded as active": (
        "Último ciclo eleitoral em que o doador foi registrado como ativo",
        "Último ciclo electoral en que el donante fue registrado como activo",
    ),
    "Name reported by the contributor on their most recent record": (
        "Nome declarado pelo contribuinte em seu registro mais recente",
        "Nombre declarado por el contribuyente en su registro más reciente",
    ),
    "Street address reported by the contributor on their most recent record": (
        "Endereço declarado pelo contribuinte em seu registro mais recente",
        "Dirección declarada por el contribuyente en su registro más reciente",
    ),
    "City reported by the contributor on their most recent record": (
        "Cidade declarada pelo contribuinte em seu registro mais recente",
        "Ciudad declarada por el contribuyente en su registro más reciente",
    ),
    "Two-letter state abbreviation reported on the most recent record": (
        "Sigla de duas letras do estado declarado no registro mais recente",
        "Sigla de dos letras del estado declarado en el registro más reciente",
    ),
    "ZIP code reported by the contributor on their most recent record": (
        "Código postal declarado pelo contribuinte em seu registro mais recente",
        "Código postal declarado por el contribuyente en su registro más reciente",
    ),
    "Latitude geocoded from the most recent record": (
        "Latitude geocodificada a partir do registro mais recente",
        "Latitud geocodificada a partir del registro más reciente",
    ),
    "Longitude geocoded from the most recent record": (
        "Longitude geocodificada a partir do registro mais recente",
        "Longitud geocodificada a partir del registro más reciente",
    ),
    "Occupation reported by the contributor on their most recent record": (
        "Ocupação declarada pelo contribuinte em seu registro mais recente",
        "Ocupación declarada por el contribuyente en su registro más reciente",
    ),
    "Employer reported by the contributor on their most recent record": (
        "Empregador declarado pelo contribuinte em seu registro mais recente",
        "Empleador declarado por el contribuyente en su registro más reciente",
    ),
    "Identifier of the contributor's most recent contribution record": (
        "Identificador do registro de contribuição mais recente do contribuinte",
        "Identificador del registro de contribución más reciente del contribuyente",
    ),
    "Date of the contributor's most recent contribution record": (
        "Data do registro de contribuição mais recente do contribuinte",
        "Fecha del registro de contribución más reciente del contribuyente",
    ),
    "Total amount contributed by the donor during the cycle": (
        "Valor total contribuído pelo doador durante o ciclo",
        "Monto total aportado por el donante durante el ciclo",
    ),
    # -- dicionario --------------------------------------------------------
    "Slug of the us_stanford_dime table the dictionary entry describes": (
        "Slug da tabela de us_stanford_dime que a entrada do dicionário descreve",
        "Slug de la tabla de us_stanford_dime que describe la entrada del diccionario",
    ),
    "Name of the column the dictionary entry describes": (
        "Nome da coluna que a entrada do dicionário descreve",
        "Nombre de la columna que describe la entrada del diccionario",
    ),
    "Code stored in the column": (
        "Código armazenado na coluna",
        "Código almacenado en la columna",
    ),
    "Temporal coverage of the dictionary entry": (
        "Cobertura temporal da entrada do dicionário",
        "Cobertura temporal de la entrada del diccionario",
    ),
    "Label the code stands for": (
        "Rótulo que o código representa",
        "Etiqueta que representa el código",
    ),
}


def missing() -> dict[str, list[str]]:
    """Return architecture strings with no translation."""
    from observations_i18n import OBSERVATIONS

    d_missing, o_missing = [], []
    for cols in arch.TABLES.values():
        for c in cols:
            if c[2] and c[2] not in DESCRIPTIONS:
                d_missing.append(c[2])
            if c[8] and c[8] not in OBSERVATIONS:
                o_missing.append(c[8])
    return {
        "descriptions": sorted(set(d_missing)),
        "observations": sorted(set(o_missing)),
    }


if __name__ == "__main__":
    m = missing()
    for kind, items in m.items():
        print(f"{kind}: {len(items)} untranslated")
        for s in items:
            print(f"  - {s}")
