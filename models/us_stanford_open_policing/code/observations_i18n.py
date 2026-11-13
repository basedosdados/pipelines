"""Portuguese and Spanish for every ``observations`` string in architecture.py.

Keyed by the English text. ``translate`` raises on a missing key, so a new or
edited observation cannot reach the backend untranslated.
"""

_BOOL = (
    "Valores TRUE/FALSE da fonte mantidos como texto; vazio quando a agência não registra a informação",
    "Valores TRUE/FALSE de la fuente mantenidos como texto; vacío cuando la agencia no registra la información",
)
_FREE = (
    "Texto livre como registrado pela agência; o formato varia entre jurisdições",
    "Texto libre tal como lo registra la agencia; el formato varía entre jurisdicciones",
)

OBSERVATIONS = {
    "Derived from date. Partition column. Null for the few stops whose agency recorded no date": (
        "Derivado de date. Coluna de partição. Nulo para as poucas abordagens sem data registrada pela agência",
        "Derivado de date. Columna de partición. Nulo para las pocas detenciones sin fecha registrada por la agencia",
    ),
    "Taken from the source file name. Partition column of the staging files": (
        "Extraído do nome do arquivo de origem. Coluna de partição dos arquivos de staging",
        "Tomado del nombre del archivo de origen. Columna de partición de los archivos de staging",
    ),
    "Not in the source. Added by Data Basis from state_abbreviation": (
        "Não consta da fonte. Adicionado pela Base dos Dados a partir de state_abbreviation",
        "No consta en la fuente. Añadido por Base de los Datos a partir de state_abbreviation",
    ),
    "Taken from the source file name. One source file per jurisdiction. Join to the jurisdiction table for its name and type": (
        "Extraído do nome do arquivo de origem. Um arquivo de origem por jurisdição. Cruze com a tabela jurisdiction para obter seu nome e tipo",
        "Tomado del nombre del archivo de origen. Un archivo de origen por jurisdicción. Cruce con la tabla jurisdiction para obtener su nombre y tipo",
    ),
    "Where the project merged several raw rows into one stop, holds their raw row numbers joined by a pipe": (
        "Quando o projeto uniu várias linhas brutas em uma abordagem, contém os números dessas linhas separados por barra vertical",
        "Cuando el proyecto unió varias filas brutas en una detención, contiene los números de esas filas separados por barra vertical",
    ),
    "Where the agency reports only the year or quarter, set to the first day of that period. Null for the few stops whose agency recorded no date": (
        "Quando a agência informa apenas o ano ou o trimestre, é fixada no primeiro dia desse período. Nula para as poucas abordagens sem data registrada pela agência",
        "Cuando la agencia informa solo el año o el trimestre, se fija en el primer día de ese período. Nula para las pocas detenciones sin fecha registrada por la agencia",
    ),
    "Sometimes the concatenation of several raw fields, such as street number and street name": (
        "Às vezes é a concatenação de vários campos brutos, como número e nome da rua",
        "A veces es la concatenación de varios campos brutos, como el número y el nombre de la calle",
    ),
    "Provided by the agency or geocoded by the project from the location. Geocodes more than 4 standard deviations from the jurisdiction's median were set to null": (
        "Fornecida pela agência ou geocodificada pelo projeto a partir de location. Geocódigos a mais de 4 desvios-padrão da mediana da jurisdição foram anulados",
        "Proporcionada por la agencia o geocodificada por el proyecto a partir de location. Los geocódigos a más de 4 desviaciones estándar de la mediana de la jurisdicción se anularon",
    ),
    "Only filled for the jurisdictions refreshed in the 2023 release": (
        "Preenchido apenas para as jurisdições atualizadas na publicação de 2023",
        "Completado solo para las jurisdicciones actualizadas en la publicación de 2023",
    ),
    "Some agencies report a neighborhood instead of a location or beat": (
        "Algumas agências informam o bairro em vez do local ou da ronda",
        "Algunas agencias informan el barrio en lugar del lugar o la ronda",
    ),
    "Police geography as recorded by the agency or, where the project had the department's shapefiles and the stop's coordinates, assigned by spatial join": (
        "Divisão policial como registrada pela agência ou, quando o projeto tinha os shapefiles do departamento e as coordenadas da abordagem, atribuída por junção espacial",
        "División policial tal como la registra la agencia o, cuando el proyecto tenía los shapefiles del departamento y las coordenadas de la detención, asignada por unión espacial",
    ),
    "Computed from the date of birth and the stop date where the agency reports a date of birth, so it can be fractional. Values outside 10-110 set to null": (
        "Calculada a partir da data de nascimento e da data da abordagem quando a agência informa a data de nascimento, por isso pode ser fracionária. Valores fora de 10-110 foram anulados",
        "Calculada a partir de la fecha de nacimiento y la fecha de la detención cuando la agencia informa la fecha de nacimiento, por lo que puede ser fraccionaria. Los valores fuera de 10-110 se anularon",
    ),
    "Standardized by the project from the agency's own categories": (
        "Padronizada pelo projeto a partir das categorias da própria agência",
        "Estandarizada por el proyecto a partir de las categorías de la propia agencia",
    ),
    "The badge number itself is withheld by the project for privacy. Where badge numbers are not unique (South Carolina statewide, Seattle) the hash also uses other officer attributes. Not comparable across jurisdictions": (
        "O número do distintivo é retido pelo projeto por privacidade. Onde os números não são únicos (South Carolina statewide, Seattle) o hash também usa outros atributos do policial. Não é comparável entre jurisdições",
        "El número de placa es retenido por el proyecto por privacidad. Donde los números no son únicos (South Carolina statewide, Seattle) el hash también usa otros atributos del policía. No es comparable entre jurisdicciones",
    ),
    "Computed from the date of birth and the stop date where available. Values outside 10-100 set to null": (
        "Calculada a partir da data de nascimento e da data da abordagem quando disponível. Valores fora de 10-100 foram anulados",
        "Calculada a partir de la fecha de nacimiento y la fecha de la detención cuando está disponible. Los valores fuera de 10-100 se anularon",
    ),
    "Free text as recorded by the agency; format varies across jurisdictions": _FREE,
    "Free text as recorded by the agency; format varies across jurisdictions. Multiple violations are separated by a pipe": (
        _FREE[0] + ". Várias infrações são separadas por barra vertical",
        _FREE[1] + ". Varias infracciones se separan con barra vertical",
    ),
    "Source values TRUE/FALSE kept as text; empty when the agency does not record it": _BOOL,
    "Source values TRUE/FALSE kept as text; empty when the agency does not record it. Set to null by the project where search_conducted is null": (
        _BOOL[0] + ". Anulado pelo projeto quando search_conducted é nulo",
        _BOOL[1] + ". Anulado por el proyecto cuando search_conducted es nulo",
    ),
    "Source values TRUE/FALSE kept as text; empty when the agency does not record it. Only defined when contraband_found is TRUE": (
        _BOOL[0] + ". Definido apenas quando contraband_found é TRUE",
        _BOOL[1] + ". Definido solo cuando contraband_found es TRUE",
    ),
    "Source values TRUE/FALSE kept as text; empty when the agency does not record it. A frisk is distinct from a search, but some agencies record frisks as a search type": (
        _BOOL[0]
        + ". A revista superficial é distinta da busca, mas algumas agências a registram como um tipo de busca",
        _BOOL[1]
        + ". El cacheo es distinto del registro, pero algunas agencias lo registran como un tipo de registro",
    ),
    "Source values TRUE/FALSE kept as text; empty when the agency does not record it. Excludes frisks where the agency distinguishes the two": (
        _BOOL[0]
        + ". Exclui revistas superficiais quando a agência distingue as duas",
        _BOOL[1] + ". Excluye los cacheos cuando la agencia distingue ambos",
    ),
    "Source values TRUE/FALSE kept as text; empty when the agency does not record it. Only defined when search_conducted is TRUE": (
        _BOOL[0] + ". Definido apenas quando search_conducted é TRUE",
        _BOOL[1] + ". Definido solo cuando search_conducted es TRUE",
    ),
    "Where a search happened but no reason was recorded, the project assumes probable cause": (
        "Quando houve busca sem motivo registrado, o projeto assume causa provável",
        "Cuando hubo registro sin motivo registrado, el proyecto asume causa probable",
    ),
    "Null for any year before 1800": (
        "Nulo para qualquer ano anterior a 1800",
        "Nulo para cualquier año anterior a 1800",
    ),
    "Taken from the source file name": (
        "Extraído do nome do arquivo de origem",
        "Tomado del nombre del archivo de origen",
    ),
    "Added by Data Basis from state_abbreviation": (
        "Adicionado pela Base dos Dados a partir de state_abbreviation",
        "Añadido por Base de los Datos a partir de state_abbreviation",
    ),
    "As named in the project's README": (
        "Conforme nomeado no README do projeto",
        "Tal como se nombra en el README del proyecto",
    ),
    "2020-04-01 for most jurisdictions; 2023-01-26 for the 12 refreshed in the 2023 update": (
        "2020-04-01 para a maioria das jurisdições; 2023-01-26 para as 12 atualizadas em 2023",
        "2020-04-01 para la mayoría de las jurisdicciones; 2023-01-26 para las 12 actualizadas en 2023",
    ),
    "Computed by Data Basis. A few jurisdictions have isolated records decades before their main coverage": (
        "Calculado pela Base dos Dados. Algumas jurisdições têm registros isolados décadas antes de sua cobertura principal",
        "Calculado por Base de los Datos. Algunas jurisdicciones tienen registros aislados décadas antes de su cobertura principal",
    ),
    "Computed by Data Basis": (
        "Calculado pela Base dos Dados",
        "Calculado por Base de los Datos",
    ),
}


def translate(text: str) -> tuple[str, str]:
    if not text:
        return "", ""
    return OBSERVATIONS[text]
