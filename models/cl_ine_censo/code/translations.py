"""Spanish -> Portuguese / English for every column description in cl_ine_censo.

Keyed on the FULL description string, never composed from a phrase glossary. A
glossary composes rather than understands: any phrase it lacks passes through
untouched, and the result reads plausibly enough to survive a skim (see the
half-English labels that nearly shipped on us_ed_nces_ccd). Translating each
string as a whole cannot produce that failure mode.

`build_metadata_payloads.py` fails the build on any description missing here, so
a new column cannot ship with Spanish text in all three language fields.
"""

from __future__ import annotations

# {spanish: (portuguese, english)}
TRANSLATIONS: dict[str, tuple[str, str]] = {
    # --- identifiers, geography, dataset scaffolding ----------------------
    "Ano del levantamiento censal": (
        "Ano do levantamento censitario",
        "Year of the census enumeration",
    ),
    "Codigo unico territorial (CUT) de la region, de dos digitos": (
        "Codigo unico territorial (CUT) da regiao, de dois digitos",
        "Unique territorial code (CUT) of the region, two digits",
    ),
    "Codigo unico territorial (CUT) de la provincia, de tres digitos": (
        "Codigo unico territorial (CUT) da provincia, de tres digitos",
        "Unique territorial code (CUT) of the province, three digits",
    ),
    "Codigo unico territorial (CUT) de la comuna, de cinco digitos": (
        "Codigo unico territorial (CUT) da comuna, de cinco digitos",
        "Unique territorial code (CUT) of the commune, five digits",
    ),
    "Identificador de la vivienda, unico en todo el pais": (
        "Identificador do domicilio, unico em todo o pais",
        "Dwelling identifier, unique nationwide",
    ),
    "Identificador del hogar dentro de la vivienda": (
        "Identificador do lar dentro do domicilio",
        "Household identifier within the dwelling",
    ),
    "Identificador de la persona dentro del hogar": (
        "Identificador da pessoa dentro do lar",
        "Person identifier within the household",
    ),
    "Identificador único de distrito": (
        "Identificador unico do distrito",
        "Unique district identifier",
    ),
    "Identificador único de zona censal": (
        "Identificador unico da zona censitaria",
        "Unique census zone identifier",
    ),
    "Identificador único de entidad agrupada (no considera división distrital)": (
        "Identificador unico da entidade agrupada (nao considera a divisao distrital)",
        "Unique identifier of the grouped entity (ignores district division)",
    ),
    "Identificador único de localidad agrupada (no considera división distrital)": (
        "Identificador unico da localidade agrupada (nao considera a divisao distrital)",
        "Unique identifier of the grouped locality (ignores district division)",
    ),
    "Código único identificador de manzana / entidad": (
        "Codigo unico identificador de quarteirao / entidade",
        "Unique identifier code of the block or entity",
    ),
    "Código de manzana urbana/aldea": (
        "Codigo do quarteirao urbano ou de aldeia",
        "Code of the urban block or village block",
    ),
    "Código de zonal censal": (
        "Codigo da zona censitaria",
        "Census zone code",
    ),
    "Código de localidad urbana/rural": (
        "Codigo da localidade urbana ou rural",
        "Urban or rural locality code",
    ),
    "Código de entidad poblada urbana/rural": (
        "Codigo da entidade povoada urbana ou rural",
        "Urban or rural populated entity code",
    ),
    "Código de categoria de entidad": (
        "Codigo da categoria da entidade",
        "Entity category code",
    ),
    "Número de distrito": ("Numero do distrito", "District number"),
    "Nombre de la region": ("Nome da regiao", "Region name"),
    "Nombre de la provincia": ("Nome da provincia", "Province name"),
    "Nombre de la comuna": ("Nome da comuna", "Commune name"),
    "Nombre del distrito censal": (
        "Nome do distrito censitario",
        "Census district name",
    ),
    "Nombre de la localidad": ("Nome da localidade", "Locality name"),
    "Nombre de la entidad poblada": (
        "Nome da entidade povoada",
        "Populated entity name",
    ),
    "Poligono del area censal": (
        "Poligono da area censitaria",
        "Polygon of the census area",
    ),
    "Nivel geografico del registro, que indica de cual de las dos capas cartograficas de INE proviene la fila": (
        "Nivel geografico do registro, que indica de qual das duas camadas cartograficas do INE a linha provem",
        "Geographic level of the record, indicating which of INE's two cartographic layers the row comes from",
    ),
    "Indica si el poligono forma parte de la cartografia base del censo (1) o fue incorporado como area complementaria (0)": (
        "Indica se o poligono faz parte da cartografia base do censo (1) ou foi incorporado como area complementar (0)",
        "Whether the polygon belongs to the census base cartography (1) or was added as a complementary area (0)",
    ),
    "Tipo de manzana censal: URBANO para manzanas urbanas y ALDEA para manzanas de aldea. Nulo en los registros de entidad rural": (
        "Tipo de quarteirao censitario: URBANO para quarteiroes urbanos e ALDEA para quarteiroes de aldeia. Nulo nos registros de entidade rural",
        "Census block type: URBANO for urban blocks and ALDEA for village blocks. Null on rural entity records",
    ),
    "Categoria de la entidad poblada segun INE: Ciudad, Pueblo, Aldea, Caserio, Parcela-Hijuela, Comunidad Indigena, entre otras": (
        "Categoria da entidade povoada segundo o INE: Cidade, Povoado, Aldeia, Lugarejo, Parcela-Hijuela, Comunidade Indigena, entre outras",
        "Category of the populated entity per INE: city, town, village, hamlet, smallholding, indigenous community, among others",
    ),
    "Estrato censal urbano o rural en que se ubica el registro": (
        "Estrato censitario urbano ou rural em que se localiza o registro",
        "Urban or rural census stratum the record falls in",
    ),
    "Estrato censal urbano o rural del area cartografica": (
        "Estrato censitario urbano ou rural da area cartografica",
        "Urban or rural census stratum of the cartographic area",
    ),
    # --- dicionario table --------------------------------------------------
    "Nombre de la tabla a la que pertenece la columna codificada": (
        "Nome da tabela a que pertence a coluna codificada",
        "Name of the table the coded column belongs to",
    ),
    "Nombre de la columna codificada": (
        "Nome da coluna codificada",
        "Name of the coded column",
    ),
    "Codigo almacenado en la columna": (
        "Codigo armazenado na coluna",
        "Code stored in the column",
    ),
    "Cobertura temporal de la correspondencia entre codigo y etiqueta": (
        "Cobertura temporal da correspondencia entre codigo e rotulo",
        "Temporal coverage of the code-to-label correspondence",
    ),
    "Etiqueta correspondiente al codigo": (
        "Rotulo correspondente ao codigo",
        "Label corresponding to the code",
    ),
    # --- person-level questionnaire ---------------------------------------
    "¿Cuál es su sexo?": ("Qual e o seu sexo?", "What is your sex?"),
    "¿Cuántos años cumplidos tiene?": (
        "Quantos anos completos tem?",
        "How old are you in completed years?",
    ),
    "Edad en quinquenios": ("Idade em quinquenios", "Age in five-year bands"),
    "¿Qué relación de parentesco tiene con la jefa o jefe de hogar?": (
        "Qual e o seu parentesco com a chefe ou o chefe do lar?",
        "What is your relationship to the head of household?",
    ),
    "¿Cuál es su estado conyugal o civil actual?": (
        "Qual e o seu estado conjugal ou civil atual?",
        "What is your current marital status?",
    ),
    "¿En qué comuna o país vivía en abril de 2019?": (
        "Em que comuna ou pais morava em abril de 2019?",
        "In which commune or country were you living in April 2019?",
    ),
    "Lugar específico de residencia hace 5 años": (
        "Local especifico de residencia ha 5 anos",
        "Specific place of residence five years ago",
    ),
    "Cuándo nació, ¿en qué comuna o país vivía su madre?": (
        "Quando voce nasceu, em que comuna ou pais morava sua mae?",
        "When you were born, in which commune or country was your mother living?",
    ),
    "Lugar específico de nacimiento": (
        "Local especifico de nascimento",
        "Specific place of birth",
    ),
    "Indica si la persona nacio en Chile o en el extranjero": (
        "Indica se a pessoa nasceu no Chile ou no exterior",
        "Whether the person was born in Chile or abroad",
    ),
    "¿En qué año llegó a vivir a Chile?": (
        "Em que ano veio morar no Chile?",
        "In which year did you come to live in Chile?",
    ),
    "¿Cuál es su nacionalidad?": (
        "Qual e a sua nacionalidade?",
        "What is your nationality?",
    ),
    "Pais de nacionalidad": (
        "Pais de nacionalidade",
        "Country of nationality",
    ),
    "Nacionalidad chilena": ("Nacionalidade chilena", "Chilean nationality"),
    "De acuerdo con sus antepasados, tradiciones y cultura, es o se considera:": (
        "De acordo com seus antepassados, tradicoes e cultura, e ou se considera:",
        "According to your ancestry, traditions and culture, you are or consider yourself:",
    ),
    "¿Es o se considera perteneciente a algún pueblo indígena u originario?": (
        "E ou se considera pertencente a algum povo indigena ou originario?",
        "Are you, or do you consider yourself, a member of an indigenous people?",
    ),
    "Pueblo originario al que declara pertenecer la persona que se autoidentifico como indigena en p28_autoid_pueblo": (
        "Povo originario ao qual declara pertencer a pessoa que se autoidentificou como indigena em p28_autoid_pueblo",
        "Indigenous people the person who self-identified as indigenous in p28_autoid_pueblo declares belonging to",
    ),
    "Afrodescendencia": ("Afrodescendencia", "Afro-descent"),
    "¿Habla o entiende una de las siguientes lenguas indígenas u originarias?": (
        "Fala ou entende uma das seguintes linguas indigenas ou originarias?",
        "Do you speak or understand one of the following indigenous languages?",
    ),
    "Habla o entiende alguna lengua originaria": (
        "Fala ou entende alguma lingua originaria",
        "Speaks or understands an indigenous language",
    ),
    "¿Cuál es su religión o credo?": (
        "Qual e a sua religiao ou credo?",
        "What is your religion or creed?",
    ),
    "Tiene religión o credo": (
        "Tem religiao ou credo",
        "Has a religion or creed",
    ),
    "Diversidades de género": ("Diversidades de genero", "Gender diversity"),
    # difficulty / disability
    "¿Tiene dificultad para ver, aun usando anteojos o lentes?": (
        "Tem dificuldade para enxergar, mesmo usando oculos ou lentes?",
        "Do you have difficulty seeing, even when wearing glasses?",
    ),
    "¿Tiene dificultad para oír, aun usando un dispositivo auditivo": (
        "Tem dificuldade para ouvir, mesmo usando um aparelho auditivo",
        "Do you have difficulty hearing, even when using a hearing aid",
    ),
    "¿Tiene dificultad para caminar o subir escaleras?": (
        "Tem dificuldade para caminhar ou subir escadas?",
        "Do you have difficulty walking or climbing stairs?",
    ),
    "¿Tiene dificultad para recordar o concentrarse?": (
        "Tem dificuldade para lembrar ou se concentrar?",
        "Do you have difficulty remembering or concentrating?",
    ),
    "¿Tiene dificultad para realizar tareas de cuidado personal, como bañarse o vestirse?": (
        "Tem dificuldade para realizar tarefas de cuidado pessoal, como tomar banho ou se vestir?",
        "Do you have difficulty with self-care tasks such as bathing or dressing?",
    ),
    "¿Tiene dificultad para hablar o comunicarse usando su idioma habitual, por ejemplo, entender o ser entendido por otras personas?": (
        "Tem dificuldade para falar ou se comunicar em seu idioma habitual, por exemplo, entender ou ser entendido por outras pessoas?",
        "Do you have difficulty speaking or communicating in your usual language, for example understanding or being understood by others?",
    ),
    "Condicion de discapacidad, derivada del conjunto de preguntas de dificultad funcional p32a a p32f": (
        "Condicao de deficiencia, derivada do conjunto de perguntas de dificuldade funcional p32a a p32f",
        "Disability status, derived from the functional difficulty questions p32a to p32f",
    ),
    # education
    "¿Asiste actualmente a la educación formal? Incluye educación parvularia, especial, básica, media y superior": (
        "Frequenta atualmente a educacao formal? Inclui educacao infantil, especial, fundamental, media e superior",
        "Do you currently attend formal education? Includes preschool, special, primary, secondary and tertiary",
    ),
    "Asistencia a nivel de educación parvularia o preescolar": (
        "Frequencia no nivel de educacao infantil ou pre-escolar",
        "Attendance at preschool level",
    ),
    "Asistencia a nivel de educación básica": (
        "Frequencia no nivel de educacao fundamental",
        "Attendance at primary level",
    ),
    "Asistencia a nivel de educación media": (
        "Frequencia no nivel de educacao media",
        "Attendance at secondary level",
    ),
    "Asistencia a nivel de educación superior": (
        "Frequencia no nivel de educacao superior",
        "Attendance at tertiary level",
    ),
    "¿Sabe leer y escribir?": (
        "Sabe ler e escrever?",
        "Can you read and write?",
    ),
    "Años de escolaridad": ("Anos de escolaridade", "Years of schooling"),
    "Logro educativo de acuerdo con la Clasificación Internacional Normalizada de la Educación": (
        "Nivel educacional alcancado segundo a Classificacao Internacional Normalizada da Educacao",
        "Educational attainment under the International Standard Classification of Education",
    ),
    # labour
    "Situación en la fuerza de trabajo": (
        "Situacao na forca de trabalho",
        "Labour force status",
    ),
    "Categoria ocupacional en el empleo (CISE recodificada): independiente, dependiente o trabajador no remunerado": (
        "Categoria ocupacional no emprego (CISE recodificada): independente, dependente ou trabalhador nao remunerado",
        "Status in employment (recoded ICSE): self-employed, employee, or unpaid worker",
    ),
    "Dependencia económica": ("Dependencia economica", "Economic dependency"),
    "CIUO-08.CL a 1 dígito": (
        "CIUO-08.CL a 1 digito",
        "ISCO-08.CL at 1-digit level",
    ),
    "CAENES a 1 dígito": ("CAENES a 1 digito", "CAENES at 1-digit level"),
    "¿En qué comuna o país se ubica su trabajo?": (
        "Em que comuna ou pais fica o seu trabalho?",
        "In which commune or country is your workplace?",
    ),
    "Lugar de trabajo": ("Local de trabalho", "Place of work"),
    "¿Cuál es el medio de transporte principal que utiliza para dirigirse a su lugar de trabajo?": (
        "Qual e o principal meio de transporte que utiliza para ir ao seu local de trabalho?",
        "What is the main means of transport you use to get to work?",
    ),
    # fertility
    "¿Cuántas hijas e hijos nacidos vivos ha tenido en total?": (
        "Quantas filhas e filhos nascidos vivos teve no total?",
        "How many live-born children have you had in total?",
    ),
    "Del total de hijas e hijos nacidos vivos, ¿cuántas son mujeres?": (
        "Do total de filhas e filhos nascidos vivos, quantas sao mulheres?",
        "Of all live-born children, how many are female?",
    ),
    "Del total de hijas e hijos nacidos vivos, ¿cuántos son hombres?": (
        "Do total de filhas e filhos nascidos vivos, quantos sao homens?",
        "Of all live-born children, how many are male?",
    ),
    "¿Cuántas hijas e hijos están vivos actualmente?": (
        "Quantas filhas e filhos estao vivos atualmente?",
        "How many of your children are currently alive?",
    ),
    "Del total de hijas e hijos que se encuentran vivos, ¿cuántas son mujeres?": (
        "Do total de filhas e filhos que estao vivos, quantas sao mulheres?",
        "Of all surviving children, how many are female?",
    ),
    "Del total de hijas e hijos que se encuentran vivos, ¿cuántos son hombres?": (
        "Do total de filhas e filhos que estao vivos, quantos sao homens?",
        "Of all surviving children, how many are male?",
    ),
    "¿En qué mes y año nació su última hija o hijo nacido vivo? Año": (
        "Em que mes e ano nasceu sua ultima filha ou filho nascido vivo? Ano",
        "In which month and year was your last live-born child born? Year",
    ),
    "¿En qué mes y año nació su última hija o hijo nacido vivo? Mes": (
        "Em que mes e ano nasceu sua ultima filha ou filho nascido vivo? Mes",
        "In which month and year was your last live-born child born? Month",
    ),
    # --- dwelling and household questionnaire ------------------------------
    "Tipo de vivienda particular": (
        "Tipo de domicilio particular",
        "Type of private dwelling",
    ),
    "Estado de ocupación de la vivienda": (
        "Situacao de ocupacao do domicilio",
        "Occupancy status of the dwelling",
    ),
    "Estado de ocupación de la vivienda (detalle)": (
        "Situacao de ocupacao do domicilio (detalhe)",
        "Occupancy status of the dwelling (detail)",
    ),
    "¿Cuál es el material de construcción en las paredes exteriores?": (
        "Qual e o material de construcao das paredes externas?",
        "What is the construction material of the exterior walls?",
    ),
    "¿Cuál es el material de construcción en la cubierta del techo?": (
        "Qual e o material de construcao da cobertura do telhado?",
        "What is the construction material of the roof covering?",
    ),
    "¿Cuál es el material de construcción en el piso?": (
        "Qual e o material de construcao do piso?",
        "What is the construction material of the floor?",
    ),
    "¿Cuántas piezas de esta vivienda se usan exclusivamente como dormitorio?": (
        "Quantos comodos deste domicilio sao usados exclusivamente como dormitorio?",
        "How many rooms in this dwelling are used exclusively as bedrooms?",
    ),
    "El agua que usa esta vivienda proviene principalmente de:": (
        "A agua que este domicilio usa provem principalmente de:",
        "The water this dwelling uses comes mainly from:",
    ),
    "¿Cuál es el sistema de distribución del agua en esta vivienda?": (
        "Qual e o sistema de distribuicao de agua neste domicilio?",
        "What is the water distribution system in this dwelling?",
    ),
    "El servicio higiénico (WC) principal de esta vivienda es o está:": (
        # "fica" rather than "esta": the latter is valid Portuguese but folds to
        # the Spanish "esta", so the residue gate cannot tell it apart.
        "O sanitario (WC) principal deste domicilio e ou fica:",
        "The main toilet (WC) of this dwelling is or is located:",
    ),
    "La electricidad de esta vivienda proviene principalmente de:": (
        "A eletricidade deste domicilio provem principalmente de:",
        "The electricity of this dwelling comes mainly from:",
    ),
    "¿Cuál es el principal medio de eliminación de basura de esta vivienda?": (
        "Qual e o principal meio de eliminacao de lixo deste domicilio?",
        "What is this dwelling's main means of refuse disposal?",
    ),
    "¿Cuántas personas residen habitualmente en esta vivienda?": (
        "Quantas pessoas residem habitualmente neste domicilio?",
        "How many people usually reside in this dwelling?",
    ),
    "1. De las personas que residen habitualmente en esta vivienda, ¿todas comparten los gastos para alimentación?": (
        "Das pessoas que residem habitualmente neste domicilio, todas compartilham as despesas de alimentacao?",
        "Of the people usually residing in this dwelling, do they all share food expenses?",
    ),
    "2. ¿Cuántos grupos tienen gastos separados de alimentación?": (
        "Quantos grupos tem despesas de alimentacao separadas?",
        "How many groups have separate food expenses?",
    ),
    "Número de hogares censados en la vivienda particular": (
        "Numero de lares recenseados no domicilio particular",
        "Number of households enumerated in the private dwelling",
    ),
    "Número de personas censadas en la vivienda": (
        "Numero de pessoas recenseadas no domicilio",
        "Number of people enumerated in the dwelling",
    ),
    "Índice de hacinamiento": ("Indice de adensamento", "Overcrowding index"),
    "Tenencia de la vivienda": ("Posse do domicilio", "Dwelling tenure"),
    "Fuente de energía o combustible para cocinar": (
        "Fonte de energia ou combustivel para cozinhar",
        "Energy or fuel source for cooking",
    ),
    "Fuente de energía o combustible para calefaccionar": (
        "Fonte de energia ou combustivel para aquecimento",
        "Energy or fuel source for heating",
    ),
    "Dispone de telefono movil en el hogar": (
        "Dispoe de telefone movel no lar",
        "Household has a mobile phone",
    ),
    "Dispone de computador en el hogar": (
        "Dispoe de computador no lar",
        "Household has a computer",
    ),
    "Dispone de tablet en el hogar": (
        "Dispoe de tablet no lar",
        "Household has a tablet",
    ),
    "Dispone de conexion a internet fija en el hogar": (
        "Dispoe de conexao de internet fixa no lar",
        "Household has a fixed internet connection",
    ),
    "Dispone de conexion a internet movil en el hogar": (
        "Dispoe de conexao de internet movel no lar",
        "Household has a mobile internet connection",
    ),
    "Dispone de conexion a internet satelital en el hogar": (
        "Dispoe de conexao de internet por satelite no lar",
        "Household has a satellite internet connection",
    ),
    "Tipologia del hogar segun su composicion: unipersonal, nuclear, extenso, compuesto o sin nucleo": (
        "Tipologia do lar segundo sua composicao: unipessoal, nuclear, estendido, composto ou sem nucleo",
        "Household typology by composition: single-person, nuclear, extended, composite, or without a nucleus",
    ),
    "Comuna con menos de 6.000 personas censadas": (
        "Comuna com menos de 6.000 pessoas recenseadas",
        "Commune with fewer than 6,000 people enumerated",
    ),
    "Tipo de operativo": (
        "Tipo de operacao censitaria",
        "Type of census operation",
    ),
}

# --- the 189 aggregate variables of the manzana-entidad and zona-localidad
# bases. Same definitions in both tables, so one entry serves both.
TRANSLATIONS.update(
    {
        # population counts
        "Personas censadas en el area": (
            "Pessoas recenseadas na area",
            "People enumerated in the area",
        ),
        "Hombres censados en el area": (
            "Homens recenseados na area",
            "Men enumerated in the area",
        ),
        "Mujeres censadas en el area": (
            "Mulheres recenseadas na area",
            "Women enumerated in the area",
        ),
        "Personas de 0 a 5 años": (
            "Pessoas de 0 a 5 anos",
            "People aged 0 to 5",
        ),
        "Personas de 6 a 13 años": (
            "Pessoas de 6 a 13 anos",
            "People aged 6 to 13",
        ),
        "Personas de 14 a 17 años": (
            "Pessoas de 14 a 17 anos",
            "People aged 14 to 17",
        ),
        "Personas de 18 a 24 años": (
            "Pessoas de 18 a 24 anos",
            "People aged 18 to 24",
        ),
        "Personas de 25 a 44 años": (
            "Pessoas de 25 a 44 anos",
            "People aged 25 to 44",
        ),
        "Personas de 45 a 59 años": (
            "Pessoas de 45 a 59 anos",
            "People aged 45 to 59",
        ),
        "Personas de 60 años o más": (
            "Pessoas de 60 anos ou mais",
            "People aged 60 or over",
        ),
        "Promedio de edad": ("Idade media", "Mean age"),
        "Personas inmigrantes internacionales (Lugar de nacimiento fuera de Chile)": (
            "Pessoas imigrantes internacionais (local de nascimento fora do Chile)",
            "International immigrants (born outside Chile)",
        ),
        "Personas con otra nacionalidad (no considera aquellos con doble nacionalidad)": (
            "Pessoas com outra nacionalidade (nao considera quem tem dupla nacionalidade)",
            "People of another nationality (excludes dual nationals)",
        ),
        "Personas que son o se consideran pertenecientes a un pueblo originario": (
            "Pessoas que sao ou se consideram pertencentes a um povo originario",
            "People who are or consider themselves members of an indigenous people",
        ),
        "Personas que son o se consideran afrodescendientes": (
            "Pessoas que sao ou se consideram afrodescendentes",
            "People who are or consider themselves of Afro-descent",
        ),
        "Personas que hablan o entienden alguna lengua indígena u originaria": (
            "Pessoas que falam ou entendem alguma lingua indigena ou originaria",
            "People who speak or understand an indigenous language",
        ),
        "Personas que tienen una religión o credo": (
            "Pessoas que tem uma religiao ou credo",
            "People who have a religion or creed",
        ),
        # disability
        "Personas con mucha dificultad o no pueden: Ver, aun usando anteojos": (
            "Pessoas com muita dificuldade ou incapazes de: enxergar, mesmo usando oculos",
            "People with severe difficulty or unable to: see, even wearing glasses",
        ),
        "Personas con mucha dificultad o no pueden: Oír, aun usando un dispositivo auditivo": (
            "Pessoas com muita dificuldade ou incapazes de: ouvir, mesmo usando aparelho auditivo",
            "People with severe difficulty or unable to: hear, even using a hearing aid",
        ),
        "Personas con mucha dificultad o no pueden: Caminar o subir escaleras": (
            "Pessoas com muita dificuldade ou incapazes de: caminhar ou subir escadas",
            "People with severe difficulty or unable to: walk or climb stairs",
        ),
        "Personas con mucha dificultad o no pueden: Recordar o concentrarse": (
            "Pessoas com muita dificuldade ou incapazes de: lembrar ou se concentrar",
            "People with severe difficulty or unable to: remember or concentrate",
        ),
        "Personas con mucha dificultad o no pueden: Realizar tareas de cuidado personas, como bañarse o vestirse": (
            "Pessoas com muita dificuldade ou incapazes de: realizar tarefas de cuidado pessoal, como tomar banho ou se vestir",
            "People with severe difficulty or unable to: perform self-care tasks such as bathing or dressing",
        ),
        "Personas con mucha dificultad o no pueden: Hablar o comunicarse usando su idioma habitual": (
            "Pessoas com muita dificuldade ou incapazes de: falar ou se comunicar em seu idioma habitual",
            "People with severe difficulty or unable to: speak or communicate in their usual language",
        ),
        "Personas con discapacidad (declara mucha dificultad o imposibilidad para realizar algunas de las actividades del módulo de salud)": (
            "Pessoas com deficiencia (declaram muita dificuldade ou impossibilidade de realizar alguma das atividades do modulo de saude)",
            "People with disability (report severe difficulty or inability in any activity of the health module)",
        ),
        # marital status
        "Personas con estado civil o conyugal: Casado": (
            "Pessoas com estado civil ou conjugal: casado",
            "People by marital status: married",
        ),
        "Personas con estado civil o conyugal: Conviviente o pareja sin acuerdo de unión civil": (
            "Pessoas com estado civil ou conjugal: em uniao consensual sem acordo de uniao civil",
            "People by marital status: cohabiting without a civil union agreement",
        ),
        "Personas con estado civil o conyugal: Conviviente civil (con acuerdo de unión civil)": (
            "Pessoas com estado civil ou conjugal: uniao civil (com acordo de uniao civil)",
            "People by marital status: civil partner (with a civil union agreement)",
        ),
        "Personas con estado civil o conyugal: Anulado, separado o divorciado": (
            "Pessoas com estado civil ou conjugal: anulado, separado ou divorciado",
            "People by marital status: annulled, separated or divorced",
        ),
        "Personas con estado civil o conyugal: Viudo": (
            "Pessoas com estado civil ou conjugal: viuvo",
            "People by marital status: widowed",
        ),
        "Personas con estado civil o conyugal: Soltero": (
            "Pessoas com estado civil ou conjugal: solteiro",
            "People by marital status: single",
        ),
        # education
        "Promedio de escolaridad de personas de 18 años o más": (
            "Media de anos de escolaridade das pessoas de 18 anos ou mais",
            "Mean years of schooling of people aged 18 or over",
        ),
        "Personas de 0 a 5 años que asisten a: Educación parvularia": (
            "Pessoas de 0 a 5 anos que frequentam: educacao infantil",
            "People aged 0 to 5 attending: preschool education",
        ),
        "Personas de 6 a 13 años que asisten a: Educación básica": (
            "Pessoas de 6 a 13 anos que frequentam: educacao fundamental",
            "People aged 6 to 13 attending: primary education",
        ),
        "Personas de 14 a 17 años que asisten a: Educación media": (
            "Pessoas de 14 a 17 anos que frequentam: educacao media",
            "People aged 14 to 17 attending: secondary education",
        ),
        "Personas de 18 a 24 años que asisten a: Educación superior": (
            "Pessoas de 18 a 24 anos que frequentam: educacao superior",
            "People aged 18 to 24 attending: tertiary education",
        ),
        "Personas con logro educacional más alto alcanzado (CINE11): Menor a primaria (incluye nunca cursó un programa educativo y Educación de la primera infancia)": (
            "Pessoas por nivel educacional mais alto alcancado (CINE11): abaixo do primario (inclui nunca ter cursado um programa educativo e educacao da primeira infancia)",
            "People by highest educational attainment (ISCED11): below primary (includes never attended an education programme and early childhood education)",
        ),
        "Personas con logro educacional más alto alcanzado (CINE11): Primaria (incluye Primaria en forma parcial, Educación primaria (nivel 1) y Educación primaria (nivel 2))": (
            "Pessoas por nivel educacional mais alto alcancado (CINE11): primario (inclui primario parcial, educacao primaria (nivel 1) e educacao primaria (nivel 2))",
            "People by highest educational attainment (ISCED11): primary (includes partial primary, primary education level 1 and level 2)",
        ),
        "Personas con logro educacional más alto alcanzado (CINE11): Secundaria (incluye Educación secundaria, con orientación general y con orientación vocacional)": (
            "Pessoas por nivel educacional mais alto alcancado (CINE11): secundario (inclui educacao secundaria de orientacao geral e vocacional)",
            "People by highest educational attainment (ISCED11): secondary (includes general and vocational secondary education)",
        ),
        "Personas con logro educacional más alto alcanzado (CINE11): Avanzado (incluye Educación terciaria de ciclo corto, con orientación vocacional, Grado de educación terciaria o nivel equivalente, Maestría y Nivel de doctorado o equivalente)": (
            "Pessoas por nivel educacional mais alto alcancado (CINE11): avancado (inclui educacao terciaria de ciclo curto de orientacao vocacional, grau de educacao terciaria ou equivalente, mestrado e doutorado ou equivalente)",
            "People by highest educational attainment (ISCED11): advanced (includes short-cycle vocational tertiary, bachelor's or equivalent, master's, and doctoral or equivalent)",
        ),
        "Personas con logro educacional más alto alcanzado (CINE11): Educación especial o diferencial": (
            "Pessoas por nivel educacional mais alto alcancado (CINE11): educacao especial",
            "People by highest educational attainment (ISCED11): special education",
        ),
        "Personas de 15 años o más que no saben leer o escribir": (
            "Pessoas de 15 anos ou mais que nao sabem ler nem escrever",
            "People aged 15 or over who cannot read or write",
        ),
        # labour
        "Personas de 15 años o más ocupadas": (
            "Pessoas de 15 anos ou mais ocupadas",
            "Employed people aged 15 or over",
        ),
        "Personas de 15 años o más desocupadas": (
            "Pessoas de 15 anos ou mais desocupadas",
            "Unemployed people aged 15 or over",
        ),
        "Personas de 15 años o más fuera de la fuerza de trabajo": (
            "Pessoas de 15 anos ou mais fora da forca de trabalho",
            "People aged 15 or over outside the labour force",
        ),
        "Personas de 15 años o más ocupadas independientes": (
            "Pessoas de 15 anos ou mais ocupadas independentes",
            "Self-employed people aged 15 or over",
        ),
        "Personas de 15 años o más ocupadas dependientes": (
            "Pessoas de 15 anos ou mais ocupadas dependentes",
            "Employees aged 15 or over",
        ),
        "Personas de 15 años o más ocupadas trabajadores no remunerados": (
            "Pessoas de 15 anos ou mais ocupadas como trabalhadores nao remunerados",
            "Unpaid workers aged 15 or over",
        ),
    }
)

# occupation (CIUO / ISCO) and activity (CAENES / ISIC) breakdowns
_CIUO = {
    "Directores, gerentes y administradores": (
        "Diretores, gerentes e administradores",
        "Managers",
    ),
    "Profesionales, científicos e intelectuales": (
        "Profissionais das ciencias e intelectuais",
        "Professionals",
    ),
    "Técnicos y profesionales de nivel medio": (
        "Tecnicos e profissionais de nivel medio",
        "Technicians and associate professionals",
    ),
    "Personal de apoyo administrativo": (
        "Pessoal de apoio administrativo",
        "Clerical support workers",
    ),
    "Trabajadores de los servicios y vendedores de comercios y mercados": (
        "Trabalhadores dos servicos e vendedores do comercio e de mercados",
        "Service and sales workers",
    ),
    "Agricultores y trabajadores calificados agropecuarios, forestales y pesqueros": (
        "Agricultores e trabalhadores qualificados agropecuarios, florestais e da pesca",
        "Skilled agricultural, forestry and fishery workers",
    ),
    "Artesanos y operarios de oficios": (
        "Artesaos e trabalhadores de oficios",
        "Craft and related trades workers",
    ),
    "Operadores de instalaciones, máquinas y ensambladores": (
        "Operadores de instalacoes, maquinas e montadores",
        "Plant and machine operators and assemblers",
    ),
    "Ocupaciones elementales": (
        "Ocupacoes elementares",
        "Elementary occupations",
    ),
    "Ocupaciones de las fuerzas armadas": (
        "Ocupacoes das forcas armadas",
        "Armed forces occupations",
    ),
}
for _es, (_pt, _en) in _CIUO.items():
    TRANSLATIONS[f"Personas ocupadas del grupo de CIUO: {_es}"] = (
        f"Pessoas ocupadas no grupo da CIUO: {_pt}",
        f"Employed people in ISCO group: {_en}",
    )

_CAENES = {
    "Agricultura, ganadería, silvicultura y pesca": (
        "Agricultura, pecuaria, silvicultura e pesca",
        "Agriculture, forestry and fishing",
    ),
    "Explotación de minas y canteras": (
        "Extracao de minerais e pedreiras",
        "Mining and quarrying",
    ),
    "Industrias manufactureras": (
        "Industrias de transformacao",
        "Manufacturing",
    ),
    "Suministro de electricidad, gas, vapor y aire acondicionado": (
        "Fornecimento de eletricidade, gas, vapor e ar condicionado",
        "Electricity, gas, steam and air conditioning supply",
    ),
    "Suministro de agua; evacuación de aguas residuales, gestión de desechos y descontaminación": (
        "Fornecimento de agua; esgoto, gestao de residuos e descontaminacao",
        "Water supply; sewerage, waste management and remediation",
    ),
    "Construcción": ("Construcao", "Construction"),
    "Comercio al por mayor y al por menor; reparación de vehículos automotores y motocicletas": (
        "Comercio por atacado e varejo; reparacao de veiculos automotores e motocicletas",
        "Wholesale and retail trade; repair of motor vehicles and motorcycles",
    ),
    "Transporte y almacenamiento": (
        "Transporte e armazenagem",
        "Transportation and storage",
    ),
    "Actividades de alojamiento y de servicio de comidas": (
        "Atividades de alojamento e alimentacao",
        "Accommodation and food service activities",
    ),
    "Información y comunicaciones": (
        "Informacao e comunicacao",
        "Information and communication",
    ),
    "Actividades financieras y de seguros": (
        "Atividades financeiras e de seguros",
        "Financial and insurance activities",
    ),
    "Actividades inmobiliarias": (
        "Atividades imobiliarias",
        "Real estate activities",
    ),
    "Actividades profesionales, científicas y técnicas": (
        "Atividades profissionais, cientificas e tecnicas",
        "Professional, scientific and technical activities",
    ),
    "Actividades de servicios administrativos y de apoyo": (
        "Atividades administrativas e servicos complementares",
        "Administrative and support service activities",
    ),
    "Administración pública y defensa; planes de seguridad social de afiliación obligatoria": (
        "Administracao publica e defesa; seguridade social obrigatoria",
        "Public administration and defence; compulsory social security",
    ),
    "Enseñanza": ("Educacao", "Education"),
    "Actividades de atención de la salud humana y de asistencia social": (
        "Atividades de atencao a saude humana e servicos sociais",
        "Human health and social work activities",
    ),
    "Actividades artísticas, de entretenimiento y recreativas": (
        "Artes, cultura, esporte e recreacao",
        "Arts, entertainment and recreation",
    ),
    "Otras actividades de servicios": (
        "Outras atividades de servicos",
        "Other service activities",
    ),
    "Actividades de los hogares como empleadores; actividades no diferenciadas de los hogares como productores de bienes y servicios para uso propio": (
        "Servicos domesticos; atividades nao diferenciadas dos domicilios como produtores de bens e servicos para uso proprio",
        "Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use",
    ),
    "Actividades de organizaciones y órganos extraterritoriales": (
        "Organismos internacionais e outras instituicoes extraterritoriais",
        "Activities of extraterritorial organisations and bodies",
    ),
}
for _es, (_pt, _en) in _CAENES.items():
    TRANSLATIONS[f"Personas ocupadas en actividades de CAENES: {_es}"] = (
        f"Pessoas ocupadas em atividades da CAENES: {_pt}",
        f"Employed people in CAENES activity: {_en}",
    )

_TRANSPORT = {
    "Auto particular": ("automovel particular", "private car"),
    "transporte público": ("transporte publico", "public transport"),
    "Caminando": ("a pe", "walking"),
    "Bicicleta": ("bicicleta", "bicycle"),
    "Motocicleta": ("motocicleta", "motorcycle"),
    "Caballo, lancha o bote": (
        "cavalo, lancha ou barco",
        "horse, launch or boat",
    ),
    "Otro medio": ("outro meio", "other means"),
}
for _es, (_pt, _en) in _TRANSPORT.items():
    TRANSLATIONS[
        f"Personas ocupadas fuera de su vivienda cuyo medio de transporte principal es: {_es}"
    ] = (
        f"Pessoas ocupadas fora de seu domicilio cujo principal meio de transporte e: {_pt}",
        f"Employed people working away from home whose main means of transport is: {_en}",
    )

# household composition, tenure, equipment and fuel
_HOGARES = {
    "Hogares censados en el area": (
        "Lares recenseados na area",
        "Households enumerated in the area",
    ),
    "Promedio de personas por hogar": (
        "Media de pessoas por lar",
        "Mean persons per household",
    ),
    "Hogares unipersonales": ("Lares unipessoais", "Single-person households"),
    "Hogares compuestos solo de personas de 60 años o más": (
        "Lares compostos apenas por pessoas de 60 anos ou mais",
        "Households made up only of people aged 60 or over",
    ),
    "Hogares con al menos una persona de 0 a 14 años": (
        "Lares com ao menos uma pessoa de 0 a 14 anos",
        "Households with at least one person aged 0 to 14",
    ),
    "Hogares con jefatura de hogar femenina": (
        "Lares com chefia feminina",
        "Female-headed households",
    ),
    "Hogares con acceso a internet": (
        "Lares com acesso a internet",
        "Households with internet access",
    ),
    "Hogares allegados": (
        "Lares agregados a outro domicilio",
        "Households sharing another household's dwelling",
    ),
    "Núcleos hacinados allegados": (
        "Nucleos familiares agregados em situacao de adensamento",
        "Overcrowded family nuclei sharing a dwelling",
    ),
    "Hogares que residen en viviendas hacinadas no ampliables": (
        "Lares que residem em domicilios adensados nao ampliaveis",
        "Households living in overcrowded dwellings that cannot be extended",
    ),
    "Déficit habitacional cuantitativo": (
        "Deficit habitacional quantitativo",
        "Quantitative housing deficit",
    ),
    "Viviendas hacinadas": ("Domicilios adensados", "Overcrowded dwellings"),
    "Viviendas irrecuperables": (
        "Domicilios irrecuperaveis",
        "Irrecoverable dwellings",
    ),
    "Viviendas particulares": ("Domicilios particulares", "Private dwellings"),
    "Viviendas particulares ocupadas": (
        "Domicilios particulares ocupados",
        "Occupied private dwellings",
    ),
    "Viviendas particulares desocupadas": (
        "Domicilios particulares desocupados",
        "Unoccupied private dwellings",
    ),
}
TRANSLATIONS.update(_HOGARES)

_EQUIP = {
    "computador": ("computador", "computer"),
    "internet fija": ("internet fixa", "fixed internet"),
    "internet móvil": ("internet movel", "mobile internet"),
    "internet por conexión satelital": (
        "internet por conexao via satelite",
        "satellite internet connection",
    ),
    "tablet": ("tablet", "tablet"),
    "teléfono móvil, celular o smartphone": (
        "telefone movel, celular ou smartphone",
        "mobile phone, cellphone or smartphone",
    ),
}
for _es, (_pt, _en) in _EQUIP.items():
    TRANSLATIONS[f"Hogares con disponibilidad equipos o servicios: {_es}"] = (
        f"Lares com disponibilidade de equipamentos ou servicos: {_pt}",
        f"Households with equipment or services available: {_en}",
    )

_FUEL = {
    "gas": ("gas", "gas"),
    "parafina o pretróleo": ("querosene ou petroleo", "kerosene or paraffin"),
    "leña": ("lenha", "firewood"),
    "pellet": ("pellet", "wood pellets"),
    "carbón": ("carvao", "coal"),
    "electricidad": ("eletricidade", "electricity"),
    "energía solar": ("energia solar", "solar energy"),
    "otra": ("outra", "other"),
    "no utiliza fuente de energía": (
        "nao utiliza fonte de energia",
        "does not use an energy source",
    ),
}
for _purpose, (_ppt, _pen) in {
    "cocinar": ("cozinhar", "cooking"),
    "calefaccionar": ("aquecimento", "heating"),
}.items():
    for _es, (_pt, _en) in _FUEL.items():
        key = f"Hogares con energía o combustible para {_purpose}: {_es}"
        TRANSLATIONS[key] = (
            f"Lares com energia ou combustivel para {_ppt}: {_pt}",
            f"Households with energy or fuel for {_pen}: {_en}",
        )

_TENENCIA = {
    "propia pagada": ("propria quitada", "owned outright"),
    "propia pagándose": (
        "propria em pagamento",
        "owned with mortgage outstanding",
    ),
    "arrendada con contrato": (
        "alugada com contrato",
        "rented with a contract",
    ),
    "arrendada sin contrato": (
        "alugada sem contrato",
        "rented without a contract",
    ),
    "cedida por trabajo o servicio": (
        "cedida por trabalho ou servico",
        "provided through work or service",
    ),
    "cedida por familiar u otro": (
        "cedida por familiar ou outro",
        "provided by a relative or other party",
    ),
    "otro (usufructo, ocupada de hecho, propiedad en sucesión o litigio)": (
        "outro (usufruto, ocupada de fato, propriedade em inventario ou litigio)",
        "other (usufruct, squatted, property in succession or dispute)",
    ),
}
for _es, (_pt, _en) in _TENENCIA.items():
    TRANSLATIONS[f"Hogares con tenencia de la vivienda: {_es}"] = (
        f"Lares com posse do domicilio: {_pt}",
        f"Households by dwelling tenure: {_en}",
    )

# dwelling materials, services and type
for _n, _pt_n in [("1", "1"), ("2", "2"), ("3", "3"), ("4", "4"), ("5", "5")]:
    TRANSLATIONS[
        f"Número de viviendas con {_n} dormitorio" + ("s" if _n != "1" else "")
    ] = (
        f"Numero de domicilios com {_pt_n} dormitorio"
        + ("s" if _n != "1" else ""),
        f"Number of dwellings with {_n} bedroom" + ("s" if _n != "1" else ""),
    )
TRANSLATIONS["Número de viviendas con 6 dormitorios o más"] = (
    "Numero de domicilios com 6 dormitorios ou mais",
    "Number of dwellings with 6 or more bedrooms",
)


def _family(
    prefix_es: str, prefix_pt: str, prefix_en: str, items: dict
) -> None:
    for es, (pt, en) in items.items():
        TRANSLATIONS[f"{prefix_es}: {es}"] = (
            f"{prefix_pt}: {pt}",
            f"{prefix_en}: {en}",
        )


_family(
    "Viviendas con materialidad paredes",
    "Domicilios por material das paredes",
    "Dwellings by wall material",
    {
        "hormigón armado": ("concreto armado", "reinforced concrete"),
        "albañilería": ("alvenaria", "masonry"),
        "tabique forrado por ambas caras": (
            "tabique revestido em ambas as faces",
            "partition clad on both sides",
        ),
        "tabique sin forro interior": (
            "tabique sem revestimento interno",
            "partition without interior cladding",
        ),
        "adobe, barro, pirca, quincha u otro material artesanal": (
            "adobe, barro, pirca, taipa ou outro material artesanal",
            "adobe, mud, drystone, wattle-and-daub or other handmade material",
        ),
        "materiales precarios o de desecho": (
            "materiais precarios ou de descarte",
            "precarious or waste materials",
        ),
    },
)

_family(
    "Viviendas con materialidad techo",
    "Domicilios por material do telhado",
    "Dwellings by roof material",
    {
        "tejas o tejuelas de arcilla, metálicas, de cemento, de madera, asfálticas o plásticas": (
            "telhas de argila, metalicas, de cimento, de madeira, asfalticas ou plasticas",
            "tiles or shingles of clay, metal, cement, wood, asphalt or plastic",
        ),
        "losa hormigón": ("laje de concreto", "concrete slab"),
        "planchas metálicas de zinc, cobre, etc": (
            "chapas metalicas de zinco, cobre, etc",
            "metal sheets of zinc, copper, etc",
        ),
        "planchas de fibrocemento tipo pizarreño": (
            "chapas de fibrocimento",
            "fibre-cement sheets",
        ),
        "fonolita o plancha de fieltro embreado": (
            "fonolita ou chapa de feltro betuminoso",
            "bituminous felt sheeting",
        ),
        "paja, coirón, totora o caña": (
            "palha, coiron, totora ou cana",
            "straw, tussock grass, reed or cane",
        ),
        "materiales precarios o de desecho": (
            "materiais precarios ou de descarte",
            "precarious or waste materials",
        ),
        "sin cubierta sólida de techo": (
            "sem cobertura solida de telhado",
            "no solid roof covering",
        ),
    },
)

_family(
    "Viviendas con materialidad piso",
    "Domicilios por material do piso",
    "Dwellings by floor material",
    {
        "parquet, piso flotante, cerámico, madera, alfombra, flexit, cubrepiso u otro similar: sobre radier o vigas de madera": (
            "parquete, piso laminado, ceramica, madeira, carpete, vinilico ou similar: sobre contrapiso ou vigas de madeira",
            "parquet, laminate, ceramic, wood, carpet, vinyl or similar: over a concrete base or wooden joists",
        ),
        "radier sin revestimiento": (
            "contrapiso sem revestimento",
            "bare concrete base",
        ),
        "baldosa de cemento": ("ladrilho de cimento", "cement tile"),
        "capa de cemento sobre tierra": (
            "camada de cimento sobre terra",
            "cement layer over earth",
        ),
        "tierra": ("terra", "earth"),
    },
)

_family(
    "Viviendas cuya fuente de agua es",
    "Domicilios cuja fonte de agua e",
    "Dwellings whose water source is",
    {
        "red pública": ("rede publica", "public network"),
        "pozo o noria": ("poco ou cacimba", "well or borehole"),
        "camión aljibe": ("caminhao-pipa", "water tanker truck"),
        "río, vertiente, estero, canal, etc": (
            "rio, nascente, corrego, canal, etc",
            "river, spring, stream, canal, etc",
        ),
    },
)

_family(
    "Viviendas con sistema de distribución",
    "Domicilios por sistema de distribuicao de agua",
    "Dwellings by water distribution system",
    {
        "llave dentro de la vivienda": (
            "torneira dentro do domicilio",
            "tap inside the dwelling",
        ),
        "con llave dentro del sitio, fuera de la vivienda": (
            "torneira no terreno, fora do domicilio",
            "tap on the plot, outside the dwelling",
        ),
        "no tiene sistema, la acarrea": (
            "nao tem sistema, transporta a agua",
            "no system, water is carried",
        ),
    },
)

_family(
    "Viviendas con servicio higiénico",
    "Domicilios por tipo de sanitario",
    "Dwellings by toilet facility",
    {
        "dentro de la vivienda, conectado al alcantarillado": (
            "dentro do domicilio, ligado a rede de esgoto",
            "inside the dwelling, connected to the sewer",
        ),
        "fuera de la vivienda, conectado al alcantarillado": (
            "fora do domicilio, ligado a rede de esgoto",
            "outside the dwelling, connected to the sewer",
        ),
        "conectado a una fosa séptica": (
            "ligado a fossa septica",
            "connected to a septic tank",
        ),
        "conectado a pozo negro": (
            "ligado a fossa rudimentar",
            "connected to a cesspit",
        ),
        "en un cajón sobre acequia o canal": (
            "em cabine sobre vala ou canal",
            "in a cubicle over a ditch or canal",
        ),
        "en un cajón conectado a otro sistema": (
            "em cabine ligada a outro sistema",
            "in a cubicle connected to another system",
        ),
        "baño químico": ("banheiro quimico", "chemical toilet"),
        "conectado a baño seco": (
            "ligado a banheiro seco",
            "connected to a dry toilet",
        ),
        "no tiene servicio higiénico": (
            "nao tem sanitario",
            "no toilet facility",
        ),
    },
)

_family(
    "Viviendas con electricidad",
    "Domicilios por fonte de eletricidade",
    "Dwellings by electricity source",
    {
        "red pública": ("rede publica", "public grid"),
        "generador diésel o bencina": (
            "gerador a diesel ou gasolina",
            "diesel or petrol generator",
        ),
        "placa solar": ("placa solar", "solar panel"),
        "energía eólica": ("energia eolica", "wind power"),
        "otro": ("outro", "other"),
        "no tiene energía eléctrica": (
            "nao tem energia eletrica",
            "no electricity",
        ),
    },
)

_family(
    "Viviendas con eliminación de basura",
    "Domicilios por destino do lixo",
    "Dwellings by refuse disposal",
    {
        "la recogen los servicios de aseo": (
            "coletado pelo servico de limpeza",
            "collected by refuse services",
        ),
        "la entierra o la quema": (
            "enterrado ou queimado",
            "buried or burned",
        ),
        "la deja en terreno eriazo, quebrada o zanja": (
            "deixado em terreno baldio, ravina ou vala",
            "left on wasteland, in a ravine or ditch",
        ),
        "la tira al río, laguna o mar": (
            "lancado em rio, lagoa ou mar",
            "thrown into a river, lagoon or the sea",
        ),
        "otro": ("outro", "other"),
    },
)

_family(
    "Viviendas con tipo de vivienda",
    "Domicilios por tipo de domicilio",
    "Dwellings by dwelling type",
    {
        "casa": ("casa", "house"),
        "departamento": ("apartamento", "apartment"),
        "vivienda tradicional indígena": (
            "domicilio tradicional indigena",
            "traditional indigenous dwelling",
        ),
        "pieza en casa antigua o conventillo": (
            "comodo em casa antiga ou cortico",
            "room in an old house or tenement",
        ),
        "mediagua, mejora, etc": (
            "barraco, puxadinho, etc",
            "shack, lean-to, etc",
        ),
        "móvil": ("movel", "mobile"),
        "otro tipo de vivienda": (
            "outro tipo de domicilio",
            "other dwelling type",
        ),
    },
)

del _family, _CIUO, _CAENES, _TRANSPORT, _EQUIP, _FUEL, _TENENCIA, _HOGARES
