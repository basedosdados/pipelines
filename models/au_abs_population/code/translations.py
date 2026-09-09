"""Portuguese and Spanish renderings of the architecture's English column text.

The architecture CSVs are written in English, matching the other ABS datasets in
this repo. The Data Basis backend stores a description and an observations field
per language, so each unique English string is translated exactly once here and
looked up by the registration script. Keyed on the English source string, so a
wording change in the architecture surfaces as a missing key rather than as a
silently stale translation.
"""

DESCRIPTIONS = {
    "Reference year of the estimate, at 30 June": (
        "Ano de referência da estimativa, em 30 de junho",
        "Año de referencia de la estimación, al 30 de junio",
    ),
    "Geographic level of the row, either state or australia": (
        "Nível geográfico da linha, estado ou australia",
        "Nivel geográfico de la fila, estado o australia",
    ),
    "Code of the Australian state or territory": (
        "Código do estado ou território australiano",
        "Código del estado o territorio australiano",
    ),
    "Name of the state, territory or national aggregate as published by the ABS": (
        "Nome do estado, território ou agregado nacional conforme publicado pelo ABS",
        "Nombre del estado, territorio o agregado nacional según publicado por el ABS",
    ),
    "Sex of the population counted": (
        "Sexo da população contabilizada",
        "Sexo de la población contabilizada",
    ),
    "Single year of age of the population counted": (
        "Idade simples da população contabilizada",
        "Edad simple de la población contabilizada",
    ),
    "ABS series identifier of the observation": (
        "Código de série do ABS da observação",
        "Código de serie del ABS de la observación",
    ),
    "Estimated resident population at 30 June": (
        "População residente estimada em 30 de junho",
        "Población residente estimada al 30 de junio",
    ),
    "Reference year of the quarterly observation": (
        "Ano de referência da observação trimestral",
        "Año de referencia de la observación trimestral",
    ),
    "Reference quarter of the observation, from 1 to 4": (
        "Trimestre de referência da observação, de 1 a 4",
        "Trimestre de referencia de la observación, de 1 a 4",
    ),
    "Demographic measure reported by the series": (
        "Medida demográfica reportada pela série",
        "Medida demográfica reportada por la serie",
    ),
    "Unit the value is published in": (
        "Unidade em que o valor é publicado",
        "Unidad en que se publica el valor",
    ),
    "Observed value of the series for the quarter": (
        "Valor observado da série no trimestre",
        "Valor observado de la serie en el trimestre",
    ),
    "Year the population is projected for, at 30 June": (
        "Ano para o qual a população é projetada, em 30 de junho",
        "Año para el cual se proyecta la población, al 30 de junio",
    ),
    "Year of the estimated resident population the projection is based on": (
        "Ano da população residente estimada que serve de base à projeção",
        "Año de la población residente estimada que sirve de base a la proyección",
    ),
    "Projection series, high, medium or low": (
        "Série de projeção, alta, média ou baixa",
        "Serie de proyección, alta, media o baja",
    ),
    "Single year of age of the projected population": (
        "Idade simples da população projetada",
        "Edad simple de la población proyectada",
    ),
    "ABS series identifier of the projected series": (
        "Código de série do ABS da série projetada",
        "Código de serie del ABS de la serie proyectada",
    ),
    "Projected population at 30 June": (
        "População projetada em 30 de junho",
        "Población proyectada al 30 de junio",
    ),
    (
        "Reference year, at 30 June for the population and the financial year "
        "ending 30 June for the components"
    ): (
        "Ano de referência, em 30 de junho para a população e o ano fiscal "
        "encerrado em 30 de junho para os componentes",
        "Año de referencia, al 30 de junio para la población y el año fiscal "
        "terminado el 30 de junio para los componentes",
    ),
    "Code of the Local Government Area": (
        "Código da área de governo local (LGA)",
        "Código del área de gobierno local (LGA)",
    ),
    "Name of the Local Government Area": (
        "Nome da área de governo local (LGA)",
        "Nombre del área de gobierno local (LGA)",
    ),
    "Code of the Australian state or territory containing the LGA": (
        "Código do estado ou território australiano que contém a LGA",
        "Código del estado o territorio australiano que contiene la LGA",
    ),
    "Number of births registered to usual residents of the area during the financial year": (
        "Número de nascimentos registrados de residentes habituais da área durante o ano fiscal",
        "Número de nacimientos registrados de residentes habituales del área durante el año fiscal",
    ),
    "Number of deaths of usual residents of the area during the financial year": (
        "Número de óbitos de residentes habituais da área durante o ano fiscal",
        "Número de defunciones de residentes habituales del área durante el año fiscal",
    ),
    "Births less deaths during the financial year": (
        "Nascimentos menos óbitos durante o ano fiscal",
        "Nacimientos menos defunciones durante el año fiscal",
    ),
    "Number of people who moved into the area from elsewhere in Australia": (
        "Número de pessoas que entraram na área vindas de outra parte da Austrália",
        "Número de personas que entraron al área procedentes de otra parte de Australia",
    ),
    "Number of people who moved out of the area to elsewhere in Australia": (
        "Número de pessoas que saíram da área para outra parte da Austrália",
        "Número de personas que salieron del área hacia otra parte de Australia",
    ),
    "Internal arrivals less internal departures": (
        "Entradas internas menos saídas internas",
        "Entradas internas menos salidas internas",
    ),
    "Number of people who moved into the area from overseas": (
        "Número de pessoas que entraram na área vindas do exterior",
        "Número de personas que entraron al área procedentes del exterior",
    ),
    "Number of people who moved out of the area to overseas": (
        "Número de pessoas que saíram da área para o exterior",
        "Número de personas que salieron del área hacia el exterior",
    ),
    "Overseas arrivals less overseas departures": (
        "Entradas do exterior menos saídas para o exterior",
        "Entradas del exterior menos salidas al exterior",
    ),
    "Area of the region in square kilometres": (
        "Área da região em quilômetros quadrados",
        "Área de la región en kilómetros cuadrados",
    ),
    "Estimated resident population per square kilometre, as published by the ABS": (
        "População residente estimada por quilômetro quadrado, conforme publicado pelo ABS",
        "Población residente estimada por kilómetro cuadrado, según publicado por el ABS",
    ),
    "Code of the Statistical Area Level 2": (
        "Código da área estatística de nível 2 (SA2)",
        "Código del área estadística de nivel 2 (SA2)",
    ),
    "Name of the Statistical Area Level 2": (
        "Nome da área estatística de nível 2 (SA2)",
        "Nombre del área estadística de nivel 2 (SA2)",
    ),
    "Code of the Statistical Area Level 3 containing the SA2": (
        "Código da área estatística de nível 3 (SA3) que contém a SA2",
        "Código del área estadística de nivel 3 (SA3) que contiene la SA2",
    ),
    "Code of the Statistical Area Level 4 containing the SA2": (
        "Código da área estatística de nível 4 (SA4) que contém a SA2",
        "Código del área estadística de nivel 4 (SA4) que contiene la SA2",
    ),
    "Code of the Greater Capital City Statistical Area containing the SA2": (
        "Código da área estatística da grande capital (GCCSA) que contém a SA2",
        "Código del área estadística de la gran capital (GCCSA) que contiene la SA2",
    ),
    "Code of the Australian state or territory containing the SA2": (
        "Código do estado ou território australiano que contém a SA2",
        "Código del estado o territorio australiano que contiene la SA2",
    ),
    "ABS series identifier uniquely identifying a time series across ABS releases": (
        "Código de série do ABS que identifica de forma única uma série temporal entre as publicações do ABS",
        "Código de serie del ABS que identifica de forma única una serie temporal entre las publicaciones del ABS",
    ),
    "Full description of the data item as published by the ABS": (
        "Descrição completa do item de dado conforme publicado pelo ABS",
        "Descripción completa del ítem de dato tal como lo publica el ABS",
    ),
    "Unit of measure of the series as published by the ABS": (
        "Unidade de medida da série conforme publicado pelo ABS",
        "Unidad de medida de la serie según publicado por el ABS",
    ),
    "Publication frequency of the series, Quarter or Annual": (
        "Frequência de publicação da série, trimestral ou anual",
        "Frecuencia de publicación de la serie, trimestral o anual",
    ),
    "ABS catalogue number the series is published under": (
        "Número de catálogo do ABS sob o qual a série é publicada",
        "Número de catálogo del ABS bajo el cual se publica la serie",
    ),
    "Title of the source ABS table the series is published in": (
        "Título da tabela de origem do ABS em que a série é publicada",
        "Título de la tabla de origen del ABS en que se publica la serie",
    ),
    "Date of the first observation of the series": (
        "Data da primeira observação da série",
        "Fecha de la primera observación de la serie",
    ),
    "Date of the last observation of the series": (
        "Data da última observação da série",
        "Fecha de la última observación de la serie",
    ),
}

OBSERVATIONS = {
    "Partition column": ("Coluna de partição", "Columna de partición"),
    (
        "australia marks the national aggregate, which is not a state and "
        "therefore carries a null state_id; state marks one of the eight states "
        "and territories"
    ): (
        "australia indica o agregado nacional, que não é um estado e por isso "
        "tem state_id nulo; state indica um dos oito estados e territórios",
        "australia indica el agregado nacional, que no es un estado y por eso "
        "tiene state_id nulo; state indica uno de los ocho estados y territorios",
    ),
    "Null on national (australia) rows, which are not a state": (
        "Nulo nas linhas nacionais (australia), que não são um estado",
        "Nulo en las filas nacionales (australia), que no son un estado",
    ),
    "Male, Female or Persons, where Persons is the ABS-published total": (
        "Male, Female ou Persons, sendo Persons o total publicado pelo ABS",
        "Male, Female o Persons, siendo Persons el total publicado por el ABS",
    ),
    (
        "Single years 0 to 99 plus the open-ended group 100 and over; stored as "
        "a string because the top group is not a number"
    ): (
        "Idades simples de 0 a 99 mais o grupo aberto 100 and over; armazenado "
        "como texto porque o grupo do topo não é um número",
        "Edades simples de 0 a 99 más el grupo abierto 100 and over; almacenado "
        "como texto porque el grupo superior no es un número",
    ),
    "Foreign key to the au_abs_population series table": (
        "Chave estrangeira para a tabela series de au_abs_population",
        "Clave foránea hacia la tabla series de au_abs_population",
    ),
    "ABS labels each quarter by its final month (Q1 March, Q2 June, Q3 September, Q4 December)": (
        "O ABS rotula cada trimestre pelo seu mês final (Q1 março, Q2 junho, Q3 setembro, Q4 dezembro)",
        "El ABS rotula cada trimestre por su mes final (Q1 marzo, Q2 junio, Q3 septiembre, Q4 diciembre)",
    ),
    (
        "Male, Female or Persons; series that ABS does not break down by sex are "
        "reported as Persons, which is what they count"
    ): (
        "Male, Female ou Persons; as séries que o ABS não desagrega por sexo são "
        "reportadas como Persons, que é o que elas contam",
        "Male, Female o Persons; las series que el ABS no desagrega por sexo se "
        "reportan como Persons, que es lo que cuentan",
    ),
    (
        "Includes estimated_resident_population, natural_increase, "
        "net_overseas_migration, net_interstate_migration, births, deaths and the "
        "interstate and overseas movement flows"
    ): (
        "Inclui estimated_resident_population, natural_increase, "
        "net_overseas_migration, net_interstate_migration, births, deaths e os "
        "fluxos de movimentação interestadual e internacional",
        "Incluye estimated_resident_population, natural_increase, "
        "net_overseas_migration, net_interstate_migration, births, deaths y los "
        "flujos de movimiento interestatal e internacional",
    ),
    (
        "Persons or Number for the state-level series, 000 for the Australia-only "
        "series ABS publishes in thousands, and Percent for the percentage change "
        "series. Where the same measure was published in both thousands and "
        "persons, the persons figure is kept"
    ): (
        "Persons ou Number para as séries estaduais, 000 para as séries apenas da "
        "Austrália que o ABS publica em milhares, e Percent para as séries de "
        "variação percentual. Quando a mesma medida foi publicada em milhares e "
        "em pessoas, mantém-se o valor em pessoas",
        "Persons o Number para las series estatales, 000 para las series solo de "
        "Australia que el ABS publica en miles, y Percent para las series de "
        "variación porcentual. Cuando la misma medida se publicó en miles y en "
        "personas, se conserva el valor en personas",
    ),
    (
        "Unit varies by row and is given by the unit column, so no fixed "
        "measurement unit is set here"
    ): (
        "A unidade varia por linha e é dada pela coluna unit, portanto nenhuma "
        "unidade de medida fixa é definida aqui",
        "La unidad varía por fila y la indica la columna unit, por lo que aquí no "
        "se define ninguna unidad de medida fija",
    ),
    "2022 for the current release; ABS re-bases the projections every few years": (
        "2022 na publicação atual; o ABS reajusta a base das projeções a cada alguns anos",
        "2022 en la publicación actual; el ABS rebasa las proyecciones cada algunos años",
    ),
    (
        "ABS series 1(A) high, 29(B) medium and 45(C) low, which differ in their "
        "assumed fertility, life expectancy, net overseas migration and interstate "
        "migration. The medium series is the one ABS describes as the most likely"
    ): (
        "Séries do ABS 1(A) alta, 29(B) média e 45(C) baixa, que diferem nas "
        "hipóteses de fecundidade, expectativa de vida, migração líquida "
        "internacional e migração interestadual. A série média é a que o ABS "
        "descreve como a mais provável",
        "Series del ABS 1(A) alta, 29(B) media y 45(C) baja, que difieren en los "
        "supuestos de fecundidad, esperanza de vida, migración neta internacional "
        "y migración interestatal. La serie media es la que el ABS describe como "
        "la más probable",
    ),
    (
        "Single years 0 to 84 plus 85 and over for the states, and 0 to 99 plus "
        "100 and over for Australia; stored as a string because the top group is "
        "not a number"
    ): (
        "Idades simples de 0 a 84 mais 85 and over para os estados, e de 0 a 99 "
        "mais 100 and over para a Austrália; armazenado como texto porque o grupo "
        "do topo não é um número",
        "Edades simples de 0 a 84 más 85 and over para los estados, y de 0 a 99 "
        "más 100 and over para Australia; almacenado como texto porque el grupo "
        "superior no es un número",
    ),
    (
        "ABS restates the whole series onto the LGA boundaries current at the "
        "release, which run ahead of the ASGS 2021 LGA directory: 24700 Merri-bek "
        "(renamed from Moreland in 2022) and 71500 East Arnhem / 71700 Groote "
        "Archipelago (split in 2023) have no 2021 entry"
    ): (
        "O ABS reexpressa toda a série nos limites de LGA vigentes na publicação, "
        "que estão à frente do diretório de LGA do ASGS 2021: 24700 Merri-bek "
        "(renomeada de Moreland em 2022) e 71500 East Arnhem / 71700 Groote "
        "Archipelago (desmembradas em 2023) não têm registro em 2021",
        "El ABS reexpresa toda la serie en los límites de LGA vigentes en la "
        "publicación, que van por delante del directorio de LGA del ASGS 2021: "
        "24700 Merri-bek (renombrada desde Moreland en 2022) y 71500 East Arnhem "
        "/ 71700 Groote Archipelago (divididas en 2023) no tienen registro en 2021",
    ),
    (
        "Taken from the components data cube; where absent it is the leading digit "
        "of the LGA code, which is the state digit in the ASGS LGA coding scheme"
    ): (
        "Obtido do data cube de componentes; quando ausente, é o primeiro dígito "
        "do código da LGA, que é o dígito do estado no esquema de codificação de "
        "LGA do ASGS",
        "Obtenido del data cube de componentes; cuando falta, es el primer dígito "
        "del código de la LGA, que es el dígito del estado en el esquema de "
        "codificación de LGA del ASGS",
    ),
    (
        "Published by ABS only for the four most recent financial years, so this "
        "column is null for earlier years of the ERP series"
    ): (
        "Publicado pelo ABS apenas para os quatro anos fiscais mais recentes, "
        "portanto esta coluna é nula nos anos anteriores da série de ERP",
        "Publicado por el ABS solo para los cuatro años fiscales más recientes, "
        "por lo que esta columna es nula en los años anteriores de la serie de ERP",
    ),
    (
        "Published with the latest release only and carried across every year, "
        "because it is a property of the boundary and the whole series is "
        "published on a single boundary vintage"
    ): (
        "Publicado apenas na última divulgação e replicado para todos os anos, "
        "por ser uma propriedade do limite territorial e por toda a série ser "
        "publicada em uma única versão de limites",
        "Publicado solo en la última divulgación y replicado para todos los años, "
        "por ser una propiedad del límite territorial y porque toda la serie se "
        "publica en una única versión de límites",
    ),
    (
        "Populated only for the latest reference year, the one year ABS publishes "
        "it for. It is deliberately not recomputed for earlier years: ABS derives "
        "density from the unrounded area while publishing area rounded to 0.1 "
        "km2, so erp / area_sqkm reproduced only 35% of SA2 values within 0.1 and "
        "erred by up to 2769"
    ): (
        "Preenchido apenas no ano de referência mais recente, o único ano para o "
        "qual o ABS a publica. Não é recalculado para anos anteriores de forma "
        "deliberada: o ABS deriva a densidade da área não arredondada, mas publica "
        "a área arredondada em 0,1 km2, de modo que erp / area_sqkm reproduziu "
        "apenas 35% dos valores de SA2 com erro de até 0,1 e errou em até 2769",
        "Rellenado solo en el año de referencia más reciente, el único año para el "
        "que el ABS la publica. No se recalcula para años anteriores de forma "
        "deliberada: el ABS deriva la densidad del área sin redondear, pero "
        "publica el área redondeada a 0,1 km2, de modo que erp / area_sqkm "
        "reprodujo solo el 35% de los valores de SA2 con error de hasta 0,1 y se "
        "desvió hasta en 2769",
    ),
    (
        "The entire series is published on the ASGS Edition 3 (2021) boundaries: "
        "ABS restates history onto current boundaries at each release, so codes "
        "are comparable across every year of this table but NOT against figures "
        "published in releases before the 2021 edition, which used ASGS 2016. Use "
        "br_bd_diretorios_au.correspondence_sa2_2016_2021 to bridge that break"
    ): (
        "Toda a série é publicada nos limites do ASGS Edição 3 (2021): o ABS "
        "reexpressa o histórico nos limites vigentes a cada divulgação, de modo "
        "que os códigos são comparáveis entre todos os anos desta tabela, mas NÃO "
        "com números publicados antes da edição de 2021, que usavam o ASGS 2016. "
        "Use br_bd_diretorios_au.correspondence_sa2_2016_2021 para vencer essa quebra",
        "Toda la serie se publica en los límites del ASGS Edición 3 (2021): el ABS "
        "reexpresa el histórico en los límites vigentes en cada divulgación, de "
        "modo que los códigos son comparables entre todos los años de esta tabla, "
        "pero NO con cifras publicadas antes de la edición de 2021, que usaban el "
        "ASGS 2016. Use br_bd_diretorios_au.correspondence_sa2_2016_2021 para "
        "salvar esa ruptura",
    ),
    "Primary key; stable ABS-wide identifier such as A2133244X": (
        "Chave primária; identificador estável em todo o ABS, como A2133244X",
        "Clave primaria; identificador estable en todo el ABS, como A2133244X",
    ),
    "Persons, Number, 000 or Percent": (
        "Persons, Number, 000 ou Percent",
        "Persons, Number, 000 o Percent",
    ),
    "3101.0 for the population estimates and 3222.0 for the projections": (
        "3101.0 para as estimativas de população e 3222.0 para as projeções",
        "3101.0 para las estimaciones de población y 3222.0 para las proyecciones",
    ),
}
