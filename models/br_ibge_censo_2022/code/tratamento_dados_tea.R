library(readxl)
library(tidyverse)
# Tabela 10145 --------------------------------------------------------
tabela10145 <- read_excel("~/BD/TEA/raw-data/tabela10145.xlsx")

# Qual o número da primeira linha onde aparece "MU"
linha_inicio <- which(tabela10145[[1]] == "MU")[1]

# Filtre o dataframe a partir dessa linha
populacao_tea_sexo_grupo_idade <- tabela10145[linha_inicio:nrow(tabela10145), ]

# Renomeando colunas
colnames(populacao_tea_sexo_grupo_idade) <- c("Rede", "id_municipio", "nome_municipio","grupo_idade","homens","mulheres")

#  preencher para baixo os valores não-NA
populacao_tea_sexo_grupo_idade <- populacao_tea_sexo_grupo_idade %>%
  fill(Rede, id_municipio, nome_municipio, .direction = "down")

# Removendo a coluna Rede
populacao_tea_sexo_grupo_idade <- populacao_tea_sexo_grupo_idade %>%
  select(-Rede,-nome_municipio)

# Pivotando para criar a coluna Sexo
populacao_tea_sexo_grupo_idade <- populacao_tea_sexo_grupo_idade %>%
  pivot_longer(
    cols = c(homens, mulheres),
    names_to = "sexo",
    values_to = "populacao_tea"
  )

# Trocando o hífem por NA
populacao_tea_sexo_grupo_idade <- populacao_tea_sexo_grupo_idade %>%
  mutate(
    populacao_tea = na_if(populacao_tea, "-"),
    ano = 2022,
    sexo = str_to_title(sexo)
  ) %>%
  select(ano,id_municipio,grupo_idade,sexo, populacao_tea)

write.csv(populacao_tea_sexo_grupo_idade, "~/BD/TEA/input/populacao_tea_sexo_grupo_idade.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10147 -----------------------------------------------------------------
tabela10147 <- read_excel("~/BD/TEA/raw-data/tabela 10147.xlsx", sheet = "Tabela")

populacao_tea_raca_cor <- tabela10147 %>%
  slice(33:n()) %>%  # seleciona da linha onde começam os municípios 
  rename( # Renomeando colunas
    nome_municipio = 1,
    Branca = 2,
    Preta = 3,
    Amarela = 4,
    Parda = 5,
    Indígena = 6
  )

# Removendo a última coluna "Fonte = IBGE"
populacao_tea_raca_cor <- populacao_tea_raca_cor %>%
  slice(-n())

# Pivotando para criar a coluna cor_raca
populacao_tea_raca_cor <- populacao_tea_raca_cor %>%
  pivot_longer(
    cols = c(Branca,Preta,Amarela,Parda,Indígena),
    names_to = "raca_cor",
    values_to = "populacao_tea"
  )

# Trocando o hífem por NA
populacao_tea_raca_cor <- populacao_tea_raca_cor %>%
  mutate(
    populacao_tea = na_if(populacao_tea, "-"),
    ano = 2022
  ) %>%
  select(ano,nome_municipio,raca_cor, populacao_tea)

# Seleciona id_municipio e nome_municipio
ids_nome <- populacao_tea_sexo_grupo_idade %>%
  distinct(id_municipio, nome_municipio)

# Adiciona a coluna id_municipio em populacao_tea_raca unindo pelo nome_municipio
populacao_tea_raca_cor <- populacao_tea_raca_cor %>%
  left_join(ids_nome, by = "nome_municipio")

# Removendo a sigla do estado do nome_municipio
populacao_tea_raca_cor$nome_municipio <- sub("\\s*\\(.*\\)$", "", populacao_tea_raca_cor$nome_municipio)

# Removendo nome_municipio
populacao_tea_raca_cor <- populacao_tea_raca_cor %>%
  select(ano,id_municipio,raca_cor,populacao_tea,-nome_municipio)

write.csv(populacao_tea_raca_cor, "~/BD/TEA/input/populacao_tea_raca_cor.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10168 ------------------------------------------------------------
tabela10168 <- read_excel("~/BD/TEA/raw-data/tabela 10168.xlsx")

populacao_tea_indigena <- tabela10145 %>%
  slice(32:n()) %>%
  rename( # Renomeando colunas
    nome_municipio = 1,
    populacao_tea = 2
  )

# Removendo a última coluna "Fonte = IBGE"
populacao_tea_indigena <- populacao_tea_indigena %>%
  slice(-n())

# Trocando o hífem por NA
populacao_tea_indigena <- populacao_tea_indigena %>%
  mutate(
    populacao_tea = na_if(populacao_tea, "-"),
    ano = 2022
  ) %>%
  select(ano,nome_municipio, populacao_tea)

# Lê o arquivo CSV com os nomes e ids dos municípios
ids_nome_muncipio <- read_csv("ids_nome_muncipio.csv")

# Faz o join para adicionar os ids dos municípios à base populacao_tea_indigena
populacao_tea_indigena <- populacao_tea_indigena %>%
  left_join(ids_nome_muncipio, by = "nome_municipio") 

# Seleciona apenas as colunas de interesse
populacao_tea_indigena <- populacao_tea_indigena %>%  
  select(ano,id_municipio,populacao_tea)
  
write.csv(populacao_tea_indigena, "~/BD/TEA/input/populacao_tea_indigena.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10148 ------------------------------------------------------------
tabela10148 <- read_excel("~/BD/TEA/raw-data/tabela10148.xlsx", sheet = "Tabela 2")

# Qual o número da primeira linha onde aparece "MU"
linha_inicio <- which(tabela10148[[1]] == "MU")[1]

# Filtre o dataframe a partir dessa linha
estudantes_tea_sexo_grupo_idade <- tabela10148[linha_inicio:nrow(tabela10148), ]

# Renomeando colunas
colnames(estudantes_tea_sexo_grupo_idade) <- c("Rede", "id_municipio", "nome_municipio","grupo_idade","total","homens","mulheres")

#  preencher para baixo os valores não-NA
estudantes_tea_sexo_grupo_idade <- estudantes_tea_sexo_grupo_idade %>%
  fill(id_municipio, .direction = "down")

# Removendo colunas
estudantes_tea_sexo_grupo_idade <- estudantes_tea_sexo_grupo_idade %>%
  select(-Rede,-nome_municipio,-total)

# Remove as linhas onde grupo_idade é igual a "Total"
estudantes_tea_sexo_grupo_idade <-estudantes_tea_sexo_grupo_idade %>%
  filter(grupo_idade != "Total")

# Pivotando para criar a coluna Sexo
estudantes_tea_sexo_grupo_idade <- estudantes_tea_sexo_grupo_idade %>%
  pivot_longer(
    cols = c(homens, mulheres),
    names_to = "sexo",
    values_to = "populacao_tea"
  )

# Trocando o hífem por NA
estudantes_tea_sexo_grupo_idade <- estudantes_tea_sexo_grupo_idade %>%
  mutate(
    populacao_tea = na_if(populacao_tea, "-"),
    ano = 2022,
    sexo = str_to_title(sexo)
  ) %>%
  select(ano,id_municipio,grupo_idade,sexo, populacao_tea)

write.csv(estudantes_tea_sexo_grupo_idade, "~/BD/TEA/input/estudantes_tea_sexo_grupo_idade.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10149 ------------------------------------------------------------
tabela10149 <- read_excel("~/BD/TEA/raw-data/tabela10149.xlsx", sheet = "Tabela 2")

# Qual o número da primeira linha onde aparece "MU"
linha_inicio <- which(tabela10149[[1]] == "MU")[1]

# Filtre o dataframe a partir dessa linha
estudantes_tea_raca_cor_grupo_idade <- tabela10149[linha_inicio:nrow(tabela10149), ]

# Renomeando colunas
colnames(estudantes_tea_raca_cor_grupo_idade) <- c("Rede", "id_municipio", "nome_municipio","grupo_idade","total","Branca","Preta","Amarela","Parda","Indígena")

#  preencher para baixo os valores não-NA
estudantes_tea_raca_cor_grupo_idade <- estudantes_tea_raca_cor_grupo_idade %>%
  fill(id_municipio, .direction = "down")

# Removendo colunas
estudantes_tea_raca_cor_grupo_idade <- estudantes_tea_raca_cor_grupo_idade %>%
  select(-Rede,-nome_municipio,-total)

# Remove as linhas onde grupo_idade é igual a "Total"
estudantes_tea_raca_cor_grupo_idade <- estudantes_tea_raca_cor_grupo_idade %>%
  filter(grupo_idade != "Total")

# Pivotando para criar a coluna raca_cor
estudantes_tea_raca_cor_grupo_idade <- estudantes_tea_raca_cor_grupo_idade %>%
  pivot_longer(
    cols = c(Branca,Preta,Amarela,Parda,Indígena),
    names_to = "raca_cor",
    values_to = "populacao_tea"
  )

# Trocando o hífem por NA
estudantes_tea_raca_cor_grupo_idade <- estudantes_tea_raca_cor_grupo_idade %>%
  mutate(
    populacao_tea = na_if(populacao_tea, "-"),
    ano = 2022
  ) %>%
  select(ano,id_municipio,grupo_idade,raca_cor, populacao_tea)

write.csv(estudantes_tea_raca_cor_grupo_idade, "~/BD/TEA/input/estudantes_tea_raca_cor_grupo_idade.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10169 ------------------------------------------------------------
tabela10169 <- read_excel("~/BD/TEA/raw-data/tabela 10169.xlsx")

estudantes_tea_indigenas <- tabela10169 %>%
  slice(33:n()) %>%  # seleciona da linha onde começam os municípios 
  rename( # Renomeando colunas
    nome_municipio = 1,
    "6 a 14 anos" = 2,
    "15 a 17 anos" = 3,
    "18 a 24 anos" = 4,
    "25 anos ou mais" = 5
  )

# Removendo a última coluna "Fonte = IBGE"
estudantes_tea_indigenas <- estudantes_tea_indigenas %>%
  slice(-n())

# Pivotando para criar a coluna raca_cor
estudantes_tea_indigenas <- estudantes_tea_indigenas %>%
  pivot_longer(
    cols = c("6 a 14 anos","15 a 17 anos","18 a 24 anos","25 anos ou mais"),
    names_to = "grupo_idade",
    values_to = "populacao_tea"
  )

# Lê o arquivo CSV com os nomes e ids dos municípios
ids_nome_muncipio <- read_csv("ids_nome_muncipio.csv")

# Faz o join para adicionar os ids dos municípios à base populacao_tea_indigena
estudantes_tea_indigenas <- estudantes_tea_indigenas %>%
  left_join(ids_nome_muncipio, by = "nome_municipio") 

# Trocando o hífem por NA
estudantes_tea_indigenas <- estudantes_tea_indigenas %>%
  mutate(
    populacao_tea = na_if(populacao_tea, "-"),
    ano = 2022
  ) %>%
  select(ano,id_municipio,grupo_idade,populacao_tea)

write.csv(estudantes_tea_indigenas, "~/BD/TEA/input/estudantes_tea_indigenas.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10146 ------------------------------------------------------------
tabela10146 <- read_excel("~/BD/TEA/raw-data/10146.xlsx")

populacao_tea_grupo_idade_nivel_escolaridade <- tabela10146 %>%
  slice(5:n()) %>%
  rename(
    nome_municipio = 1,
    grupo_idade = 2,
    creche = 3,
    pre_escola = 4,
    eja = 5,
    ensino_fundamental = 6,
    eja_ensino_fundamental = 7,
    ensino_medio = 8,
    eja_ensino_medio = 9,
    graduacao = 10,
    especializacao = 11,
    mestrado = 12,
    doutorado = 13
  )

#  preencher para baixo os valores não-NA
populacao_tea_grupo_idade_nivel_escolaridade <- populacao_tea_grupo_idade_nivel_escolaridade %>%
  fill(nome_municipio, .direction = "down")

# Lê o arquivo CSV com os nomes e ids dos municípios
ids_nome_muncipio <- read_csv("ids_nome_muncipio.csv")

# Faz o join para adicionar os ids dos municípios à base populacao_tea_indigena
populacao_tea_grupo_idade_nivel_escolaridade <- populacao_tea_grupo_idade_nivel_escolaridade %>%
  left_join(ids_nome_muncipio, by = "nome_municipio")

# Trocando o hífem por NA
populacao_tea_grupo_idade_nivel_escolaridade <- populacao_tea_grupo_idade_nivel_escolaridade %>%
  mutate(
    across(where(is.character), ~na_if(.x, "-")),
    ano = 2022
  ) %>%
  select(ano, id_municipio, grupo_idade, everything())

populacao_tea_grupo_idade_nivel_escolaridade <- populacao_tea_grupo_idade_nivel_escolaridade %>%
  select(-nome_municipio)

write.csv(populacao_tea_grupo_idade_nivel_escolaridade, "~/BD/TEA/input/populacao_tea_grupo_idade_nivel_escolaridade.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10150 ------------------------------------------------------------
tabela10150 <- read_excel("~/BD/TEA/raw-data/tabela10150.xlsx", sheet = "Tabela 2")

taxa_escolarizacao_populacao_tea_grupo_idade_sexo <- tabela10150[-1,] 

taxa_escolarizacao_populacao_tea_grupo_idade_sexo <- taxa_escolarizacao_populacao_tea_grupo_idade_sexo %>%
  rename( # Renomeando colunas
    rede = 1,
    id_municipio = 2,
    nome_municipio = 3,
    grupo_idade = 4,
    total = 5,
    homens = 6,
    mulheres = 7
  )

#  preencher para baixo os valores não-NA
taxa_escolarizacao_populacao_tea_grupo_idade_sexo <- taxa_escolarizacao_populacao_tea_grupo_idade_sexo %>%
  fill(rede, id_municipio, nome_municipio, .direction = "down")

taxa_escolarizacao_populacao_tea_grupo_idade_sexo <- taxa_escolarizacao_populacao_tea_grupo_idade_sexo %>%
  filter(grupo_idade != "Total")

taxa_escolarizacao_populacao_tea_grupo_idade_sexo <- taxa_escolarizacao_populacao_tea_grupo_idade_sexo %>%
  filter(rede == "MU") %>%
  select(-rede,-total,-nome_municipio)

taxa_escolarizacao_populacao_tea_grupo_idade_sexo <- taxa_escolarizacao_populacao_tea_grupo_idade_sexo %>%
  pivot_longer(
    cols = c(homens, mulheres),
    names_to = "sexo",
    values_to = "taxa_escolarizacao_tea"
  )

taxa_escolarizacao_populacao_tea_grupo_idade_sexo <- taxa_escolarizacao_populacao_tea_grupo_idade_sexo %>%
  mutate(
    across(everything(), ~na_if(.x, "-")),
    ano = 2022,
    sexo = str_to_title(sexo)
  ) %>%
  select(ano,id_municipio,grupo_idade,everything())


write.csv(taxa_escolarizacao_populacao_tea_grupo_idade_sexo, "~/BD/TEA/input/taxa_escolarizacao_populacao_tea_grupo_idade_sexo.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10151 ------------------------------------------------------------
tabela10151 <- read_excel("~/BD/TEA/raw-data/tabela10151.xlsx")

taxa_escolarizacao_populacao_tea_raca_cor <- tabela10151  %>%
  slice(117:n()) %>%  # seleciona da linha onde começam os municípios 
  rename( # Renomeando colunas
    nome_municipio = 1,
    grupo_idade = 2,
    Branca = 3,
    Preta = 4,
    Amarela = 5,
    Parda = 6,
    Indígena = 7
  )

#  preencher para baixo os valores não-NA
taxa_escolarizacao_populacao_tea_raca_cor <- taxa_escolarizacao_populacao_tea_raca_cor %>%
  fill(nome_municipio, .direction = "down")

# Removendo a última coluna "Fonte = IBGE"
taxa_escolarizacao_populacao_tea_raca_cor <- taxa_escolarizacao_populacao_tea_raca_cor %>%
  slice(-n())

# Pivotando para criar a coluna cor_raca
taxa_escolarizacao_populacao_tea_raca_cor <- taxa_escolarizacao_populacao_tea_raca_cor %>%
  pivot_longer(
    cols = c(Branca,Preta,Amarela,Parda,Indígena),
    names_to = "raca_cor",
    values_to = "taxa_escolarizacao_tea"
  )

# Trocando o hífem por NA
taxa_escolarizacao_populacao_tea_raca_cor <- taxa_escolarizacao_populacao_tea_raca_cor %>%
  mutate(
    taxa_escolarizacao_tea = na_if(taxa_escolarizacao_tea, "-"),
    ano = 2022
  ) %>%
  select(ano,nome_municipio, raca_cor,taxa_escolarizacao_tea)

# Lê o arquivo CSV com os nomes e ids dos municípios
ids_nome_muncipio <- read_csv("ids_nome_muncipio.csv")

# Faz o join para adicionar os ids dos municípios à base populacao_tea_indigena
taxa_escolarizacao_populacao_tea_raca_cor <- taxa_escolarizacao_populacao_tea_raca_cor %>%
  left_join(ids_nome_muncipio, by = "nome_municipio") 

# Trocando o hífem por NA
taxa_escolarizacao_populacao_tea_raca_cor <- taxa_escolarizacao_populacao_tea_raca_cor %>%
  select(ano,id_municipio,raca_cor,taxa_escolarizacao_tea)

write.csv(taxa_escolarizacao_populacao_tea_raca_cor, "~/BD/TEA/input/taxa_escolarizacao_populacao_tea_raca_cor.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10170 ------------------------------------------------------------
tabela10170 <- read_excel("~/BD/TEA/raw-data/tabela 10170.xlsx")

taxa_escolarizacao_populacao_indigena_grupo_idade <- tabela10170  %>%
  slice(6:n()) %>%  # seleciona da linha onde começam os municípios 
  rename( # Renomeando colunas
    nome_municipio = 1,
    "6 a 14 anos" = 2,
    "15 a 17 anos" = 3,
    "18 a 24 anos" = 4,
    "25 anos ou mais" = 5
  )

# Pivotando para criar a coluna grupo_idade
taxa_escolarizacao_populacao_indigena_grupo_idade <- taxa_escolarizacao_populacao_indigena_grupo_idade %>%
  pivot_longer(
    cols = c("6 a 14 anos","15 a 17 anos","18 a 24 anos","25 anos ou mais"),
    names_to = "grupo_idade",
    values_to = "taxa_escolarizacao_tea"
  )

# Trocando o hífem por NA
taxa_escolarizacao_populacao_indigena_grupo_idade <- taxa_escolarizacao_populacao_indigena_grupo_idade %>%
  mutate(
    taxa_escolarizacao_tea = na_if(taxa_escolarizacao_tea, "-"),
    ano = 2022
  ) %>%
  select(ano,nome_municipio,grupo_idade,taxa_escolarizacao_tea)


# Lê o arquivo CSV com os nomes e ids dos municípios
ids_nome_muncipio <- read_csv("ids_nome_muncipio.csv")

# Faz o join para adicionar os ids dos municípios à base populacao_tea_indigena
taxa_escolarizacao_populacao_indigena_grupo_idade <- taxa_escolarizacao_populacao_indigena_grupo_idade %>%
  left_join(ids_nome_muncipio, by = "nome_municipio") 

# Selecionando colunas de interesse
taxa_escolarizacao_populacao_indigena_grupo_idade <- taxa_escolarizacao_populacao_indigena_grupo_idade %>%
  select(ano,id_municipio,grupo_idade,taxa_escolarizacao_tea)

write.csv(taxa_escolarizacao_populacao_indigena_grupo_idade, "~/BD/TEA/input/taxa_escolarizacao_populacao_indigena_grupo_idade.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10153 ------------------------------------------------------------
tabela10153 <- read_excel("~/BD/TEA/raw-data/tabela10153.xlsx", sheet = "Tabela 2")

populacao_tea_sexo_nivel_escolaridade <- tabela10153 %>%
  slice(5:n()) %>%
  rename( # Renomeando colunas
    rede = 1,
    id_municipio = 2,
    nome_municipio = 3,
    nivel_instrucao = 4,
    total = 5,
    Homens = 6,
    Mulheres = 7
  )

#  preencher para baixo os valores não-NA
populacao_tea_sexo_nivel_escolaridade <- populacao_tea_sexo_nivel_escolaridade %>%
  fill(id_municipio, nome_municipio, .direction = "down")

# Remove as linhas onde grupo_idade é igual a "Total"
populacao_tea_sexo_nivel_escolaridade <- populacao_tea_sexo_nivel_escolaridade %>%
  filter(nivel_instrucao != "Total") %>%
  select(-total,-rede,-nome_municipio)

# Pivotando para criar a coluna Sexo
populacao_tea_sexo_nivel_escolaridade <- populacao_tea_sexo_nivel_escolaridade %>%
  pivot_longer(
      cols = c(Homens, Mulheres),
    names_to = "sexo",
    values_to = "populacao_tea"
  )

# Trocando o hífem por NA
populacao_tea_sexo_nivel_escolaridade <- populacao_tea_sexo_nivel_escolaridade %>%
  mutate(
    populacao_tea = na_if(populacao_tea, "-"),
    ano = 2022
  ) %>%
  select(ano,id_municipio,sexo,nivel_instrucao,populacao_tea)

write.csv(populacao_tea_sexo_nivel_escolaridade, "~/BD/TEA/input/populacao_tea_sexo_nivel_escolaridade.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10154 --------------------------------------------------------------
tabela10154 <- read_excel("~/BD/TEA/raw-data/tabela10154.xlsx", sheet = "Moradores em domicílios part...")

caracteristica_domicilio_banheiro_populacao_tea <- tabela10154 %>%
  slice(5:n()) %>%
  rename( # Renomeando colunas
    nome_municipio = 1,
    tipo_ligacao_rede_geral = 2,
    banheiro_exclusivo_domicilio = 3,
    um_banheiro_exclusivo_domicilio = 4,
    dois_banheiros_exclusivo_domicilio = 5,
    tres_banheiros_exclusivo_domicilio = 6,
    quatro_ou_mais_banheiros_exclusivo_domicilio = 7,
    banheiro_uso_comum_mais_de_um_domicilio = 8,
    apenas_sanitario_ou_buraco_dejetos = 9,
    sem_banheiro_ou_sanitario = 10
  )


# 
caracteristica_domicilio_banheiro_populacao_tea <- caracteristica_domicilio_banheiro_populacao_tea %>%
  mutate(
    across(everything(), ~na_if(.x, "-")),
    ano = 2022
  )

# Faz o join para adicionar os ids dos municípios à base populacao_tea_indigena
caracteristica_domicilio_banheiro_populacao_tea <- caracteristica_domicilio_banheiro_populacao_tea %>%
  fill(nome_municipio, .direction = "down") 

# Lê o arquivo CSV com os nomes e ids dos municípios
ids_nome_muncipio <- read_csv("ids_nome_muncipio.csv")

# Faz o join para adicionar os ids dos municípios à base populacao_tea_indigena
caracteristica_domicilio_banheiro_populacao_tea <- caracteristica_domicilio_banheiro_populacao_tea %>%
  left_join(ids_nome_muncipio, by = "nome_municipio") 

# Pivotando para criar a coluna grupo_idade
caracteristica_domicilio_banheiro_populacao_tea <- caracteristica_domicilio_banheiro_populacao_tea %>%
  pivot_longer(
    cols = c("banheiro_exclusivo_domicilio","dois_banheiros_exclusivo_domicilio","quatro_ou_mais_banheiros_exclusivo_domicilio","quatro_ou_mais_banheiros_exclusivo_domicilio",
             "apenas_sanitario_ou_buraco_dejetos","um_banheiro_exclusivo_domicilio",
             "tres_banheiros_exclusivo_domicilio","banheiro_uso_comum_mais_de_um_domicilio","sem_banheiro_ou_sanitario"),
    names_to = "existencia_banheiro",
    values_to = "populacao_tea"
  ) 

caracteristica_domicilio_banheiro_populacao_tea <- caracteristica_domicilio_banheiro_populacao_tea %>%
  select(ano, id_municipio, tipo_ligacao_rede_geral, existencia_banheiro, populacao_tea)

write.csv(caracteristica_domicilio_banheiro_populacao_tea, "~/BD/TEA/input/caracteristica_domicilio_banheiro_populacao_tea.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)
