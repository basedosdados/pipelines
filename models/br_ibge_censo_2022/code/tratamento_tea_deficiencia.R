library(readxl)
library(tidyverse)
library(zoo)

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
tabela10148_1_ <- read_excel("raw-data/tea/tabela10148 (1).xlsx")
estudantes_tea_sexo_grupo_idade <- tabela10148_1_
# Renomeando colunas
colnames(estudantes_tea_sexo_grupo_idade) <- c("rede","nome_municipio","grupo_idade","homens","mulheres")

estudantes_tea_sexo_grupo_idade <- estudantes_tea_sexo_grupo_idade %>%
  slice(5:(n() - 1)) %>%
  select("nome_municipio","grupo_idade","homens","mulheres")

#  preencher para baixo os valores não-NA
estudantes_tea_sexo_grupo_idade <- estudantes_tea_sexo_grupo_idade %>%
  fill(nome_municipio, .direction = "down")

# Pivotando para criar a coluna Sexo
estudantes_tea_sexo_grupo_idade <- estudantes_tea_sexo_grupo_idade %>%
  pivot_longer(
    cols = c(homens, mulheres),
    names_to = "sexo",
    values_to = "populacao_tea"
  )

# Lê o arquivo CSV com os nomes e ids dos municípios
ids_nome_muncipio <- read_csv("ids_nome_muncipio.csv")

# Faz o join para adicionar os ids dos municípios à base estudantes_tea_sexo_grupo_idade
estudantes_tea_sexo_grupo_idade<- estudantes_tea_sexo_grupo_idade %>%
  left_join(ids_nome_muncipio, by = "nome_municipio")

# Trocando o hífem por NA
estudantes_tea_sexo_grupo_idade <- estudantes_tea_sexo_grupo_idade %>%
  mutate(
    populacao_tea = as.numeric(na_if(populacao_tea, "-")),
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
tabela10149 <- read_excel("~/BD/TEA/raw-data/tea/tabela10149.xlsx", sheet = "Tabela 2")

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

estudantes_tea_grupo_idade_nivel_escolaridade <- tabela10146 %>%
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
estudantes_tea_grupo_idade_nivel_escolaridade <- estudantes_tea_grupo_idade_nivel_escolaridade %>%
  fill(nome_municipio, .direction = "down")

# Lê o arquivo CSV com os nomes e ids dos municípios
ids_nome_muncipio <- read_csv("ids_nome_muncipio.csv")

# Faz o join para adicionar os ids dos municípios à base populacao_tea_indigena
estudantes_tea_grupo_idade_nivel_escolaridade <- estudantes_tea_grupo_idade_nivel_escolaridade %>%
  left_join(ids_nome_muncipio, by = "nome_municipio")

# Trocando o hífem por NA
estudantes_tea_grupo_idade_nivel_escolaridade <- estudantes_tea_grupo_idade_nivel_escolaridade %>%
  mutate(
    across(where(is.character), ~na_if(.x, "-")),
    ano = 2022
  ) %>%
  select(ano, id_municipio, grupo_idade, everything())

estudantes_tea_grupo_idade_nivel_escolaridade <- estudantes_tea_grupo_idade_nivel_escolaridade %>%
  select(-nome_municipio)

write.csv(estudantes_tea_grupo_idade_nivel_escolaridade, "~/BD/TEA/input/populacao_tea_grupo_idade_nivel_escolaridade.csv",
          row.names = FALSE,
          na = "",
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10150 ------------------------------------------------------------
tabela10150 <- read_excel("~/BD/TEA/raw-data/tea/tabela10150.xlsx")
tabela1 <- tabela10150

# Renomeando colunas
colnames(tabela1) <- c("nome_municipio","grupo_idade","total","homens","mulheres")

tabela1 <- tabela1 %>%
  slice(5:(n() - 1))

#  preencher para baixo os valores não-NA
tabela1 <- tabela1 %>%
  fill(nome_municipio, .direction = "down")

# Remove as linhas onde grupo_idade é igual a "Total"
tabela1 <- tabela1 %>%
  filter(grupo_idade != "Total") %>%
  select(-total)

# Pivotando para criar a coluna Sexo
tabela1 <- tabela1 %>%
  pivot_longer(
    cols = c(homens, mulheres),
    names_to = "sexo",
    values_to = "taxa_escolarizacao"
  )

# Trocando o hífem por NA
tabela1 <- tabela1 %>%
  filter(nome_municipio == 'Brasil') %>%
  mutate(
    taxa_escolarizacao = na_if(taxa_escolarizacao, "-"),
    ano = 2022,
    diagnostico = "nao",
    sexo = str_to_title(sexo)
  ) %>%
  select(ano,diagnostico,grupo_idade,sexo,taxa_escolarizacao)

tabela10150 <- read_excel("~/BD/TEA/raw-data/tea/tabela10150.xlsx", sheet = "Tabela 2")
tabela2 <- tabela10150

# Renomeando colunas
colnames(tabela2) <- c("nome_municipio","grupo_idade","total","homens","mulheres")

tabela2 <- tabela2 %>%
  slice(5:(n() - 1))

#  preencher para baixo os valores não-NA
tabela2 <- tabela2 %>%
  fill(nome_municipio, .direction = "down")

# Remove as linhas onde grupo_idade é igual a "Total"
tabela2 <- tabela2 %>%
  filter(grupo_idade != "Total") %>%
  select(-total)

# Pivotando para criar a coluna Sexo
tabela2 <- tabela2 %>%
  pivot_longer(
    cols = c(homens, mulheres),
    names_to = "sexo",
    values_to = "taxa_escolarizacao"
  )

# Trocando o hífem por NA
tabela2 <- tabela2 %>%
  filter(nome_municipio == 'Brasil') %>%
  mutate(
    taxa_escolarizacao = na_if(taxa_escolarizacao, "-"),
    ano = 2022,
    diagnostico = "sim",
    sexo = str_to_title(sexo)
  ) %>%
  select(ano,diagnostico,grupo_idade,sexo,taxa_escolarizacao)

taxa_escolarizacao_tea_sexo_grupo_idade <- rbind(tabela1, tabela2)

write.csv(taxa_escolarizacao_tea_sexo_grupo_idade, "~/BD/TEA/input/taxa_escolarizacao_tea_sexo_grupo_idade.csv",
          row.names = FALSE,
          na = "",
          fileEncoding = "UTF-8",
          quote = TRUE)

# Tabela 10151 ------------------------------------------------------------
tabela1 <- read_excel("~/BD/TEA/raw-data/tea/tabela10151.xlsx", sheet = "Tabela 1")

# Renomeando colunas
colnames(tabela1) <- c("nome_municipio","grupo_idade","branca","preta","amarela","parda","indigena")

tabela1 <- tabela1 %>%
  slice(5:(n() - 1))


# Pivotando para criar a coluna Sexo
tabela1 <- tabela1 %>%
  pivot_longer(
    cols = c(branca,preta,amarela,parda,indigena),
    names_to = "raca_cor",
    values_to = "taxa_escolarizacao"
  )

# Trocando o hífem por NA
tabela1 <- tabela1 %>%
  mutate(
    taxa_escolarizacao = na_if(taxa_escolarizacao, "-"),
    ano = 2022,
    diagnostico = "nao",
    raca_cor = str_to_title(raca_cor)
  ) %>%
  select(ano,diagnostico,grupo_idade,raca_cor,taxa_escolarizacao)

tabela2 <- read_excel("~/BD/TEA/raw-data/tea/tabela10151.xlsx", sheet = "Tabela 2")

# Renomeando colunas
colnames(tabela2) <- c("nome_municipio","grupo_idade","branca","preta","amarela","parda","indigena")

tabela2 <- tabela2 %>%
  slice(5:(n() - 1))


# Pivotando para criar a coluna Sexo
tabela2 <- tabela2 %>%
  pivot_longer(
    cols = c(branca,preta,amarela,parda,indigena),
    names_to = "raca_cor",
    values_to = "taxa_escolarizacao"
  )

# Trocando o hífem por NA
tabela2 <- tabela2 %>%
  mutate(
    taxa_escolarizacao = na_if(taxa_escolarizacao, "-"),
    ano = 2022,
    diagnostico = "sim",
    raca_cor = str_to_title(raca_cor)
  ) %>%
  select(ano,diagnostico,grupo_idade,raca_cor,taxa_escolarizacao)

taxa_escolarizacao_tea_raca_cor_grupo_idade <- rbind(tabela1, tabela2)

write.csv(
  taxa_escolarizacao_tea_raca_cor_grupo_idade,
  file = "~/taxa_escolarizacao_tea_raca_cor_grupo_idade.csv",
  row.names = FALSE,
  na = "",
  fileEncoding = "UTF-8",
  quote = TRUE
)

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
