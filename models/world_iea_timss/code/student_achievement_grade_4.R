
# Desempenho do estudante #ASA ----------------------------------------------
library(tidyverse)
library(haven)
library(readxl)

# ------------------------------------------------------------------------------
# Desempenho do estudante (Grade 4) - Processamento dos dados TIMSS 2023
# ------------------------------------------------------------------------------

# Define diretórios de entrada e saída
input_dir <-  "~/BD/timms/TIMSS2023_IDB_R_G4/TIMSS2023_IDB_R_G4/2_Data Files/R Data"
output_dir <- "~/BD/timms/timms_2023/tabelas/desempenho_aluno_g4"

# Cria a pasta de saída, se ainda não existir
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Lista todos os arquivos .RData de desempenho do estudante (que começam com 'ASA')
arquivos <- list.files(
  path = input_dir,
  pattern = "^asa.*\\.RData$",  # Expressão regular para arquivos .RData iniciando com 'ASA'
  ignore.case = TRUE,
  full.names = TRUE
)

# Lê a tabela de arquitetura com os nomes originais e novos das variáveis
arquitetura_desempenho_estudante <- readxl::read_excel(
  "~/BD/timms/timms_2023/arquitetura/[arquitetura]world_iae_timss_student_achievement_grade_4.xlsx"
)

# Define colunas de interesse com base na arquitetura
colunas <- arquitetura_desempenho_estudante$original_name
colunas <- c(colunas, "country_m49", "year", "CTY")  # Adiciona variáveis auxiliares
colunas <- na.omit(colunas)  # Remove NAs

# ------------------------------------------------------------------------------

# Função para processar cada arquivo .RData individualmente
processar_arquivo_rdata <- function(caminho_arquivo, output_dir, colunas) {
  # Cria ambiente temporário para carregar os objetos
  env <- new.env()
  load(caminho_arquivo, envir = env)
  
  # Lista os objetos carregados
  objetos <- ls(env)
  
  # Verifica se há apenas um objeto no arquivo
  if (length(objetos) != 1) {
    warning(paste("Arquivo", basename(caminho_arquivo), "contém múltiplos ou nenhum objeto. Pulando."))
    return(NULL)
  }
  
  # Obtém o dataframe
  dados <- get(objetos[1], envir = env)
  
  # Garante que é um data.frame
  if (!is.data.frame(dados)) {
    warning(paste("O objeto", objetos[1], "não é um dataframe. Pulando."))
    return(NULL)
  }
  
  # Adiciona variável de ano
  dados$year <- 2023
  
  # Cria a variável 'country_m49' com base nas colunas 'CTY' e 'IDCNTRY'
  if (all(c("CTY", "IDCNTRY") %in% colnames(dados))) {
    dados$country_m49 <- ifelse(
      dados$CTY %in% c("COT", "CQU", "AAD", "ADU", "ASA"),
      NA,
      dados$IDCNTRY
    )
  } else {
    warning("Colunas 'CTY' e/ou 'IDCNTRY' não encontradas no dataframe.")
    dados$country_m49 <- NA
  }
  
  # Seleciona apenas colunas válidas
  colunas_validas <- colunas[colunas %in% colnames(dados)]
  dados <- dados[, colunas_validas, drop = FALSE]
  
  # Define nome do objeto de saída
  nome_saida <- tools::file_path_sans_ext(basename(caminho_arquivo))
  assign(nome_saida, dados)
  
  # Salva o objeto processado como .RData
  save(list = nome_saida, file = file.path(output_dir, paste0(nome_saida, ".RData")))
  
  return(paste("Arquivo salvo como:", nome_saida, ".RData"))
}

# ------------------------------------------------------------------------------

# Aplica a função de processamento a todos os arquivos listados
for (arquivo in arquivos) {
  resultado <- processar_arquivo_rdata(arquivo, output_dir, colunas)
  print(resultado)
}

# ------------------------------------------------------------------------------
# Consolidação dos dados processados em um único dataframe
# ------------------------------------------------------------------------------

# Define diretório de trabalho com os arquivos .RData processados
setwd(output_dir)

# Lista todos os arquivos .RData gerados
lista <- list.files(pattern = "\\.RData$")

# Carrega cada arquivo e extrai o dataframe contido nele
lista_dados <- lapply(lista, function(x) {
  env <- new.env()
  load(x, envir = env)
  get(ls(env)[1], envir = env)
})

# Combina todos os dataframes em um único dataframe final
world_iae_timss_student_achievement_grade_4 <- do.call(rbind, lista_dados)

# ------------------------------------------------------------------------------
# Renomeia colunas com base na arquitetura e organiza estrutura final
# ------------------------------------------------------------------------------

# Vetores de nomes antigos (originais) e novos (padronizados)
nomes_antigos <- arquitetura_desempenho_estudante$original_name
nomes_novos   <- arquitetura_desempenho_estudante$name

# Filtra apenas os nomes existentes no dataframe
nomes_validos <- nomes_antigos[nomes_antigos %in% colnames(world_iae_timss_student_achievement_grade_4)]
nomes_correspondentes <- nomes_novos[nomes_antigos %in% colnames(world_iae_timss_student_achievement_grade_4)]

# Renomeia as colunas
world_iae_timss_student_achievement_grade_4 <- world_iae_timss_student_achievement_grade_4 %>%
  rename_with(~ setNames(nomes_correspondentes, nomes_validos)[.x], .cols = nomes_validos)

# Remove a coluna 'CTY', se ainda existir
world_iae_timss_student_achievement_grade_4 <- world_iae_timss_student_achievement_grade_4 %>%
  select(-any_of("CTY"))

# Ordena colunas conforme especificação da arquitetura
ordem_desejada <- arquitetura_desempenho_estudante$name
ordem_final <- ordem_desejada[ordem_desejada %in% colnames(world_iae_timss_student_achievement_grade_4)]

# Reorganiza colunas: primeiro as padronizadas, depois as demais (ex: year, country_m49)
world_iae_timss_student_achievement_grade_4 <- world_iae_timss_student_achievement_grade_4 %>%
  select(all_of(ordem_final), everything())

# ------------------------------------------------------------------------------
# Coloca NA's com base no CODEBOOK
# ------------------------------------------------------------------------------

guia_g4 <- read_excel("~/BD/timms/TIMSS2023_IDB_R_G4/TIMSS2023_IDB_R_G4/3_Supplemental Material/1_Codebook/T23_Codebook_G4.xlsx", sheet = "ASAM8")

# Se você quiser adicionar metadados do guia ao dataframe
df_final <- arquitetura_desempenho_estudante %>%
  left_join(guia_g4, by = c("original_name" = "Variable"))

# Remove rótulos SPSS do dataset
world_iae_timss_student_achievement_grade_4 <- world_iae_timss_student_achievement_grade_4 %>%
  mutate(across(everything(), zap_labels))

# Itera por cada linha de df_final
for (i in seq_len(nrow(df_final))) {
  col_name <- df_final$name[i]
  missing_val <- df_final[["Missing Scheme Detailed: SPSS"]][i]
  
  # Pula se coluna estiver ausente ou valor faltante for NA
  if (!is.na(missing_val) && col_name %in% names(world_iae_timss_student_achievement_grade_4)) {
    world_iae_timss_student_achievement_grade_4[[col_name]] <- ifelse(
      world_iae_timss_student_achievement_grade_4[[col_name]] == missing_val,
      NA,
      world_iae_timss_student_achievement_grade_4[[col_name]]
    )
  }
}

write.csv(world_iae_timss_student_achievement_grade_4, "world_iae_timss_student_achievement_grade_4.csv", 
          row.names = FALSE, 
          na = "", 
          fileEncoding = "UTF-8",
          quote = TRUE)
