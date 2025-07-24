library(tidyverse)
library(readxl)

## 1. Configuração inicial ---------------------------------------------------
# Definir caminhos
dir_arquitetura <- "~/BD/timms/timms_2023/arquitetura"
guia_g8_path <- "~/BD/timms/TIMSS2023_IDB_R_G8/TIMSS2023_IDB_R_G8/3_Supplemental Material/1_Codebook/T23_Codebook_G8.xlsx"
guia_g4_path <- "~/BD/timms/TIMSS2023_IDB_R_G4/TIMSS2023_IDB_R_G4/3_Supplemental Material/1_Codebook/T23_Codebook_G4.xlsx"

# Mapeamento completo entre nomes de tabelas e abas
mapa_abas <- list(
  # Grade 8
  "world_iae_timss_student_achievement_grade_8" = list(guia = guia_g8_path, sheet = "BSAM8"),
  "world_iae_timss_student_context_grade_8" = list(guia = guia_g8_path, sheet = "BSGM8"),
  "world_iae_timss_school_context_grade_8" = list(guia = guia_g8_path, sheet = "BCGM8"),
  "world_iae_timss_teacher_mathematics_context_grade_8" = list(guia = guia_g8_path, sheet = "BTMM8"),
  "world_iae_timss_teacher_science_context_grade_8" = list(guia = guia_g8_path, sheet = "BTSM8"),
  
  # Grade 4
  "world_iae_timss_student_achievement_grade_4" = list(guia = guia_g4_path, sheet = "ASAM8"),
  "world_iae_timss_student_context_grade_4" = list(guia = guia_g4_path, sheet = "ASGM8"),
  "world_iae_timss_school_context_grade_4" = list(guia = guia_g4_path, sheet = "ACGM8"),
  "world_iae_timss_teacher_context_grade_4" = list(guia = guia_g4_path, sheet = "ATGM8"),
  "world_iae_timss_home_context_grade_4" = list(guia = guia_g4_path, sheet = "ASHM8")
)

## 2. Função principal revisada ----------------------------------------------
processar_tabela <- function(arquivo_arquitetura, mapa_abas) {
  # Extrai nome da tabela do nome do arquivo
  nome_tabela <- str_remove(tools::file_path_sans_ext(basename(arquivo_arquitetura)), "^\\[arquitetura\\]")
  
  # Verifica se a tabela está no mapeamento
  if (!nome_tabela %in% names(mapa_abas)) {
    message(paste("Tabela", nome_tabela, "não está no mapeamento. Pulando..."))
    return(NULL)
  }
  
  # Obtém configurações do guia
  config <- mapa_abas[[nome_tabela]]
  
  # Carrega os dados
  arquitetura <- read_excel(arquivo_arquitetura)
  guia <- read_excel(config$guia, sheet = config$sheet)
  
  # Processa os dados
  df_final <- arquitetura %>%
    filter(tolower(covered_by_dictionary) == "yes") %>%
    left_join(guia, by = c("original_name" = "Variable")) %>%
    select(name, original_name, `Value Scheme Detailed`) %>%
    filter(!is.na(`Value Scheme Detailed`),
           `Value Scheme Detailed` != "")
  
  # Se não houver dados válidos, retorna NULL
  if (nrow(df_final) == 0) return(NULL)
  
  # Cria o dicionário com tratamento robusto de NAs
  dicionario <- df_final %>%
    separate_rows(`Value Scheme Detailed`, sep = ";\\s*") %>%
    mutate(
      temp = str_split(`Value Scheme Detailed`, ":", n = 2, simplify = TRUE),
      chave = str_trim(temp[,1]),
      valor = ifelse(temp[,2] == "", NA_character_, str_trim(temp[,2]))
    ) %>%
    filter(!is.na(chave), chave != "") %>%
    mutate(
      id_tabela = nome_tabela,
      cobertura_temporal = "(1)",
      grade = ifelse(str_detect(nome_tabela, "grade_8"), "G8", "G4")
    ) %>%
    select(id_tabela, nome_coluna = name, chave, cobertura_temporal, valor, grade)
  
  return(dicionario)
}

## 3. Execução para todos os arquivos ----------------------------------------
# Lista todos os arquivos de arquitetura
arquivos_arquitetura <- list.files(
  path = dir_arquitetura,
  pattern = "^\\[arquitetura\\].*\\.xlsx$",
  full.names = TRUE
)

# Processa todos os arquivos
dicionario_completo <- map_dfr(arquivos_arquitetura, ~{
  tryCatch({
    processar_tabela(.x, mapa_abas)
  }, error = function(e) {
    message(paste("Erro ao processar", .x, ":", e$message))
    NULL
  })
})

## 4. Pós-processamento e salvamento -----------------------------------------
# Remove linhas com valores NA (se necessário)
dicionario_final <- dicionario_completo %>%
  filter(!is.na(chave), !is.na(valor)) %>%
  select(-grade)

# Verifica estrutura
glimpse(dicionario_final)

# Salva os resultados
write_csv(dicionario_final, "dicionario.csv")

# Opcional: separar por grade
#dicionario_g4 <- dicionario_final %>% filter(grade == "G4")
#dicionario_g8 <- dicionario_final %>% filter(grade == "G8")