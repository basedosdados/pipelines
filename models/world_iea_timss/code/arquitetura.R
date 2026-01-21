# 1. Carregar os dados
library(readxl)
library(writexl)
library(tidyverse)


# Arquiteturas G8 ---------------------------------------------------------
# Dados de contexto do professor de ciências ----------------------------
guia_g8 <- read_excel("~/BD/timms/TIMSS2023_IDB_R_G8/TIMSS2023_IDB_R_G8/3_Supplemental Material/1_Codebook/T23_Codebook_G8.xlsx", sheet = "BTSM8")

world_iae_timss_teacher_science_context_grade_8 <- guia_g8 %>%
  select(Variable, Label, Level, Decimals, `Value Scheme Detailed`, `Missing Scheme Detailed: SPSS`) %>%
  rename(original_name = Variable, description = Label) %>%  # Renomeia a coluna 'Variable' para 'original_name'
  mutate(
    original_name = case_when(  # Aplica o mapeamento manual
      original_name == "idcntry" ~ "country_id",
      original_name == "idpop" ~ "pop_id",
      original_name == "idgrade" ~ "grade_id",
      original_name == "idschool" ~ "school_id",
      original_name == "itlang_cq" ~ "language_school_questionnaire",
      original_name == "lcid_cq" ~ "locale_school_questionnaire",
      TRUE ~ original_name  # Caso não seja um nome no mapeamento, mantém como está
    ),
    bigquerytype = case_when(  # Lógica para criar a coluna 'bigquerytype' com base em Decimals
      Level == "Nominal" ~ "string",
      Level == "Ordinal" ~ "string",
      !is.na(Decimals) & Decimals == 0 ~ "int64",  # Se Decimals for 0, será int64
      !is.na(Decimals) & Decimals != 0 ~ "float64",  # Se Decimals for diferente de 0, será float64
      is.na(Decimals) ~ "int64",     # Caso contrário, será int64
      TRUE ~ NA_character_  # Para qualquer outro caso, deixar NA
    ),
    covered_by_dictionary = case_when(  # Lógica para a coluna 'covered_by_dictionary'
      !is.na(`Value Scheme Detailed`) & `Value Scheme Detailed` != "" ~ "yes",  # Se tiver valor, será 'yes'
      TRUE ~ "not"  # Caso contrário, será 'not'
    ),
    temporal_coverage = "(1)",
    directory_column = NA ,
    measurement_unit = NA,
    has_sensitive_data = NA,
    observations = NA,
    name = tolower(original_name),
    description = str_to_title(description)
  )

world_iae_timss_teacher_science_context_grade_8 <- world_iae_timss_teacher_science_context_grade_8 %>%
  select(name,bigquerytype,description,temporal_coverage,covered_by_dictionary,directory_column,measurement_unit,has_sensitive_data,observations,original_name)

write_xlsx(world_iae_timss_teacher_science_context_grade_8, path = "~/BD/timms/timms_2023/arquitetura/world_iae_timss_teacher_science_context_grade_8.xlsx")


# Dados de contexto do professor de matemática ----------------------------
guia_g8 <- read_excel("~/BD/timms/TIMSS2023_IDB_R_G8/TIMSS2023_IDB_R_G8/3_Supplemental Material/1_Codebook/T23_Codebook_G8.xlsx", sheet = "BTMM8")

world_iae_timss_teacher_mathematics_context_grade_8 <- guia_g8 %>%
  select(Variable, Label, Level, Decimals, `Value Scheme Detailed`, `Missing Scheme Detailed: SPSS`) %>%
  rename(original_name = Variable, description = Label) %>%  # Renomeia a coluna 'Variable' para 'original_name'
  mutate(
    original_name = case_when(  # Aplica o mapeamento manual
      original_name == "idcntry" ~ "country_id",
      original_name == "idpop" ~ "pop_id",
      original_name == "idgrade" ~ "grade_id",
      original_name == "idschool" ~ "school_id",
      original_name == "itlang_cq" ~ "language_school_questionnaire",
      original_name == "lcid_cq" ~ "locale_school_questionnaire",
      TRUE ~ original_name  # Caso não seja um nome no mapeamento, mantém como está
    ),
    bigquerytype = case_when(  # Lógica para criar a coluna 'bigquerytype' com base em Decimals
      Level == "Nominal" ~ "string",
      Level == "Ordinal" ~ "string",
      !is.na(Decimals) & Decimals == 0 ~ "int64",  # Se Decimals for 0, será int64
      !is.na(Decimals) & Decimals != 0 ~ "float64",  # Se Decimals for diferente de 0, será float64
      is.na(Decimals) ~ "int64",     # Caso contrário, será int64
      TRUE ~ NA_character_  # Para qualquer outro caso, deixar NA
    ),
    covered_by_dictionary = case_when(  # Lógica para a coluna 'covered_by_dictionary'
      !is.na(`Value Scheme Detailed`) & `Value Scheme Detailed` != "" ~ "yes",  # Se tiver valor, será 'yes'
      TRUE ~ "not"  # Caso contrário, será 'not'
    ),
    temporal_coverage = "(1)",
    directory_column = NA ,
    measurement_unit = NA,
    has_sensitive_data = NA,
    observations = NA,
    name = tolower(original_name),
    description = str_to_title(description)
  )

world_iae_timss_teacher_mathematics_context_grade_8 <- world_iae_timss_teacher_mathematics_context_grade_8 %>%
  select(name,bigquerytype,description,temporal_coverage,covered_by_dictionary,directory_column,measurement_unit,has_sensitive_data,observations,original_name)

write_xlsx(world_iae_timss_teacher_mathematics_context_grade_8, path = "~/BD/timms/timms_2023/arquitetura/world_iae_timss_teacher_mathematics_context_grade_8.xlsx")


# Dados de contexto do estudante ------------------------------------------
guia_g8 <- read_excel("~/BD/timms/TIMSS2023_IDB_R_G8/TIMSS2023_IDB_R_G8/3_Supplemental Material/1_Codebook/T23_Codebook_G8.xlsx", sheet = "BSGM8")

world_iae_timss_student_context_grade_8 <- guia_g8 %>%
  select(Variable, Label, Level, Decimals, `Value Scheme Detailed`, `Missing Scheme Detailed: SPSS`) %>%
  rename(original_name = Variable, description = Label) %>%  # Renomeia a coluna 'Variable' para 'original_name'
  mutate(
    original_name = case_when(  # Aplica o mapeamento manual
      original_name == "idcntry" ~ "country_id",
      original_name == "idpop" ~ "pop_id",
      original_name == "idgrade" ~ "grade_id",
      original_name == "idschool" ~ "school_id",
      original_name == "itlang_cq" ~ "language_school_questionnaire",
      original_name == "lcid_cq" ~ "locale_school_questionnaire",
      TRUE ~ original_name  # Caso não seja um nome no mapeamento, mantém como está
    ),
    bigquerytype = case_when(  # Lógica para criar a coluna 'bigquerytype' com base em Decimals
      Level == "Nominal" ~ "string",
      Level == "Ordinal" ~ "string",
      !is.na(Decimals) & Decimals == 0 ~ "int64",  # Se Decimals for 0, será int64
      !is.na(Decimals) & Decimals != 0 ~ "float64",  # Se Decimals for diferente de 0, será float64
      is.na(Decimals) ~ "int64",     # Caso contrário, será int64
      TRUE ~ NA_character_  # Para qualquer outro caso, deixar NA
    ),
    covered_by_dictionary = case_when(  # Lógica para a coluna 'covered_by_dictionary'
      !is.na(`Value Scheme Detailed`) & `Value Scheme Detailed` != "" ~ "yes",  # Se tiver valor, será 'yes'
      TRUE ~ "not"  # Caso contrário, será 'not'
    ),
    temporal_coverage = "(1)",
    directory_column = NA ,
    measurement_unit = NA,
    has_sensitive_data = NA,
    observations = NA,
    name = tolower(original_name),
    description = str_to_title(description)
  )

world_iae_timss_student_context_grade_8 <- world_iae_timss_student_context_grade_8 %>%
  select(name,bigquerytype,description,temporal_coverage,covered_by_dictionary,directory_column,measurement_unit,has_sensitive_data,observations,original_name)

write_xlsx(world_iae_timss_student_context_grade_8, path = "~/BD/timms/timms_2023/arquitetura/world_iae_timss_student_context_grade_8.xlsx")


# Dados de contexto da escola ---------------------------------------------
guia_g8 <- read_excel("~/BD/timms/TIMSS2023_IDB_R_G8/TIMSS2023_IDB_R_G8/3_Supplemental Material/1_Codebook/T23_Codebook_G8.xlsx", sheet = "BSGM8")

world_iae_timss_student_context_grade_8 <- guia_g8 %>%
  select(Variable, Label, Level, Decimals, `Value Scheme Detailed`, `Missing Scheme Detailed: SPSS`) %>%
  rename(original_name = Variable, description = Label) %>%  # Renomeia a coluna 'Variable' para 'original_name'
  mutate(
    original_name = case_when(  # Aplica o mapeamento manual
      original_name == "idcntry" ~ "country_id",
      original_name == "idpop" ~ "pop_id",
      original_name == "idgrade" ~ "grade_id",
      original_name == "idschool" ~ "school_id",
      original_name == "itlang_cq" ~ "language_school_questionnaire",
      original_name == "lcid_cq" ~ "locale_school_questionnaire",
      TRUE ~ original_name  # Caso não seja um nome no mapeamento, mantém como está
    ),
    bigquerytype = case_when(  # Lógica para criar a coluna 'bigquerytype' com base em Decimals
      Level == "Nominal" ~ "string",
      Level == "Ordinal" ~ "string",
      !is.na(Decimals) & Decimals == 0 ~ "int64",  # Se Decimals for 0, será int64
      !is.na(Decimals) & Decimals != 0 ~ "float64",  # Se Decimals for diferente de 0, será float64
      is.na(Decimals) ~ "int64",     # Caso contrário, será int64
      TRUE ~ NA_character_  # Para qualquer outro caso, deixar NA
    ),
    covered_by_dictionary = case_when(  # Lógica para a coluna 'covered_by_dictionary'
      !is.na(`Value Scheme Detailed`) & `Value Scheme Detailed` != "" ~ "yes",  # Se tiver valor, será 'yes'
      TRUE ~ "not"  # Caso contrário, será 'not'
    ),
    temporal_coverage = "(1)",
    directory_column = NA ,
    measurement_unit = NA,
    has_sensitive_data = NA,
    observations = NA,
    name = tolower(original_name),
    description = str_to_title(description)
  )

world_iae_timss_student_context_grade_8 <- world_iae_timss_student_context_grade_8 %>%
  select(name,bigquerytype,description,temporal_coverage,covered_by_dictionary,directory_column,measurement_unit,has_sensitive_data,observations,original_name)

write_xlsx(world_iae_timss_student_context_grade_8, path = "~/BD/timms/timms_2023/arquitetura/world_iae_timss_student_context_grade_8.xlsx")


# Dados de contexto de desempenho do aluno --------------------------------
guia_g8 <- read_excel("~/BD/timms/TIMSS2023_IDB_R_G8/TIMSS2023_IDB_R_G8/3_Supplemental Material/1_Codebook/T23_Codebook_G8.xlsx", sheet = "BSAM8")

world_iae_timss_student_achievement_grade_8 <- guia_g8 %>%
  select(
    Variable, Label, Level, Decimals,
    `Value Scheme Detailed`, `Missing Scheme Detailed: SPSS`
  ) %>%
  rename(
    original_name = Variable,
    description = Label
  ) %>%
  mutate(
    name = tolower(original_name),  
    name = case_when(
      name == "idcntry" ~ "country_id",
      name == "idgrade" ~ "grade_id",
      name == "idschool" ~ "school_id",
      name == "classid" ~ "class_id",
      name == "studid" ~ "student_id", 
      name == "itsex" ~ "student_sex",
      name == "bsdage" ~ "student_age",
      name == "itlang_sa" ~ "language_school_questionnaire",
      name == "lcid_sa" ~ "locale_school_questionnaire",
      TRUE ~ name  
    ),
    bigquerytype = case_when(
      Level %in% c("Nominal", "Ordinal") ~ "string",
      !is.na(Decimals) & Decimals == 0 ~ "int64",
      !is.na(Decimals) & Decimals != 0 ~ "float64",
      is.na(Decimals) ~ "int64",
      TRUE ~ NA_character_
    ),
    covered_by_dictionary = case_when(
      !is.na(`Value Scheme Detailed`) & `Value Scheme Detailed` != "" ~ "yes",
      TRUE ~ "not"
    ),
    temporal_coverage = "(1)",
    directory_column = NA,
    measurement_unit = NA,
    has_sensitive_data = NA,
    observations = NA,
    description = str_to_title(description)
  )


world_iae_timss_student_achievement_grade_8 <- world_iae_timss_student_achievement_grade_8 %>%
  select(name,bigquerytype,description,temporal_coverage,covered_by_dictionary,directory_column,measurement_unit,has_sensitive_data,observations,original_name)

world_iae_timss_student_achievement_grade_8 <- world_iae_timss_student_achievement_grade_8 %>%
  slice(-17:-1021)

# Obs. linhas de ano e country_m49 criadas manualmente 

write_xlsx(world_iae_timss_student_achievement_grade_8, path = "~/BD/timms/timms_2023/arquitetura/world_iae_timss_student_achievement_grade_8.xlsx")


# # Arquitetura G4 --------------------------------------------------------
# Dados de contexto de casa  ----------------------------------------------
guia_g4 <- read_excel("timms/TIMSS2023_IDB_R_G4/TIMSS2023_IDB_R_G4/3_Supplemental Material/1_Codebook/T23_Codebook_G4.xlsx", sheet = "ASHM8")

world_iae_timss_home_context_grade_4 <- guia_g4 %>%
  select(
    Variable, Label, Level, Decimals,
    `Value Scheme Detailed`, `Missing Scheme Detailed: SPSS`
  ) %>%
  rename(
    original_name = Variable,
    description = Label
  ) %>%
  mutate(
    name = tolower(original_name),  
    name = case_when(
      name == "idcntry" ~ "country_id",
      name == "idgrade" ~ "grade_id",
      name == "idschool" ~ "school_id",
      name == "idclass" ~ "class_id",
      name == "idstud" ~ "student_id", 
      name == "itlang_hq" ~ "language_school_questionnaire",
      name == "lcid_hq" ~ "locale_school_questionnaire",
      TRUE ~ name  
    ),
    bigquerytype = case_when(
      Level %in% c("Nominal", "Ordinal") ~ "string",
      !is.na(Decimals) & Decimals == 0 ~ "int64",
      !is.na(Decimals) & Decimals != 0 ~ "float64",
      is.na(Decimals) ~ "int64",
      TRUE ~ NA_character_
    ),
    covered_by_dictionary = case_when(
      !is.na(`Value Scheme Detailed`) & `Value Scheme Detailed` != "" ~ "yes",
      TRUE ~ "not"
    ),
    temporal_coverage = "(1)",
    directory_column = NA,
    measurement_unit = NA,
    has_sensitive_data = NA,
    observations = NA,
    description = str_to_title(description)
  )

# Vetor com os valores a remover
valores_a_remover <- c("IDPOP", "IDGRADER", "CTY")

# Filtrar o data frame
world_iae_timss_home_context_grade_4 <- subset(
  world_iae_timss_home_context_grade_4,
  !(original_name %in% valores_a_remover)
)

world_iae_timss_home_context_grade_4$description <- world_iae_timss_home_context_grade_4$description |>
  gsub("^Gen\\\\", "", x = _) |>                    # remove o "Gen\" no início
  gsub("\\\\", " ", x = _) |>                       # troca "\" por espaço
  trimws()  

world_iae_timss_home_context_grade_4$description <- ifelse(
  grepl("Skills", world_iae_timss_home_context_grade_4$description, ignore.case = TRUE),
  {
    frase <- gsub("Skills", "The student", world_iae_timss_home_context_grade_4$description, ignore.case = TRUE)
    frase <- trimws(frase)
    ifelse(grepl("\\?$", frase), frase, paste0(frase, "?"))
  },
  world_iae_timss_home_context_grade_4$description
)

world_iae_timss_home_context_grade_4 <- world_iae_timss_home_context_grade_4 %>%
  select(name,bigquerytype,description,temporal_coverage,covered_by_dictionary,directory_column,measurement_unit,has_sensitive_data,observations,original_name)

# Adiciona " the student" após "How Often" e "?" ao final da frase
world_iae_timss_home_context_grade_4$description <- ifelse(
  grepl("How Often", world_iae_timss_home_context_grade_4$description, ignore.case = TRUE),
  {
    frase <- gsub("(?i)(How Often)", "\\1 the student", world_iae_timss_home_context_grade_4$description, perl = TRUE)
    frase <- trimws(frase)
    ifelse(grepl("\\?$", frase), frase, paste0(frase, "?"))
  },
  world_iae_timss_home_context_grade_4$description
)

write_xlsx(world_iae_timss_home_context_grade_4, path = "~/BD/timms/timms_2023/arquitetura/world_iae_timss_home_context_grade_4.xlsx")


# Contexto do estudante ---------------------------------------------------
guia_g4 <- read_excel("TIMSS2023_IDB_R_G4/TIMSS2023_IDB_R_G4/3_Supplemental Material/1_Codebook/T23_Codebook_G4.xlsx", sheet = "ASGM8")

world_iae_timss_student_context_grade_4 <- guia_g4 %>%
  select(
    Variable, Label, Level, Decimals,
    `Value Scheme Detailed`, `Missing Scheme Detailed: SPSS`
  ) %>%
  rename(
    original_name = Variable,
    description = Label
  ) %>%
  mutate(
    name = tolower(original_name),  
    name = case_when(
      name == "idcntry" ~ "country_id",
      name == "idgrade" ~ "grade_id",
      name == "idschool" ~ "school_id",
      name == "idclass" ~ "class_id",
      name == "idstud" ~ "student_id", 
      name == "itlang_hq" ~ "language_school_questionnaire",
      name == "lcid_hq" ~ "locale_school_questionnaire",
      TRUE ~ name  
    ),
    bigquerytype = case_when(
      Level %in% c("Nominal", "Ordinal") ~ "string",
      !is.na(Decimals) & Decimals == 0 ~ "int64",
      !is.na(Decimals) & Decimals != 0 ~ "float64",
      is.na(Decimals) ~ "int64",
      TRUE ~ NA_character_
    ),
    covered_by_dictionary = case_when(
      !is.na(`Value Scheme Detailed`) & `Value Scheme Detailed` != "" ~ "yes",
      TRUE ~ "not"
    ),
    temporal_coverage = "(1)",
    directory_column = NA,
    measurement_unit = NA,
    has_sensitive_data = NA,
    observations = NA,
    description = str_to_title(description)
  )

# Vetor com os valores a remover
valores_a_remover <- c("IDPOP", "IDGRADER", "CTY")

# Filtrar o data frame
world_iae_timss_student_context_grade_4 <- subset(
  world_iae_timss_student_context_grade_4,
  !(original_name %in% valores_a_remover)
)

world_iae_timss_student_context_grade_4$description <- world_iae_timss_student_context_grade_4$description |>
  gsub("^Gen\\\\", "", x = _) |>                    # remove o "Gen\" no início
  gsub("\\\\", " ", x = _) |>                       # troca "\" por espaço
  trimws()  

world_iae_timss_student_context_grade_4$description <- ifelse(
  grepl("Skills",world_iae_timss_student_context_grade_4$description, ignore.case = TRUE),
  {
    frase <- gsub("Skills", "The student", world_iae_timss_student_context_grade_4$description, ignore.case = TRUE)
    frase <- trimws(frase)
    ifelse(grepl("\\?$", frase), frase, paste0(frase, "?"))
  },
  world_iae_timss_student_context_grade_4$description
)

world_iae_timss_student_context_grade_4 <- world_iae_timss_student_context_grade_4 %>%
  select(name,bigquerytype,description,temporal_coverage,covered_by_dictionary,directory_column,measurement_unit,has_sensitive_data,observations,original_name)

write_xlsx(world_iae_timss_student_context_grade_4, path = "~/BD/timms/timms_2023/arquitetura/world_iae_timss_student_context_grade_4.xlsx")
