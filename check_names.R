library(tidyverse)
library(data.table)
library(adtabler)

get_col_info <- function(path){
  df <- fread(input = path,
              sep = "\t",
              nrows = 10000,
              header = TRUE,
              fill = TRUE,
              logical01 = FALSE,
              integer64 = "character",
              # integer64 = "integer",
              nThread = parallel::detectCores() - 1)
  col_info <- map2(.x = df,
                   .y = names(df),
                   .f = ~ tibble(n_rows = NROW(.x),
                                 col_name = .y,
                                 class = paste0(class(.x), collapse = "___"),
                                 type = typeof(.x),
                                 all_na = all(is.na(.x))) %>%
                     mutate(
                       type = case_when(
                         type == "logical" && all_na ~ NA,
                         any(str_detect(class, "Date")) ~ "Date",
                         .default = type)
                     )) %>% list_rbind
    return(col_info)
}

check_col_names <- function(names_data, names_manual, replace = FALSE){
  x_data <- names_data %>%
    str_sep_upper("_", named = F) %>%
    str_replace_all("_", " ") %>%
    str_squish() %>%
    str_replace_all(" ", "_") %>%
    str_remove_all(" ") %>%
    # str_sep_upper("_", named = F) %>%
    str_replace_all("prime", "prim") %>%
    if_else(condition = str_detect(., "dim_bridge"), true = "dim_bridge_occ_imp_spot_radio_key", false = .)
  x_manual <- names_manual %>%
    str_sep_upper("_", named = F) %>%
    str_replace_all("_", " ") %>%
    str_squish() %>%
    str_replace_all(" ", "_") %>%
    str_remove_all(" ") %>%
    # str_sep_upper("_", named = F) %>%
    str_replace_all("prime", "prim") %>%
    if_else(condition = str_detect(., "dim_bridge"), true = "dim_bridge_occ_imp_spot_radio_key", false = .)
out <- x_data == x_manual
out_true <- all(out)
if(replace & out_true){
  return(x_manual)
}
return(out_true)
}

refs <- references_tables %>% select(- contains("adintel")) %>%
  mutate(var_name_manual_squish = str_squish(var_name_manual),
         var_name_manual_sep = str_sep_upper(var_name_manual_squish, "_", named = F),
         .after = var_name_manual) %>%
  group_by(file_type, file_name, is_dynamic) %>% nest() %>%
  mutate(data = modify(data, ~ .x %>% as.list() )) %>%
  ungroup()

occs <- occurrences_tables %>%
  select(file_type, file_name,
         var_name_manual, var_type_manual,
         precision, scale,
         description) %>%
  mutate(var_name_manual_squish = str_squish(var_name_manual),
         var_name_manual_sep = str_sep_upper(var_name_manual_squish, "_", named = F),
         .after = var_name_manual) %>%
  group_by(file_type, file_name) %>% nest() %>%
  mutate(data = modify(data, ~ .x %>% as.list() )) %>%
  ungroup()

adintel_table_initial <- get_adintel_tables("/media/filippo/sata_data_1/adintel") %>%
  mutate(data_info = map(full_path, get_col_info, .progress = TRUE))

adintel_reference_tables <- adintel_table_initial %>%
  filter(file_type == "references") %>%
  arrange(file_name, file_year)

adintel_occurrences_tables <- adintel_table_initial %>%
  filter(file_type == "occurrences") %>%
  arrange(file_name, file_year)

####

i=1
i_seq <- seq_along(adintel_reference_tables$data_info)
names_refs_ok <- rep(NA, length(i_seq))
names_refs <- as.list(names_refs_ok)
for(i in i_seq){
  file_name <- adintel_reference_tables$file_name[[i]]
  file_year <- adintel_reference_tables$file_year[[i]]

  names_data <- adintel_reference_tables %>% pull(data_info) %>% pluck(i, "col_name")
  ref_id <- which(refs$file_name == file_name)
  names_manual <- refs %>% slice(ref_id) %>% pull(data) %>% pluck(1, "var_name_manual")

  check <- check_col_names(names_data, names_manual)
  check_replace <- check_col_names(names_data, names_manual,replace = T)

  if(check){
    names_refs[[i]] <- check_replace
  }

  if(file_year == -1) {
    file_year <- ""
  }
  print(paste(file_name, file_year, "->", check))
}

###

i=1
i_seq <- seq_along(adintel_occurrences_tables$data_info)
names_occs_ok <-  rep(NA, length(i_seq))
names_occs <- as.list(names_occs_ok)
for(i in i_seq){
  file_name <- adintel_occurrences_tables$file_name[[i]]
  file_year <- adintel_occurrences_tables$file_year[[i]]

  names_data <- adintel_occurrences_tables %>% pull(data_info) %>% pluck(i, "col_name")
  ref_id <- which((occs$file_name %>% str_split_i("_-", 1)) == file_name)
  names_manual <- occs %>% slice(ref_id) %>% pull(data) %>% pluck(1, "var_name_manual")

  check <- check_col_names(names_data, names_manual)
  check_replace <- check_col_names(names_data, names_manual,replace = T)

  if(check){
    names_occs[[i]] <- check_replace
  }

  if(file_year == -1) {
    file_year <- ""
  }
  print(paste(file_name, file_year, "->", check))
}
which(!names_refs_ok)
which(!names_occs_ok)
