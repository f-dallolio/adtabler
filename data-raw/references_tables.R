library(tidyverse)
library(glue)
library(adtabler)

ref_tables0 <- read_csv("data-raw/references_input.txt", col_names = F)

ref_tables <- ref_tables0 %>%
  transmute(x1 = str_squish(X1)) %>%
  filter(!is.na(x1),
         x1 != "",
         # str_detect(X1, ".tsv", negate = T),
         str_detect(x1,"Note: For Spot", negate = T)) %>%
  mutate(x2 = str_detect(x1, ".tsv") %>% as.numeric(),
         id = cumsum(x2)) %>%
  split(f = .$id) %>%
  map(~ .x %>% slice(-1) %>%
        mutate(nms = .x %>% slice(1) %>% pull(1), .before = 1)) %>%
  list_rbind() %>%
  select(1,2) %>%
  mutate(adintel_var_name = str_split_i(x1, " ", 1),
         adintel_var_type = str_split_i(x1, " ", 2),
         nn = str_split_i(x1, " ", 3),
         description = str_remove_all(x1, paste(adintel_var_name, adintel_var_type, nn)) %>% str_squish()) %>%
  mutate(adintel_var_name = if_else(is.na(as.numeric(nn)),
                                    paste0(str_split_i(x1, " ", 1), str_split_i(x1, " ", 1)),
                                    adintel_var_name),
         adintel_var_type = if_else(is.na(as.numeric(nn)),
                                    str_split_i(x1, " ", 3),
                                    adintel_var_type),
         nn = if_else(
           is.na(as.numeric(nn)),
           str_split_i(x1, " ", 4),
           nn),
         description = if_else(
           is.na(as.numeric(nn)),
           str_remove_all(x1, paste(adintel_var_name, adintel_var_type, nn)) %>% str_squish(),
           description))  %>%
  select(-x1) %>%
  mutate(precision = str_split_i(nn, ";", 1),
         scale = str_split_i(nn, ";", 2),
         adintel_file_name = str_split_i(nms, " ", -1),
         is_dynamic = str_detect(nms, "(Dynamic)"),
         .before = description,
         .keep = "unused") %>%
  mutate(
    file_type = "references",
    file_name = str_sep_upper(adintel_file_name, "_") %>%
      str_remove_all(".tsv"),
    # adintel_static_flag = str_detect(nms, "(Dynamic)", negate = TRUE),
    .before = 1) %>%
  suppressWarnings()

references_tables <- ref_tables %>%
  mutate(adintel_var_name = case_when(file_name == "market_breaks" & precision == "3" ~ "MarketBreaksCode",
                                      file_name == "market_breaks" & precision != "3" ~ "MarketBreaksDesc",
                                      .default = adintel_var_name)) %>%
  relocate(is_dynamic, .before = file_name) %>%
  relocate(adintel_var_name, adintel_var_type, .after = adintel_file_name) %>%
  mutate(var_name_manual = adintel_var_name, # %>% str_sep_upper("_") %>% str_squish(),
         var_type_manual = adintel_var_type, # %>% adintel_to_sql(),
         # var_type_r_manual = adintel_var_type %>% adintel_to_sql(r_out = TRUE),
         .after = is_dynamic) %>%
  relocate(file_name, .after = file_type)

# names(references_tables) <- names(references_tables) %>% str_replace_all("adintel","nielsen")
#
# references_tables <- references_tables %>% rename_with(.cols = contains("nielsen"),.fn = ~ paste(.x, "references_manual", sep = "_"))
# names(references_tables) <- names(references_tables) %>% str_remove_all("nielsen_")

references_tables

usethis::use_data(references_tables, overwrite = TRUE)
