## code to prepare `media_type_table` dataset goes here
library(tidyverse)
library(adtabler)

media_type_table0 <- read_csv("data-raw/media_type_table.csv") %>%
  mutate(media_type_id = as.integer(media_type_id)) %>%
  mutate(file_type = str_sep_upper(adintel_file_type, "_"),
         file_name = str_sep_upper(adintel_file_name, "_") %>% str_remove_all(".tsv"),
         .before = 1)
media_type_table0

media_unique_keys <- read_csv("data-raw/media_unique_keys.csv")

media_type_table <- media_unique_keys %>%
  mutate(adintel_unique_key = unique_key %>% map(~ .x %>% str_remove_all('\\"') %>% str_split(",")) %>% list_c()) %>%
  unnest(cols = everything()) %>%
  mutate(adintel_unique_key = str_squish(adintel_unique_key),
         unique_key = adintel_unique_key %>% str_sep_upper("_")) %>%
  left_join(x = media_type_table0)

names(media_type_table) <- names(media_type_table) %>% str_replace_all("adintel","nielsen")
media_type_table

usethis::use_data(media_type_table, overwrite = TRUE)
