library(tidyverse)
devtools::install_github("f-dallolio/adtabler")
xxx <- read_lines("~/Dropbox/NielsenData/ref_uk2.txt")
x <- xxx[-length(xxx)] %>%
  str_split("___") %>%
  map(~ .x[length(.x)]) %>%
  unlist() %>%
  str_split_i("[\t]", 1) %>%
  str_remove_all(" ") %>%
  str_replace_all("tsv", "tsv___") %>%
  str_split_i("___", 1) %>%
  as_tibble_col("temp") %>%
  mutate(id = str_detect(temp, ".tsv") %>% as.numeric() %>% cumsum())

references_ukey <- x %>% filter(str_detect(temp, ".tsv")) %>%
  rename(file_name_ref = temp) %>%
  inner_join(x %>%
               filter(str_detect(temp, ".tsv", negate = TRUE)) %>%
               rename(col_name_ref = temp)
  ) %>%
  mutate(file_name_std = file_name_ref %>%
           str_remove_all(".tsv") %>%
           adtabler::rename_adintel(),
         col_name_std = adtabler::rename_adintel(col_name_ref))
references_ukey

usethis::use_data(references_ukey, overwrite = TRUE)
