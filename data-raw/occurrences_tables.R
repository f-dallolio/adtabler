library(tidyverse)
library(adtabler)

occurrences_docx <- "data-raw/occurrences_input.docx"
occurrences_tables <- docxtractr::read_docx(occurrences_docx) |>
  docxtractr::docx_extract_all_tbls(guess_header = F) %>%
  set_names(
    c(
      "National TV - NationalTV.tsv",
      "Spot/Local TV - SpotTV.tsv",
      "Magazine - Magazine.tsv",
      "FSI Coupon - FSICoupon.tsv",
      "Newspaper - Newspaper.tsv",
      "Radio – Radio.tsv",
      "Outdoor – Outdoor.tsv",
      "Internet – Internet.tsv",
      "Cinema – Cinema.tsv",
      "Digital – Digital.tsv"
    )
  ) %>%
  imap(~ .x %>% mutate(V0 = .y, .before = 1)) %>%
  list_rbind() %>%
  set_names(c("nms", "adintel_var_name", "adintel_var_type", "nn", "description")) %>%
  mutate(precision = str_split_i(nn, ",", 1),
         scale = str_split_i(nn, ",", 2),
         .before = description,
         .keep = "unused") %>%
  mutate(nms = str_replace_all(nms, "NationalTV.tsv", "NetworkTV.tsv")) %>%
  mutate(adintel_file_type = "Occurrences",
         adintel_file_name = nms %>% str_split_i("-", -1) %>% str_squish(),
         .keep = "unused") %>%
  mutate(file_type = adintel_file_type %>% str_sep_upper("_") %>% str_squish(),
         file_name = adintel_file_name %>% str_sep_upper("_") %>% str_remove_all(".tsv") %>% str_squish(),
         # var_name_manual = adintel_var_name %>% str_sep_upper("_") %>% str_squish(),
         # var_type = adintel_var_type %>% adintel_to_sql(),
         .before = 1) %>%
  relocate(adintel_var_name, adintel_var_type, description, .after = adintel_file_name)

names(occurrences_tables) <- names(occurrences_tables) %>% str_replace_all("adintel","nielsen")

occurrences_tables <- occurrences_tables %>% rename_with(.cols = contains("nielsen"),.fn = ~ .x %>% str_remove_all("occurrences_") %>% str_remove_all("nielsen_") %>% paste("manual", sep = "_"))

occurrences_tables <- occurrences_tables %>%
  mutate(file_name = file_name %>% str_replace_all("_tv", " tv") %>%  str_split_i("_", 1) %>% str_replace_all("fsi",  "fsi_coupon") %>%
  str_replace_all(" ", "_"))
occurrences_tables$file_name %>% unique()

usethis::use_data(occurrences_tables, overwrite = TRUE)
