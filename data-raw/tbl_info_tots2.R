devtools::install_github('f-dallolio/adtabler')
library(tidyverse)
library(adtabler)

tbl_info_tot |>
  nest(.by = file_type_std : tbl_name,
       .key = "data_info")

file_dynamic <- list.files(
  '/mnt/sata_data_1/adintel',
  full.names = TRUE,
  recursive = TRUE
) |>
  str_subset("Master_Files", negate = T) |>
  file_to_info_dynamic() |>
  select(file_type_std : file) |>
  nest(.by = file_type_std : tbl_name,
       .key = "file_info")

file_static <- list.files(
  '/mnt/sata_data_1/adintel',
  full.names = TRUE,
  recursive = TRUE
) |>
  str_subset("Master_Files", negate = F) |>
  str_subset("2021") |>
  str_subset("Latest") |>
  file_to_info_static() |>
  select(file_type_std : file) |>
  nest(.by = file_type_std : tbl_name,
       .key = "file_info")

tbl_info_tot <- file_static |>
  bind_rows(
    file_dynamic
  ) |>
  inner_join(tbl_info_tot) |>
  arrange(file_type_std,
          file_name_std,
          tbl_name)

usethis::use_data(tbl_info_tot, overwrite = T)
