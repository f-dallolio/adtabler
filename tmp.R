devtools::install_github('f-dallolio/adtabler')
library(tidyverse)
library(adtabler)

adtabler::tbl_definitions
file <- list.files(
  '/mnt/sata_data_1/adintel',
  full.names = TRUE,
  recursive = TRUE
) |>
  str_subset("Master_Files", negate = T)

info_static <- list.files(
  '/mnt/sata_data_1/adintel',
  full.names = TRUE,
  recursive = TRUE
) |>
  str_subset("Master_Files", negate = F) |>
  str_subset("2021") |>
  str_subset("Latest") |>
  as_tibble_col("file") |>
  mutate(
    year = NA,
    date_from = NA,
    date_to = NA,
    .before = file
  ) |>
  mutate(
    file_type_std = "reference",
    file_name_std = str_split_i(file, "/", -1) |>
      str_remove_all(".tsv") |>
      rename_adintel(named = FALSE),
    tbl_name = paste("ref_", file_name_std),
    .before = 1
  ) |>
  nest(.by = file_type_std : tbl_name,
         .key = "file_info")

info_dynamic <- file_to_info(file) |>
  select(file_type_std : file) |>
  nest(.by = file_type_std : tbl_name,
       .key = "file_info") |>
  inner_join(
    tbl_definitions
  )

tbl_info_tot <-
  info_dynamic |>
  bind_rows(
    info_static
  )

usethis::use_data(tbl_info_tot, overwrite = T)

col_def <- tbl_definitions |>
  slice(1) |>
  select(col_names_std, sql_datatype) |>
  unnest(everything()) |>
  mutate(
    nms = col_names_std |>
      str_pad(width = max(nchar(col_names_std)), side = "right", pad = " "),
    x = paste("  ", nms, sql_datatype)
 ) |>
  pull(x) |>
  str_flatten(',\n\t  ') |>
  glue::glue()

sql_create_tbl(
  tbl_name = tbl_definitions |>
    slice(1) |>
    select(tbl_name) |>
    pull(1),
  col_def = col_def
)
