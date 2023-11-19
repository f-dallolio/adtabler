devtools::install_github('f-dallolio/adtabler')
library(tidyverse)
library(adtabler)

adtabler::tbl_definitions
file <- list.files(
  '/mnt/sata_data_1/adintel',
  full.names = TRUE,
  recursive = TRUE
)

file_to_info(file)

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
