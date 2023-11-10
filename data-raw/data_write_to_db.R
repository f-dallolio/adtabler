devtools::install_github("f-dallolio/adtabler")
1
library(data.table)
library(tidyverse)
library(rlang)
library(glue)
library(adtabler)
library(prettyunits)

data_write_to_db <- data_info_db |>
  # filter(year |> not_na()) |>
  dplyr::transmute(
    file = full_file_name,
    year,
    media_type_id,
    tbl_name = db_name_std,
    col_names = col_info |> purrr::map(~ .x$col_name_std |> unname()),
    col_classes = col_info |> purrr::map(~ .x$datatype_r |> unname()),
    col_uk = col_info |> purrr::map(~ .x$is_uk |> unname())
  ) |>
  mutate(
    overwrite = year == min(year),
    append = !overwrite,
    .by = tbl_name
  )

usethis::use_data(data_write_to_db, overwrite = TRUE)
