
devtools::install_github("f-dallolio/adtabler")
1
library(data.table)
library(tidyverse)
library(rlang)
library(glue)
library(dm)
library(adtabler)
library(DBI)
library(RPostgres)

install.packages('S7')
library(S7)








occ_def <- read_csv("data-raw/occ_def.csv")
data_tbl_names <- tbl_info_tot |>
  summarise(media_type_id = list(str_split_comma(media_type_id) |> as.integer()),
            .by = file_name_std) |>
  unnest(media_type_id) |>
  left_join(
    occ_def |>
      transmute(media_type_id, file_name_std, tbl_name = .part_name)
  ) |>
  summarise(across(media_type_id : tbl_name, list), .by =file_name_std ) |>
  rename(data_type = file_name_std) |>
  inner_join(
    tbl_info_tot |>
      transmute(
        data_type = file_name_std,
        col_names = col_names_std,
        col_classes = col_classes,
        col_types = sql_datatype
      )
  )

fn <- function(cmd, col_names, col_classes, ... ) {
  fread(cmd = cmd, col.names = col_names, colClasses = col_classes, na.strings = "")
}


# library(furrr)
# plan(multisession, workers = 30)
#

x <- tbl_info_tot |>
  pull(file) |>
  list_c() |>
  map(decompose_file) |>
  list_rbind() |>

  rename(
    data_category = tbl_type,
    data_type = tbl_name
  ) |>

  filter(data_category == "occurrences") |>

  filter(data_type == "national_tv") |>

  filter(adintel_year == 2010) |>

  inner_join(data_tbl_names) |>

  unnest(c(media_type_id, tbl_name)) |>

  mutate(cmd = seq_along(file) |> map_chr(~ file[[.x]] |> make_grep_cmd(media_type_id = media_type_id[[.x]])))

#
# national_tv <- x |> select(cmd, col_names, col_classes) |>
#   future_pmap(fn, .progress = T)
#
#
# plan(sequential)


xx <- tbl_info_tot |>
  pull(file) |>
  list_c() |>
  map(decompose_file) |>
  list_rbind() |>

  rename(
    data_category = tbl_type
  ) |>

  filter(data_category == "references") |>
  #
  # filter(data_type == "national_tv") |>

  filter(adintel_year %in% c(NA, 2010)) |>

  inner_join(data_tbl_names |>
               select(-tbl_name) |>
               rename(tbl_name = data_type)) |>

  unnest(everything()) |>
  transmute(tbl_name_ref = tbl_name, col_names = col_names)

xx <- tibble(tbl_name_ref = 'brand',
             col_names2 = 'brand_code',
             col_names = paste(c('prim', 'scnd'), 'brand_code', sep = '_')) |>
  bind_rows(
    xx |>
      filter(tbl_name_ref == 'brand') |>
      mutate(col_names2 = col_names,
             col_names = col_names |> str_replace('brand_code', 'ter_brand_code'))
  ) |>
  bind_rows(
    xx |>
      filter(tbl_name_ref != 'brand') |>
      mutate(col_names2 = col_names)
  )
xx
xxx <- x |>
  select(
    tbl_name,
    col_names
  ) |>
  unnest(everything()) |>
  distinct() |>
  group_by(tbl_name) |>
  group_split() |>
  map(~ left_join(.x, xx)) |>
  list_rbind() |>
  filter(not_na(tbl_name_ref)) |>
  print(n=200)

xxx |> select(tbl_name_ref, col_names2) |> distinct() |> arrange(tbl_name_ref) |>
  summarise(pk2 = str_flatten_comma(col_names2), .by = tbl_name_ref) |>

  left_join(
    tbl_info_tot |>
      select(file_name_std, pk) |>
      arrange(file_name_std) |>
      rename(tbl_name_ref = file_name_std) |>
      filter(not_na(pk)) |>
      mutate(pk = pk |> str_remove_all('ad_date, '))

  )

