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

con <- dbConnect(RPostgres::Postgres(),
                 dbname = 'new_test',
                 host = '10.147.18.200',
                 port = 5432,
                 user = 'postgres',
                 password = '100%Postgres')

# tbl_info_tot <- tbl_info_tot |> select(-date_from, -date_to) |>
#   left_join(
#     tbl_dates_from_to |>
#       select(file_name_std, dynamic_flag, date_from, date_to) |>
#       summarise(dynamic_flag = unique(dynamic_flag),
#                 across(!dynamic_flag, list), .by = file_name_std)
#   ) |>
#   relocate(dynamic_flag, adintel_year, date_from, date_to, .before = file)
#   tbl_info_tot <- tbl_info_tot |>
# mutate(col_classes = col_classes |> map(unname),
#        sql_datatype = map2(col_names_std, sql_datatype, ~ fn(.x, .y)))
# usethis::use_data(tbl_info_tot, overwrite = T)
# usethis::use_data(tbl_info_tot, overwrite = T)



x <- tbl_info_tot$file |>
  unlist() |>
  # slice_vec(1) |>
  map(
  ~ .x |> decompose_file() |>
  inner_join(
    tbl_dates_from_to
  ) |>
  inner_join(
    tbl_info_tot |>
      select(file_name_std, col_names_std : sql_datatype_min, pk)
  ) |>
    transmute(
      file_name_std,
      .tbl_name = file_name_std,
      .col_names = col_names_std,
      .data_types = sql_datatype,
      .pk = pk,
      .part_name = paste(.tbl_name, paste0("y", adintel_year), sep = "__"),
      .from = unlist(date_from),
      .to = unlist(date_to)
    ) |>
    mutate(
      .part_col = if_else(is.na(.from), NA, 'ad_date'),
      .before = .part_name
    )
  # |>   pmap(sql_build_tbl)
) |>
  list_rbind() |>
  nest(.by = !(c(.tbl_name, .part_name : .to))) |>
  mutate(.tbl_name = file_name_std,
         .after = file_name_std) |>
  left_join(tbl_dates_from_to |> select(file_name_std, dynamic_flag))
x

tbl_code <- x |>
  pmap(sql_build_tbl) |> map(~.x) |> set_names(x$.tbl_name)
x$tbl_code <- tbl_code

part_code <- x |>
  filter(dynamic_flag) |>
  pull(data) |>
  map(~ .x |> pmap(sql_create_part) |> set_names(.x$.part_name))
x$part_code <- rep(NA, NROW(x)) |> as.list()
x$part_code[(x$dynamic_flag)] <- part_code

x |> filter(file_name_std == 'dates') |> pull(tbl_code)



fn <- function(col_names_std, sql_datatype){
  if_else(col_names_std == "ad_date", 'date', col_names_std)
}


