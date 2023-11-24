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
#   fn <- function(col_names_std, sql_datatype){
#     if_else(col_names_std == "ad_date", 'DATE', sql_datatype)
#   }
#
# tbl_info_tot <- tbl_info_list$tbl_info |>
#   map(~.x$sql_datatype) |>
#   as_tibble() |>
#   pivot_longer(everything(),
#                names_to = 'file_name_std', values_to = 'sql_datatype') |>
#   mutate(file_name_std = file_name_std |> str_split_i('__', 2)) |>
#   left_join(tbl_info_tot |> select( -sql_datatype)) |>
#   relocate(file_name_std, .after = 2) |>
#   relocate(sql_datatype, .after = col_classes) |>
# mutate(col_classes = col_classes |> map(unname),
#        sql_datatype = map2(col_names_std, sql_datatype, ~ fn(.x, .y)))
# tbl_info_tot$sql_datatype
#

tbl_info_tot$dynamic_flag[tbl_info_tot$file_name_std == 'dates'] <- FALSE

tbl_info_tot$file[tbl_info_tot$file_name_std == 'dates'] <- '/mnt/sata_data_1/adintel/ADINTEL_DATA_2021/nielsen_extracts/AdIntel/Master_Files/Latest/Dates.tsv'
tbl_info_tot$file[tbl_info_tot$file_name_std == 'media_type'] <- '/mnt/sata_data_1/adintel/ADINTEL_DATA_2021/nielsen_extracts/AdIntel/Master_Files/Latest/MediaType.tsv.tsv'
# usethis::use_data(tbl_info_tot, overwrite = T)
usethis::use_data(tbl_info_tot, overwrite = T)

tbl_info_tot$sql_datatype <- tbl_info_tot$sql_datatype |> map(~ .x |> str_replace_all('DATETIME', 'TIMESTAMP'))
tbl_info_tot$pk[tbl_info_tot$dynamic_flag] <-
  if_else(str_detect(tbl_info_tot$pk[tbl_info_tot$dynamic_flag], 'ad_date', negate = T),
          paste0('ad_date, ', tbl_info_tot$pk[tbl_info_tot$dynamic_flag]),
          tbl_info_tot$pk[tbl_info_tot$dynamic_flag])

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
      dynamic_flag,
      adintel_year,
      .tbl_name = file_name_std,
      .col_names = col_names_std,
      .data_types = sql_datatype,
      .pk = pk,
      .part_name = paste(.tbl_name, paste0("y", adintel_year), sep = "__"),
      .from = unlist(date_from),
      .to = unlist(date_to)
    ) |>
    mutate(
      .part_col = if_else(!dynamic_flag, NA, 'ad_date'),
      .before = .part_name
    )
  # |>   future_pmap(sql_build_tbl)
) |>
  list_rbind() |>
  nest(.by = !(c(.tbl_name, adintel_year, .part_name : .to))) |>
  mutate(.tbl_name = file_name_std,
         .after = file_name_std)

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


dbListTables(con) |>
  # str_subset("__", negate = T) |>
  walk(~dbRemoveTable(conn = con, name = .x))

walk(x$tbl_code, ~dbSendQuery(conn = con, statement = .x))
walk(x$part_code[x$dynamic_flag][[1]] , ~dbSendQuery(conn = con, statement = .x[[1]]))


for(i in 1:NROW(x)){
  part <- x |> slice(i) |> pull(part_code) |> list_c()
  if(i != 20 ) {
    for(j in seq_along(part)){
      if(not_na(part[[j]])){
        dbSendQuery(con, part[[j]])
      }
    }
  }
}
