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

tbl_info_tot$file |>  unlist()
tbl_info_tot


# |>fn_fmls_names(sql_build_tbl)[1:5] <- fn_fmls_names(sql_build_tbl)[1:5] |> str_remove_all('\\.')

xxx <- unnest(tbl_info_tot, file)
xxx$file |>
  # str_subset(no_case('brand')) |>
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
      .tbl_name = file_name_std,
      .col_names = col_names_std,
      .data_types = sql_datatype,
      .pk = pk,
      .part_col = 'ad_date',
      .part_name = paste(.tbl_name, paste0("y", adintel_year), sep = "__"),
      .from = unlist(date_from),
      .to = unlist(date_to)
    )
  # |>   pmap(sql_build_tbl)
) |> list_rbind()
x <- .Last.value |>
  mutate(.tbl_name2 = .tbl_name) |>
  nest(.by = !(c(.tbl_name, .part_name : .to))) |>
  rename(.tbl_name = .tbl_name2) |>
  relocate(.tbl_name, 1)



x$tbl_code <- x|>
  pmap(sql_build_tbl) |> map(~.x) |> set_names(x$.tbl_name)
x$part_code <- x$data |>
  map(~ .x |> pmap(sql_create_part) |> set_names(.x$.part_name))
x |>  select(.tbl_name, tbl_code, part_code)
tbl_info_tot |> print(n=200)
tbl_info_tot$file[tbl_info_tot$file_name_std == 'media_type'] <- list('/mnt/sata_data_1/adintel/ADINTEL_DATA_2021/nielsen_extracts/AdIntel/Master_Files/Latest/Dates.tsv')
usethis::use_data(tbl_info_tot, overwrite = T)



map_vec(2010:2021, ~ glue('/mnt/sata_data_1/adintel/ADINTEL_DATA_{ .x }/nielsen_extracts/AdIntel/Master_Files/Latest/Dates.tsv')) |>
  walk(~ lookup_date |> write_tsv(.x))






xx <- x |>

  nest(.by = .tbl_name) |>
  mutate(data = data |> map(~ as.list(.x) |> set_names(names(.x))))
xx



x$create_table_code <- map2(x$tbl_code, x$part_code, ~ c(list(.x), .y))
x$create_table_code$.tbl_name
