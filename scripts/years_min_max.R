library(data.table)
library(dtplyr)
library(tidyverse)
library(rlang)
library(glue)
devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(bit64)
library(dm)


adtabler::adintel_info |>
  filter(file_type == "references") |>
  pull(data_info) |>
  map(
    ~ as_tibble(.x[-c(3:5)]) |>
      mutate(col_classes_sql = adintel_to_sql(col_info_manual)) |>
      rename(
        file_type2 = file_name,
        col_classes_r = col_info_data
      ) |>
      relocate(col_classes_r, .after = col_names) |>
      select(-col_info_manual)
  ) |> list_rbind()

prod_table <- list.files(path = '/mnt/sata_data_1/new_adintel/', recursive = TRUE, full.names = TRUE) |>
  as_tibble_col(column_name = "file_path") |>
  mutate(
    file_type2 = file_path |> str_split_i("/", -2),
    file = file_path |> str_split_i("/", -1)
  ) |>
  filter(file_path  |> str_detect("references")) |>
  filter(file_type2 %in% c("advertiser", "brand", "product_categories")) |>
  mutate(year = file |> str_remove_all(".csv") |>
           str_split_i("__", -1) |>
           as.integer()) |>
  left_join(date_year_minmax)

prod_table |> print(n=200)

prod_list <- prod_table |>
  # mutate(year = paste0("y", year)) |>
  select(-file) |>
  split(prod_table$year) |>
  map(~ .x |> pivot_wider(names_from = file_type2, values_from = file_path))

df <- prod_list[[11]]

make_products_tbl <- function(df){
  year = df$year
  brd <- read_csv(df$brand, show_col_types = F) |>
    rename_with(rename_adintel)
  check_brd <- check_utf8(brd)
  print(glue::glue('"brand" { year }: { check_brand }'))

  adv <- read_csv(df$advertiser, show_col_types = F) |>
    rename_with(rename_adintel)
  check_adv <- check_utf8(adv)
  adv$adv_parent_desc |> check_utf8()
  print(glue::glue('"advertiser" { year }: { check_adv }'))

  pcc <- read_csv(df$product_categories, show_col_types = F) |>
    rename_with(rename_adintel)
  check_pcc <- check_utf8(pcc)
  print(glue::glue('"product_categories" { year }: { check_pcc }'))

  dm_products_no_key <- dm(brd, adv, pcc)
  dm_products <- dm_products_no_key |>
    dm_add_pk(table = brd, columns = c(brand_code)) |>
    dm_add_pk(table = adv, columns = c(adv_parent_code, adv_subsid_code)) |>
    dm_add_pk(table = pcc, columns = c(pcc_sub_code, product_id)) |>
    dm_add_fk(table = brd, columns = c(adv_parent_code, adv_subsid_code), ref_table = adv) |>
    dm_add_fk(table = brd, columns = c(pcc_sub_code, product_id), ref_table = pcc)
  dm_products |> dm_examine_constraints() |> suppressMessages() |> print(
  dm_flatten_to_tbl(dm = dm_products, .start = brd, .recursive = TRUE) |>
    mutate(year = year, .before = 1)
}

prod_list |> map(make_products_tbl)



# >
#   filter(year |> between(2010, 2012))

brd_df <- prod_table |>
  filter(file_type2 == "brand") |>
  mutate(df = map2(
    .x = file_path,
    .y = year,
    .f = ~ read_csv(.x) |>
      mutate(year = .y, .before = 1))
  ) |>
  pull(df) |>
  list_rbind() |>
  rename_with(rename_adintel)

brd_nest <- brd_df |>
  nest(.by = !year) |>
  mutate(n_yr = map(data, NROW) |> list_c())
brd_nest$n_yr |> summary()


adv_df <- prod_table |>
  filter(file_type2 == "advertiser") |>
  mutate(df = map2(
    .x = file_path,
    .y = year,
    .f = ~ read_csv(.x) |>
      mutate(year = .y, .before = 1))
  ) |>
  pull(df) |>
  list_rbind() |>
  rename_with(rename_adintel)

pcc_df <- prod_table |>
  filter(file_type2 == "product_categories") |>
  mutate(df = map2(
    .x = file_path,
    .y = year,
    .f = ~ read_csv(.x) |>
      mutate(year = .y, .before = 1))
  ) |>
  pull(df) |>
  list_rbind() |>
  rename_with(rename_adintel)




dm_products_no_key <- dm(brd_df, adv_df, pcc_df)
dm_products_no_key |>
  dm_add_pk(table = brd_df, columns = c(year, brand_code)) |>
  dm_add_pk(table = adv_df, columns = c(year, adv_parent_code, adv_subsid_code)) |>
  dm_add_pk(table = pcc_df, columns = c(year, pcc_sub_code, product_id)) |>
  dm_add_fk(table = brd_df, columns = c(year, adv_parent_code, adv_subsid_code), ref_table = adv_df) |>
  dm_add_fk(table = brd_df, columns = c(year, pcc_sub_code, product_id), ref_table = pcc_df) |>
  dm_examine_constraints()




