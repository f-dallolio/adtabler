library(data.table)
library(dtplyr)
library(tidyverse)
library(rlang)
library(glue)
devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(bit64)









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
           as.integer())
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


library(dm)

dm_products_no_key <- dm(brd_df, adv_df, pcc_df)
dm_products_no_key |>
  dm_add_pk(table = brd_df, columns = c(year, brand_code)) |>
  dm_add_pk(table = adv_df, columns = c(year, adv_parent_code, adv_subsid_code)) |>
  dm_add_pk(table = pcc_df, columns = c(year, pcc_sub_code, product_id)) |>
  dm_add_fk(table = brd_df, columns = c(year, adv_parent_code, adv_subsid_code), ref_table = adv_df) |>
  dm_add_fk(table = brd_df, columns = c(year, pcc_sub_code, product_id), ref_table = pcc_df) |>
  dm_examine_constraints()




