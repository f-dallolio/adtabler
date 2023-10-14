library(data.table)
library(dtplyr)
library(tidyverse)
library(rlang)
library(glue)
library(fdutils)
devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(bit64)
library(dm)

new_adintel_path <- '/mnt/sata_data_1/new_adintel/'
adv_tbl <- list.files(path = new_adintel_path, recursive = TRUE, full.names = TRUE) |>
  as_tibble_col(column_name = "file_path") |>
  mutate(
    file_path = file_path |>
      str_replace_all("//", "/"),
    file_type = file_path |>
      str_remove_all(new_adintel_path) |>
      str_split_i("/", 1),
    file_type2 = file_path |> str_split_i("/", -2),
    file = file_path |> str_split_i("/", -1)
  ) |>
  filter(file_type ==  "references",
         file_type2 == "advertiser") |>
  mutate(year = file |>
           str_remove_all(".csv") |>
           str_split_i("__", -1) |>
           as.integer()) |>
  select(-file, -file_type, -file_type2)

adv_df_0 <- map2(
  .x = adv_tbl$file_path,
  .y = adv_tbl$year,
  .f = ~ .x |> read_csv() |>
    mutate(year = .y) |>
    rename_with(rename_adintel)
) |>
  list_rbind() |>
  summarise(
    year_chr_temp = paste0(year, collapse = "_"),
    .by = !year
  ) |>
  mutate(
    row_id = row_number(),
    n = max(row_id),
    year_chr = str_c(year_chr_temp, collapse = "_"),
    .by = contains("code")
  )
adv_df_0

adv_df_old <- adv_df_0 |>
  filter(n > 1) |>
  select(- c(row_id : year_chr)) |>
  arrange(adv_parent_code, adv_subsid_code)
adv_df_old

adv_df <- adv_df_0 |>
  filter(
    row_id == max(row_id),
    .by = contains("code")
  ) |>
  mutate(
    min_year = str_split_i(year_chr, "_", 1) |>
      as.integer(),
    max_year = str_split_i(year_chr, "_", -1) |>
      as.integer(),
    .keep = "unused"
  ) |>
  select(- c(year_chr_temp, n, row_id)) |>
  left_join(
    date_year_minmax |>
      transmute(
        min_year = year,
        start_date = min_date
      )
  ) |>
  left_join(
    date_year_minmax |>
      transmute(
        max_year = year,
        end_date = max_date
      )
  ) |>
  select(!contains("year")) |>
  arrange(adv_parent_code, adv_subsid_code)


adv_dm <- dm(adv_df) |>
  dm_add_pk(adv_df, c(adv_parent_code, adv_subsid_code))
dm_examine_constraints(adv_dm)

check_utf8(adv_df)
