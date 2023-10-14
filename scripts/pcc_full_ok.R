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
pcc_table <- list.files(path = new_adintel_path, recursive = TRUE, full.names = TRUE) |>
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
         file_type2 == "product_categories") |>
  mutate(year = file |>
           str_remove_all(".csv") |>
           str_split_i("__", -1) |>
           as.integer()) |>
  select(-file, -file_type, -file_type2)

pcc_df_0 <- map2(
  .x = pcc_table$file_path,
  .y = pcc_table$year,
  .f = ~ .x |> read_csv() |> mutate(year = .y)
) |>
  list_rbind() |>
  rename_with(rename_adintel) |>
  rename(
    pcc_prod_desc = product_desc,
    pcc_prod_code = product_id
  ) |>
  mutate(
    pcc_prod_code = pcc_prod_code |>
      str_pad(width = max(nchar(pcc_prod_code)),
              side = "right",
              pad = "0"),
    pcc_id = pcc_sub_code |>
      str_c(pcc_prod_code,
            sep = ".")
  ) |>
  relocate(
    pcc_id,
    pcc_prod_code,
    pcc_sub_code,
    pcc_maj_code,
    pcc_indus_code,
    pcc_prod_desc,
    .before = 1
  ) |>
  arrange(pcc_id) |>
  summarise(
    year_chr_temp = paste0(year, collapse = "_"),
    .by = !year
  ) |>
  mutate(
    row_id = row_number(),
    n = n(),
    year_chr = str_c(year_chr_temp, collapse = "_"),
    .by = pcc_id
  )
# |>
  # nest(.by = pcc_id) |>
  # mutate(
  #   n = data |>
  #     map(NROW) |>
  #     list_c()
  # ) |>
  # unnest(everything()) |>

pcc_df_old <- pcc_df_0 |>
  filter(n > 1) |>
  select(- c(row_id : year_chr)) |>
  arrange(pcc_id)
pcc_df_old


pcc_df <- pcc_df_0 |>
  filter(
    row_id == max(row_id),
    .by = pcc_id
  ) |>
  arrange(desc(n)) |>
  mutate(
    min_year = str_split_i(year_chr, "_", 1) |>
      as.integer(),
    max_year = str_split_i(year_chr, "_", -1) |>
      as.integer(),
    .keep = "unused"
  ) |>
  select(- (year_chr_temp : n)) |>
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
  relocate(
    contains("date"),
    contains("pcc_indus"),
    contains("pcc_maj"),
    contains("pcc_sub"),
    contains("pcc_prod"),
    .before = 1
  ) |>
  select(-pcc_id)

pcc_df |> summary()

pcc_dm <- dm(pcc_df) |>
  dm_add_pk(pcc_df, c(pcc_sub_code, pcc_prod_code))
dm_examine_constraints(pcc_dm)

check_utf8(pcc_df)
