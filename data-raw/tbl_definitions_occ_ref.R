library(tidyverse)
library(adtabler)

man <- data_info_list$manual_info |>
  transmute(
    file_type_std,
    file_name_std,
    tbl_name = sql_tbl_name(file_type_std, file_name_std),
    col_names_std = col_name_std,
    col_classes = sql_define_datatypes(
      list( datatype_man, sql_length, sql_scale),
      output = r
    ),
    col_classes = if_else(
      col_names_std == "ad_date",
      "Date",
      col_classes
    ),
    sql_datatype = sql_define_datatypes(
      list( datatype_man, sql_length, sql_scale),
      output = sql
    ),
    sql_datatype = if_else(
      col_names_std == "ad_date",
      "DATE",
      sql_datatype
    ),
    sql_datatype_min = sql_define_datatypes(
      list( datatype_man, sql_length, sql_scale),
      output = sql_min
    )
  ) |>
  mutate(col_pos = seq_along(col_names_std),
         .before = col_names_std,
         .by = tbl_name)

fix_man_list <- man |>
  nest(.by = col_names_std) |>
  mutate(
    data = set_names(data, col_names_std),
    n_r = data |> map_vec(~ .x |> pull(col_classes) |> n_distinct()),
    n_sql = data |> map_vec(~ .x |> pull(sql_datatype) |> n_distinct()),
    n_sql_min = data |> map_vec(~ .x |> pull(sql_datatype_min) |> n_distinct())
  ) |>
  filter(if_any(contains("n_"), ~ .x > 1)) |>
  pull(data)

fix_man_list$ad_code$sql_datatype <- 'TEXT'
fix_man_list$spend <- fix_man_list$spend |>
  mutate(across(contains("sql_"), ~ 'NUMERIC(15, 2)'))
fix_man_list$imp2_plus <- fix_man_list$imp2_plus |>
  mutate(col_classes = 'integer',
         across(contains("sql_"), ~ "INTEGER"))
fix_man_list$ad_time <- fix_man_list$ad_time |>
  mutate(sql_datatype = 'VARCHAR(8)')
fix_man_list$genre_id <- fix_man_list$genre_id |>
  mutate(sql_datatype = 'INTEGER')

tbl_definitions <- man |>
  nest(.by = col_names_std) |>
  mutate(
    data = set_names(data, col_names_std),
    n_r = data |> map_vec(~ .x |> pull(col_classes) |> n_distinct()),
    n_sql = data |> map_vec(~ .x |> pull(sql_datatype) |> n_distinct()),
    n_sql_min = data |> map_vec(~ .x |> pull(sql_datatype_min) |> n_distinct())
  ) |>
  filter(if_all(contains("n_"), ~ .x == 1)) |>
  pull(data) |>
  c(fix_man_list) |>
  imap(~ .x |> mutate(col_names_std = .y, .before = col_classes)) |>
  list_rbind() |>
  arrange(tbl_name, col_pos) |>
  summarise(across(col_pos : sql_datatype_min, list),
            .by = c(file_type_std, file_name_std, tbl_name))

tbl_definitions <- data_info_list$unique_key |>
  mutate(keys = list(str_split_comma(col_unique_key)),
         .by = media_type_id,
         .keep = "unused") |>
  unnest(everything()) |>
  summarise(
    media_type_id = list(unique(media_type_id)),
    keys = list(unique(keys)),
    .by = c(file_type_std, file_name_std)
  ) |>
  inner_join(
    tbl_definitions
  ) |>
  relocate(tbl_name,
           .after = file_name_std) |>
  inner_join(
    data_info_list$manual_info |>
      transmute(
        file_type_std,
        file_name_std,
        col_names_std = col_name_std,
        col_names_man = col_name_man,
        col_type_man = datatype_man,
        col_dim1_man = sql_length,
        col_dim2_man = sql_scale,
        description
      ) |>
      nest(.by = c(file_type_std, file_name_std),
           .key = "adintel_manual")
  ) |>
  mutate(
    tbl_name = list(tbl_name),
    .by = c(file_type_std, file_name_std)
  )
tbl_definitions


usethis::use_data(tbl_definitions, overwrite = TRUE)
