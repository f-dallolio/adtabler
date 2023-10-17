library(tidyverse)
library(readxl)
devtools::install_github('f-dallolio/fdutils')
1
library(fdutils)
devtools::install_github('f-dallolio/adtabler')
1
library(adtabler)


adintel_dir <- "/mnt/sata_data_1/new_adintel/"

# adintel_files ----
adintel_files <- list.files(
  path = adintel_dir,
  recursive = T
) |>
  as_tibble_col(
    column_name = "file"
  ) |>
  mutate(
    file_type = file |>
      str_split_i("/", 1)
  ) |>
  nest(.by = file_type)

# unique_key_tables ----

unique_key_table <- read_excel("Dropbox/NielsenData/unique_key_table.xlsx") |>
  mutate(UniqueKey = UniqueKey |>
           str_replace_all("MediatType", "MediaType") |>
           str_remove_all(" ")) |>
  rename_with(rename_adintel) |>
  select(media_type_id, occurrence_filename, unique_key) |>
  rename(file_name = occurrence_filename) |>
  mutate(
    file_name = file_name |>
      str_remove_all(".tsv") |>
      rename_adintel() |>
      str_replace_all("national_", "network_"),
    unique_key = unique_key |>
      map(
        ~ .x |>
          str_split(",") |>
          list_c() |>
          rename_adintel() |>
          as_tibble_col(column_name = "unique_key")

      )
    )

# occurrences_temp ----
occurrences_temp <- adintel_files |>
  filter(file_type == "occurrences") |>
  pluck("data", 1) |>
  mutate(temp = file,
         file = str_c(adintel_dir, file, sep = "/")) |>
  separate_wider_delim(
    cols = temp,
    delim = "/",
    names = to_c(
      file_type,
      media_category,
      media_geo,
      media_type,
      temp
    )
  ) |>
  mutate(
    media_type_id = temp |>
      str_remove_all("ok__") |>
      str_split_i("__", 1) |>
      str_remove_all("id") |>
      as.integer(),
    .keep = "unused"
  ) |>
  select(-file_type)
#----




x <- adintel_tables |>
  filter(file_type == "occurrences") |>
  pull(var_attributes) |>
  map(as_tibble) |>
  list_rbind() |>
  select(file_name, var_name_manual : scale) |>
  mutate(
    scale = if_else(var_name_manual == "spend", 2, scale),
    col_names = rename_adintel(var_name_manual),
    var_type_manual = if_else(str_detect(col_names, "ad_date"), "date", var_type_manual),
    col_classes = adintel_to_sql(var_type_manual, r_out = TRUE),
    sql_datatype = var_name_manual |>
      to_sql_datatype(type = var_type_manual,
                      precision = precision,
                      scale = scale),
    .before = precision
  ) |>
  select(- contains("var")) |>
  nest(.by = file_name, .key = "col_info") |>
  left_join(unique_key_table)

final <- occurrences_temp |>
  nest(file = file) |>
  left_join(x) |>
  select(-file_name) |>
  relocate(media_type_id, .before = 1)



file_df <- final |>
  select(media_type_id : media_type, file) |>
  unnest(everything())
file_df







col_df <- final |>
  select(media_type_id, col_info) |>
  unnest(everything()) |>
  select(media_type_id : col_classes, sql_datatype)
col_df
ukey_df <- final |>
  select(media_type_id, unique_key) |>
  unnest(everything())
ukey_df
#
# library(dm)
#
# my_dm <- dm(file_df, col_df, ukey_df)
# my_dm <- my_dm |>
#   dm_add_pk(table = file_df, columns = media_type_id) |>
#   dm_add_pk(table = col_df, columns = c(media_type_id, col_names)) |>
#   dm_add_pk(table = ukey_df, columns = c(media_type_id, unique_key)) |>
#   dm_add_fk(table = file_df, columns = media_type_id, ref_table = col_df) |>
#   dm_add_fk(table = file_df, columns = media_type_id, ref_table = ukey_df)
# dm_draw(my_dm)
#
# dm_flatten_to_tbl(dm = my_dm, .start = file_df)
# dm_examine_constraints(my_dm)
#
#
#
#

