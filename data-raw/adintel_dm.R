devtools::install_github("f-dallolio/adtabler")
1
library(data.table)
library(tidyverse)
library(rlang)
library(glue)
library(dm)
library(adtabler)

library(S7)


adintel_tbl <- list()
adintel_tbl$info <- tbl_info_tot |>
  transmute(
    dynamic_flag,
    file_type_std,
    file_name_std,
    media_type_id = media_type_id |>
      map(~str_split_comma(.x) |>
            as.list() |>
            as.integer())
  ) |>
  unnest(media_type_id) |>
  inner_join(
    read_csv('data-raw/occ_def.csv')
    # read_csv('data-raw/occ_def.csv') |>
    # transmute(media_type_id, tbl_name = .part_name)
  ) |>
  rename(tbl_name = .part_name)

adintel_tbl$files <- tbl_info_tot |>
  select(file) |>
  unnest(everything()) |>
  mutate(data = file |>
           map(~ decompose_file(.x) |> select(-file))) |>
  unnest(everything()) |>
  rename(file_name_std = tbl_name) |>
  nest(.by = c(file_name_std)) |>
  left_join(
    adintel_tbl$info |>
      select(tbl_name, file_name_std)
  ) |> mutate(
    tbl_name = if_else(is.na(tbl_name), file_name_std, tbl_name)
  ) |>
  unnest(everything()) |>
  rename(dynamic_flag = is_dynamic,
         file_type_std = tbl_type) |>
  select(tbl_name, adintel_year, file, .before = 1) |>
  nest(.by = tbl_name)

adintel_tbl$info <- full_join(adintel_tbl$files, adintel_tbl$info) |> select(-data)




adintel_tbl$fields <- adintel_tbl$info |>
  select(
    tbl_name, dynamic_flag,file_type_std, file_name_std
  ) |>
  # select(dynamic_flag,
  #        file_type_std,
  #        file_name_std,
  #        media_type_id) |>
  full_join(
    tbl_info_tot |>
      select(dynamic_flag,file_type_std, file_name_std, col_pos:sql_datatype_min) |>
      unnest(everything()) |>
      nest(.by = !c(col_pos:sql_datatype_min),
           .key = 'fields')
  ) |>
  select( - dynamic_flag, - file_type_std, - file_name_std)

adintel_tbl$idx <-adintel_tbl$info |>
  select(
    tbl_name, dynamic_flag,file_type_std, file_name_std
  ) |>
  full_join(
    tbl_info_tot |>
      transmute(
      dynamic_flag,
      file_type_std,
      file_name_std,
      index = index |>
        map(~str_split_comma(.x))
    )
  ) |>
  select( - dynamic_flag, - file_type_std, - file_name_std)

adintel_tbl$pk <- adintel_tbl$info |>
  select(
    tbl_name, dynamic_flag,file_type_std, file_name_std
  ) |>
  full_join(
    tbl_info_tot |>
      filter(not_na(pk)) |>
      transmute(
        dynamic_flag,
        file_type_std,
        file_name_std,
        pk = pk |>
          map(~str_split_comma(.x))
      )
  ) |>
  select( - dynamic_flag, - file_type_std, - file_name_std)



library(dm)
# load('data-raw/adintel_dm.RData')

adintel_dm <- adintel_tbl |>
  map(~ .x |> unnest(everything())) |>
  as_dm()
# adintel_dm <- as_dm(adintel_tbl)
adintel_dm <- dm_add_pk(adintel_dm,
                   table = info,
                   columns = c(tbl_name))

adintel_dm <- dm_add_fk(adintel_dm,
                        table = files,
                        ref_table =  info,
                        columns = c(tbl_name))

adintel_dm <- dm_add_fk(adintel_dm,
                        table = fields,
                        ref_table =  info,
                        columns = c(tbl_name))

adintel_dm <- dm_add_fk(adintel_dm,
                        table = idx,
                        ref_table =  info,
                        columns = c(tbl_name))

adintel_dm <- dm_add_fk(adintel_dm,
                   table = pk,
                   ref_table =  info,
                   columns = c(tbl_name))



dm_examine_constraints(adintel_dm)


dm_draw(adintel_dm)

adintel_dm$info

adintel_dm |>
  dm_filter(info = (file_type_std == 'occurrences')) |>
  dm_wrap_tbl(root = info) |>
  dm_zoom_to(info)
  pull(info)


save.image(file = 'data-raw/adintel_dm.RData')
