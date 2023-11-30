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


adintel_tbl$info <- read_csv('data-raw/occ_def.csv') |>
  transmute(tbl_name = .part_name,
            media_type_id) |>
  full_join(
    tbl_info_tot |>
      transmute(file_name_std,
                file_type_std,
                dynamic_flag,
                media_type_id = media_type_id |> map(str_split_comma) |> map(parse_guess),
      ) |>
      unnest(everything()) |>
      distinct()
  ) |>
  mutate(tbl_name = if_else(is.na(tbl_name), file_name_std, tbl_name))  |>
  left_join(
    read_csv('data-raw/occ_def.csv') |>
      rename(tbl_name = .part_name) |>
      select(tbl_name,
             media_type_id,
             media_category : media_language)
  )

unique_info <- adintel_tbl$info |>
  select(-tbl_name, -media_type_id) |>
  summarise(across(everything(), ~ list(unique(.x[not_na(.x)]))))
# |>
#   as.list() |>
#   map(simplify) |>
#   list()


adintel_tbl$files <-
  tbl_info_tot |>
  select(file) |>
  unnest(everything()) |>
  mutate(data = file |>
           map(~ decompose_file(.x) |> select(-file))) |>
  unnest(everything()) |>
  rename(file_name_std = tbl_name,
         file_type_std = tbl_type,
         dynamic_flag = is_dynamic) |>
  nest(.by = !c(file, adintel_year)) |>
  full_join(
    adintel_tbl$info
  ) |>
    relocate(tbl_name,
             file_name_std,
             file_type_std,
             dynamic_flag,
             .before = 1) |>
    select(-media_type_id) |>
    unnest(everything())

adintel_tbl$info <- adintel_tbl$info |>
  left_join(adintel_tbl$files) |>
  unnest(everything()) |>
  rename(year = adintel_year) |>
  relocate(year, .before = 2)


adintel_tbl$fields <- adintel_tbl$info |>
  select(
    tbl_name, year, dynamic_flag,file_type_std, file_name_std
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
  select( - dynamic_flag, - file_type_std, - file_name_std) |>
  unnest(everything()) |>
  summarise(across(everything(), ~ list(.x)), .by = c(tbl_name, year)) |>
  select( -col_pos)
adintel_tbl$fields


adintel_tbl$idx <-adintel_tbl$info |>
  select(
    tbl_name, year, dynamic_flag,file_type_std, file_name_std
  ) |>
  full_join(
    tbl_info_tot |>
      unnest(adintel_year) |>
      transmute(
      year = adintel_year,
      dynamic_flag,
      file_type_std,
      file_name_std,
      index = index |>
        map(~str_split_comma(.x))
    )
  ) |>
  select( - dynamic_flag, - file_type_std, - file_name_std) |>
  arrange(year)
adintel_tbl$idx

adintel_tbl$pk <- adintel_tbl$info |>
  select(
    tbl_name, year, dynamic_flag,file_type_std, file_name_std
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
adintel_tbl$pk

adintel_tbl |> map(names)
# adintel_tbl$info <- adintel_tbl$info |> select(-media_type_id) |>
#   left_join(adintel_tbl$media)
adintel_tbl$info |> print(n=200)
adintel_tbl$files <- adintel_tbl$files |>
  transmute(tbl_name, file, year = adintel_year) |>
  nest(.by = tbl_name)

adintel_tbl |> map(names)
adintel_tbl_ptype <- adintel_tbl
adintel_tbl_ptype$files <-  adintel_tbl$files |> unnest(everything()) |> mutate(year = NA) |> distinct()
adintel_tbl$files |> unnest() |>
  select()

library(dm)
# load('data-raw/adintel_dm.RData')

adintel_dm_ptype <- adintel_tbl_ptype |>as_dm()
# adintel_dm <- as_dm(adintel_tbl)
# adintel_dm$info
adintel_dm_ptype <- dm_add_pk(adintel_dm_ptype,
                   table = info,
                   columns = c(tbl_name, year))

# adintel_dm_ptype$files
# adintel_dm_ptype <- dm_add_pk(adintel_dm_ptype,
#                         table = files,
#                         columns = c(tbl_name, year))

adintel_dm_ptype <- dm_add_fk(adintel_dm_ptype,
                        table = files,
                        ref_table =  info,
                        columns = c(tbl_name, year))

adintel_dm_ptype$fields
adintel_dm_ptype <- dm_add_fk(adintel_dm_ptype,
                        table = fields,
                        ref_table = info,
                        columns = c(tbl_name, year))

adintel_dm_ptype <- dm_add_fk(adintel_dm_ptype,
                        table = idx,
                        ref_table = info,
                        columns = c(tbl_name, year))

adintel_dm_ptype <- dm_add_fk(adintel_dm_ptype,
                        table = pk,
                        ref_table = info,
                        columns = c(tbl_name, year))



dm_examine_constraints(adintel_dm_ptype)

adintel_dm_ptype <- dm_ptype(adintel_dm_ptype)

dm_draw(adintel_dm_ptype)

save.image(file = 'data-raw/adintel_dm.RData')
