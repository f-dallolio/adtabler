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
  )

adintel_tbl$fields <- adintel_tbl$info |>
  select(dynamic_flag,
         file_type_std,
         file_name_std,
         media_type_id) |>
  full_join(
    tbl_info_tot |>
      select(dynamic_flag,file_type_std, file_name_std, col_pos:sql_datatype_min) |>
      unnest(everything()) |>
      nest(.by = !c(col_pos:sql_datatype_min),
           .key = 'fields')
  )



adintel_tbl$info
adintel_tbl$idx <- adintel_tbl$info |>
  select(dynamic_flag,
         file_type_std,
         file_name_std,
         media_type_id) |>
  full_join(
    tbl_info_tot |>
      transmute(
      dynamic_flag,
      file_type_std,
      file_name_std,
      index = index |>
        map(~str_split_comma(.x))
    )
  )




adintel_tbl$pk <- adintel_tbl$info |>
  select(dynamic_flag,
         file_type_std,
         file_name_std,
         media_type_id) |>
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
  )

library(dm)
adintel_dm <- as_dm(adintel_tbl)
adintel_dm <- dm_add_pk(adintel_dm,
                   table = info,
                   columns = c(dynamic_flag,
                               file_type_std,
                               file_name_std,
                               media_type_id))
adintel_dm <- dm_add_fk(adintel_dm,
                   table = pk,
                   ref_table =  info,
                   columns = c(dynamic_flag,
                               file_type_std,
                               file_name_std,
                               media_type_id))

adintel_dm <- dm_add_fk(adintel_dm,
                   table = idx,
                   ref_table =  info,
                   columns = c(dynamic_flag,
                               file_type_std,
                               file_name_std,
                               media_type_id))

dm_examine_constraints(adintel_dm)
save.image(file = 'data-raw/adintel_dm.RData')
