devtools::install_github('f-dallolio/adtabler')
library(tidyverse)
library(adtabler)

tbl_info_tot|>
  print(n = 500)

xx <- tbl_definitions_occ_ref |>
  unnest(tbl_name) |>
  select(
    file_type_std,
    file_name_std,
    tbl_name,
    col_pos,
    adintel_manual
  ) |>
  unnest(everything()) |>
  distinct()  |>
  mutate( tbl_name = tbl_name |>
            str_replace_all( "spot_tv", "local_tv" ) |>
            str_replace_all( "network_tv", "national_tv" ) ) |>
  distinct() |>
  arrange(
    file_type_std,
    file_name_std,
    tbl_name,
    col_pos,
  ) |>
  nest(.by = file_type_std : tbl_name,
       .key = "adintel_manual") |>
  full_join(
    tbl_definitions_occ_ref |>
      unnest(tbl_name) |>
      select(
        file_type_std,
        file_name_std,
        tbl_name,
        media_type_id,
        keys
      ) |>
      unnest(media_type_id) |>
      unnest(keys) |>
      mutate( tbl_name = tbl_name |>
                str_replace_all( "spot_tv", "local_tv" ) |>
                str_replace_all( "network_tv", "national_tv" ) ) |>
      distinct() |>
      summarise(
        across(media_type_id : keys, list),
        .by = file_type_std : tbl_name
      )
  )

tbl_info_tot <- tbl_info_tot |>
  mutate( tbl_name = tbl_name |>
            str_replace_all( "spot_tv", "local_tv" ) |>
            str_replace_all( "network_tv", "national_tv" ) ) |>
  left_join(xx) |>
  print(n = 200)

usethis::use_data(tbl_info_tot, overwrite = T)
