devtools::install_github('f-dallolio/adtabler')
library(tidyverse)
library(adtabler)

data_info_list$file_info
info_start <- file_info_start_end |>
  filter(n_rows > 0,
         file_type_std == "occurrences") |>
  select(file_name_std, year, date_start) |>
  mutate(date_start = as.Date(date_start)) |>
  mutate(
    date_start_type = case_when(
      all( str_sub(date_start, 6, -1) ==  '01-01' ) ~ "jan_1st",
      all( as.character(wday(date_start, label = TRUE)) == "Mon" ) ~ "wk_monday",
      all( as.character(wday(date_start, label = TRUE)) == "Sun" ) ~ "wk_sunday",
    ),
    .by = file_name_std
  ) |> print(n=200)

info_start |>
  filter(date_start_type == "wk_sunday") |>
  mutate(
    date_jan01 = as.Date(paste(year, '01-01', sep = '-')),
    ywk = tsibble::yearweek(date_jan01, week_start = 7),
    try = if_else(year(as.Date(ywk)) < year, as.Date(ywk + 1), as.Date(ywk)),
    ok = try == date_start,
    .after = year
  )

info_start |>
  filter(date_start_type == "wk_monday") |>
  pull(file_name_std) |>
  unique()

xxx <- data_info_list$file_info |>
  select(full_file_name) |>
  rename(file = full_file_name) |>
  filter(
    file |> str_detect("Occurrences")
  ) |>
  mutate(
    year = as.integer(str_split_i(file, "/", - 3)),
    file_type_std = str_split_i(file, "/", - 2) |>
      rename_adintel(named = FALSE),
    file_name_std = str_split_i(file, "/", - 1) |>
      str_remove_all(".tsv") |>
      rename_adintel(named = FALSE),
    tbl_name = paste(
      str_sub(file_type_std, 1, 3),
      file_name_std,
      sep = "__"
    )
  )

xxx |>
  inner_join(
    data_info_list$file_info |>
      rename(file = full_file_name) |>
      select(file_type_std, file_name_std, date_from, file)
  ) |>
  mutate(ad_date_from = occ_date_from(file),
         ad_date_to = occ_date_to(file),
         ok = date_from == ad_date_from,
         ok1 = lead(date_from) == ad_date_to,
         .by = file_name_std) |>
  summarise(
    okok <- all(ok, na.rm = TRUE),
    okok1 <- all(ok1, na.rm = TRUE)
  )

occ_file = xxx$file[1]

occ_date_range <- function(occ_file, date_to = FALSE) {

  file_name <- str_split_i(occ_file, "/", - 1) |>
      str_remove_all(".tsv") |>
      rename_adintel(named = FALSE)

  year <- as.integer(str_split_i(occ_file, "/", - 3))
  if( date_to ) year <- year + 1

  date_jan_01 <- as.Date(paste(year, '01-01', sep = '-'))

  yearwk_sunday <- tsibble::yearweek(date_jan_01, week_start = 7)
  yearwk_start_sunday <- as.Date(yearwk_sunday)
  yearwk_start_sunday_1 <- as.Date(yearwk_sunday + 1)
  year_start_sunday <- year(yearwk_start_sunday)
  start_date_wk_sunday <- if_else(
    year_start_sunday < year,
    yearwk_start_sunday_1,
    yearwk_start_sunday
  )

  yearwk_monday <- tsibble::yearweek(date_jan_01, week_start = 1)
  start_date_wk_monday <- as.Date(yearwk_monday)

  case_when(
    file_name == "fsi_coupon" ~ as.character(start_date_wk_sunday),
    file_name %in% c("cinema", "internet", "spot_tv", "local_tv") ~
      as.character(start_date_wk_monday),
    .default = as.character(date_jan_01)
  )
}
occ_date_from <- function(occ_file){
  occ_date_range(occ_file = occ_file, date_to = FALSE)
}
occ_date_to <- function(occ_file){
  occ_date_range(occ_file = occ_file, date_to = TRUE)
}

occ_date_from(occ_file)
occ_date_to(occ_file)



devtools::install_github('f-dallolio/adtabler')
library(tidyverse)
library(adtabler)
zzz <- data_info_list$col_info |>
  mutate(
    tbl_name = paste(
      str_sub( file_type_std, 1, 3),
      file_name_std,
      sep = "__"
    )
  ) |>
  select(tbl_name,
         col_pos,
         col_name_std,
         datatype_man,
         sql_length,
         sql_scale,
         datatype_sql,
         sql_datatype,
         datatype_r) |>
  distinct() |>
  mutate(
    manual_type = map_vec(seq_along(datatype_man), ~ c(datatype_man[[.x]],
                                     sql_length[[.x]],
                                     sql_scale[[.x]]) |>
          str_flatten(collapse = "_", na.rm = TRUE))
    )
x <- zzz$manual_type

sql_new_coldef <- function( x, out = c("r", "sql", "sql2") ) {

  sql_dim <- str_split(x, "_") |>
    map_vec(
      ~ .x |>
        slice_vec(1, negate = TRUE) |>
        str_flatten_comma()
    )

  sql_type <- adintel_to_sql(
    str_split_i(x, "_", 1)
  )

  sql_type_2 <- case_when(
    sql_type %in% c("varchar", "text") ~ "text",
    sql_type %in% c("numeric") ~ "numeric",
    .default = "integer"
  )

  if( out == "sql" ) {
    out <- map2_vec(
      sql_type,
      sql_dim,
      ~ case_when(
        .x == "varchar" ~ paste0(
          str_to_upper(.x),
          str_embrace(.y)
        ),
        .x == "numeric" ~ paste0(
          str_to_upper(.x),
          str_embrace(.y)
        ),
        .default = str_to_upper(.x)
      )
    )
  } else if ( out == "sql" ) {
    out <- map2_vec(
      sql_type_2,
      sql_dim,
      ~ case_when(
        .x == "numeric" ~ paste0(
          str_to_upper(.x),
          str_embrace(.y)
        ),
        .default = str_to_upper(.x)
      )
    )
  } else if ( out == "r" ) {
    out <- adintel_to_sql(
      str_split_i(x, "_", 1),
      r_out = TRUE
    )
  }

  return(out)

}

fn(x, out = "sql")
