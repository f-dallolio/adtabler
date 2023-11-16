#' SQL column definition block
#'
#' @param .name character.
#' @param .type character.
#' @param .n1 integer. Defaults to NA.
#' @param .n2 integer. Defaults to NA.
#' @param .constraints character. Defaults to NA.
#'
#' @return glue string.
#' @export
#'
sql_make_coldef <- function( .name, .type, .n1 = NA, .n2 = NA, .constraints = NA, ...) {
  df <- tibble::tibble(
    .name = str_pad(.name, width = max(nchar(.name)), side = "right", pad = " "),
    .type = .type,
    .n1 = .n1,
    .n2 = .n2,
    .constraints = .constraints
  )
  fn <- function( .name, .type = .type, .n1, .n2, .constraints, ...) {
    if( all( is.na(c(.n1, .n2) ) ) ) {
      .dim <- NA
    } else {
      .dim <- str_embrace( stringr::str_flatten_comma( c(.n1, .n2), na.rm = TRUE ) )
    }
    stringr::str_flatten(
      c(
        "\t",
        .name,
        .type,
        .dim,
        .constraints
      ),
      collapse = " ",
      na.rm = TRUE
    ) |> glue::glue()
  }
  purrr::pmap( df, fn ) |>
    glue::glue_collapse(sep = ", \n")
}

library(tidyverse)
library(adtabler)
col_info  <- adtabler::data_info_list$col_info |>
  mutate(sql_datatype = case_when(datatype_r == "character" ~ "TEXT",
                                  datatype_r == "integer" |
                                    (datatype_r == "double" & col_name_std != "spend" & sql_scale == 0) ~ "INTEGER",
                                  col_name_std == "spend" ~ paste0("NUMERIC(",sql_length, ", 2",")"),
                                  datatype_r == "double" ~ paste0("NUMERIC(",sql_length, ", ", sql_scale,")")))


data_info_list$col_info <- col_info |>
  filter(file_name_std |> str_detect("cinema_ad")) |>
  bind_rows(col_info |>
              filter(file_name_std |> str_detect("cinema_ad")) |>
              mutate(
                col_pos = 2,
                col_name_std = "ad_type_desc",
                is_uk = FALSE,
                datatype_r = "character",
                datatype_sql = "CHARACTER",
                sql_length = "100",
                sql_scale = NA,
                datatype_man = "character",
                datatype_std = "character",
                sql_datatype = "TEXT"
              )) |>
  full_join(col_info) |>
  arrange(file_type_std, file_name_std, media_type_id, col_pos)

usethis::use_data(data_info_list, overwrite = T)
