#' SQL column definition block
#'
#' @param .col_name character.
#' @param .col_type character.
#' @param .n1 integer. Defaults to NA.
#' @param .n2 integer. Defaults to NA.
#'
#' @return glue string.
#'
#' @name sql_helpers
NULL

#' @rdname sql_helpers
#' @export
#'
sql_define_cols <- function(.col_name, .col_type, .n1 = NA, .n2 = NA, ...) {
  df <- tibble::tibble(
    .col_name = paste0(
      stringr::str_pad(
        string = .col_name,
        width = max(nchar(.col_name)),
        side = "right",
        pad = " ")
    ),
    .col_type = .col_type,
    .n1 = .n1,
    .n2 = .n2,
  ) |>
    dplyr::mutate(
      coldef = dplyr::if_else(
        .col_type == "NUMERIC",
        glue::glue('    {.col_name} { .col_type }({.n1}, {.n2})'),
        glue::glue('    {.col_name} { .col_type }')
      )
    )

  df$col_def |>
    glue::glue_collapse(sep = ", \n")
}

#' @rdname sql_helpers
#' @export
#'
sql_create_tbl <- function(tbl_name, col_def){
  glue::glue(
    'CREATE TABLE IF NOT EXISTS { tbl_name} \n ( \n { xx } \n )'
  )
}





