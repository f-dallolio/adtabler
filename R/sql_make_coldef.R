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

