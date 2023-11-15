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
sql_make_coldef <- function( .name, .type, .n1 = NA, .n2 = NA, .constraints = NA) {
  df <- tibble::tibble(
    .name = .name,
    .type = .type,
    .n1 = .n1,
    .n2 = .n2,
    .constraints = .constraints
  )
  fn <- function( .name, .type = .type, .n1, .n2, .constraints, ...) {
    if( all( is.na(c(.n1, .n2) ) ) ) {
      .dim <- NA
    } else {
      .dim <- str_embrace( str_flatten_comma( c(.n1, .n2), na.rm = TRUE ) )
    }
    str_flatten(
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
  pmap( df, fn ) |>
    glue_collapse(sep = ", \n")
}
