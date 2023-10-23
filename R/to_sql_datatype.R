#' Create sql name - datatype
#'
#' @param name character vector.
#' @param type character vector.
#' @param precision integer vector.
#' @param scale integer vector.
#' @param adintel logical. Default is TRUE.
#'
#' @return a character vector.
#' @export
to_sql_datatype <- function(name, type, precision = NA, scale = NA, adintel = TRUE) {
  name_new <- adtabler::rename_adintel(name)
  type_new <- adtabler::adintel_to_sql(type) |>
    stringr::str_to_upper()
  if (adintel) {
    name <- name_new
    type <- type_new
  }
  if ((is.na(precision) & is.na(scale)) |
    type_new == "DATE" |
    type_new == "TEXT") {
    out <- glue::glue("{name} {type}")
  } else if (!is.na(precision) && is.na(scale)) {
    out <- glue::glue("{name} {type}({precision})")
  } else {
    out <- glue::glue("{name} {type}({precision}, {scale})")
  }
  return(out)
}
to_sql_datatype <- Vectorize(to_sql_datatype)
