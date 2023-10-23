#' Get all dynamic files paths
#'
#' @param dir string.
#' @param type string.
#' @param year integer
#' @param file string.
#'
#' @return string.
#' @export
#'
get_dynamic_file <- function(dir, year, type, file) {
  out <- glue::glue("ADINTEL_DATA_{year}/nielsen_extracts/AdIntel/{year}/{type}/{file}")
  paste(dir, out, sep = "/")
}
