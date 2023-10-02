#' Get files for static references
#'
#' @param dir string.
#' @param year integer
#' @param file string.
#' @param dir_out logical. Default is FALSE
#'
#' @return string.
#' @export
#'
get_static_file <- function(dir, year, file = NULL, dir_out = FALSE){
  out <- glue::glue("ADINTEL_DATA_{year}/nielsen_extracts/AdIntel/Master_Files/Latest")
  if(dir_out) {
    return(out)
  }
  paste(dir,
        out,
        file,
        sep = "/")
}
