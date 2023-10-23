#' To character vector
#'
#' @param ... unquoted names or one string of names separated by ','.
#'
#' @return a character vector.
#' @export
#'
to_c <- function(...) {
  out <- rlang::enquos(..., .named = TRUE)
  names(out)
}
