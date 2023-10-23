#' Turns unquoted names to strings.
#'
#' @param ... unquoted names.
#'
#' @return a character vector.
#' @export
#'
#' @examples
#' unquoted_to_chr(a, b, c)
unquoted_to_chr <- function(...) {
  out <- rlang::enquos(..., .named = TRUE)
  names(out)
}
