#' Checks for Upper Case Characters
#'
#' @param x a string.
#' @param which logical. Defauls is FALSE
#'
#' @return a vector. When which is FALSE it returns a vectors with TRUE/FALSE. When which is TRUE it returns the position of uppercase characters.
#' @export
is_upper <- function(x, which = FALSE) {
  stopifnot(is.character(x))
  if (which) {
    return(stringr::str_which(x, "^[:upper:]+$"))
  }
  stringr::str_detect(x, "^[:upper:]+$")
}
