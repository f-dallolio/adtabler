#' Remove NAs
#'
#' @param x a vector.
#'
#' @return a vector qithout NAs
#' @export
#'
na_rm <- function(x) {
  x[!is.na(x)]
}
