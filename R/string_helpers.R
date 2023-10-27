#' Title
#'
#' @param x numeric. Alternatively, character coercible to integer.
#' @param width numeric.
#'
#' @return
#' @export
#'
#' @examples
numpad <- function(x, width = 0L) {

  if( !is.numeric(x) ) x <- as.numeric(x)
  stopifnot( width)

  stringr::str_pad(
    string = x,
    width = width,
    side = "left",
    pad = "0",
  )
}



numpad2 <- function(x) {
  numpad(x, width = 2)
}

numpad4 <- function(x) {
  numpad(x, width = 4)
}

