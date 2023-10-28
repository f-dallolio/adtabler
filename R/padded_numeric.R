#' Numeric values as characters with '0' padding
#'
#' @param x numeric or character coercible to numeric.
#' @param width numeric.
#'
#' @return a character
#'
#' @name padded_numeric
NULL

#' @rdname padded_numeric
#' @export
numpad <- function(x, width = NULL) {
  x <- as_numeric2(x)
  stopifnot(is.numeric(x))
  if (is.null(x)) {
    width <- max(nchar(as.character(x)))
  }
  stringr::str_pad(
    string = x,
    width = width,
    side = "left",
    pad = "0",
  )
}

#' @rdname padded_numeric
#' @export
numpad2 <- function(x) {
  numpad(x, width = 2)
}

#' @rdname padded_numeric
#' @export
numpad4 <- function(x) {
  numpad(x, width = 4)
}
