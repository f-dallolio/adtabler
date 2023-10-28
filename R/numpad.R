#' Numeric values as chracters with '0' padding
#'
#' @param x numeric or character coercible to numeric.
#' @param width numeric.
#'
#' @return a character
#'
#' @name numpad
NULL

#' @rdname numpad
#' @export
numpad <- function(x, width = NULL) {
  x <- as.numeric(x)
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

#' @rdname numpad
#' @export
numpad2 <- function(x) {
  numpad(x, width = 2)
}

#' @rdname numpad
#' @export
numpad4 <- function(x) {
  numpad(x, width = 4)
}
