#' Check and coerce an object to a numeric type
#'
#' @param x vector to be coerced.
#' @param n expected length of vector.
#'
#' @return is_xxx2 checks if an object (including a character vector) is of type xxx.
#' The function as_numeric2 automatically returns an integer if possible and a double otherwise..
#'
#' @name check_coerce_numeric
NULL

#' @rdname check_coerce_numeric
#' @export
is_integer2 <- function(x, n = NULL) {
  if ( is.character(x) ) x <- as.integer(x)
  rlang::is_integer(x, n)
}

#' @rdname check_coerce_numeric
#' @export
is_double2 <- function(x, n = NULL) {
  if ( is.character(x) ) x <- as.double(x)
  rlang::is_double(x, n)
}

#' @rdname check_coerce_numeric
#' @export
is_numeric2 <- function(x){
  if ( is.character(x) ) x <- as.numeric(x)
  is.numeric(x)
}

#' @rdname check_coerce_numeric
#' @export
as_numeric2 <- function(x){
  out <- as.double(x)
  if (out %% 1 != 0) return(out)
  return(as.integer(out))
}
