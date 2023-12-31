#' CHelpers for "not" selections
#'
#' @param x vector.
#' @param table vector.
#'
#' @return vector.
#'
#' @name not_helpers
NULL

#' @rdname not_helpers
#' @export
not_in <- function(x, table) {
  !x %in% table
}

#' @rdname not_helpers
#' @export
`%notin%` <- function(x, table) not_in(x, table)

#' @rdname not_helpers
#' @export
not_na <- function(x) !is.na(x)

#' @rdname not_helpers
#' @export
not_null <- function(x) !is.null(x)

#' @rdname not_helpers
#' @export
not_empty <- function(x) !is_empty(x)

