#' Check if a numeric vector is integer
#'
#' @param x numeric vvctor.
#'
#' @return logical.
#' @export
#'
#' @examples
#' x_noint <- c(2.1, 3)
#' x_int <- c(2, 3)
#' is_numint(x_noint)
#' is_numint(x_int)
#'
is_numint <- function(x){
stopifnot("x must be numeric" = is.numeric(x))
  all(x%%1 == 0)
}
