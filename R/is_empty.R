#' Check for empty vector
#'
#' @param x vector.
#'
#' @return logical
#' @export
#'
is_empty <- function(x){
  length(x) == 0
}
