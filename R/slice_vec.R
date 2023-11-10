#' Vector Slicing
#'
#' @param x vector.
#' @param i integer or integer vector. Negative integers indicate positions from the end.
#'
#' @return a vector of the same kind of x.
#' @export
#'
slice_vec <- function(x, i){
  stopifnot( "abs(i) should be an integer in 1, ... , vec_size(x)" ~ max(abs(i)) <= vec_size(x) & min(abs(i)) > 0 )
  ii <- dplyr::case_when(
    i > 0 ~ i,
    i < 0 ~ vctrs::vec_size(x) + i + 1
  )
  x[ii]
}
