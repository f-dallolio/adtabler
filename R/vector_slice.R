#' Vector Slicing
#'
#' @param x vector.
#' @param i integer or integer vector. Negative integers indicate positions from the end.
#'
#' @return a vector of the same kind of x.
#'
#' @name vector_slice
NULL

#' @rdname vector_slice
#' @export
#'
slice_vec <- function(x, i, negate = FALSE){
  stopifnot( "abs(i) should be an integer in 1, ... , vec_size(x)" =
               all(abs(i) |> dplyr::between(1, vctrs::vec_size(x))))
  ii <- dplyr::case_when(
    i > 0 ~ i,
    i < 0 ~ vctrs::vec_size(x) + i + 1
  )
  out <- x[ii]
  if(negate){
    return(setdiff(x, out))
  }
  out
}

#' @rdname vector_slice
#' @export
#'
slice_first <- function(x, n, negate = FALSE){
  i <- seq.int(from = 1,to = n)
  out <- x[i]
  if(negate){
    return(setdiff(x, out))
  }
  out
}

#' @rdname vector_slice
#' @export
#'
slice_last <- function(x, n, negate = FALSE){
  id = sort(seq_along(x),decreasing = TRUE)
  i <- sort(id[seq.int(from = 1,to = n)])
  out <- x[i]
  if(negate){
    return(setdiff(x, out))
  }
  out
}
