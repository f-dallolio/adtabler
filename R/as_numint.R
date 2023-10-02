#' Numeric to integer or double
#'
#' @param x numeric vector.
#' @param warn logical.
#'
#' @return numeric or integer vector.
#' @export
#'
#' @examples
#' x_noint <- c(2.1, 3)
#' x_int <- c(2, 3)
#' as_numint(x_noint)
#' as_numint(x_noint, warn = FALSE)
#' as_numint(x_int)
as_numint <- function(x, warn = TRUE){
  if(any(x[!is.na(x)]%%1 != 0)){
    if(warn){
      warning("x is numeric but not an integer")
    }
    return(x)
  }
  as.integer(x)
}
