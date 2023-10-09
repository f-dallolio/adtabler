#' Make sure econding is 'UTF-8'
#'
#' @param x a character vector.
#' @param from a string.
#' @param to a string.
#' @param sub a string.
#'
#' @return the input character vector encoded as 'UTF-8'.
#' @export
#'
encoding_utf8 <- function(x, from = 'latin1', to = 'UTF-8', sub = '*'){
  if(is.character(x)){
    Encoding(x) <- from
    out <- iconv(x, from, to, sub)
    return(out)
  }
  x
}
