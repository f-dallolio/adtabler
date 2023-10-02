#' Character vector to Numeric or Integert
#'
#' @param x a character vector.
#'
#' @return logical.
#' @export
#'
#' @examples
#' x_nonum <- c("a", "3", "4")
#' x_num <- c("2", "3", "4.1")
#' x_int <- c("2", "3", "4")
#' chr_to_num(x_nonum)
#' chr_to_num(x_num)
#' chr_to_num(x_int)
chr_to_num <- function(x, to_int = TRUE){
  x <- as.numeric(x)
  if(to_int){
    return(as_numint(x, warn = FALSE))
  }
  x
}
