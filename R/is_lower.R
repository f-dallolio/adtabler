#' Checks for Lower Case Characters
#'
#' @param x a string.
#' @param which logical. Defauls is FALSE
#'
#' @return a vector. When which is FALSE it returns a vectors with TRUE/FALSE. When which is TRUE it returns the position of lowercase characters.
#' @export
is_lower <- function(x, which = FALSE){
  stopifnot(is.character(x))
  if(which){
    return(stringr::str_which(x, "^[:lower:]+$")
    )
  }
  stringr::str_detect(x, "^[:lower:]+$")
}
