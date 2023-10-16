#' To character vector
#'
#' @param ... unquoted names or one string of names separated by ','.
#'
#' @return a character vector.
#' @export
#'
#' @examples
#' to_c("a,b,c")
to_c <- function(...){
  if(is.character(...)){
    out <- str_remove_all(..., " ") |>
      str_split(",") |>
      unlist()
    return(out)
  }
  out <- rlang::enquos(..., .named = TRUE)
  names(out)
}
