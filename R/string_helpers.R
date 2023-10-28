#' String helpers
#'
#' @param pattern character.
#'
#' @return a character
#'
#' @name string_helpers
NULL

#' @rdname string_helpers
#' @export
no_case <- function(pattern) {
  stringr::regex(pattern, ignore_case = TRUE)
}
