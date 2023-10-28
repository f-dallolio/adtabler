#' String helpers
#'
#' @param pattern a string The pattern for which to ignore case.
#' @param x a character vector.
#' @param which logical. It returns indices instead of the actual characters.
#' @param to_lower logical. If TRUE (default) the output is all lower case.
#' @param named logical. If TRUE, the output vector has the original strings as names.
#'
#' @return Same type as input.
#'
#' @name string_helpers
NULL

#' @rdname string_helpers
#' @export
no_case <- function(pattern) {
  stringr::regex(pattern, ignore_case = TRUE)
}

#' @rdname string_helpers
#' @export
is_upper <- function(x, which = FALSE) {
  stopifnot(is.character(x))
  if (which) {
    return(stringr::str_which(x, "^[:upper:]+$"))
  }
  stringr::str_detect(x, "^[:upper:]+$")
}

#' @rdname string_helpers
#' @export
is_lower <- function(x, which = FALSE) {
  stopifnot(is.character(x))
  if (which) {
    return(stringr::str_which(x, "^[:lower:]+$"))
  }
  stringr::str_detect(x, "^[:lower:]+$")
}

#' @rdname string_helpers
#' @export
str_upper_uscore <- function(x, to_lower = TRUE){
  x <- unlist(strsplit(x, ""))
  id <- which(is_upper(x))
  x[id[id!=1]] <- paste0("_", x[id[id!=1]])
  out <- paste0(x, collapse = "")
  if( to_lower ) return(tolower(out))
  out
}

#' @rdname string_helpers
#' @export
rename_adintel <- function(x, named = TRUE){
  nms <- x
  out <- str_upper_uscore(x, to_lower = TRUE) |>
    str_replace_all("prime", "prim")
  out[str_detect(out, "dim_bridge")] <- "dim_bridge_occ_imp_spot_radio_key"
  if( named ){
    names(out) <- nms
  }
  out
}
