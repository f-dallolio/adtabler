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
str_upper_uscore <- function(x, to_lower = TRUE) {
  purrr::map_vec(x, ~ str_upper_uscore_i(.x, to_lower = to_lower))
}

str_upper_uscore_i <- function(x, to_lower = TRUE) {
  x <- unlist(strsplit(x, ""))
  no_alnum <- which(!grepl("[A-Za-z0-9]", x))
  x[no_alnum + 1] <- toupper(x[no_alnum + 1])
  z <- grep(pattern = "[A-Za-z0-9]", x = unlist(strsplit(x, "")), value = TRUE)
  n <- length(z)
  id <- seq_along(z)[-c(1, n)]
  z1 <- z[id]
  id1 <- (is_upper(z[id]) & is_lower(z[(id - 1)])) |
    (is_upper(z[id]) & is_lower(z[(id + 1)]))
  z1[id1] <- paste0("_", z1[id1])
  out <- paste0(c(z[1], z1, z[n]), collapse = "")
  if (to_lower) {
    return(tolower(out))
  }
  return(out)
}

#' @rdname string_helpers
#' @export
rename_adintel <- function(x, named = TRUE) {
  nms <- x
  nas <- which(is.na(x))
  out <- str_upper_uscore(x, to_lower = TRUE) |>
    stringr::str_replace_all("prime", "prim")
  out[nas] <- NA
  out[stringr::str_detect(out, "dim_bridge")] <- "dim_bridge_occ_imp_spot_radio_key"
  if (named) {
    names(out) <- nms
  }
  out
}

#' @rdname string_helpers
#' @export
str_split_comma <- function(x) {
  stringr::str_split(x, ", ") |> purrr::list_c()
}

#' @rdname string_helpers
#' @export
str_adintel_to_title <- function(x) {
  stringr::str_replace_all(x, "_", " ") |>
    stringr::str_to_title()
}

#' @rdname string_helpers
#' @export
str_embrace <- function(x, left = "(", right = ")") {
  paste0(left, x, right)
}

#' @rdname string_helpers
#' @export
str_embrace_sq <- function(x, left = "[", right = "]") {
  paste0(left, x, right)
}

#' @rdname string_helpers
#' @export
str_embrace_cur <- function(x, left = "{", right = "}") {
  paste0(left, x, right)
}
