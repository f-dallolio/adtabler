#' Separates String Based on Uppercase Characters
#'
#' @param x_chr a string or a character vector of the form "AaBBCccDd".
#' @param sep a string indicating the separator. Default is a space " ".
#' @param to_lower logical. Default TRUE.
#' @param to_list logical. Default FALSE.
#' @param named logical. Default TRUE.
#'
#' @return a string or a character vector of the form "Aa BB Ccc Dd" (or Aa_BB_Ccc_Dd").
#' @export
str_sep_upper <- function(x_chr,
                          sep = " ",
                          to_lower = TRUE,
                          to_list = FALSE,
                          named = TRUE) {
  # sep <- sep
  if (is.list(x_chr)) {
    x_chr <- purrr::list_c(x_chr)
  }
  x <- stringr::str_split(x_chr, "")
  if (to_lower) {
    out <- purrr::map(x, ~ str_sep_upper_i(.x, sep = sep) %>% tolower())
  } else {
    out <- purrr::map(x, ~ str_sep_upper_i(.x, sep = sep))
  }
  if (isFALSE(to_list)) {
    out <- purrr::list_c(out)
  }
  if (named) {
    out <- out %>% purrr::set_names(x_chr)
  }
  return(out)
}

str_sep_upper_i <- function(x, sep) {
  up_x <- is_upper(x)
  lo_lag <- is_lower(dplyr::lag(x))
  lo_lead <- is_lower(dplyr::lead(x))

  x_sub <- which((up_x & lo_lag) | (up_x & lo_lead))
  x[x_sub] <- paste0(" ", x[x_sub])

  stringr::str_c(x, collapse = "") %>%
    stringr::str_squish() %>%
    stringr::str_replace_all(" ", sep)
}
