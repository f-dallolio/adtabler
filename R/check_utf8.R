#' Check for the elements of a character object can be translated into valid UTF-8
#'
#' @param x a character vector or a data frame (tibble, data.table...). If data frame the function gets applied only to character columns.
#'
#' @return a string reporting 'OK' if all elements can be transmated into valid UTF-8 or a message otherwise.
#' @export
#'
#' @examples
#'
#' x <- c("fa\u00E7ile", "fa\xE7ile", "fa\xC3\xA7ile")
#' Encoding(x) <- c("UTF-8", "UTF-8", "bytes")
#' check_utf8(x)
#' check_utf8(data.frame(x=x, y = x))
check_utf8 <- function(x){
  is_df <- is.data.frame(x)
  if (is_df) {
    x_chr <- purrr::map_vec(x, is.character) |>
      which(useNames = TRUE)
    out <- purrr::map_vec(x_chr, ~ utf8::utf8_valid(x[[.x]]) |> all(na.rm = TRUE)) |>
      purrr::set_names(names(x_chr))
    no_utf8 <- names(out[!out])
    if(length(no_utf8) == 0){
      return("OK")
    } else {
      return(glue::glue('The following columns have invalid UTF-8 elements: c({glue:: glue_collapse(no_utf8, sep = ", ")})'))
    }
  }
  out <- utf8::utf8_valid(x) |> all(na.rm = TRUE)
  if(out) {
    return("OK")
  } else {
    return('There are invalid UTF-8 elements')
  }

}
