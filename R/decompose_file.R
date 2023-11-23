#' Get info from file name
#'
#' @param file string. File patth.
#' @param out_tibble logical. Default is TRUE. If TRUE the function returns a tibble. If FALSE, a list or a named character vector depending on 'out_unlist'.
#' @param out_unlist logical. Default is FALSE. If FALSE the function returns a list. If FALSE, a named character vector.
#'
#' @return a tibble, a list, or a named character vector with the follosing components: file, file_type_std, file_name_std (character), and adintel_year (integer),
#'
#' @export
decompose_file <- function(file, out_tibble = TRUE, out_unlist = FALSE) {
  file_split <- file |>
    stringr::str_remove_all(".tsv") |>
    stringr::str_replace_all("/"," ") |>
    stringr::str_squish() |>
    stringr::str_split(" ") |>
    purrr::simplify() |>
    slice_last(3)
  file_3 <- file_split[[1]] |>
    as.numeric() |>
    suppressWarnings()
  file_2 <- file_split[[2]] |>
    rename_adintel(named = FALSE)
  file_1 <- file_split[[3]] |>
    rename_adintel(named = FALSE)
  if (is.na( file_3 )) {
    adintel_year <-  NA
    file_type <- 'references'
    file_name <- file_1
  } else {
    adintel_year <- file_3
    file_type <- file_2
    file_name <- file_1
  }
  if( out_tibble ){
    out <- dplyr::tibble(
      file = file,
      file_type_std = file_type,
      file_name_std = file_name,
      adintel_year = adintel_year
    )
  } else {
    out <- list(
      file = file,
      file_type_std = file_type,
      file_name_std = file_name,
      adintel_year = adintel_year
    )
    if( out_unlist ) {
      out <- unlist( ouyt, use.names = TRUE )
    }
  }
  out
}