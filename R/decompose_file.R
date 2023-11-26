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
    rename_adintel(named = FALSE) |>
    str_replace_all('spot_tv', 'local_tv') |>
    str_replace_all('network_tv', 'national_tv')
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
      tbl_type = file_type,
      is_dynamic = not_na(adintel_year),
      tbl_name = file_name
      
    )
  } else {
    out <- list(
      file = file,
      tbl_type = file_type,
      is_dynamic = not_na(adintel_year),
      tbl_name = file_name
    )
    if( out_unlist ) {
      out <- unlist( out, use.names = TRUE )
    }
  }
  out
}
