#' Read the number of rows of a file
#'
#' @param file a character vector.
#' @param header logical.
#'
#' @return a list or a tibble/data.frame.
#'
#' @name read_nrows
NULL

#' @rdname read_nrows
read_nrows_1 <- function(x, header){
  cmd_out <- sprintf("wc -l %s", x) |>
    system(intern = TRUE) |>
    strsplit(" ") |>
    unlist() |>
    as.numeric() |>
    suppressWarnings()
  cmd_out[!is.na(cmd_out)] - header
}

#' @rdname read_nrows
#' @export
read_nrows <- function(file, header = TRUE){
  purrr::map_vec(file, read_nrows_1)
}
