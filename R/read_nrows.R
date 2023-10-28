#' Read the number of rows of a file
#'
#' @param file a character vector.
#' @param header logical.
#'
#' @return a list or a tibble/data.frame.
#'
#' @name read_files
NULL

#' @rdname read_files
read_nrows <- function(x, header = TRUE){
  cmd_out <- sprintf("wc -l %s", x) |>
    system(intern = TRUE) |>
    strsplit(" ") |>
    unlist() |>
    as.numeric() |>
    suppressWarnings()
  cmd_out[!is.na(cmd_out)] - header
}
