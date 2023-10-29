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
#' @export
read_nrows <- function(file){
  cmd_out <- sprintf("wc -l %s", file) |>
    system(intern = TRUE)
  cmd_out
}


