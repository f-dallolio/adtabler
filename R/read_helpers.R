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
read_nrows <- function(file, header = TRUE){
  has_space <- stringr::str_detect(file, " ")
  if ( has_space ){
    old_file_name <- file
    new_file_name <- stringr::str_replace_all(file, " ", "_")
    file.rename(from = file, to = new_file_name)
  }
  cmd_out <- sprintf("wc -l %s", new_file_name) |>
    system(intern = TRUE) |>
    strsplit(" ") |>
    unlist() |>
    as.numeric() |>
    suppressWarnings()
  if( has_space ) {
    file.rename(new_file_name, old_file_name)
  }
  cmd_out[!is.na(cmd_out)] - header
}


