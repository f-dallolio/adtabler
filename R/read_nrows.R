#' Read the number of rows of a file
#'
#' @param full_name a string.
#' @param df_out logical. If TRUE (default) it returns a tibble (or data.frame).
#'
#' @return a list or a tibble/data.frame.
#' @export
#'
read_nrows <- function(full_name, df_out = TRUE){
  cmd_out <- sprintf("wc -l %s", full_name) |>
    system(intern = TRUE) |>
    strsplit(" ") |>
    unlist()

  out <- list(
    full_name = cmd_out[[2]],
    n = as.integer(cmd_out[[1]])
  )

  if(df_out) {
   out <- dplyr::as_tibble(out)
  }
  out
}
