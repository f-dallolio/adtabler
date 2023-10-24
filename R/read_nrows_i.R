#' Read the number of rows of a file
#'
#' @param file a string.
#' @param header logical.
#' @param list_out logical.
#' @df_out logical.
#' @param df_out logical.
#'
#' @return a list or a tibble/data.frame.
#' @export
#'
read_nrows_i <- function(file, header = TRUE, list_out = FALSE, df_out = FALSE) {
  cmd_out <- sprintf("wc -l %s", shQuote(file)) |>
    system(intern = TRUE) |>
    strsplit(" ") |>
    unlist()

  if (list_out) {
    return(
      list(
        full_name = cmd_out[[2]],
        n = as.integer(cmd_out[[1]]) - header
      )
    )
  }

  if (df_out) {
    return(
      dplyr::tibble(
        full_name = cmd_out[[2]],
        n = as.integer(cmd_out[[1]]) - header
      )
    )
  }
  as.integer(cmd_out[[1]]) - header
}
