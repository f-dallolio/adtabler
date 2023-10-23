#' Read the number of rows of a file
#'
#' @param full_name a string.
#' @param df_out logical.
#'
#' @return a list or a tibble/data.frame.
#' @export
#'
read_nrows <- function(full_name, list_out = FALSE, df_out = FALSE){
  cmd_out <- sprintf("wc -l %s", full_name) |>
    system(intern = TRUE) |>
    strsplit(" ") |>
    unlist()

  if(list_out){
    return(
      list(
        full_name = cmd_out[[2]],
        n = as.integer(cmd_out[[1]])
      )
    )
  }

  if(df_out) {
   return(
     dplyr::tibble(
       full_name = cmd_out[[2]],
       n = as.integer(cmd_out[[1]])
     )
   )
  }
  as.integer(cmd_out[[1]])
}
