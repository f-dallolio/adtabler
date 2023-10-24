#' Read the number of rows of a file
#'
#' @param file a character vector.
#' @param header logical.
#' @param list_out logical.
#' @df_out logical.
#' @param df_out logical.
#'
#' @return a list or a tibble/data.frame.
#' @export
#'
read_nrows <- function(file, header = TRUE, list_out = FALSE, df_out = FALSE){
  if(length(file) == 1){
    return(read_nrows_i(file, header, list_out, df_out))
  } else {
    return(map_vec(.x = file, .f = ~ .x |> read_nrows_i(header, list_out, df_out)))
  }
}
