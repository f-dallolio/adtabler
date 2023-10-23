#' Test data type of tables vs manual
#'
#' @param x_data vector.
#' @param x_manual vector.
#' @param check logical.
#'
#' @return character vector.
#' @export
#'
test_data_type <- function(x_data, x_manual, check = FALSE) {
  stopifnot(all(c(is.vector(x_data), is.vector(x_manual))))
  stopifnot(all(c(!is.list(x_data), !is.list(x_manual))))

  dat_type <- typeof(x_data)
  man_type <- x_manual %>% tolower()

  out <- case_when(
    dat_type == "integer" &&
      man_type %in% c(
        "integer", "smallint", "bigint",
        "var_char", "character", "text"
      ) ~ "integer",
    dat_type == "character" &&
      man_type %in% c("var_char", "character", "text") ~ man_type,
    dat_type == "double" &&
      man_type %in% c("num", "numeric", "decimal", "double") ~ "numeric",
    .default = NA
  )
  if (check) {
    return(!is.na(out))
  }
  return(out)
}
