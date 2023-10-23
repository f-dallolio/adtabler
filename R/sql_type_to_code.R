#' Make sql code for data type
#'
#' @param var_type vector
#' @param precision_var  vector
#' @param scale_var  vector
#' @param check logical
#'
#' @return vector
#' @export
#'
sql_type_to_code <- function(var_type, precision_var, scale_var, check = T) {
  type <- adintel_to_sql(var_type)
  case_when(type == "text" ~ "text",
    type == "numeric" ~ glue::glue("{var_type}({precision_var},{precision_var})"),
    .default = glue("{var_type}({precision_var})")
  )
}
