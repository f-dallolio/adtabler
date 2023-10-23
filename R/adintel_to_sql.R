#' Replace "adintel" data types with "(postgre)SQL" data types
#'
#' @param adintel_type a character vector.
#' @param check logical. Default is TRUE.
#'
#' @return a character vector.
#' @export
#'
#' @import dplyr
#' @import tidyr
#' @import stringr
#' @import purrr
#' @import glue
#'
#' @examples
#' adintel_type <- c("Tinyint", "Char", "c")
#' adintel_to_sql(adintel_type)
adintel_to_sql <- function(adintel_type, r_out = FALSE) {
  var_type <- tolower(adintel_type)

  if (r_out) {
    out <- dplyr::case_when(
      var_type %in% c("int", "integer") ~ "integer",
      var_type %in% c("smallint", "tinyint") ~ "integer",
      var_type %in% c("bigint") ~ "integer",
      var_type %in% c("char", "character", "varchar") ~ "character",
      var_type %in% c("num", "numeric", "decimal", "double") ~ "double",
      var_type %in% c("text") ~ "character",
      var_type %in% c("date") ~ "Date",
      var_type %in% c("time") ~ "difftime",
      .default = NA
    )
  } else {
    out <- dplyr::case_when(
      var_type %in% c("int", "integer") ~ "integer",
      var_type %in% c("smallint", "tinyint") ~ "smallint",
      var_type %in% c("bigint") ~ "bigint",
      # var_type %in% c("varchar") ~ "varchar",
      # var_type %in% c("char", "character") ~ "character",
      var_type %in% c("varchar", "char", "character") ~ "varchar",
      var_type %in% c("text") ~ "text",
      var_type %in% c("num", "numeric", "decimal", "double") ~ "numeric",
      var_type %in% c("date") ~ "date",
      var_type %in% c("time") ~ "time",
      .default = NA
    )
  }

  out_na_id <- which(is.na(out))

  if (length(out_na_id) == 0) {
    out_na_unique <- glue('"{unique(adintel_type[out_na_id])}"')
    out_na_adintel <- glue_collapse(out_na_unique, sep = ", ")

    if (length(out_na_unique) == 1) {
      out_na_warning <- glue("aditel_type == {out_na_adintel} resulted in NA")
      warning(out_na_warning, call. = T, )
    } else if (length(out_na_unique) > 1) {
      out_na_warning <- glue("aditel_type %in% c({out_na_adintel}) resulted in NA")
      warning(out_na_warning, call. = T)
    } else {
      return(out) |> suppressWarnings()
    }
  }

  return(out)
}
