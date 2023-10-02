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
adintel_to_sql <- function(adintel_type, check = TRUE, r_out = FALSE){

  var_type <- tolower(adintel_type)

  if(r_out){
    out <- dplyr::case_when(
      var_type %in% c("int", "integer") ~ "integer",
      var_type %in% c("smallint", "tinyint") ~ "integer",
      var_type %in% c("bigint")  ~ "integer",
      var_type %in% c("char", "character", "varchar") ~ "character",
      var_type %in% c("num", "numeric", "decimal", "double")  ~ "double",
      var_type %in% c("text") ~ "character",
      var_type %in% c("date") ~ "Date",
      var_type %in% c("time") ~ "difftime",
      .default = NA
    )
  } else {
    out <- dplyr::case_when(
      var_type %in% c("int", "integer") ~ "integer",
      var_type %in% c("smallint", "tinyint") ~ "smallint",
      var_type %in% c("bigint")  ~ "bigint",
      var_type %in% c("varchar") ~ "varchar",
      var_type %in% c("char", "character") ~ "character",
      var_type %in% c("num", "numeric", "decimal", "double")  ~ "numeric",
      var_type %in% c("text") ~ "text",
      var_type %in% c("date") ~ "date",
      var_type %in% c("time") ~ "time",
      .default = NA
    )
  }

  out_na_id <- which(is.na(out))

  if(length(out_na_id) == 0){

    out_na_unique <- glue('"{unique(adintel_type[out_na_id])}"')
    out_na_adintel <- glue_collapse(out_na_unique, sep = ", ")

    if(length(out_na_unique) == 1){

      out_na_warning <- glue("aditel_type == {out_na_adintel} resulted in NA")

    } else {

      out_na_warning <- glue("aditel_type %in% c({out_na_adintel}) resulted in NA")

    }

    warning(out_na_warning, call. = FALSE)

  }

  if(check){

    check_tbl <- dplyr::tibble(adintel = adintel_type, out = out) %>%
      distinct()

    for(i in seq_along(check_tbl$adintel)){

      if(i == max(seq_along(check_tbl$adintel))){

        print(
          glue::glue("{check_tbl$adintel[i]} -> {check_tbl$out[i]} \n\n")
        )

      } else {

        print(
          glue::glue("{check_tbl$adintel[i]} -> {check_tbl$out[i]}")
        )

      }
    }
  }

  return(out)

}
