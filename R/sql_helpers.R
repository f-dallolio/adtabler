#' SQL column definition block
#'
#' @param .col_name character.
#' @param .col_type character.
#' @param .n1 integer. Defaults to NA.
#' @param .n2 integer. Defaults to NA.
#'
#' @return glue string.
#'
#' @name sql_helpers
NULL

#' @rdname sql_helpers
#' @export
#'
sql_new_coldef <- function( x, out = c("r", "sql", "sql2") ) {
  sql_dim <- str_split(x, "_") |>
    map_vec(
      ~ .x |>
        slice_vec(1, negate = TRUE) |>
        str_flatten_comma()
    )
  sql_type <- adintel_to_sql(
    str_split_i(x, "_", 1)
  )
  sql_type_2 <- case_when(
    sql_type %in% c("varchar", "text") ~ "text",
    sql_type %in% c("numeric") ~ "numeric",
    .default = "integer"
  )
  if( out == "sql" ) {
    out <- map2_vec(
      sql_type,
      sql_dim,
      ~ case_when(
        .x == "varchar" ~ paste0(
          str_to_upper(.x),
          str_embrace(.y)
        ),
        .x == "numeric" ~ paste0(
          str_to_upper(.x),
          str_embrace(.y)
        ),
        .default = str_to_upper(.x)
      )
    )
  } else if ( out == "sql" ) {
    out <- map2_vec(
      sql_type_2,
      sql_dim,
      ~ case_when(
        .x == "numeric" ~ paste0(
          str_to_upper(.x),
          str_embrace(.y)
        ),
        .default = str_to_upper(.x)
      )
    )
  } else if ( out == "r" ) {
    out <- adintel_to_sql(
      str_split_i(x, "_", 1),
      r_out = TRUE
    )
  }
  return(out)
}

#' @rdname sql_helpers
#' @export
#'
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
#' @rdname sql_helpers
#' @export
#'
sql_define_cols <- function(.col_name, .col_type, .n1 = NA, .n2 = NA, ...) {
  df <- tibble::tibble(
    .col_name = paste0(
      stringr::str_pad(
        string = .col_name,
        width = max(nchar(.col_name)),
        side = "right",
        pad = " ")
    ),
    .col_type = .col_type,
    .n1 = .n1,
    .n2 = .n2,
  ) |>
    dplyr::mutate(
      coldef = dplyr::if_else(
        .col_type == "NUMERIC",
        glue::glue('    {.col_name} { .col_type }({.n1}, {.n2})'),
        glue::glue('    {.col_name} { .col_type }')
      )
    )

  df$col_def |>
    glue::glue_collapse(sep = ", \n")
}

#' @rdname sql_helpers
#' @export
#'
sql_create_tbl <- function(tbl_name, col_def){
  glue::glue(
    'CREATE TABLE IF NOT EXISTS { tbl_name} \n ( \n { col_def } \n )'
  )
}





