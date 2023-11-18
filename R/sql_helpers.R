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
sql_tbl_name <- function(.file_type_std, .file_name_std) {
  out <- dplyr::case_when(
    .file_type_std == "market_breaks" ~ paste(
      'mb',
      .file_name_std,
      sep = "__"
    ),
    .file_type_std == "universe_estimates" ~ stringr::str_replace_all(
      .file_name_std, "ue_", "ue__"
    ),
    .default = paste(
      stringr::str_sub(.file_type_std, 1, 3),
      .file_name_std,
      sep = "__"
    )
  )

  out |> str_replace_all("spot_tv", "local_tv") |>
    str_replace_all("network_tv", "national_tv")
}

#' @rdname sql_helpers
#' @export
#'
sql_define_datatypes <- function( .list_manual, .str_manual = NULL, output) {

  output <- names(rlang::enquos( output, .named = TRUE ))

  output <- match.arg(arg = output, choices = c("r", "sql_min", "min", "sql_std", "std", "sql"))
  output <- dplyr::case_when(
    stringr::str_detect(output, pattern = "min") ~ "sql_min",
    stringr::str_detect(output, pattern = "std") | (output == "sql") ~ "sql_std",
    .default = output
  )

  if ( not_null(.str_manual) ) {
    sql_dim <- stringr::str_split(.str_manual, "_") |>
      purrr::map_vec(
        ~ .x |>
          slice_vec(1, negate = TRUE) |>
          stringr::str_flatten_comma(na.rm = TRUE)
      )
    sql_type <- adintel_to_sql(
      stringr::str_split_i(.str_manual, "_", 1)
    )
  } else {
    stopifnot( length(.list_manual) == 3 )
    stopifnot( is.character(.list_manual[[1]]) )
    stopifnot( is.integer( as.integer(.list_manual[[2]]) ) )
    stopifnot( is.integer( as.integer(.list_manual[[3]]) ) )
    sql_type <- adintel_to_sql(.list_manual[[1]])
    sql_dim <- purrr::map2_vec(
      .x = .list_manual[[2]],
      .y = .list_manual[[3]],
      ~ stringr::str_flatten_comma( c(.x, .y), na.rm = TRUE )
    )
  }


  sql_type_2 <- dplyr::case_when(
    sql_type %in% c("varchar", "text") ~ "text",
    sql_type %in% c("numeric") ~ "numeric",
    .default = "integer"
  )
  if( output == "sql_std" ) {
    out <- purrr::map2_vec(
      sql_type,
      sql_dim,
      ~ dplyr::case_when(
        .x == "varchar" ~ paste0(
          stringr::str_to_upper(.x),
          str_embrace(.y)
        ),
        .x == "numeric" ~ paste0(
          stringr::str_to_upper(.x),
          str_embrace(.y)
        ),
        .default = stringr::str_to_upper(.x)
      )
    )
  } else if ( output == "sql_min" ) {
    out <- purrr::map2_vec(
      sql_type_2,
      sql_dim,
      ~ dplyr::case_when(
        .x == "numeric" ~ paste0(
          stringr::str_to_upper(.x),
          str_embrace(.y)
        ),
        .default = stringr::str_to_upper(.x)
      )
    )
  } else if ( output == "r" ) {
    out <- adintel_to_sql(sql_type,
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
    out_na_unique <- glue::glue('"{unique(adintel_type[out_na_id])}"')
    out_na_adintel <- glue::glue_collapse(out_na_unique, sep = ", ")

    if (length(out_na_unique) == 1) {
      out_na_warning <- glue::glue("aditel_type == {out_na_adintel} resulted in NA")
      warning(out_na_warning, call. = T, )
    } else if (length(out_na_unique) > 1) {
      out_na_warning <- glue::glue("aditel_type %in% c({out_na_adintel}) resulted in NA")
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





