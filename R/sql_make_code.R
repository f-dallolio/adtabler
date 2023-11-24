#' SQL codes
#'
#' @param .con DBIconnection.
#' @param .tbl_name character.
#' @param .col_names character.
#' @param .data_types character.
#' @param .pk character.
#'
#' @return SQL.
#'
#' @name sql_tbl_creator
NULL

#' @rdname sql_tbl_creator
#' @export
sql_make_fields <- function( .tbl_name, .col_names, .data_types, .pk = NA, ... ) {

  if('ad_date' |> not_in(.col_names)){
    .col_names <- c('ad_date', .col_names)
    .data_types <- c('DATE', .data_types)
    .pk = c('ad_date', .pk)
  }

  .x = list(
    .tbl_name = .tbl_name,
    .col_names = .col_names,
    .data_types= .data_types,
    .pk = .pk
  )
  fields_out <- glue::glue_data(
    .x = .x,
    '    {.col_names }  { .data_types }', #.con = .con
  )
  no_pk <- is.na(.pk) || is.null(.pk) ||is_empty(.pk)
  if( !no_pk ) {
    fields_out <- fields_out |>
      c(
        glue::glue( '    CONSTRAINT { .tbl_name }__pkey PRIMARY KEY ({ .pk })' )
      )
  }
  fields_out <- fields_out |>
    glue::glue_collapse(sep = ', \n')

  return(fields_out)
}

#' @rdname sql_tbl_creator
#' @export
sql_build_tbl <- function( .tbl_name, .col_names, .data_types, .pk = NA, .part_col, ...) {


  fields <- sql_make_fields(
    .tbl_name = .tbl_name,
    .col_names = .col_names,
    .data_types = .data_types,
    .pk = .pk
  )

  out <- glue::glue(
    "\n
    CREATE TABLE IF NOT EXISTS { .tbl_name }
    (
    { fields }
    )
    "
  )
  if( not_na(.part_col) ) {
    out <- glue::glue('{ out } { sql_part_by_range(.part_col) }')
    # out <- glue::glue('{ out } { sql_part_by_list(.part_col) }')
  }
  DBI::SQL(out)

}

#' @rdname sql_tbl_creator
#' @export
sql_part_by_range <- function(.part_col, ....) {
  glue::glue("PARTITION BY RANGE ({ .part_col })") |> DBI::SQL()
}

#' @rdname sql_tbl_creator
#' @export
sql_part_by_list <- function(.part_col, ....) {
  glue::glue("PARTITION BY LIST ({ .part_col })") |> DBI::SQL()
}

#' @rdname sql_tbl_creator
#' @export
sql_create_part <- function(.tbl_name, .part_name, .from = NULL, .to = NULL, .values = NULL,...) {
  by_range <- not_null(c(.from, .to)) & is.null(.values)
  by_list <- not_null(.values) & is.null(c(.from, .to))
  if( by_range ) {
    out <- glue::glue(
      "\n
      CREATE TABLE { .part_name } PARTITION OF { .tbl_name }
        FOR VALUES FROM ('{ .from }') TO ('{ .to }')"
    ) |> DBI::SQL()
  }
  if( by_list ) {
    vals <- glue::glue("'{ str_split_comma(.values) }'") |>
      glue::glue_collapse(sep = ", ")
    out <- glue::glue(
      "\n
      CREATE TABLE { .part_name } PARTITION OF { .tbl_name }
        FOR VALUES IN ({ vals })"
    ) |> DBI::SQL()
  }
  return(out)
}
