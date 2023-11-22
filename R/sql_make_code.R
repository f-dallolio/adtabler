#' SQL codes
#'
#' @param .con DBIconnection.
#' @param .tbl_name character.
#' @param .col_names character.
#' @param .data_types character.
#' @param .pk character.
#'
#' @return
#'
#' @name not_helpers
NULL

#' @rdname not_helpers
#' @export
sql_make_fields <- function( .con, .tbl_name, .col_names, .data_types, .pk = NA ) {
  .x = tibble(
    .col_names,
    .data_types
  )
  fields_out <- glue_data(
    .x = .x,
    '    { .col_names } {tolower( .data_types) }', #.con = .con
  )
  has_pk <- not_na(.pk)
  if( has_pk ) {
    fields_out <- fields_out |>
      c(
        glue('   CONSTRAINT { .tbl_name }_pkey PRIMARY KEY ({ .pk })'#, .con = .con,
        )
      )
  }
  fields_out |>
    glue_sql_collapse(sep = ', \n')
}

#' @rdname not_helpers
#' @export
sql_create_tbl <- function( .con, .tbl_name, .col_names,
                            .data_types, .pk = NULL, .year,
                            .date_from, .date_to) {

  fields <- sql_make_fields(
    .con = .con,
    .col_names = .col_names,
    .data_types = .data_types,
    .tbl_name = .tbl_name,
    .pk = .pk
  )

  create_table <- glue(
    "\n
    CREATE TABLE IF NOT EXISTS { .tbl_name }
    (
    { fields }
    )
    PARTITION BY RANGE (ad_date)"
    # ,
    # .con = .con
  ) |> DBI::SQL()

  create_partition <- glue(
    "\n
    CREATE TABLE { .tbl_name }_y{ .year } PARTITION OF {.tbl_name}
        FOR VALUES FROM ('{ .date_from }') TO ('{ .date_to }')
        PARTITION BY RANGE (media_type_id)
    "
    # ,
    # .con = .con
  ) |> DBI::SQL()

  list(
    create_table = create_table ,
    create_partition = create_partition
  )

}
#' @rdname not_helpers
#' @export
sql_create_part_tbl <- function(.part_type, .part_name, .tbl_name, .part_values) {
  match.arg(arg = .part_type, choices = c("LIST", "RANGE"))
  if ( .part_type == 'LIST') {
    .values <- .part_values
    .part_def <- glue::glue(
      "\n
      CREATE TABLE { .part_name } PARTITION OF { .tbl_name }
          FOR VALUES IN ( { .values } )
      "
    )
  } else {
    stopifnot( length(.part_values) == 2 & names(.part_values) != c(".value_from", ".value_from") )
    .value_from <- .part_values[[1]]
    .value_to <- .part_values[[2]]
    .part_def <- glue::glue(
      "\n
    CREATE TABLE { .part_name } PARTITION OF { .tbl_name }
        FOR VALUES FROM ( { .value_from } ) TO ( { .value_to } )
    "
    )
  }
  return(
    DBI::SQL(.part_def)
  )
}


