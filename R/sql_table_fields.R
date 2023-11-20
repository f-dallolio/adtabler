sql_table_fields <- function(.data, .col_names, .col_types, ...) {

  .data <- dplyr::as_tibble(.data)

  .names <- rlang::enquos(.col_names , .named = TRUE)
  col_names <- stringr::str_remove_all( names(.names), '\"' )

  .types <- rlang::enquos(.col_types , .named = TRUE)
  col_types <- stringr::str_remove_all( names(.types), '\"' )

  dots <- rlang::enquos(... , .named = TRUE)
  dot_names <- stringr::str_remove_all( names(dots), '\"' )

  sel_names <- c(col_names, col_types, dot_names)



  .fields <- .data |>
    dplyr::select(
      dplyr::all_of(sel_names)
    ) |>
    as.list() |>
    purrr::list_transpose() |>
    purrr::map_chr(
      .f = ~ c("   ", .x) |>
        str_flatten(collapse = "  ", na.rm = TRUE)
    )
  class(.fields) <- c("sql_tbl_fields", class(.fields))
  invisible(.fields)

  # paste0(
  #   "CREATE TABLE ", .tbl_name, " (\n",
  #   paste(.fields, collapse = ",\n"), "\n)\n"
  # ) |> cat()
  #
}
