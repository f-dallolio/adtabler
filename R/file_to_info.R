#' Get info from file name
#'
#' @param .dyn_data_file character vector.
#'
#' @return tibble.
#' @export
#'
file_to_info <- function(.dyn_data_file) {
  fn <- function(file){
    x <- file |>
      stringr::str_remove_all(".tsv") |>
      stringr::str_replace_all("/", " ") |>
      stringr::str_squish()
    year <- stringr::str_split_i(x, " ", -3) |>
      as.integer()
    file_type_std <- stringr::str_split_i(x, " ", -2) |>
      rename_adintel(named = FALSE)
    file_name_std <- stringr::str_split_i(x, " ", -1) |>
      rename_adintel(named = FALSE)
    tbl_name = sql_tbl_name(file_type_std, file_name_std)

    col_names_std <- data.table::fread(file = file, nrows = 1) |>
      names() |>
      rename_adintel(named = FALSE)
    col_pos <- seq_along(col_names_std) |> numpad2()

    tibble::tibble(
      file_type_std = file_type_std,
      file_name_std = file_name_std,
      tbl_name,
      year,
      col_pos = list(col_pos),
      col_names_std = list(col_names_std),
      file = .dyn_data_file
    )
  }
  purrr::map(.dyn_data_file, fn) |>
    purrr::list_rbind()

}
