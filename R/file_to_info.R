#' Get info from file name
#'
#' @param .dyn_data_file character vector.
#'
#' @return tibble.
#' @export
#'
file_to_info <- function(.dyn_data_file) {
  out <- .dyn_data_file |>
    map(
      ~ .x |> stringr::str_remove_all(".tsv") |>
        stringr::str_replace_all("/", " ") |>
        stringr::str_squish() |>
        stringr::str_split(" ") |>
        unlist() |>
        slice_last(3)  |>
        rename_adintel(named = FALSE) |>
        purr::set_names("year", "file_type_std", "file_name_std") |>
        tibble::as_tibble_row() |>
        dplyr::mutate(tbl_name = sql_tbl_name(file_type_std, file_name_std),
                      file = .x)
    ) |>
    purrr::list_rbind() |>
    dplyr::mutate(keep = file |> purrr::map_lgl(~ NROW(data.table::fread(file = .x, nrows = 1)) > 0)) |>
    dplyr::filter(keep) |>
    dplyr::select(-keep) |>
    dplyr::relocate(year, .before = file)
  out
}
