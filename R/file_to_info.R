#' Get info from file name
#'
#' @param file character vector.
#'
#' @return glue string.
#'
#' @name file_to_info
NULL

#' @rdname file_to_info
#' @export
#'
file_to_info_dynamic <- function(file) {
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
    tbl_name <- sql_tbl_name(file_type_std, file_name_std)

    df <- data.table::fread(file = file, nrows = 1000)

    file_col_names <- df |>
      names()
    file_col_names_std <- file_col_names |>
      rename_adintel(named = FALSE)

    col_pos <- seq_along(file_col_names_std)

    file_classes <- map_vec(df, typeof) |> set_names(file_col_names_std)
    file_classes[is.logical(file_classes)] <- NA

    tibble::tibble(
      file_type_std = file_type_std,
      file_name_std = file_name_std,
      tbl_name,
      year,
      col_pos = list(col_pos),
      file_col_names_std = list(file_col_names_std),
      file_col_names = list(file_col_names),
      file_classes = list(file_classes),
      file = file,
      n_rows = NROW(df)
    )
  }
  out <- purrr::map(file, fn) |>
    purrr::list_rbind()

  out |>
    transmute(
      file_type_std, file_name_std, tbl_name,
      year,
      file,
      has_rows = n_rows > 0,
      date_from = file |> occ_date_range(year = year),
      date_to = file |> occ_date_range(year = year + 1),
      file_col_names_std,
      file_col_names,
      file_classes
    ) |>
    distinct() |>
    relocate(
      date_from, date_to, .before = file
    ) |>
    filter(has_rows) |>
    select(-has_rows)
}

#' @rdname file_to_info
#' @export
#'
file_to_info_static <- function(file) {
  fn <- function(file) {
    file_type_std <- "references"
    file_name_std <- file |>
      str_remove_all(".tsv") |>
      str_split_i("/", -1) |>
      rename_adintel(named = FALSE)
    tbl_name <- paste("ref", file_name_std, sep =  "__")
    year <- NA
    df <- data.table::fread(file = file, nrows = 1000)
    file_col_names <- df |> names()
    file_col_names_std <- file_col_names |>
      rename_adintel(named = FALSE)
    col_pos <- seq_along(file_col_names_std)
    file_classes <- map_vec(df, typeof) |> set_names(file_col_names_std)
    file_classes[is.logical(file_classes)] <- NA
    tibble::tibble(
      file_type_std = file_type_std,
      file_name_std = file_name_std,
      tbl_name,
      year,
      col_pos = list(col_pos),
      file_col_names_std = list(file_col_names_std),
      file_col_names = list(file_col_names),
      file_classes = list(file_classes),
      file = file,
      n_rows = NROW(df)
    )
  }
  out <- purrr::map(file, fn) |>
    purrr::list_rbind()

  out |>
    transmute(
      file_type_std, file_name_std, tbl_name,
      year = NA,
      file,
      has_rows = n_rows > 0,
      date_from = NA,
      date_to = NA,
      file_col_names_std,
      file_col_names,
      file_classes
    ) |>
    distinct() |>
    relocate(
      date_from, date_to, .before = file
    ) |>
    filter(has_rows) |>
    select(-has_rows)
}

