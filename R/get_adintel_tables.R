#' Get Adintel Tables
#'
#' @param adintel_dir a string
#'
#' @return a tibble
#' @export
#'
#'
get_adintel_tables <- function(adintel_dir) {
  adintel_all_files <- list.files(adintel_dir, recursive = T, full.names = F) %>%
    tibble::as_tibble_col(column_name = "adintel_files")

  year_files <- adintel_all_files %>%
    filter(str_detect(adintel_files, "Master_Files", negate = T)) %>%
    mutate(
      year = str_split_i(adintel_files, "/", -3) %>% chr_to_num(),
      file_type = str_split_i(adintel_files, "/", -2),
      file = str_split_i(adintel_files, "/", -1),
      full_path = get_dynamic_file(adintel_dir, year, file_type, file),
      .keep = "unused"
    )

  year_file_types <- unique(year_files$file_type)

  adintel_years <- unique(year_files$year)
  latest_year <- max(adintel_years)

  static_refereces_files <- adintel_all_files %>%
    filter(str_detect(
      adintel_files,
      get_static_file(
        dir = adintel_dir,
        year = latest_year,
        dir_out = TRUE
      )
    )) %>%
    transmute(
      year = -1,
      file_type = "References",
      file = adintel_files %>% str_split_i("/", -1),
      full_path = paste(adintel_dir, adintel_files, sep = "/")
    )

  adintel_data_table <- dplyr::bind_rows(year_files, static_refereces_files) %>%
    dplyr::mutate(dynamic_flag = year >= 0, .before = full_path) %>%
    dplyr::rename_with(~ paste0("adintel_", .x), -year)

  adintel_data_table_out <- adintel_data_table %>%
    mutate(
      file_type = str_sep_upper(adintel_file_type, "_") %>%
        str_replace_all("__", "_"),
      file_name = str_sep_upper(adintel_file, "_") %>%
        str_remove_all(".tsv") %>%
        str_replace_all("__", "_"),
      .after = year
    ) %>%
    dplyr::mutate(
      is_dynamic = adintel_dynamic_flag,
      file_year = year,
      full_path = adintel_full_path,
      .after = file_name, .keep = "unused"
    )

  names(adintel_data_table_out) <- str_replace_all(names(adintel_data_table_out), "adintel", "nielsen")
  adintel_data_table_out %>%
    mutate(
      size_kb = file.size(full_path) / 1024,
      KB = paste(round(size_kb, 3), "KB"),
      size_mb = size_kb / 1024,
      MB = paste(round(size_mb, 3), "MB"),
      size_gb = size_mb / 1024,
      GB = paste(round(size_gb, 3), "GB")
    ) %>%
    mutate(
      size = if_else(size_mb > 100, GB, MB), size_mb = round(size_mb),
      .before = full_path
    ) %>%
    select(-c(size_kb, size_gb, KB, MB, GB))
}
