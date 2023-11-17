#' Starting and Ending Dates of Occurrences Files
#'
#' @param occ_file character vector.
#' @param date_to logical. FALSE is equivalent to 'occ_date_from' and TRUE to 'occ_date_to
#'
#' @return vector of dates as characters.
#'
#' @name occurrences_dates_from_to
NULL

#' @rdname occurrences_dates_from_to
#' @export
#'
occ_date_range <- function(occ_file, date_to = FALSE) {

  file_name <- stringr::str_split_i(occ_file, "/", - 1) |>
    stringr::str_remove_all(".tsv") |>
    rename_adintel(named = FALSE)

  year <- as.integer(stringr::str_split_i(occ_file, "/", - 3))
  if( date_to ) year <- year + 1

  date_jan_01 <- as.Date(paste(year, '01-01', sep = '-'))

  yearwk_sunday <- tsibble::yearweek(date_jan_01, week_start = 7)
  yearwk_start_sunday <- as.Date(yearwk_sunday)
  yearwk_start_sunday_1 <- as.Date(yearwk_sunday + 1)
  year_start_sunday <- lubridate::year(yearwk_start_sunday)
  start_date_wk_sunday <- if_else(
    year_start_sunday < year,
    yearwk_start_sunday_1,
    yearwk_start_sunday
  )

  yearwk_monday <- tsibble::yearweek(date_jan_01, week_start = 1)
  start_date_wk_monday <- as.Date(yearwk_monday)

  dplyr::case_when(
    file_name == "fsi_coupon" ~ as.character(start_date_wk_sunday),
    file_name %in% c("cinema", "internet", "spot_tv", "local_tv") ~
      as.character(start_date_wk_monday),
    .default = as.character(date_jan_01)
  )
}

#' @rdname occurrences_dates_from_to
#' @export
#'
occ_date_from <- function(occ_file){
  occ_date_range(occ_file = occ_file, date_to = FALSE)
}

#' @rdname occurrences_dates_from_to
#' @export
#'
occ_date_to <- function(occ_file){
  occ_date_range(occ_file = occ_file, date_to = TRUE)
}
