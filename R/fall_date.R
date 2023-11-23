#' Date for Fall basaed on September eqwuinox
#'
#' @param year integer vector.
#' @param yearweek logical. If TRUE (default), the date reflects the Monday of the week of the Equinox. If False, the date of the equinox.
#' @param out_tibble logical. If TRUE (default), the function returns a tibble. A list otherwise.
#'
#' @return A tibble or a list.
#' @export
#'
fall_date <- function(year,yearweek = TRUE) {
  fall_date <- rep(NA, length(year))
  no_na_id <- not_na(year)
  fall_date_0 <- fmdates:::equinox(year[no_na_id], season = 'sep') |>
    as.Date()
  if( yearweek ){
    fall_date_0 <- fall_date_0 |>
      tsibble::yearweek() |> as.Date()
  }
  fall_date[no_na_id] <- as.character(fall_date_0)
  fall_date
}


