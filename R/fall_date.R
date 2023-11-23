#' Date for Fall basaed on September eqwuinox
#'
#' @param year integer vector.
#' @param yearweek logical. If TRUE (default), the date reflects the Monday of the week of the Equinox. If False, the date of the equinox.
#' @param out_tibble logical. If TRUE (default), the function returns a tibble. A list otherwise.
#'
#' @return A tibble or a list.
#' @export
#'
fall_date <- function(year, yearweek = TRUE, out_tibble = TRUE){
  dates <- paste0(year, "-01-01") |> as.Date()
  dates1 <- paste0(year + 1, "-01-01") |> as.Date()
  fall_date <- fmdates:::equinox(year, season = 'sep') |>
    as.Date()
  fall_date1 <- fmdates:::equinox(year + 1, season = 'sep') |>
    as.Date()
  if( yearweek ){
    fall_date <-  fall_date |>
      tsibble::yearweek() |> as.Date()
    fall_date1 <-  fall_date1 |>
      tsibble::yearweek() |> as.Date()
  }
  out <- tibble(
    year = year,
    fall_date = as.character(fall_date),
    fall_date_p1year = as.character(fall_date1)
  )
  if( !out_tibble ){
    out <- as.list(out)
  }
  out
}
