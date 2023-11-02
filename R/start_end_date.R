#' Start and End Date in a file
#'
#' @param year integer.
#' @param file character
#'
#' @return dates.
#' @export
#'
start_end_date <- function(year, file){

  t_start <- Sys.time()

  date_vec <- c(make_date(year - 1,12),
                make_date(year),
                make_date(year + 1,1))
  date_vec_month <- seq(min(date_vec), max(date_vec), by = 1) |> str_sub(1,7) |> unique()
  cmd_vec_month <- sprintf(glue::glue("grep -m 1 -Eoh '^{ date_vec_month }' %s"), file)

  glue::glue("\n\nParsing for Months") |> print()
  out_month <- future_map(cmd_vec_month,
                          ~ system(command = .x, intern = T) |>
                            suppressWarnings(),
                          .progress = TRUE) |>
    purrr::list_c() |>
    sort() |>
    lubridate::ym()


  out_range <- range(out_month)

  year_start <- year(out_range)[1]
  month_start <- month(out_range)[1]
  dates_start <- make_date(year_start, month_start)
  if(year_start < year){
    dates_start <- sort(dates_start, decreasing = TRUE)
  }
  cmd_vec_start <- cmd_vec_month <- sprintf(glue::glue("grep -m 1 -Eoh '^{ dates_start }' %s"), file)

  year_end <- year(out_range)[2]
  month_end <- month(out_range)[2]
  dates_end <- make_date(year_end, month_end)
  cmd_vec_end <- cmd_vec_month <- sprintf(glue::glue("grep -m 1 -Eoh '^{ dates_end }' %s"), file)

  glue::glue("\n\nParsing for Start Date") |> print()
  x_start <- future_map(cmd_vec_start,
                        ~ system(command = .x, intern = T) |> suppressWarnings(),
                        .progress = TRUE) |>
    list_c() |>
    as.Date()
  glue::glue("\n\nParsing for End Date") |> print()
  x_end <- future_map(cmd_vec_end,
                      ~ system(command = .x, intern = T) |>
                        suppressWarnings(),
                      .progress = TRUE) |>
    list_c() |>
    as.Date()

  t_end <- Sys.time()
  elapsed <- t_end - t_start
  print(elapsed)
  size_mb <- file.size(file)/1024^2
  size_gb <- size_mb/1024
  if(size_mb >= 1000){
    print( paste(round(size_gb, 2), "GB") )
  } else {
    print( paste(round(size_mb, 2), "MB") )
  }

  c(min(x_start), max(x_end))
}
