grep_ad_date_ymd <- function(year = NULL, month = NULL, day = NULL) {
  if (is.null(year)) {
    year <- "[0-9]{4}"
  } else {
    if (is.numeric(month)) {
      year <- stringr::str_pad(string = as.integer(year), width = 4, side = "left", pad = "0")
    } else {
      stop("month must be NULL or integer")
    }
  }

  if (is.null(month)) {
    month <- "[0-9]{2}"
  } else {
    if (is.numeric(month)) {
      month <- stringr::str_pad(string = as.integer(month), width = 2, side = "left", pad = "0")
    } else {
      stop("month must be NULL or integer")
    }
  }

  if (is.null(day)) {
    day <- "[0-9]{2}"
  } else {
    if (is.numeric(day)) {
      day <- stringr::str_pad(as.integer(day), 2, "left", "0")
    } else {
      stop("day must be NULL or integer")
    }
  }
  paste(year, month, day, sep = "-")
}
grep_ad_date_ymd(year = 2010:2012, month = 2, day = 1)
grep_ad_date_ymd()
grep_ad_date_ymd(year = 2010, month = 2:4)

grep_ad_date <- function(x = NULL) {
  if (is.null(x)) {
    return(grep_ad_date_ymd())
  }
  x <- as.Date(x)
  year <- lubridate::year(x) |>
    as.integer()
  month <- lubridate::month(x) |>
    as.integer()
  day <- lubridate::day(x) |>
    as.integer()
  grep_ad_date_ymd(year, month, day)
}
grep_ad_date(x = "2014-01-07")

grep_ad_time <- function(h = NULL, m = NULL, s = NULL) {
  if (is.null(h)) h <- "[0-9]{2}"
  if (is.null(m)) m <- "[0-9]{2}"
  if (is.null(s)) s <- "[0-9]{2}"
  paste(h, m, s, sep = ":") |>
    paste0("[.0-9]*")
}
grep_ad_time(h = 12,38,12)

grep_market_code <- function(market_code = NULL) {
  if (is.null(market_code)) market_code <- "[0-9A-Za-z]{1,3}"
}
grep_media_type_id <- function(media_type_id = NULL) {
  if (is.null(media_type_id)) media_type_id <- "[0-9]{1,2}"
}

yearweek_by_year <- function(year) {
  year <- as.integer(year)
  out <- vector(mode = "list", length = length(year))

  for (i in seq_along(year)) {
    wks <- 1:52 |>
      stringr::str_pad(width = 2, side = "left", pad = "0")

    if (tsibble::is_53weeks(year[[i]])) {
      wks <- c(wks, 53)
    }
    out[[i]] <- paste0(year[[i]], " W", wks)
  }
  out |>
    unlist() |>
    tsibble::yearweek()
}
yearweek_by_year(2010:2021)
yearweek_range <- function(x, .named = TRUE) {
  stopifnot(tsibble::is_yearweek(x))
  n <- length(x)
  x1 <- as.Date(x[[1]])
  x2 <- as.Date(x[[n]] + 1) - 1
  out <- as.Date(x1:x2)
  if (.named) {
    names(out) <- tsibble::yearweek(out)
  }
  out
}
yearweek_by_year(2010:2021) |> yearweek_range()


# grep_f <- function(ad_date = NULL,
#                         ad_time = NULL,
#                         market_code = NULL,
#                         media_type_id = NULL){
#   if(is.null(ad_date)) ad_date <- grep_ad_date()
#   if(is.null(ad_time)) ad_time <- grep_ad_time()
#   if(is.null(market_code)) market_code <- grep_market_code()
#   if(is.null(media_type_id)) media_type_id <- grep_media_type_id()
#   tidyr::expand_grid(ad_date, ad_time, market_code, media_type_id)
# }
#
# grep_make_f()
#
# grep_f_no_time <- function(ad_date = NULL,
#                            market_code = NULL,
#                            media_type_id = NULL){
#   if(is.null(ad_date)) ad_date <- grep_ad_date()
#   if(is.null(market_code)) market_code <- grep_market_code()
#   if(is.null(media_type_id)) media_type_id <- grep_media_type_id()
#   tidyr::expand_grid(ad_date, market_code, media_type_id)
# }

grep_file <- make_grep_file(
  media_type_id = media_type_id,
  ad_date = date_min:date_max
)
tmp <- tempfile(fileext = ".tsv")
write_tsv(grep_file, tmp, col_names = F)
cmd <- sprintf(glue("grep -Ef { tmp } %s"), input_file)
