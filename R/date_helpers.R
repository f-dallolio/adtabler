#' Helpers for dates
#'
#' @param x numeric or character.
#' @param y,m,d,sep characters.
#' @param as_chr logical.
#'
#' @return is_xxx returns TRUE or FALSE, as_xxx transforms input in the desired character format.
#'
#' @name date_helpers
NULL

#' @rdname date_helpers
#' @export
is_year <- function(x){
  if( all(as.integer(x) > 0L & as.integer(x) < 9999, na.rm = T) ){
    return(TRUE)
  } else {
    return(FALSE)
  }
}

#' @rdname date_helpers
#' @export
as_year <- function(x){
  stopifnot("x must be coercible to a numeric between 1 and 9999" = is_year(x))
  numpad(as.integer(x), width = 4)
}

#' @rdname date_helpers
#' @export
is_month <- function(x){
  if( all(as.integer(x) %in% seq(1L, 12L), na.rm = TRUE) ){
    return(TRUE)
  } else {
    return(FALSE)
  }
}

#' @rdname date_helpers
#' @export
as_month <- function(x){
  stopifnot("x must be coercible to a numeric between 1 and 12" = is_month(x))
  numpad(as.integer(x), width = 2)
}

#' @rdname date_helpers
#' @export
is_day <- function(x){
  if( all(as.integer(x) %in% seq(1L, 31L)) ){
    return(TRUE)
  } else {
    return(FALSE)
  }
}

#' @rdname date_helpers
#' @export
as_day <- function(x){
  stopifnot("x must be coercible to a numeric between 1 and 31" = is_day(x))
  numpad(as.integer(x), width = 2)
}

#' @rdname date_helpers
#' @export
make_date <- function(y, m = NULL, d = NULL, as_chr = FALSE){
  stopifnot( is_year(y),
             "m must be coercible to a numeric between 1 and 12 or left empty" = is_month(m) | is.null(m),
             "d must be coercible to a numeric between 1 and 31 or left empty" = is_day(d) ||  is.null(d) )

  if( is.null(m) ) m <- as_month(1:12)
  if( is.null(d) ) d <- as_day(1:31)
  grd <- expand.grid(yy = y, mm = m, dd = d)
  out_date <- as.Date( paste(grd$yy, grd$mm, grd$dd, sep = "-") )
  out <- out_date[!is.na(out_date)] |> sort()
  if( !as_chr) {
    return( out )
  }
  as.character( out )
}

#' @rdname date_helpers
#' @export
make_date_grep <- function(y = NULL, m = NULL, d = NULL, sep = "-"){
  stopifnot( "y must be coercible to a numeric between 1 and 9999" = is_month(m) | is.null(m),
             "m must be coercible to a numeric between 1 and 12 or left empty" = is_month(m) | is.null(m),
             "d must be coercible to a numeric between 1 and 31 or left empty" = is_day(d) ||  is.null(d) )

  if( is.null(y) ) {
    y <- "[1-9]{4}"
  } else {
    y <- numpad4(y)
  }
  if( is.null(m) ) {
    m <- "[1-9]{2}"
  } else {
    m <- numpad2(m)
  }
  if( is.null(d) ) {
    d <- "[1-9]{2}"
  } else {
    d <- numpad2(d)
  }
  grd <- expand.grid(yy = y, mm = m, dd = d)
  paste(
    grd$yy,
    grd$mm,
    grd$dd,
    sep = sep) |>
    sort()
}

