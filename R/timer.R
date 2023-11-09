#' Timer
#'
#' @param start Sys.time object0
#' @param n_pre integer.
#' @param n_post integer.
#' @param msg character.
#'
#' @return character
#' @export
#'
#' @examples
#' t0 <- Sys.time()
#' Sys.sleep(5)
#' timer(start = t0, n_pre = 1, n_post = 2, msg = "Time elapsed: {.x}")
timer <- function(start, n_pre = 0, n_post = 0, msg = "Time elapsed: {.x}"){
  .x <- prettyunits::pretty_dt(Sys.time() - start)
  .pre <- paste0(rep("\n", n_pre, collapse = ""))
  .post <- paste0(rep("\n", n_post), collapse = "")
  .msg <- paste0(.pre, "\n", msg, "\n", .post)
  glue::glue(.msg)
}
