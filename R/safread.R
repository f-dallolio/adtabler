#' Safe fread
#'
#' @export
safread_warn <- function(){
  fn <- purrr::quietly(data.table::fread)
  .args <- as.list(match.call()[-1])
  out <- do.call(fn, .args)
  if( adtabler::is_empty(out$warnings) & adtabler::is_empty(out$messages) ) {
    return(out$result)
  } else {
    return(out[-1])
  }
}
rlang::fn_fmls(safread_warn) <- rlang::fn_fmls(data.table::fread)
