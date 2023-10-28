#' Value Matching
#'
#' @param x vector or NULL: the values to be matched.
#' @param table vector or NULL: the values to be matched against.
#' @param which logical. If TRUE (default) the function returns the position of the elements of x without a match in the name of the output value.
#'
#' @return logical.
#' @export
#'
all_match <- function(x, table, which = TRUE) {
  out <- all(match(x = x, table = table, nomatch = FALSE, incomparables = FALSE))
  if(which){
    nomatch_id <- which(!match(x, table, nomatch = FALSE, incomparables = FALSE))
    names(out) <- names(out) <- glue::glue("FALSE:c({ paste0(nomatch_id, collapse = \",\")})")
  }
  return(out)
}
