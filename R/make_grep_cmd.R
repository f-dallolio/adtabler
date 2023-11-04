#' Create cmd
#'
#' @param file a string.
#' @param ... .
#' @param first_n an integer.
#' @param opts a string.
#'
#' @return a vector.
#' @export
#'
make_grep_cmd <- function(file, ..., first_n = NULL, opts = "-wE"){
  df <- data.table::fread(file = file, nrows = 1)
  ptrn0 <- as.list( rep("[ .:0-9a-zA-Z]*", NCOL(df) ) )
  names(ptrn0) <- rename_adintel(names(df))

  dots <- rlang::enquos(..., .named = TRUE)
  names(dots) <- rename_adintel(names(dots))
  nms_match0 <- match(names(dots), names(ptrn0))
  nms_match <- names(ptrn0)[na_rm(nms_match0)]
  nms_na <- setdiff(names(dots), nms_match)

  if( !is_empty(nms_na) ){
    warning( paste( "Names not in the dataset:", nms_na ) )
  }

  new_dots <- dots[nms_match]
  grid <- tidyr::expand_grid(!!!new_dots)
  ptrn0[names(grid)] <- grid
  ptrn1 <- as.list( tibble::as_tibble( ptrn0 ) )
  ptrn <- purrr::map_vec(purrr::list_transpose(ptrn1), ~ stringr::str_flatten(.x, "\t"))

  sprintf( glue::glue("grep -m {first_n} -wE '{ptrn}' %s"), file)
}
