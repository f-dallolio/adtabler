#' Replace string (gsub) in file using 'sed'
#'
#' @param file_in character.
#' @param file_out string.
#' @param pattern string.
#' @param replacement string.
#' @param all logical.
#' @param tmp_pattern string.
#' @return a list indicating: input file, output file, pattern: replacement, exit code.
#'
#' @name sed_replace
NULL

#' @rdname sed_replace
#' @export
sed_replace <- function(file_in, file_out = NULL, pattern, replacement, all = FALSE, tmp_pattern = "sed_temp_") {
  if (all) {
    opts <- "g"
  } else {
    opts <- ""
  }
  if (is.null(file_out)) {
    file_out <- tempfile(pattern = tmp_pattern)
  }
  sed_cmd <- glue::glue("s/{pattern}/{replacement}/{opts}")
  sys_cmd <- sprintf(glue::glue("sed {shQuote(sed_cmd)} {file_in} > {file_out}"))
  out <- system(command = sys_cmd)
  list_out <- list(
    sed_cmd,
    file_in = file_in,
    file_out = file_out,
    pattern = pattern,
    replacement = replacement,
    exit_code = out
  )
  list_out
}

#' @rdname sed_replace
#' @export
sed_replace_all <- function(file_in, file_out = NULL, pattern, replacement, mp_pattern = "sed_temp_"){
  sed_replace(file_in, file_out, pattern, replacement, all = TRUE)
}
