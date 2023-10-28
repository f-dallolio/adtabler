#' Replace string (gsub) in file using 'sed'
#'
#' @param file_in character.
#' @param file_out string.
#' @param pattern string.
#' @param replacement string.
#' @param all logical.
#' @param tmp_pattern string.
#'
#' @return a list indicating: input file, output file, pattern: replacement, exit code.
#' @export
#'
sed_replace <- function(file_in, file_out = NULL, pattern, replacement, all = FALSE, tmp_pattern = "sed_temp_") {
  if (all) {
    opts <- "g"
  } else {
    opts <- ""
  }
  if (is.null(file_out)) {
    file_out <- tempfile(pattern = "sed_temp_")
  }
  sed_cmd <- glue::glue("s/{pattern}/{replacement}/{opts}")
  sed_cmd <- glue::glue("s/{pattern}/{replacement}/")
  sys_cmd <- sprintf(glue::glue("sed '{sed_cmd}' {file_in} > {file_out}"))
  out <- system(sys_cmd)
  list_out <- list(
    file_in = file_in,
    file_out = file_out,
    pattern = pattern,
    replacement = replacement,
    exit_code = out
  )
  list_out
}

#' @describeIn sed_replace Replace string (gsub) in file using 'sed'
sed_replace_all <- function(file_in, file_out = NULL, pattern, replacement){
  sed_replace(file_in, file_out, pattern, replacement, all = TRUE)
}
