sed_replace_all <- function(file_in, file_out = NULL, pattern, replacement){
  sed_replace(file_in, file_out, pattern, replacement, all = TRUE)
}
