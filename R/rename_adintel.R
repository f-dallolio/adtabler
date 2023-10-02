#' Rename Adintel Variables
#'
#' @param x character vector.
#'
#' @return character vector.
#' @export
#'
rename_adintel <- function(x, named = FALSE){
  nms <- x
  out <- x %>%
    str_sep_upper("_", named = FALSE) %>%
    str_replace_all("_", " ") %>%
    str_squish() %>%
    str_replace_all(" ", "_") %>%
    str_remove_all(" ") %>%
    str_replace_all("prime", "prim") %>%
    if_else(condition = str_detect(., "dim_bridge"), true = "dim_bridge_occ_imp_spot_radio_key", false = .)
  if(named){
    names(out) <- nms
  }
  out
}
