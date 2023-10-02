#' Rename Adintel Variables
#'
#' @param name_manual character vector.
#'
#' @return character vector.
#' @export
#'
rename_adintel_cols <- function(x, named = F){
  x %>%
    str_sep_upper("_", named = named) %>%
    str_replace_all("_", " ") %>%
    str_squish() %>%
    str_replace_all(" ", "_") %>%
    str_remove_all(" ") %>%
    str_replace_all("prime", "prim") %>%
    if_else(condition = str_detect(., "dim_bridge"), true = "dim_bridge_occ_imp_spot_radio_key", false = .)
}
