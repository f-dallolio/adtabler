library(dplyr)
library(tidyr)
library(purrr)
library(rlang)
library(stringr)
# library(data.table)
library(adtabler)

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

media1 <- media_type_table |>
  transmute(
    file_name,
    media_type,
    media_type_id,
    is_local = national_local == "local",
    is_spanish = spanish_flag,
    media_category,
    media_subcategory,
    media_type_desc = nielsen_media_definition
  ) |>
  distinct()

media2 <- media_type_table |>
  transmute(
    media_type_id,
    ukey = nielsen_unique_key,
  ) |>
  nest(ukey = ukey, .by = media_type_id) |>
  mutate(ukey = map(ukey, ~ rename_adintel(.x[[1]], named = T)),
         ukey = map(ukey, ~ c(.x[4], .x[-4])))

media_tbl <- media1 |> left_join(media2) |>
  relocate(ukey, .after = media_type_id)


x <- media_tbl |>
  group_by(, file_name, media_type) |>
  group_split() |>
  map(~as.list(.x))

for(i in seq_along(x)){
  xx <-  x[[i]]

 x[[i]][["ukey"]] <- x[[i]][["ukey"]][[1]]
}



f_nms <- map(x, ~ .x$file_name) |> list_c()

x_lst <- tibble(file_name = f_nms, id = seq_along(f_nms)) |>
  group_by(file_name) |>
  mutate(cur_group_id()) |>
  summarise(xx = list(x[id]))

x_lst$xx

xxx <- x_lst$xx
names(xxx) <- x_lst$file_name

xxx[1]

for(i in seq_along(xxx)){
  x_i <- xxx[[i]]
  nms_i <- rep("a", length(x_i))
  for(j in seq_along(x_i)){
    nms_j <- paste0(
      "id",
      str_pad(x_i[[j]]$media_type_id, 2, "left", pad = "0"),
      "__",
      x_i[[j]]$media_type
    )
    nms_i[[j]] <- nms_j
  }
  names(xxx[[i]]) <- nms_i
}

xxx[[10]]
x_lst$media_info <- xxx

media_type_info <- x_lst |> select(-xx)
media_type_info$media_info
media_type_info$media_info[[1]]

saveRDS(media_type_info,
        file = "Documents/r_wd/adtabler/data/media_type_info.rds")
save(media_type_info,
     file = "Documents/r_wd/adtabler/data/media_type_info.rda")















