library(tidyverse)
library(readxl)
devtools::install_github('f-dallolio/fdutils')
1
library(fdutils)
devtools::install_github('f-dallolio/adtabler')
1
library(adtabler)

load('~/Dropbox/NielsenData/occurrences_info/occurrences_info.RData')

file_df
col_df
ukey_df

file_df <- file_df |>
  distinct() |>
  # filter(media_category == "tv") |>
  arrange(media_geo, media_type_id)
file_df |> print(n=500)
i=9

make_new_df <- function(i, media_df, col_df, ukey_df, return_df = FALSE ){

  t0 <- Sys.time()
  print(t0)

  i_df <- media_df[i,]
  i_col_df <- col_df |>
    filter(media_type_id == i_df$media_type_id)
  i_ukey_df <- ukey_df |>
    filter(media_type_id == i_df$media_type_id)
  file <- i_df$file
  col_names <- i_col_df$col_names
  col_classes <- i_col_df$col_classes
  key <- i_ukey_df$unique_key

  print('Reading table')
  new_df <- data.table::fread(file = file, sep = ",")
  new_df <- new_df |> set_names(col_names)

  if(NROW(new_df) > 0) {

    if("ad_time" %in% col_names){
      nchar_ad_time <- nchar(new_df[["ad_time"]][1])
      if(nchar_ad_time > 8){

        print('Cut ad_time to 8 characters')

        new_df[["ad_time"]] <- new_df[["ad_time"]] |>
          str_sub(start = 1, end = 8)
      }
    }

    print('Check classes')
    classes <- map(new_df, ~ .x |> class()) |>
      map(~ .x[[length(.x)]]) |>
      as_tibble() |>
      pivot_longer(everything(), names_to = "col_names",
                   values_to = "df_classes") |>
      mutate(col_classes = col_classes,
             ok_classes = col_classes == df_classes)

    to_chr <- classes |>
      filter(df_classes == "integer" & col_classes == "character" ) |>
      pull(col_names)

    print('Fix int to chr')
    j=1
    for(j in seq_along(to_chr)){
      nms <- to_chr[[j]]
      new_df[[nms]] <- new_df[[nms]] |> as.character()
    }
    rm(j)

    print('Check classes')
    new_classes <- map(new_df, ~ .x |> class()) |>
      map(~ .x[[length(.x)]]) |>
      as_tibble() |>
      pivot_longer(everything(), names_to = "col_names",
                   values_to = "df_classes") |>
      mutate(col_classes = col_classes,
             ok_classes = col_classes == df_classes,
             all_na = FALSE)

    print('Check for all NAs columns')
    which_logical <- which(new_classes$df_classes == "logical")
    if(length(which_logical) > 0){
      j=which_logical[[1]]
      for(j in which_logical){
        new_classes$all_na[[j]] <- new_df[[j]] |> is.na() |> all()
      }
      na_col_names <- new_classes |> filter(all_na) |> pull(col_names)
    } else {
      na_col_names = ""
    }

    print('Check UTF-8')
    utf_ok <- new_df |> check_utf8()

    print('Set keys')
    new_df <- new_df |> data.table::setkeyv(key)

    t_end <- Sys.time()
    elapsed <- t_end - t0

    i_df$classes <- list(new_classes)
    i_df$unique_keys <-  data.table::key(new_df) |>
      as_tibble_col(column_name = "unique_keys") |>
      list()
    i_df$utf8 <- utf_ok |>
      as_tibble_col(column_name = "utf_ok") |>
      list()
    i_df$col_all_na = na_col_names |>
      as_tibble_col(column_name = "col_all_na") |>
      list()

    if(return_df){
      i_df$df <- list(new_df)
    }

    print('DONE in:')
    print(elapsed)

    return(i_df)
  }
}

# test <- make_new_df(i = 1,
#                     media_df = file_df,
#                     col_df = col_df,
#                     ukey_df = ukey_df,
#                     return_df = FALSE)
# test

file_df |>
  arrange(media_type_id, media_type) |>
  pull(media_category) |>
  unique()

media_df <- file_df |>
  filter(media_category == "online" ,
         media_type == "internet")
# print(media_df, n=200)

map_seq <- seq_along(media_df$file)

# out <- vector(mode = "list", length = length(map_seq))
# i=72
# tictoc::tic()
# for(i in seq_along(map_seq)) {
#   print(str_c('Iteration number: ', i))
#   out[[i]] <-  make_new_df(
#     i = i,
#     media_df = media_df,
#     col_df = col_df,
#     ukey_df = ukey_df,
#     return_df = FALSE
#   )
# }
# out_df <- out |> list_rbind()
# tictoc::toc()
#

library(furrr)
plan(multisession, workers = 4)
# plan(sequential)

tictoc::tic()
out <- map_seq |> future_map(
  ~ make_new_df(
    i = .x,
    media_df = media_df,
    col_df = col_df,
    ukey_df = ukey_df,
    return_df = FALSE
  ),
  .progress = TRUE
)
out_df <- out |> list_rbind()
tictoc::toc()

beepr::beep(sound = "fanfare")

plan(sequential)

save(out_df, media_df, col_df, ukey_df, make_new_df, file = "~/Documents/r_wd/adtabler/scripts/temp/internet_make_new_df.RData")

