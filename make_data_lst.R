library(dplyr)
library(tidyr)
library(purrr)
library(rlang)
library(stringr)
# library(data.table)
library(adtabler)

list <- rlang::dots_list(.named = TRUE)

adtabler::adintel_tables$var_attributes

xdf <- adintel_tables |>
  select(- var_attributes) |>
  rename(year = file_year) |>
  unnest(everything())

get_adintel_info <- function(path){
  x <- data.table::fread(
    path,
    sep = "\t",
    strip.white = TRUE,
    nrows = 10000
  )
  out <- map(x, ~ rev(class(.x))[1]) |>  unlist()
  all_na <- map(x, ~ all(is.na(.x))) |>  unlist()
  out[which(all_na)] <- "all_NA"
  out
}

path <- "/media/filippo/One Touch/nielsen_data/adintel/ADINTEL_DATA_2010/nielsen_extracts/AdIntel/2010/Impressions/ImpNationalTV.tsv"

lst <- xdf |>
  mutate(file_size_mb = file.size(path)/1024^2,
         col_info_data = map(path, get_adintel_info)) |>
  mutate(avg_size_mb = mean(file_size_mb),
         n_years = n_distinct(year),
         .by = c(file_type, file_name)) |>
  nest(.by = c(file_type,
               file_name,
               avg_size_mb,
               is_dynamic,
               n_years)) |>
  mutate(info = map(data, ~ .x |>
                      as.list() |>
                      list_transpose() |>
                      pluck(1)),
         .keep = "unused")

lst |> print(n=100)

adintel_info <- adintel_tables |>
  filter(!file_type %in% c("impressions",
                           "market_breaks",
                           "universe_estimates")) |>
  select(c(1,2), var_attributes) |>
  mutate(manual_info = map(
    var_attributes,
    ~ .x$var_type_manual |> set_names(.x$var_name_manual))
  ) |>
  select(- var_attributes) |>
  right_join(lst) |>
  mutate(
    data_info = map2(
      info, manual_info,
      ~ .x |> c( col_info_manual = list(.y) )
      ),
    data_info = imap(
      data_info,
      ~ c(list(file_type = file_type[.y],
               file_name = file_name[.y]),
          .x,
          list(col_names = rename_adintel_cols(names(.x$col_info_data)))
        )
      ),
    n_years = n_years |> na_if(n_years == 1)
  ) |>
  select(- c(is_dynamic, manual_info, info)) |>
  mutate(avg_size_mb = (avg_size_mb) |> round(3)) |>
  left_join(
    adintel_tables |> select(file_type, file_name, file_year, path)
  )



for(i in seq_along(adintel_info$data_info)){

  nm <- str_remove_all(
    string = adintel_info$data_info[[i]][["path"]],
    pattern = "/media/filippo/One Touch/nielsen_data/adintel/"
  ) |>
  str_replace_all(
    pattern = "ADINTEL_DATA_2010/nielsen_extracts/AdIntel/2010/",
    replacement = "ADINTEL_DATA_{year}/nielsen_extracts/AdIntel/{year}/"
  )

  adintel_info$data_info[[i]][["path"]] <- nm

  adintel_info$data_info[[i]][["year"]] <- adintel_info$file_year[[i]]
  to_na <- adintel_info$data_info[[i]][["year"]] == -1
  if(any(to_na)){
    pluck(adintel_info$data_info, i, "year")[to_na] <- NA
  }
}

adintel_info <- adintel_info |>
  select(-path, -file_year)



media_type_info <- readRDS("Documents/r_wd/adtabler/data/media_type_info.rds")


adintel_info <- adintel_info |> left_join(media_type_info)



adintel_info

adintel_info$data_info[[9]]
adintel_info$media_info[[9]]

adintel_info$data_info[[23]]





saveRDS(adintel_info,
        file = "Documents/r_wd/adtabler/data/adintel_info.rds")
save(adintel_info,
     file = "Documents/r_wd/adtabler/data/adintel_info.rda")
















