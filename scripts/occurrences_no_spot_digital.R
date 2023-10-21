library(data.table)
library(dtplyr)
library(tidyverse)
library(rlang)
library(glue)
# devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(bit64)

to_utf_8 <- function(x){
  if(is.character(x)){
    Encoding(x) <- "latin1"
    out <- iconv(x, "latin1", "UTF-8")
    return(out)
  } else {
    x
  }
}
ad_time_to_8char <- function(x){
  str_sub(string = as.character(x), start = 1, end = 8)
}
make_grep_file <- function(media_type_id = NULL, ad_date = NULL, market_code = NULL){
  ad_date <- as.Date(ad_date)
  if(is.null(ad_date)) ad_date <- "[0-9]{4}[-][0-9]{2}[-][0-9]{2}"
  if(is.null(market_code)) market_code <- "[0-9]{1,3}"
  if(is.null(media_type_id)) media_type_id <- "[0-9]{1,2}"
  ad_time <- c("[0-9][0-9][:][0-9][0-9][:][0-9][0-9][.0-9]*")
  out <- expand_grid(ad_date, ad_time, market_code, media_type_id)
  return(out)
}


adintel_dir <- "/mnt/sata_data_1/adintel/"
new_adintel_dir <- "/mnt/sata_data_1/new_adintel/"





file_df <- list.files(adintel_dir, full.names = T, recursive = T) |>
  str_subset("Occurrences") |>
  # str_subset("Digital", negate = T) |>
  # str_subset("Spot_TV", negate = T) |>
  as_tibble_col("input_file") |>
  mutate(file_type = str_split_i(input_file, "/", -2) |> rename_adintel(),
         file_type2 = str_split_i(input_file, "/", -1) |> rename_adintel() |> str_remove_all(".tsv"),
         file_year = str_split_i(input_file, "/", -3) |> as.numeric(),
         date_min1 = str_c(file_year - 1 ,"12", "24", sep = "-") |> as.Date(),
         date_max1 = str_c(file_year ,"06", "30", sep = "-") |> as.Date(),
         date_min2 = date_max1 + 1,
         date_max2 = str_c(file_year + 1 ,"01", "06", sep = "-") |> as.Date()) |>
  relocate(input_file, .after = -1) |>
  pivot_longer(contains("date")) |>
  mutate(chunk = str_replace_all(name, "date_m[a-z][a-z]", "chunk"),
         name = rep(c("date_min", "date_max"), max(row_number())/2),
         .before = value) |>
  pivot_wider() |>
  nest(.by = c(file_type, file_type2))


media_df <- media_type_table |>
  select(-nielsen_unique_key, -unique_key) |>
  distinct() |>
  rename(file_type2 = file_name) |>
  left_join(file_df) |>
  unnest(everything()) |>
  mutate(file_type2 = file_type2 |>
           str_replace( "spot", "local") |>
           str_replace("network", "national")) |>
  mutate(
    new_dir = paste( new_adintel_dir, file_type, media_category,
                     national_local, media_subcategory,
                     sep = "/" ),
    new_file = paste0( new_dir, "/id", str_pad(media_type_id, 2, "left", "0"),
                       "__", file_year, ".csv" )) |>
  select(file_type, file_type2, media_type_id, input_file, new_dir, new_file) |>
  distinct() |>
  filter( file_type2 |> str_detect("digital", negate = T) ) |>
  filter( file_type2 |> str_detect("local_tv", negate = T) )


new_dirs <- unique(media_df$new_dir)
for (i in seq_along(new_dirs)){
  if(!dir.exists(new_dirs[[i]]))  dir.create(new_dirs[[i]], recursive = TRUE)}

data_df <- media_df

i=1
seq <- seq_along(data_df$new_file)

for(i in seq){

  xdf <- slice(data_df, i)
  input_file <- xdf$input_file
  new_file <- xdf$new_file

  cat_string <- str_c(
    str_split_i(new_file, "/", -4), "/",
    str_split_i(new_file, "/", -3), "/",
    str_split_i(new_file, "/", -2), "/",
    str_split_i(new_file, "/", -1)
  )
  print(glue("\n\n Starting iteration { i }/{ max(seq) }: { cat_string } at:"))
  t_init <- Sys.time()
  print(glue("{ t_init } \n\n\n"))

  new_df <- fread(
    input = input_file,
    sep = "\t"
    # header = FALSE,
    # nThread = 30
  )

  if(any(str_detect(names(new_df), "AdTime"))){
    print("Cutting AdTime to xx:xx:xx")
    new_df$AdTime <- new_df$AdTime |> ad_time_to_8char()
  }

  t_end <- Sys.time()
  t_total <- t_end - t_init
  print(t_total)

  fwrite(
    x = new_df,
    file = new_file,
    nThread = 30,

  )
  t_end2 <- Sys.time()
  t_total2 <- t_end2- t_init
  print(t_total2)
  print(glue("\n\n Finished iteration { i }/{ max(seq) }: { cat_string } \n\n"))
}

# source("~/Documents/r_wd/adtabler/scripts/adtabler_fn_no_spot_digital.R")



