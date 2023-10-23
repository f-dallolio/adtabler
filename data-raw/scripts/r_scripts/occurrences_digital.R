library(data.table)
library(dtplyr)
library(tidyverse)
library(rlang)
library(glue)
# devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(bit64)


adintel_dir <- "/mnt/sata_data_1/adintel/"
new_adintel_dir <- "/mnt/sata_data_1/new_adintel/"


make_grep_file <- function(media_type_id = NULL, ad_date = NULL, market_code = NULL) {
  ad_date <- as.Date(ad_date)
  if (is.null(ad_date)) ad_date <- "[0-9]{4}[-][0-9]{2}[-][0-9]{2}"
  if (is.null(market_code)) market_code <- "[0-9]{1,3}"
  if (is.null(media_type_id)) media_type_id <- "[0-9]{1,2}"
  ad_time <- c("[0-9][0-9][:][0-9][0-9][:][0-9][0-9][.0-9]*")
  # out <- expand_grid(ad_date, ad_time, market_code, media_type_id)
  out <- expand_grid(ad_date, market_code, media_type_id)
  return(out)
}


file_df <- list.files(adintel_dir, full.names = T, recursive = T) |>
  str_subset("Occurrences") |>
  str_subset("Digital") |>
  as_tibble_col("input_file") |>
  mutate(
    file_type = str_split_i(input_file, "/", -2) |> rename_adintel(),
    file_type2 = str_split_i(input_file, "/", -1) |> rename_adintel() |> str_remove_all(".tsv"),
    file_year = str_split_i(input_file, "/", -3) |> as.numeric(),
    date_min1 = str_c(file_year - 1, "12", "24", sep = "-") |> as.Date(),
    date_max1 = str_c(file_year, "01", "31", sep = "-") |> as.Date(),
    date_min2 = date_max1 + 1,
    date_max2 = str_c(file_year, "03", "01", sep = "-") |> as.Date(),
    date_max2 = date_max2 - 1,
    date_min3 = date_max2 + 1,
    date_max3 = str_c(file_year, "03", "31", sep = "-") |> as.Date(),
    date_min4 = date_max3 + 1,
    date_max4 = str_c(file_year, "04", "30", sep = "-") |> as.Date(),
    date_min5 = date_max4 + 1,
    date_max5 = str_c(file_year, "05", "31", sep = "-") |> as.Date(),
    date_min6 = date_max5 + 1,
    date_max6 = str_c(file_year, "06", "30", sep = "-") |> as.Date(),
    date_min7 = date_max6 + 1,
    date_max7 = str_c(file_year, "07", "31", sep = "-") |> as.Date(),
    date_min8 = date_max7 + 1,
    date_max8 = str_c(file_year, "08", "31", sep = "-") |> as.Date(),
    date_min9 = date_max8 + 1,
    date_max9 = str_c(file_year, "09", "30", sep = "-") |> as.Date(),
    date_min10 = date_max9 + 1,
    date_max10 = str_c(file_year, "10", "31", sep = "-") |> as.Date(),
    date_min11 = date_max10 + 1,
    date_max11 = str_c(file_year, "11", "30", sep = "-") |> as.Date(),
    date_min12 = date_max11 + 1,
    date_max12 = str_c(file_year + 1, "01", "06", sep = "-") |> as.Date()
  ) |>
  relocate(input_file, .after = -1) |>
  pivot_longer(contains("date")) |>
  mutate(
    chunk = str_replace_all(name, "date_m[a-z][a-z]", "chunk"),
    name = rep(c("date_min", "date_max"), max(row_number()) / 2),
    .before = value
  ) |>
  pivot_wider() |>
  nest(.by = c(file_type, file_type2))


media_df <- media_type_table |>
  select(-nielsen_unique_key, -unique_key) |>
  distinct() |>
  rename(file_type2 = file_name) |>
  left_join(file_df) |>
  unnest(everything()) |>
  mutate(file_type2 = file_type2 |>
    str_replace("spot", "local") |>
    str_replace("network", "national")) |>
  mutate(
    new_dir = paste(new_adintel_dir, file_type, media_category,
      national_local, media_subcategory,
      sep = "/"
    ),
    new_file = paste0(
      new_dir, "/id", str_pad(media_type_id, 2, "left", "0"),
      "__", file_year, "__", chunk, ".csv"
    )
  )


new_dirs <- unique(media_df$new_dir)
for (i in seq_along(new_dirs)) {
  if (!dir.exists(new_dirs[[i]])) dir.create(new_dirs[[i]], recursive = TRUE)
}


fn <- function(media_type_id, date_min, date_max, input_file, new_file) {
  grep_file <- make_grep_file(
    media_type_id = media_type_id,
    ad_date = date_min:date_max
  )
  tmp <- tempfile(fileext = ".tsv")
  write_tsv(grep_file, tmp, col_names = F)
  cmd <- sprintf(glue("grep -Ef { tmp } %s"), input_file)
  t_init <- Sys.time()
  print(glue("{ t_init } \n\n\n"))
  new_df <- fread(
    cmd = cmd,
    sep = "\t",
    header = FALSE,
    nThread = 30,
    key = c("V1")
  )
  out <- NROW(new_df)
  print(glue("n rows: { out }"))
  t_end <- Sys.time()
  t_total <- t_end - t_init
  print(t_total)
  fwrite(
    x = new_df,
    file = new_file,
    nThread = 30,
  )
  t_end2 <- Sys.time()
  t_total2 <- t_end2 - t_init
  print(t_total2)
  return(out)
}

data_df <- media_df |>
  filter(
    file_type2 == "digital",
    file_year %in% 2010:2022
  )

i <- 1
seq <- seq_along(data_df$new_file)
numrows <- seq
for (i in seq) {
  xdf <- slice(data_df, i)
  media_type_id <- xdf$media_type_id
  date_min <- xdf$date_min
  date_max <- xdf$date_max
  input_file <- xdf$input_file
  new_file <- xdf$new_file

  cat_string <- str_split_i(new_file, "/", -1)
  print(glue("\n\n Starting iteration { i }/{ max(seq) }: { cat_string } at:"))

  numrows[i] <- fn(media_type_id, date_min, date_max, input_file, new_file)

  print(glue("\n\n Finished iteration { i }/{ max(seq) }: { cat_string } \n\n"))
}


# source("~/Documents/r_wd/adtabler/scripts/adtabler_fn_digital_2021.R")
