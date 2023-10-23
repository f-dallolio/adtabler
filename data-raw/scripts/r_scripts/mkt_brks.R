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

market_breaks_df <- list.files(adintel_dir, full.names = T, recursive = T) |>
  as_tibble_col("input_file") |>
  filter(
    input_file |> str_detect("Master_File", negate = T),
    input_file |> str_detect("Market_Breaks")
  ) |>
  mutate(
    file_type = str_split_i(input_file, "/", -2) |>
      rename_adintel(),
    file_type2 = str_split_i(input_file, "/", -1) |>
      str_remove_all(".tsv") |>
      rename_adintel(),
    file_year = str_split_i(input_file, "/", -3) |> as.numeric(),
    new_dir = str_c(new_adintel_dir, file_type2, sep = "/"),
    new_file = str_c(new_dir, "/", file_type2, "__", file_year, ".csv")
  )
dir.create(market_breaks_df$new_dir |> unique())

seq <- seq_along(market_breaks_df$input_file)
i <- 1
for (i in seq) {
  file_name <- market_breaks_df$new_file[i] |> str_split_i("/", -1)
  new_df <- market_breaks_df$input_file[i] |>
    fread(
      sep = "\t"
    )
  new_df |> fwrite(
    file = market_breaks_df$new_file[i]
  )
  print(str_c(i, file_name, sep = " - "))
}

# source("~/Documents/r_wd/adtabler/scripts/mkt_brks.R")
