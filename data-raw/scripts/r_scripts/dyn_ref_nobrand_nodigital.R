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

dyn_ref_df <- list.files(adintel_dir, full.names = T, recursive = T) |>
  as_tibble_col("input_file") |>
  filter(
    input_file |> str_detect("Master_File", negate = T),
    input_file |> str_detect("References")
  ) |>
  mutate(
    file_type = str_split_i(input_file, "/", -2) |>
      rename_adintel(),
    file_type2 = str_split_i(input_file, "/", -1) |>
      str_remove_all(".tsv") |>
      rename_adintel(),
    file_year = str_split_i(input_file, "/", -3) |> as.numeric(),
    new_dir = str_c(new_adintel_dir, file_type, "dynamic", file_type2, sep = "/"),
    new_file = str_c(new_dir, "/", file_type2, "__", file_year, ".csv")
  )

new_dirs <- unique(dyn_ref_df$new_dir)
for (i in seq_along(new_dirs)) {
  if (!dir.exists(new_dirs[[i]])) dir.create(new_dirs[[i]], recursive = TRUE)
}

ref_nobrand_nodigital_df <- dyn_ref_df |>
  filter(file_type2 != "brand") |>
  filter(file_type2 |> str_detect("digital_creative", negate = TRUE))

sfread <- quietly(fread)
sfread <- safely(sfread, otherwise = NA)

seq <- seq_along(ref_nobrand_nodigital_df$input_file)
bad_out <- tibble(
  input_file = ref_nobrand_nodigital_df$input_file,
  errors = rep(NA_character_, length(seq)),
  warnings = rep(NA_character_, length(seq))
)
i <- 1

for (i in seq) {
  xdf <- slice(ref_nobrand_nodigital_df, i)
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

  safe_df <- sfread(
    input = input_file,
    sep = "\t",
  )

  has_error <- length(safe_df$error) != 0
  has_warning <- length(safe_df$result$warnings) != 0
  all_ok <- !has_error & !has_warning


  if (all_ok) {
    new_df <- safe_df$result$result
    fwrite(x = new_df, file = new_file)
  } else if (has_warning) {
    bad_out$warnings[i] <- safe_df$result$warnings
  } else if (has_error) {
    bad_out$errors[i] <- safe_df$error
  }

  t_end2 <- Sys.time()
  t_total2 <- t_end2 - t_init
  print(t_total2)
  print(glue("\n\n Finished iteration { i }/{ max(seq) }: { cat_string } \n\n"))
}

# source("~/Documents/r_wd/adtabler/scripts/dyn_ref_nobrand_nodigital.R")
