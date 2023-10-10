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

dyn_ref_df <-  list.files(adintel_dir, full.names = T, recursive = T) |>
  as_tibble_col("input_file") |>
  filter(input_file |> str_detect("Master_File",negate = T),
         input_file |>  str_detect("References")) |>
  mutate(
    file_type = str_split_i(input_file, "/" , -2) |>
      rename_adintel(),
    file_type2 = str_split_i(input_file, "/" , -1) |>
      str_remove_all(".tsv") |>
      rename_adintel(),
    file_year = str_split_i(input_file, "/" , -3) |> as.numeric(),
    new_dir = str_c(new_adintel_dir, file_type, "dynamic", file_type2, sep = "/"),
    new_file = str_c(new_dir, "/", file_type2, "__", file_year, ".csv")
  )

new_dirs <- unique(dyn_ref_df$new_dir)
for (i in seq_along(new_dirs)){
  if(!dir.exists(new_dirs[[i]]))  dir.create(new_dirs[[i]], recursive = TRUE)}

brand_df <- dyn_ref_df |>
  filter(file_type2 == "brand")


seq <- seq_along(brand_df$input_file)
i=1
for(i in seq) {

  xdf <- slice(brand_df, i)
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

  old_df <- fread(
    input = input_file,
    sep = "",
    quote = ""
  )

  old_names <- names(old_df) |> str_split("\t") |> list_c()

  new_df_chr <- old_df[[1]] |> str_replace_all('\t\"', '\"')

  new_df <- fread(
    text = new_df_chr,
    sep = "\t"
  )

  names(new_df) <- old_names
  fwrite(x = new_df, file = new_file)

  t_end2 <- Sys.time()
  t_total2 <- t_end2- t_init
  print(t_total2)
  print(glue("\n\n Finished iteration { i }/{ max(seq) }: { cat_string } \n\n"))

}

# source("~/Documents/r_wd/adtabler/scripts/dyn_ref_brand.R")

