library(data.table)
library(dtplyr)
library(tidyverse)
library(rlang)
library(glue)
# devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(bit64)



sfread <- quietly(fread)
sfread <- safely(sfread, otherwise = NA)




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
    new_dir = str_c(new_adintel_dir, file_type, "dynamic", file_type2, sep = "/") ,
    new_file = str_c(new_dir, "/", file_type2, "__", file_year, ".csv")
  )

new_dirs <- unique(dyn_ref_df$new_dir)
for (i in seq_along(new_dirs)){
  if(!dir.exists(new_dirs[[i]]))  dir.create(new_dirs[[i]], recursive = TRUE)}



ref_digital_df_big <- dyn_ref_df |>
  filter(file_type2 != "brand") |>
  filter(file_type2 |> str_detect("digital_creative")) |>
  mutate(size_gb = file.size(input_file)/1024^3,
         .before = file_year) |>
  filter(size_gb > 10)

first_col <- ref_digital_df_big |>
  filter(size_gb == max(size_gb)) |>
  pull(input_file) |>
  fread(
    # sep = "",
    select = 1,
    header = FALSE,
    skip = 1
  )


ref_digital_df_big_chunks <-
  ref_digital_df_big[1,] |>
  bind_rows(ref_digital_df_big[1,]) |>
  bind_rows(ref_digital_df_big[1,]) |>
  bind_rows(ref_digital_df_big[1,]) |>
  bind_rows(ref_digital_df_big[2,]) |>
  bind_rows(ref_digital_df_big[2,]) |>
  bind_rows(ref_digital_df_big[2,]) |>
  bind_rows(ref_digital_df_big[2,]) |>
  mutate(
    id = 1:2 |> rep(each = 4),
    chunk = str_c("chunk", 1:4, sep = "_") |> rep(2),
    low = ((30000000 * 0:3) + 1) |> rep(2),
    hi = (30000000 * 1:4) |> rep(2),
    new_file = str_c(new_dir, "/", file_type2, "__", file_year, "__", chunk, ".csv"),
    .before = 1
  ) |>
  select(id:hi, new_file)

seq <- seq_along(ref_digital_df_big$input_file)
bad_out <- tibble(
  input_file = ref_digital_df_big$input_file,
  errors = rep(NA_character_, length(seq)),
  warnings = rep(NA_character_, length(seq))
)
i=1

for(i in seq) {

  xdf <- slice(ref_digital_df_big, i)
  input_file <- xdf$input_file

  cat_string <- str_c(
    str_split_i(input_file, "/", -4), "/",
    str_split_i(input_file, "/", -3), "/",
    str_split_i(input_file, "/", -2), "/",
    str_split_i(input_file, "/", -1)
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


  if(all_ok){

    j = 1
    chunk_df <- ref_digital_df_big_chunks |>
      filter(id == i)

    for(j in seq_along(chunk_df$new_file)){

      chunk <- chunk_df$chunk[j]
      new_file <- chunk_df$new_file[j]
      low <- chunk_df$low[j]
      hi <- chunk_df$hi[j]
      nr <- NROW(safe_df$result$result)


      new_df <- safe_df$result$result[low : min(hi, nr), ]
      fwrite(x = new_df, file = new_file)

      print(chunk)
    }
  } else if (has_warning) {
    bad_out$warnings[i] <- safe_df$result$warnings
  } else if (has_error){
    bad_out$errors[i] <- safe_df$error
  }

  t_end2 <- Sys.time()
  t_total2 <- t_end2- t_init
  print(t_total2)
  print(glue("\n\n Finished iteration { i }/{ max(seq) }: { cat_string } \n\n"))
}

# source("~/Documents/r_wd/adtabler/scripts/dyn_ref_digital_big.R")



