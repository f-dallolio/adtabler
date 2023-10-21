library(data.table)
library(dtplyr)
library(tidyverse)
library(rlang)
library(glue)
devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(bit64)

adintel_dir <- "/mnt/sata_data_1/adintel/"
new_adintel_ref_dir <- "/mnt/sata_data_1/new_adintel/references/static"
dir.create(new_adintel_ref_dir)

lat1_to_utf8 <- function(x){
  if(is.character(x)){
    Encoding(x) <- "latin1"
    out <- iconv(x, "latin1", "UTF-8",sub='*')
    return(out)
  }
  x
}


stat_ref_df <-  list.files(adintel_dir, full.names = T, recursive = T) |>
  as_tibble_col("input_file") |>
  filter(input_file |> str_detect("Master_File"),
         input_file |> str_detect("Latest"),
         input_file |> str_detect("2021")) |>
  mutate(size = file.size(input_file),
         size = size |> round(3),
         file_type2 = str_split_i(input_file, "/", -1) |>
           str_remove_all(".tsv") |>
           rename_adintel(),
         new_file = str_c(new_adintel_ref_dir, "/", file_type2, ".csv"))

stat_ref_df

i=1
i_seq <- seq_along(stat_ref_df$input_file)
utf8_ok <- rep(NA, length(i_seq))
for(i in i_seq){
  input_file <- stat_ref_df$input_file[i]
  new_file <- stat_ref_df$new_file[i]
  file_name <- new_file |> str_split_i("/", -1)

  new_df <- read_tsv(input_file)
  utf8_ok[[i]] <- check_utf8(new_df)

  write_csv(new_df, new_file)
  print(str_c(i, file_name, sep = " - "))

}
