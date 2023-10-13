library(data.table)
library(dtplyr)
library(tidyverse)
library(rlang)
library(glue)
# devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(bit64)


lat1_to_utf8 <- function(x){
  if(is.character(x)){
    Encoding(x) <- "latin1"
    out <- iconv(x, "latin1", "UTF-8",sub='*')
    return(out)
  }
  x
}

adintel_dir <- "/mnt/sata_data_1/adintel/"
new_adintel_dir <- "/mnt/sata_data_1/new_adintel/"

ue_df <-  list.files(adintel_dir, full.names = T, recursive = T) |>
  as_tibble_col("input_file") |>
  filter(input_file |> str_detect("Master_File",negate = T),
         input_file |>  str_detect("Universe")) |>
  mutate(
    file_type = str_split_i(input_file, "/" , -2) |>
      rename_adintel(),
    file_type2 = str_split_i(input_file, "/" , -1) |>
      str_remove_all(".tsv") |>
      rename_adintel(),
    file_year = str_split_i(input_file, "/" , -3) |> as.numeric(),
    new_dir = str_c(new_adintel_dir, file_type, file_type2, sep = "/") ,
    new_file = str_c(new_dir, "/", file_type2, "__", file_year, ".csv")
  )
ue_df
new_dirs <- unique(ue_df$new_dir)
for (i in seq_along(new_dirs)){
  if(!dir.exists(new_dirs[[i]]))  dir.create(new_dirs[[i]], recursive = TRUE)}

i=1
for(i in seq_along(ue_df$input_file)){
  input_file <- ue_df$input_file[i]
  new_file <- ue_df$new_file[i]
  file_name <- new_file |> str_split_i("/", -1)

  new_df_temp <- read_tsv(input_file)
  new_df <- new_df_temp |> mutate(across(where(is.character), lat1_to_utf8))

  fwrite(new_df, new_file)
  print(str_c(i, file_name, sep = " - "))

}
