devtools::install_github("f-dallolio/adtabler")

library(tidyverse)
library(adtabler)
library(vctrs)
library(rlang)

file <- adtabler::data_write_to_db$file

str_split(file[[1]], "/")[[1]] |>
  slice_vec(i = -3:-1) |>
  rename_adintel() |>
  str_remove_all("_tsv")

  max(abs(i)) <= vec_size(x) & min(abs(i)) > 0
