x <- list.files("~/Documents/r_wd/adtabler/data/", full.names = T) |>
  stringr::str_subset("row_numbers_") |>
  sort()

i=1
for(i in seq_along(x)){
  load(x[i])
  if(i == 1) {
    out <- lst
  } else {
    out <- c(out, lst)
  }
  print(i)
}

library(tidyverse)
library(adtabler)
out |>
  stringr::str_split(" ", simplify = TRUE) |>
  `colnames<-`(c("n_rows", "full_file_name")) |>
  tibble::as_tibble() |>
  mutate(n_rows_no_header = as_numeric2(n_rows),
         n_rows_w_header = n_rows_no_header - 1) |>
  select(-n_rows)
  view()

load("~/Documents/r_wd/adtabler/data/row_numbers_2017.rda")
