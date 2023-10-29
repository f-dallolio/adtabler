x <- list.files("~/Documents/r_wd/adtabler/data/", full.names = T) |>
  stringr::str_subset("row_numbers_") |>
  sort()

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
get_row_numbers <- out |>
  stringr::str_split(" ", simplify = TRUE) |>
  `colnames<-`(c("n_rows", "full_file_name")) |>
  tibble::as_tibble() |>
  mutate(n_lines = as_numeric2(n_rows),
         n_rows = n_rows_no_header - 1) |>
  select(-n_rows)

usethis::use_data(get_row_numbers, overwrite = TRUE)
