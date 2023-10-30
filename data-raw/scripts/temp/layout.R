library(tidyverse)
library(adtabler)

data("layout_list")
# data("adintel_files")

layout_list
adintel_files

adintel_files |>
  unnest(everything()) |>
  nest(.by = c(contains("_std"), col_pos)) |>
  distinct() |>
  full_join(layout_list) |>
  arrange(row) |>
  unnest(everything())

x
