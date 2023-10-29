library(tidyverse)
library(adtabler)

data("layout_list")
data("adintel_files")

layout_list

x <- adintel_files |>
  unnest(everything()) |>
  filter(str_detect(file_name, "lock", negate = T)) |>
  nest(.by = c(contains("_std"), col_pos)) |>
  distinct() |>
  full_join(layout_list |>
              mutate(file_type_std = if_else(file_name_std == "ue_market_breaks", "universe_estimates", file_type_std))) |>
  arrange(row) |>
  unnest(everything())

x
