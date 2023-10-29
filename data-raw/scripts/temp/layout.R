library(tidyverse)
library(adtabler)

data("layout_list")
data("adintel_files")
#
# types <- tibble(
#   file_type_layout = c("impressions","market_break_impressions_and_ue","occurrence","reference","universe_estimates"),
#   file_type_std = rename_adintel(c("Impressions","Market_Breaks","Occurrences","References","Universe_Estimates"))
# )
#
# layout_list0 <- layout_list
# layout_list <- layout_list0 |>
#   unnest(everything()) |>
#   unnest(everything()) |>
#   filter(!is.na(extract_name) |
#            !is.na(file_type) |
#            !is.na(extract_column)) |>
#   rename(file_type_layout = file_type,
#          file_name_layout = extract_name,
#          col_names_layout = extract_column) |>
#   mutate(file_name_layout = if_else(file_name_layout == "NationalTV","NetworkTV", file_name_layout),
#          file_name_std = rename_adintel(file_name_layout),
#          col_names_std = rename_adintel(col_names_layout)) |>
#   inner_join(types) |>
#   select(-file_type_layout) |>
#   relocate(file_type_std, .before = file_name_std) |>
#   mutate(row = row_number(),
#          .before = 1)

layout_list <- layout_list |>
  # select(row, contains("_std")) |>

adintel_files |>
  unnest(everything()) |>
  filter(str_detect(file_name, "lock", negate = T)) |>
  select(contains("_std"), col_pos) |>
  distinct() |>
  filter(str_detect(file_name_std, "market_break")) |> print(n=500)
  full_join(layout_lookup)|> view()


# save(layout_list0, file = "~/Documents/r_wd/adtabler/data/layout_list.rda")
