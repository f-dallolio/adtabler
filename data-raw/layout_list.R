library(tidyverse)
library(readxl)

fn <- function(x){
  x <- x |>
    select(-folder) |>
    mutate(id = cumsum(as.numeric(!is.na(extract_name))))
  y <- x |>
    select(-extract_column) |>
    filter(!is.na(extract_name)) |>
    left_join(x |> select(id, extract_column))
  y
}

file <- "~/Dropbox/NielsenData/adintel_file_layouts.xlsx"
sheets <- strsplit(
  "Occurrence, Reference, Impressions, Universe Estimates, Market Break Impressions and UE",
  ", ") |>
  unlist()
var_names <- sheets |> adtabler::rename_adintel()

layout_list <- map2(sheets,var_names,
                    ~read_excel(file, sheet = .x) |>
                      rename_with(~ tolower(str_replace_all(.x, " ", "_"))) |>
                      filter(str_detect(folder, "FOLDER", negate = T)  | is.na(folder),
                             !is.na(extract_column)) |>
                      fn() |>
                      select(-id) |>
                      nest(.by = extract_name) |>
                      mutate(file_type = .y,
                             .before = 1)) |>
  list_rbind() |>
  nest(.by = file_type)

usethis::use_data(layout_list, overwrite = TRUE)
