library(tidyverse)
library(adtabler)

str_split_comma <- function(x) str_split(x, ",")
unnest_all <- partial(unnest, cols = everything())

refs_uk <- adtabler::data_info_list$unique_key |>
  filter(file_type_std == "references") |>
  select(file_name_std, col_unique_key) |>
  distinct() |>
  mutate(col_unique_key = col_unique_key |>
           str_split(",") |>
           map(rename_adintel)) |>
  rename(references_uk = file_name_std)
refs_uk

occs_uk <- adtabler::data_info_list$unique_key |>
  filter(file_type_std == "occurrences") |>
  select(file_name_std, col_unique_key) |>
  distinct() |>
  mutate(col_unique_key = col_unique_key |>
           str_split(",") |>
           map(rename_adintel)) |>
  rename(occurrences_uk = file_name_std)
occs_uk |>
  mutate(x = map(col_unique_key, str_flatten_comma) |> list_c(),
         xx = x |> map_vec(~ grep(",", .x)))

occs_data <- adtabler::data_info_list$file_info |>
  filter(file_type_std == "occurrences") |>
  select(file_name_std, col_name_data) |>
  distinct() |>
  mutate(col_name_data = col_name_data |>
           str_split(",") |>
           map(rename_adintel)) |>
  rename(occurrences = file_name_std)

fk_table <- expand_grid(occs_data, refs_uk) |>
  mutate(nomatch = map2(col_unique_key, col_name_data,
                        ~ setdiff(.x, .y) |>
                          length()) |>
           unlist()) |>
  filter(nomatch == 0) |>
  select(-col_name_data, - nomatch) |>
  unnest_all() |>
  rename(fk = col_unique_key)


fread_safe <- possibly(data.table::fread, NA)
dir.create("~/Dropbox/NielsenData/refs_test/")
ref_test <- adtabler::data_info_list$file_info |>
  filter(file_type_std == "references",
         year == 2010 | is.na(year)) |>
  select(file_name_std, full_file_name) |>
  mutate(df = full_file_name |> map(~ fread(file = .x,
                                            nrows = 10000,
                                            nThread = parallel::detectCores() - 1,
                                            showProgress = F) |>
                                      as_tibble(),
                                    .progress = TRUE) |>
           set_names(file_name_std)) |>
  mutate(file_out = glue::glue("~/Dropbox/NielsenData/refs_test/{names(df)}.csv"))
occs_test$file_out
walk2(occs_test$df, occs_test$file_out, ~ write_csv(.x, .y))

dir.create("~/Dropbox/NielsenData/occs_test/")
occs_test <- adtabler::data_info_list$file_info |>
  filter(file_type_std == "occurrences",
         year == 2010 | is.na(year)) |>
  select(file_name_std, full_file_name) |>
  slice(-1) |>
  mutate(df = full_file_name |> map(~ fread(file = .x,
                                            nrows = 10000,
                                            nThread = parallel::detectCores() - 1,
                                            showProgress = F) |>
                                      as_tibble(),
                                    .progress = TRUE) |>
           set_names(file_name_std)) |>
  mutate(file_out = glue::glue("~/Dropbox/NielsenData/occs_test/{names(df)}.csv"))
occs_test$file_out
walk2(occs_test$df, occs_test$file_out, ~ write_csv(.x, .y))
