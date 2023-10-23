library(tidyverse)
library(data.table)
library(glue)
devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(tsibble)

latest_year <- 2021
adintel_dir <- "/mnt/sata_data_1/adintel/"

adintel_files <- list.files(adintel_dir,
                            full.names = TRUE,
                            recursive = TRUE) |>
  as_tibble_col(to_c(full_name))

master_files <- adintel_files |>
  filter (str_detect(full_name, regex('master_files', ignore_case = T)),
          str_detect(full_name, regex('latest', ignore_case = T)),
          str_detect(full_name, as.character(latest_year)) )

dyn_files <- adintel_files |>
  filter (str_detect(full_name, regex('master_files', ignore_case = T), negate = TRUE) ) |>
  mutate(year = full_name |>
           str_split_i("/", -3) |> as.integer(),
         file_type = full_name |>
           str_split_i("/", -2) |>
           tolower(),
         file_name = full_name |>
           str_split_i("/", -1)
         )

master_files |> print(n=200)

occur_files <- str_subset(string = adintel_files, regex('occurrences', ignore_case = T))
