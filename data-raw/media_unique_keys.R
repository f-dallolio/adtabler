library(readr)
library(tidyverse)
devtools::install_github("f-dallolio/adtabler")
1
library(adtabler)

media_unique_keys <- read_csv("data-raw/media_unique_keys.csv")
names(media_unique_keys) <- adtabler::to_c(media_type_id, media_type, file_name, nielsen_unique_key)
media_unique_keys <- media_unique_keys |>
  mutate(
    file_name = file_name |>
      str_replace_all("National", "Network"),
    nielsen_unique_key = nielsen_unique_key |>
      str_replace_all(", ", "___"),
    unique_key = nielsen_unique_key |>
      str_split("___") |>
      map(rename_adintel) |>
      map(~ str_c(.x, collapse = "___")) |>
      list_c()
  )

usethis::use_data(media_unique_keys, overwrite = TRUE)
