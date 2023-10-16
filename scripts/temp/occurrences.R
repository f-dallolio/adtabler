library(tidyverse)
devtools::install_github('f-dallolio/fdutils')
1
library(fdutils)
devtools::install_github('f-dallolio/adtabler')
1
library(adtabler)

to_c <- function(...){
  if(is.character(...)){
    out <- str_remove_all(..., " ") |>
      str_split(",") |>
      unlist()
    return(out)
  }
  out <- rlang::enquos(..., .named = TRUE)
  names(out)
}



adintel_dir <- "/mnt/sata_data_1/new_adintel/"

adintel_files <- list.files(
  path = adintel_dir,
  recursive = T
) |>
  as_tibble_col(
    column_name = "file"
  ) |>
  mutate(
    file_type = file |>
      str_split_i("/", 1)
  ) |>
  nest(.by = file_type)

occurrences_info <- adintel_files |>
  filter(file_type == "occurrences") |>
  pluck("data", 1) |>
  mutate(temp = file,
         file = str_c(adintel_dir, file, sep = "/")) |>
  separate_wider_delim(
    cols = temp,
    delim = "/",
    names = to_c(
      file_type,
      media_category,
      media_geo,
      media_type,
      temp
      )
    ) |>
  mutate(
    media_type_id = temp |>
      str_remove_all("ok__") |>
      str_split_i("__", 1) |>
      str_remove_all("id") |>
      as.integer(),
    .keep = "unused"
  )
occurrences_info

adintel_info |>
  filter(file_type == 'occurrences') |>
  pull(media_info) |>
  map(~ .x |> pluck(1)) |>
  map(~ .x |> as_tibble()) |>
  list_rbind()


|>
  map(as_tibble)
