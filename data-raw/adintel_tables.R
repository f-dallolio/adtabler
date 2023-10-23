library(tidyverse)
library(data.table)
# devtools::install_github("f-dallolio/adtabler")
library(adtabler)

refs <- references_tables %>%
  mutate(across(c(precision, scale), as.integer)) %>%
  nest(
    var_name_manual = var_name_manual,
    var_type_manual = var_type_manual,
    precision = precision,
    scale = scale,
    .by = c(file_type, file_name)
  ) %>%
  mutate(
    across(c(
      var_name_manual,
      var_type_manual,
      precision,
      scale
    ), ~ map(.x, as.list))
  )


occs <- occurrences_tables %>%
  mutate(across(c(precision, scale), as.integer)) %>%
  nest(
    var_name_manual = var_name_manual,
    var_type_manual = var_type_manual,
    precision = precision,
    scale = scale,
    .by = c(file_type, file_name)
  ) %>%
  mutate(
    across(c(
      var_name_manual,
      var_type_manual,
      precision,
      scale
    ), ~ map(.x, as.list))
  )



adintel_tables <- get_adintel_tables(adintel_dir = "/media/filippo/One Touch/nielsen_data/adintel") %>%
  rename(path = full_path) %>%
  nest(
    file_year = file_year,
    path = path,
    .by = c(
      file_type,
      file_name,
      is_dynamic
    )
  ) %>%
  left_join(
    bind_rows(refs, occs)
  ) %>%
  mutate(
    file_year = map(file_year, pull),
    path = map(path, pull),
    var_attributes = map(
      .x = seq_along(var_name_manual),
      .f = ~ pairlist(
        file_type = file_type[[.x]],
        file_name = file_name[[.x]],
        is_dynamic = is_dynamic[[.x]],
        var_name_manual = var_name_manual[[.x]][[1]],
        var_type_manual = var_type_manual[[.x]][[1]],
        precision = precision[[.x]][[1]],
        scale = scale[[.x]][[1]]
      )
    )
  ) %>%
  select(-var_name_manual:-scale)

usethis::use_data(adintel_tables, overwrite = TRUE)
