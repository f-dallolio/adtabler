devtools::install_github("f-dallolio/adtabler")
1
library(data.table)
library(tidyverse)
library(rlang)
library(glue)
library(dm)
library(adtabler)
library(DBI)
library(RPostgres)

install.packages('S7')
library(S7)


adintel_tbl_id <- new_class(
  name = 'adintel_tbl_id',
  properties = list(
    adintel_year,
    tbl_type,
    tbl_name,

  )
)
adintel_fields <- new_class(
  name = 'adintel_cols',
  properties = list(
    col_names = class_character,
    col_types = class_character
  )
)
adintel_partition <- new_class(
  name = 'adintel_partition',
  properties = list(
    adintel_name = class_character,
    part_by = class_character,
    part_name = class_character,
    values =  new_property(range | class_atomic)
  )
)
adintel_idx <- new_class(
  name = 'adintel_idx',
  properties = list(
    adintel_name = class_character,
    idx_cols = class_character
  )
)
adintel_pk <- new_class(
  name = 'adintel_pk',
  properties = list(
    pk_cols = class_character
  )
)
adintel_fk <- new_class(
  name = 'adintel_fk',
  properties = list(
    child_fk_cols = class_character,
    parent_tbl = class_character,
    parent_fk_cols = class_character
  )
)
adintel_keys <- adintel_pk | adintel_fk
data.table <- new_S3_class(
  class = 'data.table',
  constructor = function(.data = class_data.frame){
    data.table::setDT(.data)
  }
)
x <- data.table(mtcars)
S3Class(x)
attributes
S7_object()

adintel_tbl_attr <- new_class(
  name = 'adintel_tbl',
  properties = list(
    tbl_id = adintel_id,
    fields = adintel_fields,
    partition = adintel_partition,
    idx = adintel_idx,
    keys = adintel_keys
  )
)
my_tbladintel_tbl()




occ_def <- read_csv("data-raw/occ_def.csv")
data_tbl_names <- tbl_info_tot |>
  summarise(media_type_id = list(str_split_comma(media_type_id) |> as.integer()),
            .by = file_name_std) |>
  unnest(media_type_id) |>
  left_join(
    occ_def |>
      transmute(media_type_id, file_name_std, tbl_name = .part_name)
  ) |>
  summarise(across(media_type_id : tbl_name, list), .by =file_name_std ) |>
  rename(data_type = file_name_std) |>
  inner_join(
    tbl_info_tot |>
      transmute(
        data_type = file_name_std,
        col_names = col_names_std,
        col_classes = col_classes,
        col_types = sql_datatype
      )
  )

fn <- function(cmd, col_names, col_classes, ... ) {
  fread(cmd = cmd, col.names = col_names, colClasses = col_classes, na.strings = "")
}


library(furrr)
plan(multisession, workers = 30)


x <- tbl_info_tot |>
  pull(file) |>
  list_c() |>
  map(decompose_file) |>
  list_rbind() |>

  rename(
    data_category = tbl_type,
    data_type = tbl_name
  ) |>

  filter(data_category == "occurrences") |>

  filter(data_type == "national_tv") |>

  filter(adintel_year == 2010) |>

  inner_join(data_tbl_names) |>

  unnest(c(media_type_id, tbl_name)) |>

  mutate(cmd = seq_along(file) |> map_chr(~ file[[.x]] |> make_grep_cmd(media_type_id = media_type_id[[.x]])))


x |> select(cmd, col_names, col_classes) |>
  future_pmap(fn, .progress = T)


plan(sequential)




