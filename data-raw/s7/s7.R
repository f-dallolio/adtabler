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






make_ymd <- function(year, month, day){
  stopifnot(is.numeric(c(year,month,day)))
  as.Date(str_flatten(c(year, numpad2(month), numpad2(day)), collapse = "-"))
}

code_idx <- function(tbl_name, idx_cols){
  idx_name_cols <- glue::glue_collapse(idx_cols, sep = "_")
  idx_cols <- glue::glue_collapse(idx_cols, sep = ",")
  idx_name <- glue::glue('idx__{ tbl_name }__{ idx_name_cols }')
  glue::glue('\n
CREATE INDEX IF NOT EXISTS { idx_name } ON { tbl_name }({ idx_cols })
') |> DBI::SQL()
}

pluck2 <- function(x, ...) {
  `[`(x, ...)
}

# range ----
range <- new_class(
  name = "range",
  properties = list(
    from = class_numeric,
    to = class_numeric
  ),
  validator = function(self) {
    if( not_empty(self@to) & not_empty(self@from)){
      if ( self@to <= self@from ) "@to must be greater than @from"
    }
  }
)


# tbl_partition ----
tbl_partition <- new_class(
  name = 'tbl_partition',
  properties = list(
    tbl_name = class_character,
    part_by = class_character,
    part_name = class_character,
    values =  new_property(range | class_atomic)
  )
)
add_tbl_partition <- new_generic('sql_tbl_partition', 'x')
method(add_tbl_partition, tbl_partition) <- function(x){
  if( inherits(x@values, 'range') ){
    partition_by <- glue::glue("PARTITION BY RANGE ({ x@part_by })") |> DBI::SQL()
    create_partition_table <-  glue::glue(
      "\n
      CREATE TABLE { x@part_name } PARTITION OF { x@tbl_name }
          FOR VALUES FROM ({ x@values@from }) TO ({ x@values@to })"
    ) |> DBI::SQL()
  } else {
    partition_by <- glue::glue("PARTITION BY LIST ({ x@part_by })") |> DBI::SQL()
    create_partition_table <-  glue::glue(
      "\n
      CREATE TABLE { x@part_name } PARTITION OF { x@tbl_name }
          FOR VALUES IN ('{ x@values }')"
    ) |> DBI::SQL()
  }
  partition_idx <- code_idx(tbl_name = x@tbl_name, idx_cols = x@part_by)
  list(
    partition_by = partition_by,
    create_partition_table = create_partition_table,
    partition_idx = partition_idx
  )
}
add_tbl_partition(tbl_partition(tbl_name = 'name', part_by = 'col', part_name = 'name__col', values = range(0,1)))


# tbl_fields ----
tbl_fields <- new_class(
  name = 'tbl_cols',
  properties = list(
    tbl_name = class_character,
    col_names = class_character,
    col_types = class_character
  )
)
tbl_fields()
make_tbl_fields <-  new_generic('make_tbl_fields', 'x')
method(make_tbl_fields, tbl_fields) <- function(x) {
  x_data <- list(
    col_names = x@col_names,
    col_types = x@col_types
  )
  glue::glue_data(
    .x = x_data,
    '    {col_names }  { col_types }'
  ) |> glue::glue_collapse(sep = ', \n')
}
make_tbl_fields(tbl_fields(col_names = c('name1', 'name2'), col_types = c('type1', 'type2')))


# tbl_pk ----
tbl_pk <- new_class(
  name = 'tbl_pk',
  properties = list(
    tbl_name = class_character,
    pk_cols = class_character
  )
)
tbl_pk()
sql_add_pk <-  new_generic('sql_add_pk', 'x')
method(sql_add_pk, tbl_pk) <- function(x) {
  tbl_name <- x@tbl_name
  pk_cols <- x@pk_cols
  glue::glue(' \n
ALTER TABLE { tbl_name } ADD PRIMARY KEY ({ pk_cols })
  ') |> DBI::SQL()
}
sql_add_pk(tbl_pk(tbl_name = 't1', pk = 'x1'))

# tbl_fk ----
tbl_fk <- new_class(
  name = 'tbl_fk',
  properties = list(
    tbl_name = class_character,
    child_fk_cols = class_character,
    parent_tbl = class_character,
    parent_fk_cols = class_character
  )
)
tbl_fk()
sql_add_fk <-  new_generic('sql_add_fk', 'x')
method(sql_add_fk, tbl_fk) <- function(x) {
  child_tbl <- x@tbl_name
  child_fk_cols <- glue::glue_collapse(x@child_fk_cols, sep = "_")
  parent_tbl <- x@parent_tbl
  parent_fk_cols <- glue::glue_collapse(x@parent_fk_cols, sep = "_")
  constraint_name <- glue::glue('fk__{ child_tbl }__{ child_fk_cols }__{ parent_tbl }__{ parent_fk_cols }')
  out <- glue::glue('\n
ALTER TABLE { child_tbl }
ADD CONSTRAINT { constraint_name }
FOREIGN KEY ( fk_cols )
REFERENCES { parent_tbl } ({ parent_key_cols })'
  ) |> DBI::SQL()
  out
}
# tbl_idx ----
tbl_idx <- new_class(
  name = 'tbl_idx',
  properties = list(
    tbl_name = class_character,
    idx_cols = class_character
  )
)
sql_add_idx <- new_generic('sql_add_idx', 'x')
method(sql_add_idx, tbl_idx) <- function(x){
  code_idx(tbl_name = x@tbl_name, idx_cols = x@idx_cols)
}
sql_add_idx(x = tbl_idx(tbl_name = 'tbl_name', idx_cols = c('idx_cols', 'idx_cols2')))


# nms <- list( tbl_fields, tbl_partition, tbl_pk, tbl_fk, tbl_idx) |> map(~ prop(.x, 'properties') |> names()) |>
#   set_names("tbl_fields", "tbl_partition", "tbl_pk", "tbl_fk", "tbl_idx")
# x <- tbl_info_tot |>
#   unnest(file) |>
#   nest(.by = file)
#
# .data <- x|>
#   # slice(1) |>
#   # pull(file) |>
#   mutate(file_info = file |> map(~ decompose_file(.x, out_tibble = F)),
#          data = data |>  map(~ .x |>
#                                transmute(
#                                  col_names = col_names_std,
#                                  col_classes = col_classes,
#                                  col_types = sql_datatype,
#                                  pk_cols = pk,
#                                  child_fk_cols = fk |> pluck(1, "child_fk_cols") |> list(),
#                                  parent_tbl = fk |> pluck(1, "parent_table") |> list(),
#                                  parent_fk_cols = fk |> pluck(1, "parent_key_cols") |> list(),
#                                  idx_cols = list(str_split_comma(index))
#                                ) |> as.list() |> map(~ .x[[1]] ) )
#          ) |>
#   transmute(
#     file = file,
#     data_list = map2(file_info, data, c)
#   ) |>
#   mutate(
#     tbl_type = data_list |> map_chr(~.x |> pluck("tbl_type")),
#     tbl_name = data_list |> map_chr(~.x |> pluck("tbl_name")),
#     data_list = data_list |> set_names(paste(tbl_type, tbl_name, sep = "__")),
#     .before = data_list
#   )
#
# .list <- .data$data_list[[1]]
# add_part_prop <- function(.list){
#   if ( not_na(.list$adintel_year) ) {
#     if( not_in('ad_date', .list$col_names) ) {
#       .list$col_names <- c('adintel_year', .list$col_names)
#       .list$col_classes <- c('integer', .list$col_classes)
#       .list$col_types = c('INTEGER', .list$col_types)
#       .list$part_by <- 'adintel_year'
#       .list$part_name <- paste0(.list$tbl_name, '__y', .list$adintel_year)
#       .list$values <- .list$adintel_year
#     } else {
#       .list$part_by <- 'ad_date'
#       .list$part_name <- paste0(.list$tbl_name, '__y', .list$adintel_year)
#       .list$values <- adtabler::tbl_dates_from_to |>
#         filter(
#           file_type_std == .list$tbl_type,
#           file_name_std == .list$tbl_name,
#           adintel_year == .list$adintel_year
#         ) |>
#         transmute(from = as.Date(date_from), to = as.Date(date_to)) |>
#         as.list() |> with(range(from, to))
#     }
#   }
#   .list
# }
# adintel_ref_list <- .data$data_list |> map(add_part_prop)
# str(adintel_ref_list)
# usethis::use_data(adintel_ref_list, overwrite = T)
#
# xxx <- out_list[[1]]
# map(nms, ~ pluck2(xxx, .x))
