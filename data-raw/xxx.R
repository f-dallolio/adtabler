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

con <- dbConnect(RPostgres::Postgres(),
                 dbname = 'new_test',
                 host = '10.147.18.200',
                 port = 5432,
                 user = 'postgres',
                 password = '100%Postgres')

new_adintel_file <- function(
  adintel_file = character(),
  col_names = character(),
  col_classes = character(),
  sql_datatypes = character(),
  pk = character(),
  fk = list(),
  idx = character(),
  ...
) {
  tbl_definition <- decompose_file(adintel_file, out_tibble = F)[-1]
  tbl_columns = rlang::dots_list(
    col_names,
    col_classes,
    sql_datatypes,
    .named = TRUE
  )
  tbl_constraints = rlang::dots_list(
    pk,
    fk,
    idx =str_split_comma(idx),
    .named = TRUE
  )
  structure(
    .Data = adintel_file,
    class = 'adintel_file',
    tbl_definition = tbl_definition,
    tbl_columns = tbl_columns,
    tbl_constraints = tbl_constraints
  )
}
print.adintel_file <- function(x, ...) {
  print(unclass(x), ...)
  invisible(x)
}
is_adintel_file <- function(x, ...) {
  inherits(x, 'adintel_file')
}


x <- tbl_info_tot |>
  filter(
    file_name_std== 'magazine'
  ) |>
  transmute(
      file = file |> pluck(1, 1),
      tbl_name = file |> decompose_file(out_tibble = FALSE) |> pluck('tbl_name', 1),
      col_names = col_names_std,
      col_classes = col_classes,
      col_types = sql_datatype,
      pk_cols = pk,
      child_fk_cols = fk |> pluck(1, "child_fk_cols") |> list(),
      parent_tbl = fk |> pluck(1, "parent_table") |> list(),
      parent_fk_cols = fk |> pluck(1, "parent_table") |> list(),
      idx_cols = list(str_split_comma(index))
  ) |>
  map(simplify)

x
x$adintel_file <- x$adintel_file[[1]]
x$fk <- as.list(x$fk[[1]])
file <- x |> with(
  adintel_file |>
    new_adintel_file(
      col_names,
      col_classes,
      sql_datatypes,
      pk,
      fk,
      idx
    )
)
attributes(file)
is_adintel_file(file)


new_adintel_tbl <- function(
    .dt,
    attrib){
  # = list(
    # tbl_type = character(),
    # is_dynamic = logical(),
    # tbl_name = character(),
    # col_names = character(),
    # col_classes = character(),
    # sql_datatypes = character(),
    # partiitoned = list(),
    # pk = character(),
    # fk = list(),
    # idx = character())
    # # ...

  # if( is_empty(partitioned) ) {
  #   partitoned <- FALSE
  # } else {
  #   partitioned <- TRUE
  # }

  # list(
  #   class = c('adintel_tbl', class(.dt)),
  #   tbl_type = tbl_type,
  #   is_dynamic = is_dynamic,
  #   tbl_name = tbl_name,
  #   col_names = col_names,
  #   col_classes = col_classes,
  #   sql_datatypes = sql_datatypes,
  #   partitioned = partitioned,
  #   pk = pk,
  #   fk = fk,
  #   idx = idx
  # ) |>
  #
 c(list(class = c('adintel_tbl', class(.dt))), attrib) |>   iwalk(~ setattr(x = .dt, name = .y, value = .x))
  return(.dt)

  # structure(
  #   .Data = .dt#,
  #   # class = class(.dt)),
  # # #   tbl_type = tbl_type,
  # # #   is_dynamic = is_dynamic,
  # # #   tbl_name = tbl_name,
  # # #   col_names = col_names,
  # # #   col_classes = col_classes,
  # # #   sql_datatypes = sql_datatypes,
  # # #   partitioned = partitioned,
  # # #   pk = pk,
  # # #   fk = fk,
  # ##   idx = idx
  #   )
}

print.adintel_tbl <- function(x, ...) {
  print(unclass(x), ...)
  invisible(x)
}
is_adintel_tbl <- function(x, ...) {
  inherits(x, 'adintel_file')
}

x <- tbl_info_tot

list_names <- x$file_type_std |>
  paste(x$file_name_std, sep = "__")

i=1
for_seq <- seq_along(list_names)
list_attributes <- map(for_seq, ~ list())# |> set_names(list_names)
for(i in for_seq) {
  list_attributes[[i]] <-
  tbl_info_tot |>
  slice(i) |>
  transmute(
    tbl_type = file_type_std,
    is_dynamic = dynamic_flag,
    tbl_name = file_name_std,
    col_names = col_names_std,
    col_classes = col_classes |>
      map2(col_names, ~ .x |> set_names(.y)),
    sql_datatypes = sql_datatype |>
      map2(col_names, ~ .x |> set_names(.y)),
    partitioned = FALSE,
    pk = pk |>
      map(str_split_comma),
    fk = fk,
    idx = index |>
      map(str_split_comma)
  ) |>
  map(simplify)
  list_attributes[[i]]$fk <-list_attributes[[i]]$fk[[1]] |> as.list()
}
.dt |> unclass()
list_attributes
x <- decompose_file(file = "/mnt/sata_data_1/adintel/ADINTEL_DATA_2010/nielsen_extracts/AdIntel/2010/Occurrences/Magazine.tsv", out_tibble = F)[c(2, 4)]
xxx <- list_attributes |> map(~ tibble(tbl_type = .x$tbl_type, tbl_name = .x$tbl_name, attributes = list(.x))) |>
  list_rbind()

df <- fread(file = "/mnt/sata_data_1/adintel/ADINTEL_DATA_2010/nielsen_extracts/AdIntel/2010/Occurrences/Magazine.tsv")
do.call(what = 'new_adintel_tbl', c(df, xxx$attributes[[1]]))


xxx$attributes[[1]] |>
  iwalk(~ setattr(x = .dt, name = .y, value = .x))
.dt |> str()

as_name(map )


str(df)
is_adintel_tbl(df)
11function(file, list_attributes){
  decompose_file(file, out_tibble = FALSE)
  map(list_attributes[[1]]$tbl_type ==)
}
purrr:::pluck_raw(list_attributes, list(tbl_type = 'occurrences', tbl_name = 'magazine'))

do.call(what = '', args = list(c(list(.dt = .dt), ))


 pluck(list_attributes, "occurrences__magazine") |> iwalk(~ setattr(.dt, name = .y, value = .x))
setattr(.dt, 'class', c('adintel_tbl', class(.dt)))
str(.dt)



pluck2 <- function(x, ...) {
  `[`(x, ...)
}
pluck2('x', c('a', 'b'), 'c')
pluck2(list_attributes[[1]], c('tbl_name', 'tbl_type')) |> match.call() |> call_args_names()
f <- function(...)as.list(match.call(expand.dots = T)[-1])
f(c('a',"b"))

list_attributes2 <- list_attributes |> map(~ tibble(tbl_type = .x$tbl_type, tbl_name = .x$tbl_name, attributes = list(.x))) |> list_rbind()

file <- '/mnt/sata_data_1/adintel/ADINTEL_DATA_2010/nielsen_extracts/AdIntel/2010/Occurrences/Magazine.tsv'
attrib <- decompose_file(file, out_tibble = F) |> pluck2(c('tbl_type', 'tbl_name')) |> as_tibble_row() |> inner_join(list_attributes2) |> pluck('attributes', 1)
df <- fread_adintel(file = file, col_names_std = attrib$col_names, col_classes = attrib$col_classes)
attrib |> iwalk(~ df |> setattr(name = .y, value = .x))
df |> is_adintel_tbl()
str(df)
