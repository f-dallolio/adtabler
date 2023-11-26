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
  tbl_definition <- decompose_file(file, out_tibble = F)[-1]
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
      adintel_file = (file)[[1]][1],
      col_names = (col_names_std),
      col_classes = (col_classes),
      sql_datatypes = (sql_datatype),
      pk = pk,
      fk = fk,
      idx = index
  ) |> 
  map(simplify)
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


