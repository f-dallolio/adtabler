library(data.table)
library(tidyverse)
library(tsibble)
library(rlang)
library(glue)
library(adtabler)
library(dm)
library(DBI)
library(RPostgres)

.con <- dbConnect(
  Postgres(),
  dbname = 'new_test',
  host = '10.147.18.200',
  port = 5432,
  user = 'postgres',
  password = '100%Postgres'
)

sql_make_fields <- function( .con, .tbl_name, .col_names, .data_types, .pk = NULL ) {
  .x = tibble(
    .col_names,
    .data_types
  )
  fields_out <- glue_data(
    .x = .x,
    '    { .col_names } {tolower( .data_types) }', #.con = .con
  )
  has_pk <- not_null(.pk)
  if( has_pk ) {
    fields_out <- fields_out |>
      c(
        glue('   CONSTRAINT { .tbl_name }_pkey PRIMARY KEY ({ .pk })'#, .con = .con,
              )
      )
  }
  fields_out |>
    glue_sql_collapse(sep = ', \n')
}

x <- tbl_info_tot |>
  select(file_name_std, data_info) |>
  filter(file_name_std == "national_tv") |>
  unnest(everything())

fields <- sql_make_fields(.con = .con, .col_names = x$col_names_std, .data_types = x$sql_datatype)

.pk <- keys$pk |>
  select(-autroincrement) |>
  mutate(parent_key_cols = parent_key_cols |>
           map_vec(~ as.character(.x[[1]]))) |>
  summarise(pk = list(parent_key_cols),
            .by = parent_table) |>
  mutate(pk = pk |> set_names(parent_table)) |>
  pull(pk) |>
  pluck("national_tv") |>
  glue_sql_collapse(", ")

.tbl_name <- "national_tv"



fn <-

.tbl_name <- "national_tv"
.col_names = x$col_names_std
.data_types = x$sql_datatype
.year = 2010
.date_from = "2010-01-01"
.date_to = "2011-01-01"


.statement = fn(.con = .con,
   .tbl_name = .tbl_name,
   .col_names = .col_names,
   .data_types = .data_types,
   .pk = .pk,
   .year = .year,
   .date_from = .date_from,
   .date_to = .date_to)

.statement$create_table |> SQL()
.statement$create_partition |> SQL()

# dbRemoveTable(conn = .con, name = .tbl_name)
RPostgres::dbSendQuery(conn = .con, statement = .statement$create_table)
RPostgres::dbSendQuery(conn = .con, statement = .statement$create_partition)
