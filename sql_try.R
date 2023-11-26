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

# dbListTables(conn = con) |> walk(~ dbRemoveTable(conn = con, name = .x))

.data <- tbl_info_tot |>
  filter(file_name_std == "cinema") |>
  select(-year : -date_to) |>
  inner_join(
    file_info_start_end |>
      filter(file_name_std == 'cinema') |>
      select(file_name_std,
             year : date_end) |>
      rename(
        date_from = date_start,
        date_to = date_end
      ) |>
      summarise(across(everything(), list),
                .by = file_name_std)
  ) |>
  unnest(year : date_to) |>
  slice(1) |>
  transmute(

    file = file,
    col_classes,
    .tbl_name = file_name_std,
    .col_names = col_names_std,
    .data_types = sql_datatype,
    .pk = pk,
    .part_col = 'media_type_id',
    .part_name = year |> map(~ paste(.tbl_name, .x , sep = "__")),
    # .from = date_from,
    # .to = map_vec(date_to, ~ as.character(as.Date(.x) + 1)),
    .values = media_type_id
  )
.data


dbRemoveTable(con, 'cinema')
st1 <- .data |>
  select(
    any_of(
      fn_fmls_names(sql_build_tbl)
    )
  ) |>
  pmap(sql_build_tbl)
st1
dbSendQuery(conn = con, statement = st1[[1]])


st2 <- .data |>
  select(
    any_of(
      fn_fmls_names(sql_create_part)
    )
  ) |>
  unnest(everything()) |>
  pmap(sql_create_part)
st2
st2 |> walk(~ dbSendQuery(conn = con, statement = .x[[1]]))

x <- fread(file = .data$file[[1]][1], col.names = .data$.col_names[[1]], colClasses = unname(.data$col_classes[[1]]), na.strings = "") |>
  rename_with(rename_adintel) |>
  mutate(ad_date = as.Date(ad_date))

dbWriteTable(
  conn = con,
  'cinema',
  x,
  append = T
)
