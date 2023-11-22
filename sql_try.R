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

load("~/Documents/r_wd/adtabler/data/tbl_info_tot_2.rda")
tbl_info_tot_2

fn_fmls_names(sql_make_fields)
fn_fmls_names(sql_create_tbl)

x_tbl <- tbl_info_tot_2 |>
  filter(
    file_type_std %in% c('occurrences')
  ) |>
  slice(1) |>
  transmute(
    .tbl_name = file_name_std,
    .col_names = col_names_std,
    .data_types = sql_datatype,
    .pk = pk.x
  ) |>
  # unnest(everything())
  # map(~.x |> simplify()) |>
  pmap_vec(sql_create_tbl)
x_tbl

x_part <- tbl_info_tot_2 |>
  filter(
    file_type_std %in% c('occurrences')
  ) |>
  slice(1) |>
  transmute(
    .tbl_name = file_name_std,
    .part_name = year,
    .from = date_from,
    .to = date_to
  ) |>
  unnest(everything()) |>
  mutate(
    .part_name = glue('{ .tbl_name }__y{ .part_name }') |>
      as.character()
  ) |>
  summarise(
    across(.part_name : .to, list),
    .by = .tbl_name
  ) |>
  pmap(sql_create_part)
x_part

x_part0 <- tbl_info_tot_2 |>
  filter(
    file_type_std %in% c('occurrences')
  ) |>
  slice(1) |>
  transmute(
    .tbl_name = file_name_std,
    .part_col = "ad_date"
  )
x_part0 |>
  select(-.tbl_name) |>
  pmap_vec(sql_part_by_range)


sql_part_by_range <- function(.part_col) {
  glue("PARTITION BY RANGE ( .part_col )") |> SQL()
}
sql_create_part <- function(.tbl_name, .part_name, .from = NULL, .to = NULL, .values = NULL) {
  by_range <- not_null(c(.from, .to)) & is.null(.values)
  by_list <- not_null(.values) & is.null(c(.from, .to))
  if( by_range ) {
    out <- glue(
      "\n
      CREATE TABLE { .part_name } PARTITION OF { .tbl_name }
        FOR VALUES FROM ('{ .from }') TO ('{ .to }')"
    ) |> SQL()
  }
  if( by_list ) {
    out <- glue(
      "\n
      CREATE TABLE { .part_name } PARTITION OF { .tbl_name }
        FOR VALUES IN ('{ .values }')"
    ) |> SQL()
  }
  return(out)
}






.tbl_name = x$.tbl_name
.col_names = x$.col_names
.data_types = x$.data_types
.pk = x$.pk

sql_make_fields(.tbl_name, .col_names, .data_types)

|> pmap(sql_make_fields)


x <- tbl_info_tot |>

  slice(9) |>


  select(file_type_std : data_info) |>
  # unnest(file_info) |>
  # filter(
  #   is.na(year)
  # ) |>



  left_join(
    unique_key |>
      mutate(file_name_std = case_when(
        file_name_std == "spot_tv" ~ 'local_tv',
        file_name_std == "network_tv" ~ 'national_tv',
        .default = file_name_std)
      ) |>
      summarise(
        across(media_type_id : col_unique_key, list),
        .by = !media_type_id : col_unique_key,
      )
  )

x2 <- x |>
  bind_cols(
    x$data_info[[1]] |>
      map(list) |>
      as_tibble()
  ) |>
  transmute(
    .tbl_name = file_name_std,
    .col_names = col_names_std,
    .data_types = sql_datatype,
    .pk = col_unique_key
  ) |>
  map(~ .x)
x2$.pk <- x2$.pk[[1]][1]
x2
statement <- x2 |>
  pmap_vec(sql_create_tbl)
statement2 <- glue("{statement} PARTITION BY RANGE (ad_date)") |> SQL()

dbSendStatement(conn = con, statement = statement2)

make_part <- `glue("
CREATE TABLE magazine_y2010 PARTITION OF magazine
    FOR VALUES FROM ('2010-01-01') TO ('2011-01-01')
"`) |> SQL()
dbSendStatement(conn = con, statement = make_part)

st <- SQL('CREATE INDEX magazine__ad_date ON magazine (ad_date)')
dbSendStatement(conn = con, statement = st)

