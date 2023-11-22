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

make_part <- glue("
CREATE TABLE magazine_y2010 PARTITION OF magazine
    FOR VALUES FROM ('2010-01-01') TO ('2011-01-01')
") |> SQL()
dbSendStatement(conn = con, statement = make_part)

st <- SQL('CREATE INDEX magazine__ad_date ON magazine (ad_date)')
dbSendStatement(conn = con, statement = st)

