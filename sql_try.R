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

pk_id <- vector(mode = 'list', length = length(tbl_info_tot_2$file_type_std))
i=15
for(i in seq_along(tbl_info_tot_2$file_type_std)) {

  pk_id[[i]] <- tbl_info_tot_2 |>
    slice(i) |>
    pluck('pk.x', 1) |>
    is.null() |>
    print()


  # x_tbl[[i]] <- tbl_info_tot_2 |>
  #   # filter(
  #   #   file_type_std %in% c('occurrences')
  #   # ) |>
  #   slice(i) |>
  #   transmute(
  #     .tbl_name = file_name_std,
  #     .col_names = col_names_std,
  #     .data_types = sql_datatype,
  #     .pk = is.null(simplify(pk.x))
  #   )
  # if( x_tbl[[i]]$.pk ) {
  #   x_tbl[[i]]$.pk <- NA
  # } else {
  #
  # }

}
tbl_info_tot_2$pk <- rep(NA, i)
tbl_info_tot_2$pk[!unlist(pk_id)] <- tbl_info_tot_2$pk.x[!unlist(pk_id)] |> simplify()
tbl_info_tot_2 |> print(n=200)

tbl_info_tot_2_temp <- tbl_info_tot_2 |>
  select(-pk.x, -pk.y) |>
  relocate(pk, .after = file)

tbl_info_tot_2_temp$media_type_id[[46]] <- NA
tbl_info_tot_2_temp$media_type_id <- tbl_info_tot_2_temp$media_type_id |> map_vec(str_flatten_comma)

tbl_info_tot_2_temp |> print(n=200)

unique_key
index_tbl <- readRDS('~/Dropbox/NielsenData/index_tbl.RDS') |>
  mutate(file_name_std = file_name_std |>
           str_replace_all("network_tv", 'national_tv') |>
           str_replace_all("spot_tv", 'local_tv')) |>
  summarise(
    index = str_flatten_comma(index),
    # index = list(index),
    # index_name = list(index_name),
    .by = file_name_std
  )
# |>
#   mutate(
#     index = map2(index, index_name, ~ .x |>  set_names(.y)),
#     .keep = 'unused'
#   )
index_tbl

x_dates <- lookup_date |>
  map_vec(typeof) |>
  as_tibble_col('col_classes') |>
  mutate(col_names_std = rename_adintel(names(lookup_date)),
         .before = 1) |>
  left_join(
    lookup_date |>
      summarise(across(where(is.character), ~max(nchar(.x)))) |>
      pivot_longer(everything(), names_to = 'col_names_std', values_to = 'p')
  ) |>
  mutate(
    col_pos = seq_along(col_classes),
    sql_datatype = case_when(
      col_classes == 'character' ~ as.character(glue::glue('VARCHAR({p})')),
      col_classes == 'integer' ~ 'INTEGER',
      col_classes == 'logical' ~ 'BOOLEAN',
    ),
    sql_datatype_min = case_when(
      col_classes == 'character' ~ 'TEXT',
      col_classes == 'integer' ~ 'INTEGER',
      col_classes == 'logical' ~ 'BOOLEAN',
    )
  ) |>
  mutate(
    file_type_std = 'references',
    file_name_std = 'dates',
    .before = 1
  ) |>
  summarise(across(!p, list),
            .by = file_type_std:file_name_std) |>
  relocate(col_pos, .before = col_names_std) |>
  mutate(
    year = NA,
    date_from = NA,
    date_to = NA,
    file = NA
  ) |>
  bind_cols(
    tbl_info_tot_2_temp |>
      filter(file_name_std == 'dates') |>
      select(pk, media_type_id)
  ) |>
  mutate(index = 'ad_date')

tbl_info_tot_2_temp <- tbl_info_tot_2_temp |>
  select(-pk) |>
  full_join(
    keys$pk |>
      select(-autroincrement) |>
      summarise(
        pk = parent_key_cols |> map_vec(~ as.character(.x)) |> str_flatten_comma(),
        .by = parent_table
      ) |>
      mutate(
        file_name_std = str_replace_all(parent_table, 'date', 'dates'),
        .keep = 'unused',
        .before = 1
      )
  ) |>
  full_join(
    index_tbl |>
      mutate(
        file_name_std = file_name_std |>
          str_replace_all('network_tv', 'national_tv') |>
          str_replace_all('spot_tv', 'local_tv')
      )
  ) |>
  filter(file_name_std != 'dates') |>
  bind_rows(x_dates) |>
  full_join(
    keys$fk |>
      select(-on_delete) |>
      mutate(file_name_std = child_table) |>
      nest(.by = file_name_std,
           .key = 'fk')
  ) |>
  select(-data) |>
  arrange(file_type_std, file_name_std)

tbl_info_tot <- tbl_info_tot_2_temp
usethis::use_data(tbl_info_tot, overwrite = T)


tbl_info_tot_2_temp|>
  print(n=200)






  print(n=200)



tbl_info_tot_2_temp <-

x_tbl <- tbl_info_tot_2 |>
  filter(
    file_type_std %in% c('occurrences')
  ) |>
  slice(1) |>
  transmute(
    .tbl_name = file_name_std,
    .col_names = col_names_std,
    .data_types = sql_datatype,
    .pk =.pk,
    .part_col = .col_names |> map_vec(~ if_else("ad_date" %in% .x, "ad_date", NA))
  )
x_tbl |> pmap_vec(sql_build_tbl)
#
#   mutate(
#     .pk = if_else(.pk_null, "NA", .pk[[1]])
#   )
# |>
#   unnest(.pk)
#   map(~.x |> simplify()) |>

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

