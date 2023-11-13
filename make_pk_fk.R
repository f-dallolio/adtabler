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
                 dbname = 'test_2010',
                 host = '10.147.18.200', # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
                 port = 5432, # or any other port specified by your DBA
                 user = 'postgres',
                 password = '100%Postgres')


db_data <- data_write_to_db |>
  transmute(
    tbl_name = paste(
      str_split_i(tbl_name, "__", 1),
      str_split_i(tbl_name, "__", 2),
      sep = "__"),
    col_names = col_names,
    uk = map2(col_names, col_uk, ~ .x[.y])
  ) |>
  summarise(
    across(everything(), ~ .x[1]),
    .by = tbl_name
  ) |>
  filter(
    tbl_name %in%  dbListTables(con)# |> str_subset("local_tv", negate = T))
  )

my_dm <- dm_from_con(con = con, table_names = dbListTables(con))

ref_data <- db_data |>
  filter(str_detect(tbl_name, "ref_")) |>
  bind_rows(
    tibble(
      tbl_name = "ref__date",
      col_names = NA,
      uk = list("ad_date")
    )
  )
occ_data <- db_data |>
  filter(str_detect(tbl_name, "occ_"))


seq_i <- seq_along(ref_data$tbl_name)
i = 1
for(i in seq_i) {
  table_i <- ref_data$tbl_name[[i]]
  uk_i <- ref_data$uk[[i]]

  my_dm <- my_dm |>
    dm_add_pk(table = !!as.name(table_i), columns = uk_i)
}

my_dm |> dm_get_all_pks()

my_dm |> dm_examine_constraints()




my_dm_fk <- my_dm

my_dm_fk <- my_dm_fk |>
  dm_add_fk(table = ref_dyn__brand,
            columns = ref_data |> filter(tbl_name == 'ref_dyn__advertiser') |>  pluck("uk", 1),
            ref_dyn__advertiser) |>
  dm_add_fk(table = ref_dyn__brand,
            columns = ref_data |> filter(tbl_name == 'ref_dyn__product_categories') |>  pluck("uk", 1),
            ref_dyn__product_categories)



fk_data_nobrand <- expand_grid(
  tbl_name_1 = occ_data |> pull(tbl_name) |> unique(),
  tbl_name_2 = ref_data |>
    filter(str_detect(tbl_name, "product_categories", negate = T),
           str_detect(tbl_name, "brand", negate = T),
           str_detect(tbl_name, "advertiser", negate = T)) |>
    pull(tbl_name) |> unique()
)

occ = fk_data_nobrand |>
  slice(7) |>
  pull(1)
ref = fk_data_nobrand |>
  slice(7) |>
  pull(2)

nms = occ_data |>
  filter(tbl_name == occ) |>
  pluck("col_names", 1)
uk = ref_data |>
  filter(tbl_name == ref) |>
  pluck("uk", 1)

find_fk <- function(occ, ref, occ_data, ref_data){

  nms = occ_data |>
    filter(tbl_name == occ) |>
    pluck("col_names", 1)
  uk = ref_data |>
    filter(tbl_name == ref) |>
    pluck("uk", 1)

  if( setdiff(uk, nms) |> is_empty() ){
    ref_col = str_flatten_comma(uk)
  } else {
    ref_col = NA
  }

  ref_col

}

find_fk2 <- partial(find_fk, occ_data = occ_data, ref_data = ref_data)

fk_data_nobrand$ref_col <- fk_data_nobrand |>
  rename(occ = tbl_name_1,
         ref = tbl_name_2) |>
  # slice(1) |>
  map(as.list) |>
  pmap_vec(find_fk2)
fk_data_nobrand <- fk_data_nobrand |>
  filter(ref_col |> not_na(),
         str_detect(tbl_name_2, "digital", negate = T))

fk_data_nobrand |>
  print(n=200)



seq_i <- seq_along(fk_data_nobrand$tbl_name_1)
i = 13
for(i in seq_i) {
  table_1_i <- fk_data_nobrand$tbl_name_1[[i]]
  table_2_i <- fk_data_nobrand$tbl_name_2[[i]]
  ref_col_i <- str_split_comma(fk_data_nobrand$ref_col[[i]])

  # if( table_1_i == "occ__local_tv" & table_2_i == "ref_dyn__distributor" ){
  #   ref_col_i = setdiff(ref_col_i, "media_type_id")
  # }

  my_dm_fk <- my_dm_fk |>
    dm_add_fk(table = !!as.name(table_1_i), columns = c(ref_col_i), ref_table = !!as.name(table_2_i))
}

my_dm_fk |> dm_examine_constraints()

keys_2010 <- list(
  pk = my_dm_fk |> dm_get_all_pks(),
  fk = my_dm_fk |> dm_get_all_fks()
)

usethis::use_data(keys_2010, overwrite = TRUE)

dbDisconnect(con)
