#| output: false
devtools::install_github("f-dallolio/adtabler")
1
library(data.table)
library(tidyverse, quietly = TRUE)
library(tsibble, quietly = TRUE)
library(rlang, quietly = TRUE)
library(glue, quietly = TRUE)
library(adtabler, quietly = TRUE)
library(furrr)
library(DBI)
library(RPostgres)
# Connect to a specific postgres database i.e. Heroku
con <- dbConnect(RPostgres::Postgres(),
                 dbname = 'test',
                 host = '10.147.18.200', # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
                 port = 5432, # or any other port specified by your DBA
                 user = 'postgres',
                 password = '100%Postgres')


ref_tbl_dyn <- data_info_list$file_info |>
  filter(file_type_std == "references",
         !is.na(year)) |>
  select(file_name_std, year, full_file_name) |>
  mutate(db_tbl_name = paste("dyn_ref", file_name_std, sep = "__")) |>
  inner_join(
    data_info_list$col_info |>
      filter(file_type_std == "references") |>
      select(file_name_std, datatype_r) |>
      mutate(datatype_r = if_else(is.na(datatype_r), "character", datatype_r)) |>
      nest(col_classes = datatype_r)
  ) |>
  mutate(col_classes = col_classes |> map(~ .x |> pull(1))) |>
  arrange(file_name_std, year)

safe_fread <- quietly(fread)

seq_i <- seq_along(ref_tbl_dyn$full_file_name)
t0_tot <- Sys.time()
for(i in seq_i){
  tbl_name <- ref_tbl_dyn$db_tbl_name[[i]]
  year_i <- ref_tbl_dyn$year[[i]]
  t0_i <- Sys.time()
  print(glue::glue("\n\ntbl {i}/{length(ref_tbl_dyn$full_file_name)}: {tbl_name} {year_i}\n\n"))

  my_tbl_safe <- safe_fread(file = ref_tbl_dyn$full_file_name[[i]], encoding = "Latin-1", colClasses = unname(ref_tbl_dyn$col_classes[[i]]))

  if(tbl_name == "dyn_ref__brand" & year_i %in% 2018:2021){
    tmp <- fread(file = ref_tbl_dyn$full_file_name[[i]], sep = "", quote = "") |> #,skip = 2754102, header = FALSE) |>
      pull(1) |>
      str_replace_all("\t\"", "\"")
    nms <- fread(file = ref_tbl_dyn$full_file_name[[i]], nrows = 10) |> names()
    my_tbl_safe <- safe_fread(text = tmp, encoding = "Latin-1", colClasses = unname(ref_tbl_dyn$col_classes[[i]]))
    names(my_tbl_safe$result) <- nms
  }

  if(tbl_name == "dyn_ref__distributor" & year_i %in% 2018){
    tmp <- fread(file = ref_tbl_dyn$full_file_name[[i]], sep = "", quote = "") #|> #,skip = 2754102, header = FALSE) |>
    tmp1 <- tmp |> slice(1:22035)
    tmp2 <- tmp |> slice(22036:22038) |> pull(1) |> str_flatten()
    tmp3 <- tmp |> slice(22039:NROW(tmp)) |> pull(1)
    tmp_new <- c(names(tmp), tmp1, tmp2, tmp3) |> list_c()
    my_tbl_safe <- safe_fread(text = tmp_new, encoding = "Latin-1", colClasses = unname(ref_tbl_dyn$col_classes[[i]]))
  }

  if(my_tbl_safe$output != "" | !is_empty(my_tbl_safe$warnings) |!is_empty(my_tbl_safe$messages) ){
    warn_list <- my_tbl_safe[-1]
    print(i)
    stop()
  } else {
    my_tbl <- my_tbl_safe$result |>
      as_tibble() |>
      rename_with(rename_adintel, everything()) |>
      mutate(year = year_i, .before = 1,
             across(where(is.character), ~ .x |> rlang::as_utf8_character()))
  }
  if(tbl_name %in% dbListTables(con)){
    RPostgres::dbWriteTable(conn = con, name = tbl_name, value = my_tbl, append = TRUE, )
  } else{
    print(glue::glue("\n\nFIRST {tbl_name}\n\n"))
    RPostgres::dbWriteTable(conn = con, name = tbl_name, value = my_tbl, overwrite = TRUE)
  }
  t1_i <- Sys.time()
  elapsed_i <- t1_i - t0_i
  print(glue::glue("\n\ntbl {i}/{length(ref_tbl_dyn$full_file_name)} ({tbl_name}) completed in: {elapsed_i}"))
}



ref_tbl <- data_info_list$file_info |>
  filter(file_type_std == "references",
         is.na(year)) |>
  select(file_name_std, year, full_file_name) |>
  mutate(db_tbl_name = paste("ref", file_name_std, sep = "__"))

i=1
for(i in seq_along(ref_tbl$full_file_name)){
  tbl_name <- ref_tbl$db_tbl_name[[i]]
  print(glue::glue("starting tbl {i}/{length(ref_tbl$full_file_name)}: {tbl_name}"))
  my_tbl <- fread(file = ref_tbl$full_file_name[[i]] ) |>
    as_tibble()
  dbWriteTable(conn = con, name = tbl_name, value = my_tbl)

}


t1_tot <- Sys.time()
elapsed_tot <- t1_tot - t0_tot

dbDisconnect(con)
