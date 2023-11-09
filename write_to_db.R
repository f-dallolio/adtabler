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
library(dbplyr)
library(prettyunits)
library(DBI)
library(RPostgres)

xxx <- data_info_db |>
  filter(year |> is.na()) |>
  transmute(file = full_file_name,
            year,
            tbl_name = db_name_std,
            col_names = col_info |> map(~ .x$col_name_std |> unname()),
            col_classes = col_info |> map(~ .x$datatype_r |> unname()),
            col_uk = col_info |> map(~ .x$is_uk |> unname()),
            overwrite = is.na(overwrite),
            append = !overwrite)

dbname <- 'adintel'
host <- '10.147.18.200'
port <- 5432
con <- dbConnect(
  RPostgres::Postgres(),
  dbname = dbname,
  host = host,
  port = port,
  user = 'postgres',
  password = rstudioapi::askForPassword()
)

file = xxx$file[[1]]
year = xxx$year[[1]]
tbl_name = xxx$tbl_name[[1]]
col_names = xxx$col_names[[1]]
col_classes = xxx$col_classes[[1]]
col_classes[col_names == "ad_date"] <- "IDate"
col_uk = xxx$col_uk[[1]]
overwrite = xxx$overwrite[[1]]
append = xxx$append[[1]]

xx_list <- as.list(xxx)

write_to_db <- function(con = {{con}}, file, year, tbl_name, col_names, col_classes, col_uk, overwrite, append){

  read_info <- str_flatten(c(tbl_name, year), collapse = " - ", na.rm = TRUE)
  print(glue::glue("Starting: \t {read_info}"))

  t0_fun <- Sys.time()

  has_ad_time <- "ad_time" %in% col_names

  is_ref_dyn <- str_detect(tbl_name, "ref_dyn__")
  is_ref <- str_detect(tbl_name, "ref__")

  col_uk <- col_names[col_uk]
  col_classes[col_names == "ad_date"] <- "IDate"

  t0_fread <- Sys.time()
  df <- fread(file = file, colClasses = col_classes, col.names = col_names, key = col_uk, nThread = parallel::detectCores() - 2, encoding = "UTF-8")
  elapsed(t0_fread, msg = 'File read in \t {.x}') |> print()

  if(has_ad_time){
    df$ad_time <- str_sub(df$ad_time, 1, 8)
  }

  if(is_ref_dyn){
    df  <- df |> mutate(year = year, .before = 1)
  }

  t0_dbwrite <- Sys.time()
  RPostgres::dbWriteTable(conn = con,
                          name = tbl_name,
                          value = df,
                          overwrite = overwrite,
                          append = append)
  elapsed(t0_dbwrite, n_post = 2, msg = 'Table to DB in \t {.x}') |> print()

}

p_write_to_db <- partial(.f = write_to_db, con = con)

pmap(xx_list, p_write_to_db)

dbDisconnect(con)
