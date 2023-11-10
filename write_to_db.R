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

# data_write_to_db <- data_info_db |>
#   filter(year |> not_na()) |>
#   transmute(file = full_file_name,
#             year,
#             tbl_name = db_name_std,
#             col_names = col_info |> map(~ .x$col_name_std |> unname()),
#             col_classes = col_info |> map(~ .x$datatype_r |> unname()),
#             col_uk = col_info |> map(~ .x$is_uk |> unname()),
#             overwrite = is.na(overwrite),
#             append = !overwrite)

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

file = data_write_to_db$file[[1]]
year = data_write_to_db$year[[1]]
media_type_id = data_write_to_db$media_type_id[[1]]
tbl_name = data_write_to_db$tbl_name[[1]]
col_names = data_write_to_db$col_names[[1]]
col_classes = data_write_to_db$col_classes[[1]]
col_classes[col_names == "ad_date"] <- "IDate"
col_uk = data_write_to_db$col_uk[[1]]
overwrite = data_write_to_db$overwrite[[1]]
append = data_write_to_db$append[[1]]


write_to_db <- function(con = {{con}}, file, year, tbl_name, col_names, col_classes, col_uk, overwrite, append){

  read_info <- stringr::str_flatten(c(tbl_name, year), collapse = " - ", na.rm = TRUE)
  print(glue::glue("Starting: \t\t {read_info}"))

  t0_fun <- Sys.time()

  has_ad_time <- "ad_time" %in% col_names

  is_ref_dyn <- stringr::str_detect(tbl_name, "ref_dyn__")
  is_ref <- stringr::str_detect(tbl_name, "ref__")

  col_uk <- col_names[col_uk]
  col_classes[col_names == "ad_date"] <- "IDate"

  t0_fread <- Sys.time()

  if(tbl_name == "ref_dyn__brand" & year %in% 2018:2021){
    tbl_tmp <- data.table::fread(
      file = file,
      sep = "",
      quote = ""
    ) |>
      dplyr::pull(1) |>
      stringr::str_replace_all("\t\"", "\"")
    df <- data.table::fread(
      text = tbl_tmp,
      colClasses = col_classes,
      col.names = col_names,
      key = col_uk,
      nThread = parallel::detectCores() - 2,
      encoding = "UTF-8"
    )
  } else
    if(tbl_name == "ref_dyn__distributor" & year %in% 2018){
      tbl_tmp <- data.table::fread(
        file = file,
        sep = "",
        quote = ""
      )
      tmp1 <- tbl_tmp |> dplyr::slice(1:22035)
      tmp2 <- tbl_tmp |> dplyr::slice(22036:22038) |> dplyr::pull(1) |> stringr::str_flatten()
      tmp3 <- tbl_tmp |> dplyr::slice(22039:NROW(tbl_tmp)) |> dplyr::pull(1)
      tmp_new <- c(names(tbl_tmp), tmp1, tmp2, tmp3) |> list_c()
      df <- data.table::fread(
        text = tmp_new,
        colClasses = col_classes,
        col.names = col_names,
        key = col_uk,
        nThread = parallel::detectCores() - 2,
        encoding = "UTF-8"
      )
    } else
    {
      df <- data.table::fread(
        file = file,
        colClasses = col_classes,
        col.names = col_names,
        key = col_uk,
        nThread = parallel::detectCores() - 2,
        encoding = "UTF-8"
      )
    }

  timer(t0_fread, msg = 'File read in \t {.x}') |> print()

  if(has_ad_time){
    df$ad_time <- stringr::str_sub(df$ad_time, 1, 8)
  }

  if(is_ref_dyn){
    df  <- df |> dplyr::mutate(year = year, .before = 1)
  }

  t0_dbwrite <- Sys.time()

  RPostgres::dbWriteTable(conn = con,
                          name = tbl_name,
                          value = df,
                          overwrite = overwrite,
                          append = append)

  timer(t0_dbwrite, msg = 'Table to DB in \t {.x}') |> print()

  timer(t0_fun, n_post = 2, msg = 'Done in \t {.x} \t {read_info}') |> print()
}

data_write_to_db_list <- as.list(data_write_to_db)

write_to_db_con <- function(con){
  purrr::partial(.f = write_to_db, con = con)
}
pwalk(data_write_to_db_list, p_write_to_db)

dbDisconnect(con)
