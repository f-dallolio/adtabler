devtools::install_github("f-dallolio/adtabler")
1
library(data.table)
library(tidyverse)
library(rlang)
library(glue)
library(adtabler)

library(DBI)
library(RPostgres)
con <- dbConnect(RPostgres::Postgres(),
                 dbname = 'test_2010',
                 host = '10.147.18.200', # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
                 port = 5432, # or any other port specified by your DBA
                 user = 'postgres',
                 password = '100%Postgres')

#    Write DB - References    ----


data_db <- rlang::fn_fmls_names(adintel_read_tsv) |>
  slice_last(1, negate = T) |>
  select(.data = data_write_to_db) |>
  filter(str_detect(tbl_name, "ref_")
  )

data_tbl_min_year  <- data_db |>
  mutate(year = if_else(str_detect(file, "Latest"), NA, file |> str_split_i("/", -3))) |>
  # filter(!is.na(year)) |>
  # print(n = 200) |>
  mutate(year = as.integer(year)) |>
  group_by(tbl_name) |>
  filter(year %in% c(min(year))) |>
  mutate(overwrite = TRUE,
         append = !overwrite,
         df = NA) |>


  filter(year %in% c(NA, 2010)) |>


  group_split()


data_tbl=data_tbl_min_year[[14]]

fn <- function(data_tbl){

  seq_i <- seq_along(data_tbl$tbl_name)
  i=1
  for( i in seq_i) {

    tbl_name_i <- data_tbl$tbl_name[i]
    file_i <- data_tbl$file[i]
    year_i <- data_tbl$year[i]

    print( glue::glue("\n\n
                    Starting \t {tbl_name_i} - {year_i}") )

    t0 <- Sys.time()

    x_i <- data_tbl |>
      slice(i) |>
      select(any_of(rlang::fn_fmls_names(adintel_read_tsv) |> slice_last(1, negate = T)))

    data_n_1 <- fread(x_i$file, nrows = 1)
    data_nms_i <- data_n_1 |>  names() |> rename_adintel() |> unname()
    data_class_i <- map_vec(data_n_1, class) |> unname()

    nms_i <- x_i |>
      select(col_names) |>
      unnest(everything()) |>
      pull(col_names)

    class_i <- x_i |>
      select(col_classes) |>
      unnest(everything()) |>
      pull(col_classes)

    uk_i <- x_i |>
      select(col_names, col_uk) |>
      unnest(everything()) |>
      filter(col_uk) |>
      pull(col_names)

    # if( any(match(data_nms_i, nms_i) |> is.na()) ) {
    #   flag_nms <- TRUE
    #   nms_i <- data_nms_i
    # } else {
    #   flag_nms <- FALSE
    # }

    # if( any(match(data_class_i, class_i) |> is.na()) ) {
    #   flag_class <- TRUE
    #   class_i <- data_class_i
    # } else {
    #   flag_class <- FALSE
    # }

    # if( any(c(flag_class, flag_nms)) ) {
      df_i <- fread(file = file_i, col.names = nms_i, colClasses = class_i, na.strings = "") |>
        mutate(across(where(is.character), ~ iconv(.x, 'latin1', 'UTF-8'))) |>
        as_tibble()
    # } else {
    #   df_i <- x_i |>
    #     map(as.list)  |>
    #     pmap(adintel_read_tsv) |>
    #     pluck(1) |>
    #     mutate(across(where(is.character), ~ iconv(.x, 'latin1', 'UTF-8'))) |>
    #     as_tibble()
    # }




    all_unique <- df_i |>
      nest(.by = any_of(uk_i)) |>
      mutate(n = ! data |> map_vec(~ NROW(.x) > 1)) |>
      pull(n) |>
      any()

    uk_not_na <- df_i |>
      select(any_of(uk_i)) |>
      summarise(across(any_of(uk_i), ~ any(is.na(.x)))) |>
      pivot_longer(everything(), names_to = "col_names", values_to = "has_na") |>
      filter(has_na) |>
      pull(col_names) |>
      is_empty()

    if( all_unique & uk_not_na ) {

      overwrite_i <- TRUE
      append_i = ! overwrite_i

      # if(is.na(year_i)){
      #   df_i <- df_i |> select(-year)
      # }

      if( tbl_name_i == "ref_dyn__distributor" ){

        df_i <- df_i |>
          bind_rows(
            df_i |> filter(media_type_id == 5) |>  mutate(media_type_id = 13)
          ) |>
          bind_rows(
            df_i |> filter(media_type_id == 5) |>  mutate(media_type_id = 14)
          ) |>
          bind_rows(
            df_i |> filter(media_type_id == 5) |>  mutate(media_type_id = 24)
          )

      }

      RPostgres::dbWriteTable(conn = con, name = tbl_name_i, value = df_i, overwrite = overwrite_i, append = append_i)

      data_tbl$df[i] <- "ok"

      print( glue::glue("Finished \t {tbl_name_i} - {year_i} \t {timer(t0)}") )

    } else {

      stop(glue::glue("Issue in \t {tbl_name_i} - {year_i}: "))

    }

  }

}
safe_fn <- safely(fn)

walk(data_tbl_min_year, fn)



library(tsibble)
ref_date <- lookup_date |>
  mutate(across(!where(is.numeric), as.character))

RPostgres::dbWriteTable(conn = con, name = "ref__date", value = ref_date, overwrite = TRUE)

# -----
#    Write DB - Occurrences    ----


data_db_occ <- rlang::fn_fmls_names(adintel_read_tsv) |>
  slice_last(1, negate = T) |>
  select(.data = data_write_to_db) |>
  filter(str_detect(tbl_name, "occ_"),
         str_detect(tbl_name, "local_tv", negate = TRUE),
         str_detect(tbl_name, "digital", negate = TRUE))

data_tbl_occ_2010  <- data_db_occ |>
  mutate(tbl_name = tbl_name |> str_split("__") |> list_c() |> slice_vec(1:2) |> paste(collapse = "__"),
         .by = tbl_name) |>
  summarise(across(contains("col_"), ~ .x[1]), .by = c(tbl_name, file)) |>
  select(-col_uk) |>
  filter(str_detect(file, "2010"))

x <- data_tbl_occ_2010


seq_i <- seq_along(data_tbl_occ_2010$tbl_name)
i=1

for( i in seq_i ) {
  tbl_name_i <-  x$tbl_name[[i]]
  file_i <- x$file[[i]]
  col_names_i <- x$col_names[[i]]
  col_classes_i <- x$col_classes[[i]]

  df_i <- fread(file = file_i, col.names = col_names_i, colClasses = col_classes_i, na.strings = "") |>
    mutate(across(where(is.character), ~ iconv(.x, 'latin1', 'UTF-8'))) |>
    as_tibble()

  RPostgres::dbWriteTable(conn = con, name = tbl_name_i, value = df_i, overwrite = TRUE, append == FALSE)

  print(glue::glue("{i} / {max(seq_i)}"))
}




data_db_occ_localtv <- rlang::fn_fmls_names(adintel_read_tsv) |>
  slice_last(1, negate = T) |>
  select(.data = data_write_to_db) |>
  filter(str_detect(tbl_name, "occ_"),
         str_detect(tbl_name, "local_tv"))

data_tbl_occ_localtv_2010  <- data_db_occ_localtv |>
  mutate(tbl_name = tbl_name |> str_split("__") |> list_c() |> slice_vec(1:2) |> paste(collapse = "__"),
         .by = tbl_name) |>
  summarise(across(contains("col_"), ~ .x[1]), .by = c(tbl_name, file)) |>
  select(-col_uk) |>
  filter(str_detect(file, "2010"))

x <- data_tbl_occ_localtv_2010

gc()

seq_i <- seq_along(data_tbl_occ_localtv_2010$tbl_name)
i=1

for( i in seq_i ) {

  t0 <- Sys.time()
  print(t0)

  tbl_name_i <-  x$tbl_name[[i]]
  file_i <- x$file[[i]]
  col_names_i <- x$col_names[[i]]
  col_classes_i <- x$col_classes[[i]]

  df_i <- adintel_read_tsv(tbl_name = tbl_name_i, file = file_i, col_names = col_names_i, col_classes = col_classes_i)

  RPostgres::dbWriteTable(conn = con, name = tbl_name_i, value = df_i, overwrite = TRUE, append == FALSE)

  print(glue::glue("{i} / {max(seq_i)} in {timer(t0)}"))
}

# ----
#    Write DB - Occurrences -----

