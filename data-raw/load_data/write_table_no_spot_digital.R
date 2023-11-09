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

dbname <- 'test'
host <- '10.147.18.200'
port <- 5432


con <- dbConnect(
  RPostgres::Postgres(),
  dbname = dbname,
  host = host,
  port = port,
  user = 'postgres',
  password = rstudioapi::askForPassword(prompt = pswd_prompt)
)





my_data <- data_info_list$file_info |>
  filter(
    file_type_std %in% c("references", "occurrences"),
    file_name_std |> not_in(c("digital", "spot_tv")),
    file_name_std |> str_detect("digital_", negate = TRUE),
    n_rows > 0
  ) |>
  select(
    year,
    file_type_std,
    file_name_std,
    full_file_name,
    col_name_std
  ) |>
  relocate(
    full_file_name,
    col_name_std,
    .before = 2
  ) |>
  mutate(
    tbl_name = paste(str_sub(file_type_std, 1, 3),
                     str_replace_all(file_name_std, "network_tv", "national_tv"),
                     sep = "__"),
    .after = full_file_name
  ) |>
  mutate(
    overwrite = year %in% c(min(year), NA),
    append = !overwrite,
    has_ad_time = str_detect(col_name_std, "ad_time"),
    .by = tbl_name,
    .after = tbl_name
  ) |>
  arrange(tbl_name, year) |>
  inner_join(
    data_info_list$col_info |>
      summarise(col_name_std = str_flatten_comma(col_name_std),
                col_classes = str_flatten_comma(datatype_r),
                .by = file_name_std)
  ) |>
  relocate(col_classes,
           .after = col_name_std)

seq_i <- seq_along(my_data$full_file_name)
i=109

seq_i <- seq_i[seq_i>= 109]

for( i in seq_i) {

  year_i <- my_data$year[[i]]
  file <- my_data$full_file_name[[i]]
  col_names <- my_data$col_name_std[[i]] |> str_split_comma() |> unname() |> rename_adintel()
  col_classes <- my_data$col_classes[[i]] |> str_split_comma() |> unname()
  col_classes[col_names == "ad_date"] <- "IDate"
  tbl_name <- my_data$tbl_name[[i]]
  has_ad_time_i <- my_data$has_ad_time[[i]]
  overwrite_i <- my_data$overwrite[[i]]
  append_i <- my_data$append[[i]]

  if(tbl_name == "ref__brand" & year_i %in% 2018:2021){
    tmp <- fread(file = file, sep = "", quote = "") |> #,skip = 2754102, header = FALSE) |>
      pull(1) |>
      str_replace_all("\t\"", "\"")
    nms <- fread(file = file, nrows = 10) |> names() |> rename_adintel()
    my_tbl <- fread(text = tmp, encoding = "Latin-1", col.names = col_names, colClasses = col_classes)
    names(my_tbl) <- nms
  } else if ( tbl_name == "ref__distributor" & year_i %in% 2018) {
    tmp <- fread(file = file, sep = "", quote = "") #|> #,skip = 2754102, header = FALSE) |>
    tmp1 <- tmp |> slice(1:22035)
    tmp2 <- tmp |> slice(22036:22038) |> pull(1) |> str_flatten()
    tmp3 <- tmp |> slice(22039:NROW(tmp)) |> pull(1)
    tmp_new <- c(names(tmp), tmp1, tmp2, tmp3) |> list_c()
    my_tbl <- fread(text = tmp_new, encoding = "Latin-1", col.names = col_names, colClasses = col_classes)
  } else {
    my_tbl <- fread(file = file, encoding = "Latin-1", col.names = col_names, colClasses = col_classes)
  }

  if(has_ad_time_i) {
    my_tbl$ad_time <- my_tbl$ad_time |> str_sub(1, 8)
  }

  my_tbl <- my_tbl |>
    mutate(across(where(is.character), ~ rlang::as_utf8_character(.x)))

  dbWriteTable(conn = con,
               name = tbl_name,
               value = my_tbl,
               overwrite = overwrite_i,
               append = append_i)

  print(paste(i, year_i, tbl_name))
}
