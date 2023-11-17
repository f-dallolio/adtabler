devtools::install_github("f-dallolio/adtabler")
1
library(data.table)
library(tidyverse)
library(rlang)
library(glue)
library(adtabler)

local_tv_test <- data_info_list$col_info |> distinct() |>
  filter(file_name_std == "network_tv") |>
  mutate(file_name_std = file_name_std |> str_replace_all("spot_tv", "local_tv")) |>
  transmute(
    tbl_name = paste( str_sub(file_type_std, 1, 3), file_name_std, sep = "__" ),
    # media_type_id,
    col_uk = if_else(is_uk, col_name_std, NA),
    col_def = col_name_std |> str_pad(width = max(nchar(col_name_std)), side = "right", pad = " ") |>
      paste(sql_datatype)
  ) |>
  summarise(
    col_uk = list( col_uk[not_na(col_uk)] ),
    col_def = list( col_def ),
    .by = c(
      tbl_name,
      # media_type_id
    )
  ) |> full_join(
    data_info_list$file_info |>
      filter(file_name_std == "network_tv") |>
      mutate(file_name_std = file_name_std |> str_replace_all("spot_tv", "local_tv")) |>
      transmute(
        tbl_name = paste( str_sub(file_type_std, 1, 3), file_name_std, sep = "__" ),
        date_from,
        date_to,
        file = full_file_name
      ) |>
      nest(.by = tbl_name)
  ) |>
  unnest(data)

data_info_list$file_info |>
  filter(file_type_std == "occurrences") |>
  # mutate(file_name_std = file_name_std |> str_replace_all("spot_tv", "local_tv")) |> |
  summarise(
    date_1 = all(str_sub(date_from, 6, -1) == '01-01'),
    .by = file_name_std
  ) |>
  filter(!date_1) |>
  inner_join(data_info_list$file_info |>
               select(year, file_name_std, date_from)) |>
  mutate(
    wk_01 = tsibble::yearweek(paste(year, "01", "01", sep = "-")),
    wday_01 = wday(as.Date(paste(year, "01", "01", sep = "-")), label = T),
    wk_start = tsibble::yearweek(date_from),
    wday_start = as.character(wday(date_from, label = T)),
    try = wk_01 == wk_start,
    date_start_type = if_else(wday_start == "Sun", "yearweek_sunday", "yearweek_monday")
  ) |>
  select(file_name_std, date_start_type) |>
  distinct()

ywk <- .Last.value

minus <- function(a, b) a - b

data_info_list$file_info |>
  filter(file_type_std == "occurrences") |>
  select(file_name_std, year, date_from) |>
  mutate(try = get_start_date(file_name_std, year),
         jan_01_day = wday(as.Date(paste(year, '01', '01', sep = '-')), week_start = 7) - 1,
         # wk = week(date_from),
         # ywk = tsibble::yearweek(ymd(paste(year, '01-01', sep = '-')), week_start = 7) |> as.Date(),
         # diff = as.Date(date_from) - ywk,
         ok = date_from == try,
         # wday = wday(as.Date(date_from), week_start = 7),
         # wday1 = wday(as.Date(paste(year, '01', '01', sep = '-')), week_start = 1) / 7,
         ) |> print(n = 300)
|>

  # mutate(file_name_std = file_name_std |> str_replace_all("spot_tv", "local_tv")) |> |
  summarise(
    date_1 = all(str_sub(date_from, 6, -1) == '01-01'),
    .by = file_name_std
  ) |>
  full_join(ywk) |>
  mutate(date_start_type = case_when(
    is.na(date_start_type) ~ "{year}-01-01",
    date_start_type == "yearweek_sunday" ~ "paste(year, '01', '01', sep = '-') |> tsibble::yearweek(week_start = 7)"
    )
  )

get_start_date <- function(name, year = 2010){
  jan_01 <- ymd(paste(year, '01', '01', sep = '-'))
  jan_01_day <- wday(jan_01)
  fsi_coupon_start <- as.Date(tsibble::yearweek( jan_01, week_start = 7))
  fsi_coupon_start1 <- as.Date(tsibble::yearweek( jan_01, week_start = 7) + 1)
  # fsi <- c(fsi_coupon_start, fsi_coupon_start1)
  # fsi <- as.character(fsi[which.min(abs(fsi - jan_01))])

  case_when(
    name == "fsi_coupon" ~ as.character(fsi_coupon_start),
    name %in% c('cinema', 'internet', 'spot_tv', 'local_tv') ~
      as.character(as.Date(tsibble::yearweek( jan_01, week_start = 1))),
    .default = as.character(jan_01)
  )
}
