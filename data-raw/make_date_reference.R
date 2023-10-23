library(tidyverse)
library(tsibble)
date_reference <- tibble(
  date = (as.Date("2000-01-01"):as.Date("2049-12-31")) |> as.Date(),
  ddist = seq_along(date) - 1,
  year = year(date),
  is_53w = is_53weeks(year),
  yyear = yearweek(date, week_start = 1) |> str_split_i(" W", 1) |> as.integer(),
  yquarter = yearquarter(date) |> str_split_i(" Q", 2) |> as.integer(),
  ymonth = str_split_i(date, "-", 2) |> as.integer(),
  yweek = yearweek(date, week_start = 1) |> str_split_i(" W", 2) |> as.integer(),
  wday = lubridate::wday(date, week_start = 1),
  wdayl = lubridate::wday(date, label = TRUE, abbr = TRUE, week_start = 1)
)
save(date_reference, file = "Documents/r_wd/adtabler/data/date_reference.rda")
