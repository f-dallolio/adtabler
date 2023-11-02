library(tidyverse)
devtools::install_github("f-dallolio/adtabler")
library(adtabler)

get_1_n <- function(x) x[c(1, length(x))]


library(furrr)
library(parallel)
plan(multisession, workers = detectCores()-2)

t_start <- Sys.time()

year <- 2010
file <- "/mnt/sata_data_1/adintel/ADINTEL_DATA_2010/nielsen_extracts/AdIntel/2010/Occurrences/SpotTV.tsv"

date_vec <- c(make_date(year - 1,12),
              make_date(year),
              make_date(year + 1,1))
date_vec_month <- seq(min(date_vec), max(date_vec), by = 1) |> str_sub(1,7) |> unique()
cmd_vec_month <- sprintf(glue::glue("grep -m 1 -Eoh '^{ date_vec_month }' %s"), file)

out_month <- future_map(cmd_vec_month,
                 ~ system(command = .x, intern = T) |>
                   suppressWarnings(),
                 .progress = TRUE) |>
  purrr::list_c() |>
  sort() |>
  lubridate::ym()
out_range <- range(out_month)

year_start <- year(out_range)[1]
month_start <- month(out_range)[1]
dates_start <- make_date(year_start, month_start)
if(year_start < year){
  dates_start <- sort(dates_start, decreasing = TRUE)
}
cmd_vec_start <- cmd_vec_month <- sprintf(glue::glue("grep -m 1 -Eoh '^{ dates_start }' %s"), file)

year_end <- year(out_range)[2]
month_end <- month(out_range)[2]
dates_end <- make_date(year_end, month_end)
cmd_vec_end <- cmd_vec_month <- sprintf(glue::glue("grep -m 1 -Eoh '^{ dates_end }' %s"), file)

x_start <- future_map(cmd_vec_start, ~ system(command = .x, intern = T) |> suppressWarnings(), .progress = TRUE) |> list_c()
x_end <- future_map(cmd_vec_end, ~ system(command = .x, intern = T) |> suppressWarnings(), .progress = TRUE) |> list_c()

t_end <- Sys.time()
elapsed <- t_end - t_start
print(elapsed)

plan(sequential)

trigger = FALSE
i=1
out_start0 <- dates_start[1]
while(!trigger){
  print(dates_start[i])
  out_start <- out_start0
  out_start0 <- system(command = cmd_vec_start[i], intern = T) |> suppressWarnings()
  trigger <- length(out_start0) == 0
  i = i+1
}


trigger = FALSE
i=1
out_end0 <- dates_end[1]
# while(!trigger){
#   print(dates_end[i])
#   out_end <- out_end0
#   out_end0 <- system(command = cmd_vec_end[i], intern = T) |> suppressWarnings()
#   trigger <- length(out_end0) == 0
#   i = i+1
# }

tmp <- tempfile(fileext = ".csv")
tibble(dates = c(dates_start, dates_end)) |>
  write_csv(tmp)
read_csv(tmp)
x <- data.table::fread(
  cmd = sprintf("grep -Eohf %s",paste(tmp,  file)),
  nThread = parallel::detectCores()-2,
  verbose = T)

x
