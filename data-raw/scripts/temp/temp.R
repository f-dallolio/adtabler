
xxx <- occurrences_files |>
  mutate(min_date = min_date_year(file = full_file_name, year = year) )

xx <- xxx |> mutate(min_date_yw = min_date |> date_to_yweek(),
              min_date_yw2 = yweek_to_date(min_date_yw),
              test = min_date == min_date_yw2)
xx |> pull(test) |> all(na.rm = T)

xxx

year = 2010
file <-  "/mnt/sata_data_1/adintel/ADINTEL_DATA_2010//nielsen_extracts/AdIntel/2010/Occurrences/SpotTV.tsv"
y <- rep(seq(year - 1, year + 1), each = 12) |> numpad4()
m <- rep(seq(1, 12), length(unique(y))) |> numpad2()
# length(y) == length(m)

ym_in_data <- function(y, m, file){
  cmd_lgl <- glue("grep -E -m 1 -c '{ y }-{ m }-[0-9][0-9]' %s") |>
    as.character()  |> sprintf(shQuote(file))
  suppressWarnings(system(cmd_lgl, intern = TRUE) == "1")
}

x <- ymd_min

ymd_in_data <- function(x, file){
  cmd_lgl <- glue("grep -E -m 1 -c '{ x }' %s") |>
    as.character()  |> sprintf(shQuote(file))
  # print(cmd_lgl)
  suppressWarnings(system(cmd_lgl, intern = TRUE) == "1")
}


library(furrr)
plan( multisession, workers = floor(parallel::detectCores()/2) )
ym0 <- future_map2(.x = y, .y = m, .f = ~ ym_in_data(y = .x, m = .y, file = file), .progress = TRUE) |>
  list_c()
plan( sequential )

ym1 <-  paste(y[ym0], m[ym0], sep = "-") |> sort()
ymd_min <- ym1[1] |> paste(numpad2(31:1), sep = "-")
ymd_max <- ym1[length(ym1)] |> paste(numpad2(31:1), sep = "-")

i=1
min_out <- NA
max_out <- NA
for(i in seq_along(ymd_min)){

  if(is.na(min_out)){
    out_min0 <- ymd_min[[i]] |> ymd_in_data(file)
    if(!out_min0) {
      if(i == 1){
        id <- i
      } else {
        id <- i - 1
      }
      min_out <- ymd_min[[id]]
    }
  }

  if(is.na(max_out)){
    out_max0 <- ymd_max[[i]] |> ymd_in_data(file)
    if(!out_max0) {
      if(i == 1){
        id <- i
      } else {
        id <- i - 1
      }
      max_out <- ymd_max[[id]]
    }
  }
  min_max <- c(min_out, max_out)
  print(min_max)
  if(!any(is.na( min_max))) break
}
min_out
max_out



m1 <-

new_min_date_year_i <- function(file, year){

  seq_month <- c(1, 12 : 1, 12) |> numpad2()
  seq_year <- c(year + 1, rep(year, 12), year - 1) |> numpad4()

  # check by month first
  i = 1
  for (i in seq_along(seq_month)){
    iter_out <- i
    m <- seq_month[i]
    y <- seq_year[[i]]
    cmd_lgl <- glue("grep -E -m 1 -c '{ y }-{ m }-[0-9][0-9]' %s") |>
      as.character()  |> sprintf(shQuote(file))
    out_zero <- suppressWarnings(system(cmd_lgl, intern = TRUE) == "0")
    if(out_zero) break
  }

  seq_days <- (31 : 1) |> numpad2()
  # seq_month2 <- seq_month[[iter_out]]
  # seq_year2 <- seq_year[[iter_out]]

  i = 1
  for (i in seq_along(seq_days)){
    d <- seq_days[[i]]
    cmd_lgl <- glue("grep -E -m 1 -c '{ y }-{ m }-{ d }' %s") |>
      as.character()  |> sprintf(shQuote(file))
    out_zero <- suppressWarnings(system(cmd_lgl, intern = TRUE) == "0")
    if( out_zero ) {
      # return(
        print(glue("{ y }-{ m }-{ as.integer(d) + 1 }"))
      break
      # )
    }
  }
}


new_min_date_year_i <- function(file, year){

  y <- rep(seq(year - 1, year + 1), each = 12) |> numpad4()
  m <- rep(seq(1, 12), length(unique(y))) |> numpad2()
  # length(y) == length(m)

  ym_in_data <- function(y, m, file){
    cmd_lgl <- glue("grep -E -m 1 -c '{ y }-{ m }-[0-9][0-9]' %s") |>
      as.character()  |> sprintf(shQuote(file))
    suppressWarnings(system(cmd_lgl, intern = TRUE) == "1")
  }


  map2(.x = y, .y = m, .f = ~ ym_in_data(y = .x, m = .y, file = file), .progress = TRUE) |>
    list_c()

  expand_grid(
    year = seq(year - 1, year + 1) |> numpad4()
    month = rep(seq(1, 12)) |> numpad2(),

  )

  # check by month first
  i = 1
  for (i in seq_along(seq_month)){
    iter_out <- i
    m <- seq_month[i]
    y <- seq_year[[i]]
    cmd_lgl <- glue("grep -E -m 1 -c '{ y }-{ m }-[0-9][0-9]' %s") |>
      as.character()  |> sprintf(shQuote(file))
    out_zero <- suppressWarnings(system(cmd_lgl, intern = TRUE) == "0")
    if(out_zero) break
  }

  seq_days <- (31 : 1) |> numpad2()
  # seq_month2 <- seq_month[[iter_out]]
  # seq_year2 <- seq_year[[iter_out]]

  i = 1
  for (i in seq_along(seq_days)){
    d <- seq_days[[i]]
    cmd_lgl <- glue("grep -E -m 1 -c '{ y }-{ m }-{ d }' %s") |>
      as.character()  |> sprintf(shQuote(file))
    out_zero <- suppressWarnings(system(cmd_lgl, intern = TRUE) == "0")
    if( out_zero ) {
      # return(
      print(glue("{ y }-{ m }-{ as.integer(d) + 1 }"))
      break
      # )
    }
  }
}




  out_last <- check_previous_year_i(file, year)


  if (is.na(out_temp)) {
    out <-  system(cmd_out, intern = TRUE) |>
      unique() |>
      sort() |>
      suppressWarnings()
    if(length(out) == 0){
      return(NA)
    } else {
      return(out[[1]])
    }
  } else {
    out_temp
  }
}
