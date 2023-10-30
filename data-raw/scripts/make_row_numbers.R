year = 2021
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst,file = file_out)
toc()

year = 2020
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst, file = file_out)
toc()

year = 2019
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst, file = file_out)
toc()

year = 2018
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst, file = file_out)
toc()

year = 2017
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst, file = file_out)
toc()

year = 2016
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst, file = file_out)
toc()

year = 2015
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst, file = file_out)
toc()

year = 2014
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst, file = file_out)
toc()

year = 2013
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst, file = file_out)
toc()

year = 2012
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst, file = file_out)
toc()

year = 2011
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst, file = file_out)
toc()

year = 2010
file_out <- glue::glue("~/Documents/r_wd/adtabler/data/row_numbers_{year}.rda")
tic()
file <- adintel_files |>
  pull(full_file_name) |>
  str_subset(as.character(year))

lst <- map(file, read_nrows, .progress = T) |> list_c()
save(lst, file = file_out)
toc()
