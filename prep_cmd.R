library(rlang)
library(data.table)
library(dtplyr)
library(tidyverse)
library(tsibble)
library(glue)
library(adtabler)



# Functions ----
make_input_file <- function(adintel_dir, file_type, file_type2, file_year){
  glue( "{ adintel_dir }/ADINTEL_DATA_{ file_year }/nielsen_extracts/AdIntel/{ file_year }/{ file_type }/{ file_type2 }.tsv" )
}

make_output_file <- function(new_adintel_dir, file_type, file_type2, file_year, year, month ){
  new_file_type <- rename_adintel(file_type)
  new_file_type2 <- rename_adintel(file_type2) |>
    str_replace_all("spot", "local") |>
    str_replace_all("network", "national")
  month_pad <- str_pad(month, width = 2, side = "left", pad = "0")
  new_file <- glue("{ year }_{ month_pad }_{ new_file_type2 }.csv")
  new_dir <- glue( "{ new_adintel_dir }/{ new_file_type }/{ new_file_type2 }/{ year }/" )
  new_path <- glue("{ new_dir }/{ new_file }")
}

make_cur_year <- function(adintel_dir, new_adintel_dir, file_type, file_type2, file_year){
  file_year <-  as.numeric(file_year)
  tibble(
    adintel_dir,
    new_adintel_dir,
    file_type,
    file_type2,
    file_year,
    year = file_year,
    month = 1:12
  ) |>
    mutate(input_file = make_input_file(adintel_dir, file_year, file_type, file_type2),
           output_file = make_output_file(new_adintel_dir, file_year, file_type, file_type2, year, month),
           append = FALSE) |>
    as.list() |>
    list_transpose() |>
    set_names(glue("{file_type2}_{file_year}"))
}

make_prev_year <- function(adintel_dir, new_adintel_dir, file_type, file_type2, file_year){
  file_year <-  as.numeric(file_year)
  out <- tibble(
    adintel_dir,
    new_adintel_dir,
    file_type,
    file_type2,
    file_year,
    year = file_year - 1,
    month = 12
  ) |>
    mutate(input_file = make_input_file(adintel_dir, file_year, file_type, file_type2),
           output_file = make_output_file(new_adintel_dir, file_year, file_type, file_type2, year, month),
           append = TRUE) |>
    as.list() |>
    list_transpose()
  out
}

make_grep <- function(year , month){
  month_pad <- str_pad(month, width = 2, side = "left", pad = "0")
  command <- glue("grep -P '{ year }-{ month_pad }-[0-9]{{2}}' %s")
  command
}

make_cmd <- function(command, input_file){
  sprintf(command, file)
}

is_yw <- tsibble::is_yearweek
is_ym <- tsibble::is_yearmonth
is_yq <- tsibble::is_yearquarter
as_yw <- tsibble::yearweek
as_ym <- tsibble::yearmonth
as_yq <- tsibble::yearquarter
yw <- tsibble::makeyearweek
ym <- tsibble::makeyearmonth
yq <- tsibble::makeyearquarter
yw_date <- function(x){
  as.date(ywx)
}

x <- c(yw(2010,1):yw(2012,1),yw(2011,1))

x |> as.Date() |> `names<-`(yw(.))

,
  "2002-01-01",
  "2020-01-01") |> yearweek() |> range()

yw(2010,1)

# ------
data("adintel_info")
data("media_type_table")
data("adintel_tables")


adintel_dir <- "/mnt/sata_data_1/adintel/"
new_adintel_dir <- "/mnt/sata_data_1/new_adintel/"


ad_date <- as_date('2010-01-01') : as_date("2010-01-10") |> as_date()

make_grep_file <- function(media_type_id = NULL, ad_date = NULL, market_code = NULL){
  if(is.null(ad_date)) ad_date <- "[0-9]{4}-[0-9]{2}-[0-9]{2}"
  if(is.null(market_code)) market_code <- "[0-9]{1,3}"
  if(is.null(media_type_id)) media_type_id <- "[0-9]{1,2}"
  ad_time <- "[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{7}"
  out <- expand_grid(ad_date, ad_time, market_code, media_type_id)
  # out <- glue("{ ad_date }\t{ ad_time }\t{ market_code }\t{ media_type_id}") |> with(data = df)
  return(out)
}


out <- make_grep_f(media_type_id = 5, ad_date = ad_date)
tmp <- "Documents/temp_tsv"
write_tsv(out, file = tmp, col_names = F)
read_lines(tmp)

cmd <- sprintf("grep -Ef ~/Documents/temp_tsv %s", "/mnt/sata_data_1/adintel/ADINTEL_DATA_2010/nielsen_extracts/AdIntel/2010/Occurrences/SpotTV.tsv")
x <- fread(cmd = cmd, verbose = T)
x$V1 |> unique() |> sort()
# Files ----

occurrences_col_info <- adintel_tables |>
  filter(file_type == "occurrences") |>
  rename(occurrences_file = file_name) |>
  select(occurrences_file, var_attributes) |>
  mutate(data = map(var_attributes,
                    ~ as.list(.x)[c("var_name_manual", "var_type_manual")]),
         data = map(data, ~ .x |> as.data.frame() |> as_tibble()),
         .keep = "unused") |>
  unnest(everything()) |>
  summarise(
    col_names_chr = str_c(rename_adintel(var_name_manual), collapse = ","),
    col_classes_chr = str_c(rename_adintel(var_type_manual), collapse = ","),
    keys = str_c("media_type_id", "ad_date", "ad_time", sep = ","),
    keys = if_else(str_detect(col_names_chr,",ad_time", negate = T),
                   str_remove_all(keys, ",ad_time"),
                   keys),
    .by = c(occurrences_file)
  ) |>

occurrences_files <- list.files(path = adintel_dir, recursive = T, full.names = FALSE) |>
  str_subset("Occurrences") |>
  as_tibble_col(column_name = "ad_files") |>
  mutate(adintel_dir,
         new_adintel_dir,
         file_type = ad_files |> str_split_i("/", -2),
         file_type2 = ad_files |> str_split_i("/", -1) |>
           str_remove_all(".tsv"),
         occurrences_file = file_type2 |> rename_adintel(),
         file_year = ad_files |> str_split_i("/", -3),
         .keep = "unused") |>
  nest(.key = "info", .by = occurrences_file)


local_tv_files <- occurrences_files |>
  filter(occurrences_file == "spot_tv") |>
  pluck("info", 1) |>


national_tv_files <- occurrences_files |>
  filter(occurrences_file == "network_tv") |>
  pluck("info", 1)

# ----

local_tv_list <- local_tv_files |>
  as.list() |>
  list_transpose(simplify = F)



lst0 <- local_tv_list[[1]]
year_lst_prev <- make_prev_year(adintel_dir, new_adintel_dir, file_type, file_type2, file_year) |> with(data = lst0)
year_lst_cur <- make_cur_year(adintel_dir, new_adintel_dir, file_type, file_type2, file_year) |> with(data = lst0)
lst <- c(year_lst_prev, year_lst_cur)[[1]]


fn <- function(lst){
  iwalk(lst, ~ assign(.y, .x, pos = 1))
  cmd <- sprintf( make_grep(year, month), input_file )
  out <- fread(
    cmd = cmd
  )


}

  names(year_lst_prev) |> map_vec(str2lang)

  lst_prev <- with(
    data = lst,
    make_prev_year(adintel_dir, new_adintel_dir, file_type, file_type2, file_year)
  )

  cmd <- with(
    data = lst_prev,
    sprintf( make_grep(year, month), input_file )
  )

  fread(
    cmd = cmd,
    sep = "\t",

  )

}



year_lst_prev



year_lst[[1]]

year_lst1[[1]]

iwalk(lst, ~ assign(x = .y, value = .x, pos = 1))

input_file <- make_input_file(adintel_dir, file_type, file_type2, file_year)
input_file <- make_output_file(adintel_dir, file_type, file_type2, file_year, year, month)





file_year <-  as.numeric(file_year)
tibble(
  adintel_dir,
  new_adintel_dir,
  file_type,
  file_type2,
  file_year,
  year = file_year,
  month = 1:12
) |>
  mutate(input_file = make_input_file(adintel_dir, file_year, file_type, file_type2),
         output_file = make_output_file(new_adintel_dir, file_year, file_type, file_type2, year, month),
         append = FALSE) |>
  as.list() |>
  list_transpose() |>
  set_names(glue("{file_type2}_{file_year}_current"))




lst$input_file <- with(
  data = lst,
  expr =

)




environmentName()








xyz <- with(data = local_tv_files,
     make_cur_year(adintel_dir, new_adintel_dir, file_type[1], file_type2[1], file_year[1]))

cmd <- with(xyz[[1]], make_grep(adintel_dir, file_year, file_type, file_type2, year , month) |>
  sprintf(input_file))

print(cmd)
x <- fread(cmd, sep = "\t", verbose = TRUE)



cmd <- map(xyz[1:2],
           ~ with(
             .x,
             make_grep(adintel_dir, file_year, file_type, file_type2, year, month)))
cmd[[1]] <-cmd[[1]] |>
  paste(paste(xyz[1]$SpotTV_2010_current$input_file))

cmd <- cmd |> paste(collapse = " | ")


cmd
fread(cmd, verbose = T, sep = "\t")


