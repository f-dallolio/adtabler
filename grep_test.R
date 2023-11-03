devtools::install_github("f-dallolio/adtabler")
library(data.table)
library(tidyverse)
library(adtabler)

data_info_list$file_info |>
  filter(file_type_std == "occurrences",
         file_name_std == "spot_tv") |>
  distinct(col_name_std)

data_info_list$file_info |>
  filter(file_type_std == "occurrences",
         file_name_std == "spot_tv") |>
  distinct(col_name_std)

data_info_list$file_info |>
  filter(file_type_std == "references",
         file_name_std == "market") |>
  pull(full_file_name) |>
  read_tsv() |>
  pull(MarketCode) |>
  map_vec(~ .x |> str_split_1("") |> is_numeric2()) |>
  all()

file <- data_info_list$file_info |>
  filter(file_type_std == "occurrences",
         file_name_std == "spot_tv",
         year == 2010) |>
  pull(full_file_name)

nms <- data_info_list$file_info |>
  filter(file_type_std == "occurrences",
         file_name_std == "spot_tv",
         year == 2010) |>
  pull(col_name_std) |>
  str_split_comma()
nms_collapsed <- nms |>
  str_flatten("\t")

pattern <- c("^[0-9]{4}[-][0-9]{2}[-][0-9]{2}","[0-9]{2}[:][0-9]{2}[:][0-9]{2}.*","[0-9]{3}","5") |>
  str_flatten("\t")

add <- function(x1, x2){
  x1+x2
}
`+`



ad_date_grep(2010,01,01)
ad_time_grep <- function(h = NULL, m = NULL, s = NULL){
  if(is.null(h)){
    hh <- "[0-9]{2}"
  } else {
    hh <- numpad2(as.numeric(h))
  }
  if(is.null(m)){
    mm <- "[0-9]{2}"
  } else {
    mm <- numpad2(as.numeric(m))
  }
  if(is.null(s)){
    ss <- "[0-9]{2}"
  } else {
    ss <- numpad2(as.numeric(s))
  }
  paste0(
    paste(c(hh, mm, ss), collapse = ":"),
    ".*"
  )
}
market_code_grep <- function(market_code = NULL){
  if(is.null(market_code)) {
    return("[0-9]{3}")
  }
  numpad(as.numeric(market_code, 3))
}
media_type_id_grep <- function(media_type_id = NULL){
  if(is.null(media_type_id)){
    return("[0-9]+")
  } else {
    return(as.numeric(media_type_id))
  }
}



w1 <- as.Date(14610) |>
  tsibble::yearweek() |>
  as.Date() |>
  add(0:6)

ymd(w1)

tmp <- tempfile()
www <- tibble(
  as.character(ymd(w1)),
  ad_time_grep(),
  market_code_grep(),
  media_type_id_grep(5)
 )
names(www) <- nms[seq_along(www)]
write_tsv(www, tmp)
#
#
#
# |>
#   as.list() |>
#   list_transpose() |>
#   map_vec(~ paste(.x, collapse = "\t"))
# www |>
#   # as_tibble_col("x") |>
#   # write_tsv(pattern_file, col_names = F)
#   paste(collapse = "\n") |>
#   write_lines(tmp)

read_lines(tmp)

# cmd <- sprintf( glue::glue("grep -Pw '{www[1]}' %s"), file )
cmd <- sprintf( glue::glue("grep -E -w -f {tmp} %s"), file )


# t1 <- Sys.time()
# x <- system(cmd, intern = TRUE)
# xx <- fread(text = c(nms, x))
# t2 <- Sys.time()
# t_xx <- t2-t1
# t_xx

t1 <- Sys.time()
yy <- fread(cmd = cmd, col.names = nms)
# names(yy) <- nms
t2 <- Sys.time()
t_yy <- t2-t1
t_yy
yy


pattern_5 <- c("[0-9]{4}-[0-9]{2}-[0-9]{2}","[0-9]{2}:[0-9]{2}:[0-9]{2}.*","[0-9]{3}","5") |>
  str_flatten("\t")
pattern_13 <- c("[0-9]{4}-[0-9]{2}-[0-9]{2}","[0-9]{2}:[0-9]{2}:[0-9]{2}.*",".{3}","13") |>
  str_flatten("\t")
pattern_14 <- c("[0-9]{4}-[0-9]{2}-[0-9]{2}","[0-9]{2}:[0-9]{2}:[0-9]{2}.*","[0-9]{3}","14") |>
  str_flatten("\t")
pattern_24 <- c("[0-9]{4}-[0-9]{2}-[0-9]{2}","[0-9]{2}:[0-9]{2}:[0-9]{2}.*","[0-9]{3}","24") |>
  str_flatten("\t")

pt <- c(pattern_5, pattern_13, pattern_14, pattern_24) |>
  map_vec(~ sprintf( glue::glue("grep -m 100 -Ew '{.x}' %s"), file ))|>
  map(~ fread(cmd = .x, header = F))

cmd <- sprintf( glue::glue("grep -m 10 -E '^{ptrn}$' %s"), file )
zzz <- fread(cmd = cmd, header = F)
zzz


cmd1 <- sprintf( glue::glue("grep -m 10 -P '{www |> slice(1) |> str_flatten('\t')}' %s"), file )
yy1 <- fread(cmd = cmd1, header = F)
# identical(yy, xx, single.NA = F)


ptrn <- fread(file, nrows = 1) |>
  mutate(across(everything(), ~ ".*")) |>
  rename_with(rename_adintel, everything()) |>
  mutate(media_type_id = "XX") |>
  as.list() |>
  list_c() |>
  str_flatten("\t")

as.character(c(5,13,14,24)) |>
  map(~ str_replace_all(ptrn, "XX", .x)) |>
  map_vec(~ sprintf( glue::glue("grep -m 1000 -Ew '^{.x}$' %s"), file )) |>
  map(~ fread(cmd = .x, header = F))

occurrences_categories <- occurrences_categories |> filter(file_type_std == "occurrences")
  usethis::use_data(occurrences_categories, overwrite = T)







