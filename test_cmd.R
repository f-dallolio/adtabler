
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(tsibble)

encode_chr <- function(x, from = "latin1", to = "UTF-8", ...){
  Encoding(x) <- from
  iconv(x, from = from, to = to, ...)
}

load("Documents/r_wd/adtabler/data-raw/adintel_info_list.rda")
load("Documents/r_wd/adtabler/data/media_type_table.rda")

media_type_table |>
  select(file_name, media_type, media_type_id) |>
  distinct()

colClasses <- adintel_info_list$occurrences$spot_tv$col_info_manual |>
  adtabler::adintel_to_sql(r_out = T)
col.names <- adintel_info_list$occurrences$spot_tv$col_names


file_year <- 2021
spot <- glue::glue("/mnt/e49b98b9-f855-4560-8453-64fa45844c21/adintel/ADINTEL_DATA_{file_year}/nielsen_extracts/AdIntel/{file_year}/Occurrences/SpotTV.tsv")
file.size(spot)/1024^3

year = "2020"
month = "12"

library(tictoc)
tic()
# write("Documents/digital_2021_01.txt")
cmd_glue1 <- paste0("grep --text '",year,"\\-", month, "\\-' %s")
command1 <- sprintf(cmd_glue1, spot)
command1

df1 <- data.table::fread(
  cmd = command1,
  sep = "\t",
  # quote = "",
  header = FALSE,
  skip = 1,
  nThread = 30,
  # nrows = 1000000,
  colClasses = colClasses,
  integer64 = "character",
  strip.white = TRUE,
  col.names = col.names,
  key = c("media_type_id", "ad_date", "ad_time"),
  # encoding = "latin1",
  verbose = TRUE,
  fill = TRUE,
  blank.lines.skip = TRUE
)
toc()

tic()
df2 <- df1 |>
  as_tibble() |>
  mutate(across(where(is.character),
                ~ encode_chr(.x, from = "latin1", to = "UTF-8")))
toc()





min(df1$ad_time)

spot2 <- "/mnt/e49b98b9-f855-4560-8453-64fa45844c21/adintel/ADINTEL_DATA_2021//nielsen_extracts/AdIntel/2021/Occurrences/SpotTV.tsv"

command2 <- sprintf(
  'grep --text "\\-12\\-" %s' ,
  spot2
)

df2 <- data.table::fread(
  # input = spot,
  cmd = command2,
  # sep = "\t",
  # quote = "",
  # header = FALSE,
  # skip = 1,
  # nThread = 24,
  colClasses = colClasses,
  integer64 = "character",
  # strip.white = TRUE,
  col.names = col.names,
  encoding = "Latin-1",
  verbose = TRUE
  # blank.lines.skip = TRUE
) |> as_tibble()
