library(tidyverse)
library(data.table)
library(adtabler)

path <-
path
var_attributes <- adtabler::adintel_tables |>
  filter(file_name == "brand") %>% pull(var_attributes) %>% pluck(1)
col_names <- var_attributes$var_name_manual %>% str_sep_upper("_", named = F)
col_classes <- names <- var_attributes$var_type_manual %>% adintel_to_sql()

fix_brand_df <- function(path, col_names, col_classes){
  tmpfile <- tempfile(fileext = ".tsv")
  data.table::fread(path, sep = "", quote = "", nThread = 24, header = F) %>%
    pluck(1) %>%
    str_remove_all('\t\"') %>%
    str_remove_all('\"') %>%
    write_lines(tmpfile)
  data.table::fread(tmpfile, sep = "\t", quote = "", nThread = 24, col.names = col_names, colClasses = col_classes)
}

fix_brand_df(path, col_names, col_classes)





paths <- adtabler::adintel_tables |>
  filter(file_name == "brand") %>% pull(path) %>% pluck(1)
map

x4 <- data.table::fread(path, sep = "\t", quote = "", nThread = 24, select = "AdDate")
rbind(x1,x3)
""
