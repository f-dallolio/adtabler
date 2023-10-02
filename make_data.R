library(tidyverse)
library(data.table)
# devtools::install_github("f-dallolio/adtabler")
library(adtabler)


brand_tables <- adintel_tables %>%
  filter(file_name == "brand")

path <- brand_tables$
