library(data.table)
library(dtplyr)
library(tidyverse)
library(rlang)
library(glue)
# devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(bit64)


adintel_dir <- "/mnt/sata_data_1/adintel/"
new_adintel_dir <- "/mnt/sata_data_1/new_adintel/"

pcc_df <-  list.files(adintel_dir, full.names = T, recursive = T) |>
  as_tibble_col("input_file") |>
  filter(input_file |> str_detect("Master_File",negate = T),
         input_file |>  str_detect("References") ) |>
  filter( str_detect(input_file, "ProductCategories" ) |
            str_detect(input_file, "Advertiser" ) |
            str_detect(input_file, "Brand" )) |>
  mutate(value = str_split_i(input_file, "/" , -1),
         dir = input_file |>  str_remove_all(value),
         name = value |> str_remove_all( ".tsv"),
         file_year = str_split_i(input_file, "/" , -3)) |>
  select(- input_file) |>
  pivot_wider() |>
  rename_with(.cols = everything(), rename_adintel)


dir <- pcc_df$dir[1]
br <- pcc_df$brand[1]
ad <- pcc_df$advertiser[1]
pc <- pcc_df$product_categories[1]
file_year <- pcc_df$file_year[1]

br_df <- fread(
  paste(dir, br, sep = "/")
)
ad_df <- fread(
  paste(dir, ad, sep = "/")
)
pc_df <- fread(
  paste(dir, pc, sep = "/")
)

brand_ref <- br_df |> left_join(ad_df) |> left_join(pc_df) |>
  mutate(file_year = file_year,
         .before = 1)

f <- function(x){
  if(is.character(x)){
    return(unique(x))
  }
}

uni_list <- map(brand_ref, f)[c("BrandDesc",
  "BrandVariant",
  "AdvParentDesc",
  "AdvSubsidDesc",
  "PCCSubDesc",
  "PCCMajDesc",
  "PCCIndusDesc",
  "ProductDesc") ]

brand_ref |>
  group_by(BrandCode) |>
  tally() |> summary()





cmd <- sprintf("grep -Pn '\t\"' %s",
               x$input_file[9])
df0 <- fread(
  cmd = cmd,
  sep = "",
  quote = "",
  header = F
)
id0 <- df[[1]] |> str_split_i(":", 1) |> as.numeric()

df1 <- fread(
  x$input_file[9],
  sep = "",
  quote = "",
  header = F
)
df1$V1[id0] <- df1$V1[id0] |> str_replace_all('\t\"', '\"')

df2 <- fread(
  text = df1$V1,
  sep = '\t'
)
df2
# df1 <- df |>
#   str_which('\t\"')



