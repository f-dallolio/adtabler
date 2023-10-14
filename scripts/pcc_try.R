library(data.table)
library(dtplyr)
library(tidyverse)
library(rlang)
library(glue)
devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(bit64)
library(dm)


# adtabler::adintel_info |>
#   filter(file_type == "references") |>
#   pull(data_info) |>
#   map(
#     ~ as_tibble(.x[-c(3:5)]) |>
#       mutate(col_classes_sql = adintel_to_sql(col_info_manual)) |>
#       rename(
#         file_type2 = file_name,
#         col_classes_r = col_info_data
#       ) |>
#       relocate(col_classes_r, .after = col_names) |>
#       select(-col_info_manual)
#   ) |> list_rbind()

prod_table <- list.files(path = '/mnt/sata_data_1/new_adintel/', recursive = TRUE, full.names = TRUE) |>
  as_tibble_col(column_name = "file_path") |>
  mutate(
    file_type2 = file_path |> str_split_i("/", -2),
    file = file_path |> str_split_i("/", -1)
  ) |>
  filter(file_path  |> str_detect("references")) |>
  filter(file_type2 %in% c("brand", "advertiser", "product_categories")) |>
  mutate(year = file |> str_remove_all(".csv") |>
           str_split_i("__", -1) |>
           as.integer())

prod_table |> print(n=200)

prod_list <- prod_table |>
  mutate(year = paste0("y", year)) |>
  select(-file) |>
  split(prod_table$year) |>
  map(~ .x |>
        pivot_wider(names_from = file_type2, values_from = file_path) |>
        mutate(year = year |> str_remove_all("y") |> as.integer()))

df <- prod_list[[9]]

make_products_tbl <- function(df){
  year = df$year
  brd <- read_csv(df$brand, show_col_types = F) |>
    rename_with(rename_adintel) |>
    distinct()
  check_brd <- check_utf8(brd)
  print(glue::glue('"brand" { year }: { check_brd }'))

  adv <- read_csv(df$advertiser, show_col_types = F) |>
    rename_with(rename_adintel) |>
    distinct()
  if (year == 2018) {
    adv <- adv |>
      summarise(adv_parent_desc = paste(unique(adv_parent_desc), collapse = "__"),
                adv_subsid_desc = paste(unique(adv_subsid_desc), collapse = "__"),
                .by = c(adv_parent_code, adv_subsid_code))
  }
  check_adv <- check_utf8(adv)
  print(glue::glue('"advertiser" { year }: { check_adv }'))

  pcc <- read_csv(df$product_categories, show_col_types = F) |>
    rename_with(rename_adintel)  |>
    distinct()
  check_pcc <- check_utf8(pcc)
  print(glue::glue('"product_categories" { year }: { check_pcc }'))

  dm_products_no_key <- dm(brd, adv, pcc)
  dm_products <- dm_products_no_key |>
    dm_add_pk(table = brd, columns = c(brand_code)) |>
    dm_add_pk(table = adv, columns = c(adv_parent_code, adv_subsid_code)) |>
    dm_add_pk(table = pcc, columns = c(pcc_sub_code, product_id)) |>
    dm_add_fk(table = brd, columns = c(adv_parent_code, adv_subsid_code), ref_table = adv) |>
    dm_add_fk(table = brd, columns = c(pcc_sub_code, product_id), ref_table = pcc)
  dm_products |> dm_examine_constraints() |> suppressMessages() |> print()
  dm_flatten_to_tbl(dm = dm_products, .start = brd, .recursive = TRUE) |>
    mutate(year = year, .before = 1)
}

all_products <- vector(mode = "list", length = length(prod_list))

i_seq <- seq_along(all_products)
for (i in seq_along(i_seq)) {
  all_products[[i]] <- make_products_tbl(prod_list[[i]])
}

products_df <- all_products |>
  list_rbind() |>
  # select(year, brand_code, where(is.character), product_id) |>
  relocate(adv_parent_desc, adv_subsid_desc, .after = brand_variant)


products_df |>
  summarise(chr_year = paste0(year, collapse = "_"),
            .by = !year)

.Last.value |> view()







# brand_df <- map2(brand_table$file_path, brand_table$year, ~ read_csv(.x) |>
#                    mutate(year = .y, .before = 1) |>
#                    rename_with(~ rename_adintel(.x), .cols = everything())) |>
#   list_rbind() |>
#   left_join(date_year_minmax) |>
#   arrange(pcc_sub_code, product_id, brand_code) |>
#   select(-min_date, -max_date) |>
#   summarise(chr_year = paste0(year, collapse = "_"),
#             .by = !year)
#
#
# x <-brand_df |>
#   select(chr_year) |>
#   distinct() |>
#   mutate(year = chr_year |>
#            map(~ .x |> str_split("_") |>
#                  list_c() |>
#                  as.integer() |>
#                  as_tibble_col(column_name = "year"))) |>
#   unnest(everything()) |>
#   mutate(gr = group_consecutive(as.numeric(year)),
#          .by = chr_year) |>
#   summarise(min_year = min(year),
#             max_year = max(year),
#             .by = c(chr_year,gr)) |>
#   select(-gr) |>
#   left_join(date_year_minmax |> select(year, min_date),
#             by = c("min_year" = "year")) |>
#   left_join(date_year_minmax |> select(year, max_date),
#             by = c("max_year" = "year"))
#
# brand_df |> left_join(x |> nest(.by = chr_year))
#
# c(1,3,5) |>
#
# pull(1) |>
#   map(~str_split(.x, "_"))
#
#
# |>
#   nest(year = year)
# brand_df |>
#   pull(year) |>
#   map(~ .x |>
#         mutate(gr = group_consecutive(year)) |>
#         summarise(min = min(year),
#                   max = max(year),
#                   .by = gr) |>
#         select(-gr), .progress = T)
#
# brand_df |>
#   mutate(data = data |> )
#
#
#
# x <- c(2010, 2013:2018, 2020, 2021)
# group_consecutive <- function(x){
#   c(TRUE, diff(x) != 1) |> cumsum()
# }
#
# brand_dm <- dm(brand_df)
#
# brand_list_1 <- brand_df |> decompose_table(new_id_column = brand_names_id, brand_desc, brand_variant)
# brand_list_1$child_table |>
#   distinct()|> decompose_table(new_id_column = brand_dates_id, year, contains("date"))
#
# brand_dm <- brand_dm |>
#   dm_add_pk(table = brand_df, columns = c(year, brand_code))
# dm_examine_constraints(brand_dm)
#
# nest(.by = c(brand_desc, brand_variant)) |>
#   mutate(brand_id = row_number())
#
# x <- .Last.value
#
#
# names(brand_df) <- names(brand_df) |>
#   rename_adintel()
#
# brand_df$brand_code |> table() |> sort(decreasing = T)
#
# i=1
# for(i in seq_along(brand_table$file_path)){
#   file <- brand_table$file_path[[i]]
#   year <- brand_table$year[[i]]
#   if(i == 1){
#     brand_df <- read_csv(file)
#   } else {
#     brand |>
#   }
# }
#
#
#
#
#
# |>
#   filter(file_type2 %in% c("advertiser", "brand", "product_categories")) |>
#
#
#   prod_table |> print(n=200)



x <- all_products



xx_1 <- x |>
  nest(year = year) |>
  # mutate(name = str_c("y", year),
  #        value = 1,
  #        .keep = "unused") |>
  # pivot_wider() |>
  arrange(pcc_indus_code, pcc_maj_code, pcc_sub_code, product_id, brand_desc) |>
  separate_wider_delim(cols = adv_parent_desc, names = c("adv_parent_desc_1", "adv_parent_desc_2"), delim = "__", cols_remove = TRUE, too_few = "align_start") |>
  pivot_longer(adv_parent_desc_1 : adv_parent_desc_2, values_to = "adv_parent_desc", values_drop_na = TRUE) |>
  select(-name) |>
  separate_wider_delim(cols = adv_subsid_desc, names = c("adv_subsid_desc_1", "adv_subsid_desc_2"), delim = "__", cols_remove = TRUE, too_few = "align_start") |>
  pivot_longer(adv_subsid_desc_1 : adv_subsid_desc_2, values_to = "adv_subsid_desc", values_drop_na = TRUE) |>
  select(-name) |>
  relocate(year, .after = -1)

xx_2 <-  xx_1 |>
  unnest(everything()) |>
  distinct()

xx_2 |> mutate(name = str_c("y", year),
               value = 1,
               .keep = "unused") |>
  pivot_wider()

xxx <- xx_2 |>
  nest(year = year) |>
  mutate(year = year |> map(~ .x |> transmute(name = str_c("y", year), value = 1) |> pivot_wider()))

xxx$year
mutate(n = data |> map(~ n_distinct(year1 = year)) |> list_c(),
       data = data |> map() )

c(1, as.numeric(diff(c(2010:2018, 2020:2021)) == 1 )) |> cumsum()

