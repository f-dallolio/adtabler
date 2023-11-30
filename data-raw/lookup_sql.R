 |> |>
tbl_info_tot$sql_datatype |>
  list_c() |>
  str_split_i("\\(", 1) |>
  unique()

new_lookup_sql <- read_csv('~/Documents/sqlr.csv') |>
  mutate(id = row_number(),
         type1 = Name |> str_split_i("\\[", 1) |> str_squish(),
         type2 = Aliases |> str_split_i("\\[", 1) |> str_squish(),
         spec = Name |> str_split_i("\\[", 2) |>
           # str_remove_all(" ") |>
           str_remove_all("\\(") |>
           str_remove_all("\\)")|>
           str_remove_all("\\]") |>
           str_squish(),
         .before = 1,
         .keep = 'unused') |>
  # pivot_longer(cols = contains("type")) |>
  mutate(datatype_std = map2(rename_adintel(type1), rename_adintel(type2),~ .x |> c(.y[not_na(.y)]))) |>
  mutate(spec2 = if_else(str_detect(spec, 'with time zone'), 'with_time_zone', NA),
         spec = spec |> str_remove_all('with time zone') |> str_squish(),
         .after =spec) |>
  unnest(everything())|>
  left_join(adtabler::lookup_datatype |>
              select(datatype_r, datatype_std)) |>
  mutate(
    datatype_r_full = case_when(
      str_detect(Description, "integer") ~ "integer",
      str_detect(Description, "boolean") ~ "logical",
      str_detect(Description, "logical") ~ "logical",
      str_detect(Description, "character") ~ "character",
      str_detect(Description, "date") ~ "character",
      str_detect(Description, "time") ~ "character",
      str_detect(Description, "text") ~ "character",
      str_detect(Description, "number") ~ "double",
      str_detect(Description, "numeric") ~ "double",
      str_detect(Description, "amount") ~ "double"
    ),
    datatype_r = case_when(
      is.na(datatype_r) & str_detect(datatype_std, 'time')~ "POSIXct",
      is.na(datatype_r) & str_detect(datatype_std, 'date')~ "POSIXct",
      is.na(datatype_r) & not_na(datatype_r_full) ~ datatype_r_full,
      .default = datatype_r
    )
  ) |>
  select(- datatype_std) |>
  distinct() |>
  mutate(
    type2 = str_split_i(type2, ', ', 1),
    across(everything(), list),
    .by = !contains("datatype_r")
  ) |>
  unnest(everything()) |>
  print(n=200)

xx <- tbl_info_tot$sql_datatype |> list_c() |> str_split_i("\\(", 1) |> rename_adintel() |> unique()

x1 <- new_lookup_sql|>
  select(id:type2) |>
  pivot_longer(2:3, values_drop_na = T) |>
  summarise(value = list(value), .by = id) |>
  mutate(any_ok = value |>  map(~ which(.x %in% xx) |> str_flatten_comma()) |> simplify() |> as.integer() |>
           replace_na(1),
         type = map2_chr(value, any_ok, ~ .x[[.y]])) |>
  unnest(value) |>
  select(-2:-3) |>
  distinct() |>
  inner_join(
    new_lookup_sql |>
      select(-type1, -type2)
  ) |>
  print(n=200)

x1$spec[not_na(x1$spec2)] <- glue('{x1$spec[not_na(x1$spec2)]} {x1$spec2[not_na(x1$spec2)]}')
lookup_sql <- x1 |>
  select(-spec2) |>
  distinct() |>
  rename(col_class_full = datatype_r,
         col_class = datatype_r_full,
         col_type = type)
lookup_sql$col_class_full[lookup_sql$col_type == 'interval'] <- 'difftime'
lookup_sql |> print(n=100)
usethis::use_data(lookup_sql, overwrite = T)


usethis::use_data(lookup_sql, overwrite = TRUE)
