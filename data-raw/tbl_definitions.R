library(tidyverse)
library(adtabler)

tbl_definitions_occ_ref <- tbl_definitions

tbl_definitions <- tbl_definitions |>
  unnest(tbl_name) |>
  select(-media_type_id : - keys, -adintel_manual)



 x <- list.files("/mnt/sata_data_1/adintel", full.names = T, recursive = T) |>
   str_subset("Master_Files", negate = T) |>
    file_to_info_dynamic() |>
    mutate(dynamic = TRUE) |>
    bind_rows(
       list.files( '/mnt/sata_data_1/adintel', full.names = TRUE, recursive = TRUE ) |>
          str_subset("Master_Files", negate = F) |>
          str_subset("2021") |>
          str_subset("Latest") |>
          file_to_info_static() |>
          mutate(dynamic = FALSE)
   )  |>
    select(-year : -file, -file_col_names) |>
    mutate(col_pos = file_classes |> map(seq_along),
           .before = file_col_names_std) |>
    unnest(everything()) |>
    mutate( file_classes = if_else(file_classes == "logical", NA, file_classes) ) |>
    distinct() |>
    rename(col_names_std = file_col_names_std) |>
    summarise(file_classes = file_classes[not_na(file_classes)][1],
              .by = !file_classes) |>
    full_join(
       tbl_definitions |>
          select(file_type_std : sql_datatype_min) |>
          unnest(everything()) |>
          distinct()
    ) |>
    mutate(
       tbl_name = tbl_name |>
          str_replace_all("spot_tv", "local_tv") |>
          str_replace_all("network_tv", "national_tv")
    ) |>
    arrange(file_type_std, file_name_std, tbl_name) |>
    select(-dynamic, -file_classes ) |>
    distinct() |>
    nest(.by = col_names_std) |>
    mutate(data = data |>
              set_names(col_names_std),
           data = data |>
              map2(col_names_std, ~ .x |>
                      mutate(col_names_std = .y,
                             .after = col_pos )
                   )
           ) |>
    distinct()
 |>
    unnest(everything())
 x

 tbl_info_tot <- x$data |> list_rbind() |>
    nest(.by = c( col_classes, sql_datatype_min )) |>
    mutate( col_classes = if_else(sql_datatype_min == 'TEXT', 'character', col_classes) ) |>
    unnest(everything()) |>
    distinct() |>
    relocate(sql_datatype_min, .after = sql_datatype) |>
    relocate(col_classes, .before = sql_datatype) |>
    arrange(file_type_std, file_name_std, tbl_name, col_pos) |>
    summarise(across(col_pos : sql_datatype_min, list),
              .by = file_type_std : tbl_name) |>
    arrange(file_type_std, file_name_std, tbl_name)


 |>
    nest(.by = file_col_names_std) |>
    mutate(data = data |>
              set_names(file_col_names_std),
           data = data |>
              map2(file_col_names_std, ~ .x |> mutate(file_col_names_std = .y, .after = col_pos )),
           has_na = data |> map_vec( ~ any(is.na(.x$file_classes)) )) |>
    filter(has_na)
 x$data
 |>
    pull(data)
x
    summarise( )
    filter()
    arrange(file_type_std, file_name_std, tbl_name, col_pos) |>
    distinct()
 |>
    summarise(across(col_pos : file_classes, list),
              .by = file_type_std : tbl_name) |>
    arrange(file_type_std, file_name_std, tbl_name)




|>
    filter()
    mutate(col_pos = file_classes |> map(seq_along),
           .before = file_col_names_std) |>
    filter(file_name_std |> not_in( tbl_definitions_occ_ref$file_name_std ))
    rename(col_names_std = file_col_names_std,
           col_classes = file_classes) |>
    unnest(everything()) |>
    mutate(sql_datatype = NA,
           sql_datatype_min = NA) |>
    distinct() |>
    summarise(
       across(col_pos : sql_datatype_min, list),
       .by = file_type_std : tbl_name
    )



 x |> print(n=1000)

 |>
    filter(tbl_name |> not_in(tbl_definitions$tbl_name))


   map(~ .x |> file_to_info_dynamic()) |>
   list_rbind() |>
   select(-year : -file, -file_col_names) |>
   distinct() |>
   mutate(col_pos = file_classes |> map(seq_along),
          .before = file_col_names_std) |>
   filter(tbl_name |> not_in(tbl_definitions$tbl_name)) |>
   rename(col_names_std = file_col_names_std,
          col_classes = file_classes) |>
   unnest(everything()) |>
   mutate(sql_datatype = NA,
          sql_datatype_min = NA) |>
   bind_rows(tbl_definitions |> unnest(everything())) |>
   mutate(col_classes = if_else(col_classes == "logical", NA, col_classes)) |>
   nest(.by = col_names_std) |>
   mutate(data = set_names(data, col_names_std)) |>
   pull(data) |>
   imap(~ .x |>
          mutate(
            col_classes = col_classes[not_na(col_classes)][1]
          ) |>
          mutate(
            across(
              sql_datatype : sql_datatype_min,
              ~ case_when(
                !all(is.na(.x)) ~ .x[not_na(.x)][1],
                all(is.na(.x)) & col_classes == "character" ~ "TEXT",
                all(is.na(.x)) & col_classes == "integer" ~ "INTEGER",
                .default = NA
              )
            )
          ) |>
          mutate(col_names_std = .y, .after = col_pos)
        ) |>
   list_rbind() |>
   distinct() |>
   arrange(tbl_name, col_pos) |>
   mutate( col_classes = if_else( col_names_std %in% c("impression_start", "impression_end"), "character", col_classes),
           sql_datatype = if_else( col_names_std %in% c("impression_start", "impression_end"), "DATETIME", sql_datatype),
           sql_datatype_min = if_else( col_names_std %in% c("impression_start", "impression_end"), "TEXT", sql_datatype_min)) |>
   summarise(across(col_pos : sql_datatype_min, list),
             .by = file_type_std : tbl_name) |>
   distinct()|>
    left_join(tbl_definitions_occ_ref |>
                 unnest(tbl_name) |>
                 select( -col_pos : -sql_datatype_min),
              by = c("file_type_std", "file_name_std", "tbl_name"))

tbl_definitions <-  x

usethis::use_data(tbl_definitions_occ_ref, overwrite = T)
usethis::use_data(tbl_definitions, overwrite = T)
