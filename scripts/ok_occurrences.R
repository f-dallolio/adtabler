library(data.table)
library(dtplyr)
library(tidyverse)
library(rlang)
library(glue)
devtools::install_github("f-dallolio/adtabler")
library(adtabler)
library(bit64)


fn <- function(...){
  out <- enquos(..., .named = TRUE)
  names(out)
}

check_utf8 <- function(x){
  is_df <- is.data.frame(x)
  if (is_df) {
    x_chr <- map_vec(x, is.character) |>
      which(useNames = TRUE)
    out <- map_vec(x_chr, ~ utf8::utf8_valid(x[[.x]]) |> all()) |>
      set_names(names(x_chr))
    no_utf8 <- names(out[!out])
    if(length(no_utf8) == 0){
      return("OK")
    } else {
      return(str_c('The following columns have invalid UTF-8 elements: c(',
                   glue:: glue_collapse(no_utf8, sep = ", "), ')'))
    }
  } else {
    out <- utf8::utf8_valid(x) |> all()
    if(out) {
      return("OK")
    } else {
      return('There are invalid UTF-8 elements')
    }
  }
}



paths_df <- list.files(path = '/mnt/sata_data_1/new_adintel/', recursive = TRUE, full.names = TRUE) |>
  as_tibble_col(column_name = "file_path") |>
  # mutate(size_mb = file.size(file_path)/1024^2) |>
  mutate(
    short_path = list.files(path = '/mnt/sata_data_1/new_adintel/', recursive = TRUE),
    num_subfolders = short_path |> str_count("/"),
    file_type = short_path |> str_split_i("/", 1),
    file = short_path |> str_split_i("/", -1)
  )  |>
  nest(.by = file_type)
paths_df    |> print(n = 700)




occurrences_df <- paths_df |>
  filter(file_type == "occurrences") |>
  unnest(data) |>
  filter(str_detect(file, "ok__", negate = T)) |>
  mutate(
    file = file |>
      str_remove_all(".csv"),
    media_type_id = file |>
      str_split_i("__", 1) |>
      str_remove_all("id") |>
      as.integer(),
    file_year = file |>
      str_split_i("__", 2) |>
      as.integer(),
    chunk = file |>
      str_split_i("__", 3) |>
      str_remove_all("chunk") |>
      as.integer()
  ) |>
  nest(.by = media_type_id)






occurrences_ukey_table <- list(
  network_tv = fn(AdDate, AdTime, MarketCode, MediaTypeID, DistributorCode, AdCode, GrpPercentage, MonitorPlusProgramCode) |> rename_adintel(),
  spanish_language_network_tv = fn(AdDate, AdTime, MarketCode, MediaTypeID, DistributorCode, AdCode, MonitorPlusProgramCode) |> rename_adintel(),
  cable_tv = fn(AdDate, AdTime, MarketCode, MediaTypeID, DistributorCode, AdCode, MonitorPlusProgramCode) |> rename_adintel(),
  spanish_language_cable_tv = fn(AdDate, AdTime, MarketCode, MediaTypeID, DistributorCode, AdCode) |> rename_adintel(),
  syndicated_tv = fn(AdDate, AdTime, MarketCode, MediaTypeID, DistributorCode, AdCode, NielsenProgramCode, TelecastNumber) |> rename_adintel(),
  spot_tv = fn(AdDate, AdTime, MarketCode, MediaTypeID, DistributorCode, AdCode) |> rename_adintel(),
  network_clearance_spot_tv = fn(AdDate, AdTime, MarketCode, MediaTypeID, DistributorCode, AdCode) |> rename_adintel(),
  syndicated_clearance_spot_tv = fn(AdDate, AdTime, MarketCode, MediaTypeID, DistributorCode, AdCode) |> rename_adintel(),
  local_regional_cable_tv = fn(AdDate, AdTime, MarketCode, MediaTypeID, DistributorCode, AdCode) |> rename_adintel(),
  national_magazine = fn(AdDate, MarketCode, MediaTypeID, DistributorID, Spend, AdCode, AdNumber) |> rename_adintel(),
  local_magazine = fn(AdDate, MarketCode, MediaTypeID, DistributorCode, Spend, AdCode) |> rename_adintel(),
  fsi_coupon = fn(AdDate, MarketCode, MediaTypeID, AdCode, SourceCode, OffValue, CouponID) |> rename_adintel(),
  national_newspaper = fn(AdDate, MarketCode, MediaTypeID, DistributorCode, Spend, AdCode, NewspAdSize, NewspEventCode, NewspSecCode) |> rename_adintel(),
  national_sunday_supplement = fn(AdDate, MarketCode, MediaTypeID, DistributorCode, Spend, AdCode, NewspAdSize, NewspEventCode, NewspSecCode) |> rename_adintel(),
  local_newspaper = fn(AdDate, MarketCode, MediaTypeID, DistributorCode, Spend, AdCode, NewspAdSize, NewspEventCode, NewspSecCode) |> rename_adintel(),
  local_sunday_supplement = fn(AdDate, MarketCode, MediaTypeID, DistributorCode, Spend, AdCode, NewspAdSize, NewspEventCode, NewspSecCode) |> rename_adintel(),
  network_radio = fn(AdDate, MarketCode, MediaTypeID, DistributorCode, AdCode) |> rename_adintel(),
  spot_radio = fn(AdDate, MarketCode, MediaTypeID, DistributorCode, AdCode, AdTime, RadioDaypartID) |> rename_adintel(),
  outdoor = fn(AdDate, MarketCode, MediaTypeID, DistributorCode, AdCode) |> rename_adintel(),
  national_internet = fn(AdDate, MarketCode, MediaTypeID, DistributorCode, AdCode) |> rename_adintel(),
  local_internet = fn(AdDate, MarketCode, MediaTypeID, DistributorCode, AdCode) |> rename_adintel(),
  national_cinema = fn(AdDate, MarketCode, MediaTypeID, AdCode) |> rename_adintel(),
  regional_cinema = fn(AdDate, MarketCode, MediaTypeID, AdCode) |> rename_adintel(),
  national_digital = fn(AdDate, MarketCode, MediaTypeID, DistributorCode, AdCode, AdPlatformId, AdTypeId) |> rename_adintel()
) |>
  imap(~ tibble(name = .y, unique_key = .x)) |>
  list_rbind() |>
  nest(unique_key = unique_key) |>
  full_join(
    media_type_table |>
      transmute(
        file_type,
        file_type2 = file_name,
        media_type_id,
        name = nielsen_media_definition |>
          rename_adintel() |>
          str_remove_all("/")
      ) |>
      distinct()
  )
occurrences_ukey_table





adintel_df_names <- adintel_tables$var_attributes |>
  map(as_tibble) |>
  list_rbind() |>
  filter(!is.na(var_name_manual),
         file_type == "occurrences") |>
  rename(file_type2 = file_name) |>
  mutate(col_names_r = var_name_manual |>
           rename_adintel(),
         col_classes_r = var_type_manual |>
           adintel_to_sql(r_out = TRUE),
         col_names_sql = var_name_manual |>
           rename_adintel(),
         col_classes_sql = var_type_manual |>
           adintel_to_sql(),
         .before = precision) |>
  select(- var_name_manual, - var_type_manual, -is_dynamic) |>
  nest(.by = c(file_type, file_type2), .key = "data_adintel") |>
  full_join(occurrences_ukey_table) |>
  mutate(col_names = data_adintel |>
           map(~ .x |> select(col_names_r)),
         col_classes = data_adintel |>
           map(~ .x |> select(col_classes_r)),
         col_info_sql = data_adintel |>
           map(~ .x |> select(-col_names_r, -col_classes_r))
  ) |>
  select(-data_adintel)
adintel_df_names




col_names_ukey <- adintel_df_names |>
  full_join(occurrences_df) |>
  mutate(file_path = data |> map(~.x |> select(file_path))) |>
  relocate(media_type_id,
           file_path,
           .after = name) |>
  mutate(ukey_ok = map2(unique_key, col_names, ~ .x[[1]] %in% .y[[1]] |> set_names(.x[[1]])),
         ukey_ok_all = ukey_ok |> map(~ all(.x)) |> list_c())
col_names_ukey
col_names_ukey |> pull(ukey_ok_all) |> all()


files_df <- col_names_ukey |>
  unnest(file_path) |>
  arrange(media_type_id) |>
  select(file_path, unique_key, col_names, col_classes)

i=1
file_seq  <- seq_along(files_df$file_path)
file_seq_len <- length(file_seq)
utf8_out <- rep(NA, file_seq_len)


for (i in file_seq[-(1:252)]){

  file <- files_df$file_path[i]
  file_name <- file |> str_split_i("/", -1)
  file_name_id <- file_name |> str_remove_all(".csv")
  file_dir <- file |> str_remove_all(file_name)
  new_file <- paste0(file_dir, "ok__", file_name)
  key <- files_df$unique_key[[i]][[1]]
  col.names <- files_df$col_names[[i]][[1]]
  colClasses <- files_df$col_classes[[i]][[1]]
  file_size <- file.size(file)/1024^2
  if (file_size >= 1024) {
    file_size <- str_c((file_size/1024) |> round(2), " GB")
  } else {
    file_size <- str_c(file_size |> round(2), " MB")
  }

  t0 <- Sys.time()
  t0_chr <- as.character(t0) |> str_split_i('\\.', 1)
  print( str_c('Starting ', i, '/', file_seq_len, ' - ', file_name, " (", file_size,")") )
  print( str_c('Starting time: ', t0_chr) )

  temp_df <- fread(
    file = file,
    colClasses = colClasses,
    col.names = col.names,
    key = key,
    nThread = 24
  )

  if(NROW(temp_df) > 0){

    has_ad_time <- "ad_time" %in% col.names
    if (has_ad_time) {
      len_ad_time <- nchar(temp_df$ad_time[1])
      if(len_ad_time > 8){
        temp_df$ad_time <- str_sub(temp_df$ad_time, 1, 8)
      }
    }

    t1 <- Sys.time()
    elapsed1 <- t1 - t0
    print('Data loaded in:')
    print(elapsed1)

    utf8_out[i] <- check_utf8(temp_df)

    t2 <- Sys.time()
    elapsed2 <- t2 - t1
    print('UTF8 checked in:')
    print(elapsed2)

    fwrite(
      x = temp_df,
      file = new_file,
      nThread = 24
    )

    t3 <- Sys.time()
    elapsed3 <- t3 - t0
    print( str_c('Finished ', i, '/', file_seq_len, ' - ', file_name, " (", file_size,")", ' in:') )
    print(elapsed3)


  }

}

# source('~/Documents/r_wd/adtabler/scripts/ok_occurrences.R')
