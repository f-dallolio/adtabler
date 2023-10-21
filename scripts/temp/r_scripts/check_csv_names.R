library(tidyverse)
library(adtabler)

get_occurrences_names <- function(full_name){

  adintel_name <- data.table::fread(
    file = full_name,
    header = F,
    nrows = 1
  ) |>
    unlist(use.names = FALSE)

  new_adintel_name <- rename_adintel(adintel_name)

  dplyr::tibble(
    full_name,
    new_adintel_name) |>
    dplyr::mutate(
      file = full_name |>
        stringr::str_split_i("/", -1),
      .after = full_name
    ) |>
    nest(adintel_name = adintel_name) |>
    mutate(adintel_name = adintel_name |> map(~.x[[1]]))


}



install.packages("sys")

read_nrows <- function(full_name, df_out = TRUE){
  cmd_out <- sprintf("wc -l %s", full_name) |>
    system(intern = TRUE) |>
    strsplit(" ") |>
    unlist()

  out <- list(
    full_name = cmd_out[[2]],
    n = as.integer(cmd_out[[1]])
  )

  if(df_out) return(as_tibble(out))
  out
}


read_nrows(full_name)



x <-  |>
  as_tibble_col("out") |>
  separate_wider_delim(cols = out, delim = " ", names = c("n", "full_name")) |>
  mutate(n_rows = n |> as.integer(),
         .after = full_name,
         .keep = "unused")


x <- sys::exec_internal(cmd = "wc", args = c("-l", full_name))


x$stdout |> sys::as_text()



adintel_dir <- "/mnt/sata_data_1/adintel/"
adintel_files <- list.files(adintel_dir, recursive = T, full.names = T)
full_name <- adintel_files[[1]]

get_occurrences_names(full_name)



|>
  map(~ data.table::fread(.x, header = F, nrows = 1)) |>
  unlist(use.names = F) |>
  unique()




adintel_renamed <-  rename_adintel(adintel_names)




occ_dir <- "/mnt/sata_data_1/new_adintel/occurrences/"
files_temp_names <- list.files(occ_dir, recursive = T, full.names = T) |>
  map(~ data.table::fread(.x, header = F, nrows = 1) |>
        unlist(use.names = F) |>
        as_tibble_col("col_names") |>
        mutate(full_name = .x,
               file = str_split_i(full_name, "/", -1),
               media_type_id = file |> str_split_i("__", 1) |>
                 str_remove_all("id") |>
                 as.integer(),
               names_ok = all(col_names %in% c(adintel_names, adintel_renamed)),
               num_cols = max(row_number()))) |>
  list_rbind() |>
  nest(col_names = col_names)

files_good_names <- files_temp_names |>
  filter(names_ok) |>
  mutate(col_names = col_names |> map(~ .x[[1]] |> rename_adintel()))

files_test <- files_good_names |>
  unnest(everything()) |>
  distinct(media_type_id, num_cols, col_names) |>
  nest(col_names = col_names) |>
  mutate(unique_media_type_id = n_distinct(media_type_id) == n(),
         .by = media_type_id)

if (all(files_test$unique_media_type_id)) {
  cat("ALL GOOD!!! \n")
} else {
  message("Problems")
}


files_bad_names <- files_temp_names |>
  filter(!names_ok) |>
  unnest(everything()) |>
  nest(data = c(full_name, file, col_names))





occurrences_info <- list.files("Documents/r_wd/adtabler/scripts/temp/",
                               full.names = T, recursive = T) |>
  str_subset("out_") |>
  str_subset(".rds") |>
  map(read_rds) |>
  list_rbind() |>
  mutate(col_names = classes |> map(~ .x$col_names)) |>
  select(media_type_id : file, col_names) |>
  unnest(everything()) |>
  nest(.by = file)

x1 <- occurrences_info |> unnest(file) |> mutate(file = str_split_i(file, "/", -1))
x2 <- new_names |> mutate(file0 = file,
                    file = str_split_i(file, "/", -1))

anti_join(x2, x1) |> print(n=200)
