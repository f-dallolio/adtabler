library(tidyverse)
library(data.tree)

adintel_info <- read_rds(file = "Documents/r_wd/adtabler/data/adintel_info.rds")


x <- adintel_info |>
  filter(file_type == "occurrences") |>
  unnest(media_info) |>
  mutate(id = cur_group_id(), .by = file_name) |>
  mutate(id1 = row_number())

m_info <- x |> pull(media_info)

d_info <- x |>
  select(file_name, data_info) |>
  distinct() |>
  pull(data_info)

avg_file_size_mb <- adintel_info |>
  filter(file_type == "occurrences") |>
  select(file_name, avg_size_mb)
avg_file_size_mb <- avg_file_size_mb$avg_size_mb |>
  set_names(avg_file_size_mb$file_name)

i <- 1
for (i in seq_along(d_info)) {
  names(d_info[[i]])[4] <- "file_path"
  names(d_info[[i]])[5] <- "avg_file_size_mb"
  d_info[[i]][5] <- avg_file_size_mb[i]
  d_info[[i]] <- d_info[[i]][-c(1, 2)]
  print(names(d_info[[i]]))
  iidd <- x$id1[x$id == i]
  d_info[[i]]$media_info <- as.list(m_info[iidd])
}

nms0 <- adintel_info |>
  filter(file_type == "occurrences")

data_info <- d_info |> set_names(nms0$file_name)
adintel_occurrences <- tibble(
  file_type = "occurrences",
  file_name = nms0$file_name
)
adintel_occurrences$data_info <- data_info
adintel_occurrences



adintel_no_occurrences <- adintel_info |>
  filter(file_type != "occurrences")

no_occ_avg_file_size_mb <- adintel_no_occurrences |>
  select(file_type, file_name, avg_size_mb)
no_occ_avg_file_size_mb <- no_occ_avg_file_size_mb$avg_size_mb |>
  set_names(no_occ_avg_file_size_mb$file_name)

i <- 1
for (i in seq_along(adintel_no_occurrences$data_info)) {
  names(adintel_no_occurrences$data_info[[i]])[4] <- "file_path"
  names(adintel_no_occurrences$data_info[[i]])[5] <- "avg_file_size_mb"
  adintel_no_occurrences$data_info[[i]]["avg_file_size_mb"] <- adintel_no_occurrences$avg_size_mb[i]
  adintel_no_occurrences$data_info[[i]]["avg_file_size_mb"] |> print()
  adintel_no_occurrences$data_info[[i]] <- adintel_no_occurrences$data_info[[i]][-c(1, 2)]
  print(names(adintel_no_occurrences$data_info[[i]]))
  # iidd <- x$id1[x$id == i]
  # d_info[[i]]$media_info <- as.list(m_info[iidd])
}
adintel_no_occurrences <- adintel_no_occurrences |> select(file_type, file_name, data_info)
adintel_no_occurrences$data_info[1]

adintel_info_new <- adintel_occurrences |> bind_rows(adintel_no_occurrences)
adintel_info_new$data_info <- adintel_info_new$data_info |> set_names(adintel_info_new$file_name)
adintel_info_new <- adintel_info_new |>
  select(file_type, data_info) |>
  nest(.by = file_type, .key = "data_info")

adintel_info_list <- adintel_info_new$data_info |>
  set_names(adintel_info_new$file_type) |>
  map(~ .x$data_info)
adintel_info_list |> as.Node()

saveRDS(object = adintel_info_list, file = "Documents/r_wd/adtabler/data-raw/adintel_info_list.rds")
save(adintel_info_list, file = "Documents/r_wd/adtabler/data-raw/adintel_info_list.rda")
