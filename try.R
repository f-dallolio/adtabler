library(tidyverse)
library(data.table)
library(adtabler)

chunker <- function(path, chunk = 1, nrows = 50000000, col_classes = NULL){
  skip <- nrows * chunk + 1 - nrows
  df <- fread(
    input = path,
    skip = skip,
    nrows = nrows,
    colClasses = col_classes,
    # col.names = col_names,
    key = "V1",
    # index = index,
    fill = TRUE, nThread = 28
  )
  chunk_file_name <- paste0("chunk_", chunk, ".rds")
  dir <- "/media/filippo/sata_data_1/new_adintel/2018_spot_tv/"
  if(!dir.exists(dir)){
    dir.create(dir)
  }
  saveRDS(df, file = paste0("/media/filippo/sata_data_1/new_adintel/2018_spot_tv/",chunk_file_name))
  !any(is.na(df[[1]]))
}

path <- "/media/filippo/sata_data_1/adintel/ADINTEL_DATA_2018/nielsen_extracts/AdIntel/2018/Occurrences/SpotTV.tsv"

chunker(path = path)

chunk <-  1
no_nas <-  TRUE
whicle(no_nas){
  no_nas <- chunker(path)
  gc()
  print(chunk)
  chunk <- chunk + 1
}
