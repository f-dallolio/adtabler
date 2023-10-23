library(DBI)
drv <- RPostgres::Postgres()
dbname <- "test"
host <- "10.147.18.200"
port <- 5432
user <- "postgres"
password <- "100%Postgres"
con <- dbConnect(
  drv = drv,
  dbname = dbname,
  host = host,
  port = port,
  user = user,
  password = password
)
# dbDisconnect(con)

library(tidyverse)
library(dm)
load("~/Dropbox/NielsenData/brand_refs/data/products_references.RData")

copy_dm_to(dest = con, dm = my_dm, temporary = FALSE, progress = TRUE)

tbls <- dbListTables(con)
con_dm <- dm_from_con(con, table_names = tbls, learn_keys = TRUE)
dm_draw(con_dm)
con_dm$adv_df
