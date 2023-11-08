#| output: false
devtools::install_github("f-dallolio/adtabler")
1
library(data.table)
library(tidyverse, quietly = TRUE)
library(tsibble, quietly = TRUE)
library(rlang, quietly = TRUE)
library(glue, quietly = TRUE)
library(adtabler, quietly = TRUE)
library(furrr)




tbl_list <- data_info_list$unique_key |>
  select(file_type_std, file_name_std, media_type_id) |>
  filter(!is.na(media_type_id)) |>
  nest(.by = !media_type_id) |>
  inner_join(
    data_info_list$file_info |>
      filter(n_rows > 0) |>
      select(file_type_std, file_name_std, year, full_file_name)
  ) |>
  unnest(everything()) |>
  inner_join(
    data_info_list$col_info |>
      filter(file_type_std == "occurrences",
      ) |>
      select(file_name_std, col_pos, col_name_std, datatype_r) |>
      nest(col_info = -file_name_std)
  ) |>
  unnest(everything()) |>
  nest(.by = c(file_type_std, file_name_std, media_type_id, year, full_file_name))



tbl_list <- tbl_list |> mutate(overwrite = FALSE, append = FALSE) |>
  mutate(
    min_year = min(year),
    overwrite = year == min_year,
    append = year != min_year,
    .by = c(file_type_std, file_name_std, media_type_id)
  ) |>
  arrange(media_type_id, year) |>
  mutate(cmd = map2_vec(.x = full_file_name,
                        .y = media_type_id,
                        .f = ~ make_grep_cmd(file=.x, media_type_id = .y, first_n = 1000)),
         .by = c(file_type_std, file_name_std, media_type_id))



safe_fread <- function(...){
  fn <- quietly(.f = data.table::fread)
  out <- fn(...)
  # return(out)
  good_out <- all(c(out$output == "", is_empty(out$warnings), is_empty(out$messages)) )
  if(good_out){
    return(
      list(good_out = good_out,
           result = out[[1]])
    )
  } else {
    return(
      list(good_out = good_out,
           result = out[-1])
    )
  }
}


tbl_list$good_out <- NA
tbl_list$tbl_name <- NA
tbl_list$warn_out <- list(NA)
tbl_list$df <- list(NA)
tbl_list$duration <- NA


i <- 1
seq_i = seq_along(tbl_list$cmd)

i_last <- 1
seq_i = seq_i[seq_i >= i_last]


for ( i in seq_i ) {

  t0_i <- Sys.time()
  t0_chr <- as.character(t0_i) |>
    str_sub(1, 19)

  cmd_i <- tbl_list$cmd[[i]]
  year_i <- tbl_list$year[[i]]
  file_type_i <- tbl_list$file_type_std[[i]]
  file_name_i <- tbl_list$file_name_std[[i]]
  media_type_id_i <- numpad2(tbl_list$media_type_id[[i]])
  col.names_i <- tbl_list$data[[i]]$col_name_std
  colClasses_i <- unname(tbl_list$data[[i]]$datatype_r)
  tbl_name_i <- glue::glue("occ__media_id_{ media_type_id_i }")

  print_i <- paste(file_type_i, paste(file_name_i, media_type_id_i, sep = "_"), year_i, sep = " - ")

  print(
    glue::glue(
    "
    Iteration: \t {i} / {max(seq_i)}: \t {print_i}
    Start reading at {t0_chr} \n
    "
    )
  )

  out_temp <- safe_fread(cmd = cmd_i,
                         col.names = col.names_i,
                         colClasses = colClasses_i,
                         nThread = parallel::detectCores() - 1)

  tbl_list$good_out[[i]] <- out_temp$good_out
  tbl_list$tbl_name[[i]] <- tbl_name_i

  if( out_temp$good_out ) {

    if ( "ad_date" %in% names(out_temp$result) ) {
      out_temp$result$ad_date <- out_temp$result$ad_date |>
        as.IDate()
    }
    if( "ad_time" %in% names(out_temp$result) ) {
      out_temp$result$ad_time <- out_temp$result$ad_time |>
        stringr::str_sub(start = 1, end = 8)
    }

    tbl_list$df[[i]] <- out_temp$result

    # overwrite_i <- tbl_list$overwrite[[i]]
    # append_i <- tbl_list$append[[i]]
    # dbWriteTable(conn = con, name = tbl_name_i, value = out_temp$result, overwrite = overwrite_i, append = append_i)

  } else {
    tbl_list$warn_out[[i]] <- out_temp$result
  }

  t1_i <- Sys.time()
  t1_chr <- as.character(t1_i) |>
    str_sub(1, 19)
  elapsed <- difftime(t1_i, t0_i) |>
    round(2)

  tbl_list$duration[[i]] <- elapsed

  print(
    glue::glue(
    "
    Finished at {t1_chr}.
    Outcome OK: \t {out_temp$good_out}. \n
    ----------
    "
    )
  )

}

tbl_list <- tbl_list |>
  mutate(
    df = df |>
      set_names( paste(tbl_name, year, sep = "__") )
  )

save(tbl_list, file = "~/Documents/r_wd/adtabler/xxx.RData")





tbl_final <- tbl_list |>
  inner_join(
    occurrences_categories |>
      select(media_type_id,
             file_name,
             tbl_db_name)
  ) |>
  filter(is.na(warn_out)) |>
  summarise(
    db_tbl = list(list_rbind(df)),
    .by = c(file_name)
  ) |>
  mutate(tbl_db_name = paste("occ", file_name, sep = "__"),
         db_tbl = db_tbl |>
           set_names(tbl_db_name) |>
           map(~ .x |> arrange(ad_date)))
tbl_final
tbl_final$db_tbl


library(DBI)
library(RPostgres)
dbname <-  'test'
host <-  '10.147.18.200'
port <-  5432
user_prompt <- glue::glue(
  "USERNAME for database:
\"{ dbname }\" on { host } (port {port})

"
)
pswd_prompt <- glue::glue(
  "USERNAME for database:
\"{ dbname }\" on { host } (port {port})

"
)

con <- dbConnect(
  RPostgres::Postgres(),
  dbname = dbname,
  host = host,
  port = port,
  user = 'postgres',
  password = rstudioapi::askForPassword(prompt = pswd_prompt)
)

walk2(.x = tbl_final$tbl_db_name,
      .y = tbl_final$db_tbl,
     .f = ~ dbWriteTable(conn = con,
                         name = .x,
                         value = .y,
                         overwrite = TRUE,
                         ))


data_info_list$col_info |>
  filter(datatype_sql |> adtabler::not_na())

dbDisconnect(con)
