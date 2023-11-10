#' Title
#'
#' @param con a db connection.
#' @param file string.
#' @param year integer.
#' @param tbl_name string.
#' @param col_names character vector.
#' @param col_classes character vector.
#' @param col_uk character vector.
#' @param overwrite logical.
#' @param append logical.
#'
#' @return nothing. Must be used (write_to_db_con) with purrr::pwalk
#' @name write_to_db
NULL

#' @rdname write_to_db
#' @export
#'
write_to_db <- function(con, file, year, tbl_name, col_names, col_classes, col_uk, overwrite, append){

  read_info <- stringr::str_flatten(c(tbl_name, year), collapse = " - ", na.rm = TRUE)
  print(glue::glue("Starting: \t\t {read_info}"))

  t0_fun <- Sys.time()

  has_ad_time <- "ad_time" %in% col_names

  is_ref_dyn <- stringr::str_detect(tbl_name, "ref_dyn__")
  is_ref <- stringr::str_detect(tbl_name, "ref__")

  col_uk <- col_names[col_uk]
  col_classes[col_names == "ad_date"] <- "IDate"

  t0_fread <- Sys.time()

  if(tbl_name == "ref_dyn__brand" & year %in% 2018:2021){
    tbl_tmp <- data.table::fread(
      file = file,
      sep = "",
      quote = ""
    ) |>
      dplyr::pull(1) |>
      stringr::str_replace_all("\t\"", "\"")
    df <- data.table::fread(
      text = tbl_tmp,
      colClasses = col_classes,
      col.names = col_names,
      key = col_uk,
      nThread = parallel::detectCores() - 2,
      encoding = "UTF-8"
    )
  } else if (tbl_name == "ref_dyn__distributor" & year %in% 2018){
    tbl_tmp <- data.table::fread(
      file = file,
      sep = "",
      quote = ""
    )
    tmp1 <- tbl_tmp |> dplyr::slice(1:22035)
    tmp2 <- tbl_tmp |> dplyr::slice(22036:22038) |> dplyr::pull(1) |> stringr::str_flatten()
    tmp3 <- tbl_tmp |> dplyr::slice(22039:NROW(tbl_tmp)) |> dplyr::pull(1)
    tmp_new <- c(names(tbl_tmp), tmp1, tmp2, tmp3) |> list_c()
    df <- data.table::fread(
      text = tmp_new,
      colClasses = col_classes,
      col.names = col_names,
      key = col_uk,
      nThread = parallel::detectCores() - 2,
      encoding = "UTF-8"
    )
  } else if (not_na(media_type_id)) {
    cmd <- make_grep_cmd(
      file = file,
      media_type_id = media_type_id
    )
    df <- data.table::fread(
      cmd = cmd,
      colClasses = col_classes,
      col.names = col_names,
      key = col_uk,
      nThread = parallel::detectCores() - 2,
      encoding = "UTF-8"
    )
  } else {
    df <- data.table::fread(
      file = file,
      colClasses = col_classes,
      col.names = col_names,
      key = col_uk,
      nThread = parallel::detectCores() - 2,
      encoding = "UTF-8"
    )
  }

  timer(t0_fread, msg = 'File read in \t {.x}') |> print()

  if(has_ad_time){
    df$ad_time <- stringr::str_sub(df$ad_time, 1, 8)
  }

  if(is_ref_dyn){
    df  <- df |> dplyr::mutate(year = year, .before = 1)
  }

  t0_dbwrite <- Sys.time()

  RPostgres::dbWriteTable(conn = con,
                          name = tbl_name,
                          value = df,
                          overwrite = overwrite,
                          append = append)

  timer(t0_dbwrite, msg = 'Table to DB in \t {.x}') |> print()

  timer(t0_fun, n_post = 2, msg = 'Done in \t {.x} \t {read_info}') |> print()
}

#' @rdname write_to_db
#' @export
write_to_db_con <- function(con){
  purrr::partial(.f = write_to_db, con = con)
}
