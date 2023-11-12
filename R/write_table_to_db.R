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
uk_unique_fn <- function(.df, .uk){
  out <- .df |>
    mutate(row_id = row_number(), .before = 1) |>
    mutate(
      n = n(),
      .by = all_of(.uk)
    ) |>
    filter(n > 1) |>
    select(-n) |>
    mutate(across(everything(), n_distinct),
           .by = all_of(.uk)) |>
    pivot_longer(-c(row_id, .uk)) |>
    filter(value > 1) |>
    distinct()
  return(
    list(
      all_unique = NROW(out) == 0,
      df = out
    )
  )
}

#' @rdname write_to_db
#' @export
#'
adintel_read_tsv <- function(tbl_name, file, col_names = NA, col_classes = NA, col_uk = NA, out_df_only = FALSE){

  t0_fread <- Sys.time()

  year <- as_numeric2(stringr::str_split_i(file, "/", -3))

  media_type_id <- NA
  if( stringr::str_detect(tbl_name, "occ__") ) {
    media_type_id <- stringr::str_split_i(tbl_name, "__", 3) |>
      stringr::str_split_i("_", 1) |>
      as.integer()
  }

  read_info <- stringr::str_flatten(c(tbl_name, year), collapse = " - ", na.rm = TRUE)
  print(glue::glue("Starting: \t\t {read_info}"))

  has_ad_time <- "ad_time" %in% col_names

  is_ref_dyn <- stringr::str_detect(tbl_name, "ref_dyn__")
  is_ref <- stringr::str_detect(tbl_name, "ref__")

  col_uk <- col_names[col_uk]
  col_classes[col_names == "ad_date"] <- "IDate"

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

  if(has_ad_time){
    df$ad_time <- stringr::str_sub(df$ad_time, 1, 8)
  }

  if(is_ref_dyn){
    df  <- df |> dplyr::mutate(year = year, .before = 1)
  }

  uk_not_na <- df |>
    select(dplyr::any_of(col_uk)) |>
    summarise(across(everything(), ~.x |> is.na() |> any())) |>
    pivot_longer(everything())


  uk_unique_list <- uk_unique_fn(.df = df, .uk = col_uk)

  timer(t0_fread, msg = 'File read in \t {.x}') |> print()

  if(out_df_only) {
    if(!uk_unique_list$all_unique) {
      warning("UK not unique: check!")
    }
    return(df)
  }

  return(
    list(
      df = df,
      uk_not_na = uk_not_na,
      uk_unique = uk_unique_list$all_unique,
      no_unique = uk_unique_list$df
    )
  )
}
rlang::fn_fmls(adintel_read_tsv)[c("col_names","col_classes","col_uk")] <- rlang::fn_fmls(data.table::
                                                                                            fread)[c("col.names","colClasses","key")]
#' @rdname write_to_db
#' @export
#'
write_to_db <- function(con, tbl_name, df, overwrite, append){

  t0_dbwrite <- Sys.time()

  RPostgres::dbWriteTable(conn = con,
                          name = tbl_name,
                          value = df,
                          overwrite = overwrite,
                          append = append)

  timer(t0_dbwrite, msg = 'Table to DB in \t {.x}') |> print()
}
