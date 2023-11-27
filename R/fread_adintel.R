#' Function of 'fread' with corrections for adintel datasets
#'
#' @param file string.
#' @param col_names_std character vector.
#' @param col_classes character vector.
#' @param ... for future development.
#'
#' @return a data.table.
#' @export
#'
fread_adintel <- function(file, col_names_std, col_classes, ...){

  fn_read <- partial(
    .f = data.table::fread,
    colClasses = purrr::simplify(unname(col_classes)),
    col.names = purrr::simplify(unname(col_names_std)),
    na.strings = "",
    encoding = 'Latin-1'
  )

  x <- decompose_file(file)
  tbl_name <- x$tbl_name
  year <- x$adintel_year

  if ( tbl_name == 'brand' & year >= 2018 ) {
    cmd <- sprintf(glue("grep -P -n '\t\"'  {file}"))
    out_bad <- system(cmd, intern = T)
    bad_rows_num <- out_bad |>
      stringr::str_split_i(":", 1) |>
      as.integer()
    good_rows <-  out_bad |>
      stringr::str_split_i(":", 2) |>
      stringr::str_replace_all('\t\"', '\"')
    text <- data.table::fread(file = file, sep = "", quote = "", header = FALSE)[[1]]
    text[bad_rows_num] <- good_rows
    out <- fn_read(text = text)

  } else if ( tbl_name == "distributor" & year == 2018){
    tmp <- data.table::fread(file = file, sep = "", quote = "")
    tmp1 <- tmp |> dplyr::slice(1:22035)
    tmp2 <- tmp |> dplyr::slice(22036:22038) |>
      dplyr::pull(1) |>
      stringr:: str_flatten()
    tmp3 <- tmp |>
      dplyr::slice(22039 : NROW(tmp)) |>
      dplyr::pull(1)
    text <- c(names(tmp), tmp1, tmp2, tmp3) |> purrr::list_c()
    out <- fn_read(text = text)
  } else {
    out <- fn_read( file = file )
  }

  out |>
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(col_names_std[col_classes == "character"]),
        ~ .x |> iconv(from = 'latin1', to = 'UTF-8')
      )
    )

}
