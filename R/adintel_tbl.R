new_adintel_tbl <- function(x = data.frame(),
                            file_type_std = character(),
                            file_name_std = character(),
                            key = character(),
                            r_type = character(),
                            sql_type = character(),
                            sql_length = integer(),
                            sql_scale = integer(),
                            sql_constraints = character()){
  names_og <- names(x)
  names(x) <- adtabler::rename_adintel(names(x))
  if(is.na(sql_constraints)) sql_constraints <- rep(sql_constraints, length(sql_type))
  has_na <- lapply(x, function(x) any(is.na(x))) |> unlist() |>  unname()
  old_class <- class(x)
  new_class <- c("adintel_tbl", old_class)
  structure(.Data = x,
            file_type_std = file_type_std,
            file_name_std = file_name_std,
            key = key,
            has_na = has_na,
            r_type = r_type |> unname(),
            sql_type = sql_type |> unname(),
            sql_length = sql_length |> unname() |> as.integer(),
            sql_scale = sql_scale |> unname() |>  as.integer(),
            sql_constraints = sql_constraints |> unname(),
            class = new_class)
}

adintel_tbl_from_file <- function(file,
                                  file_type_std,
                                  file_name_std,
                                  key = NA,
                                  r_type,
                                  sql_type,
                                  sql_length = NA,
                                  sql_scale = NA,
                                  sql_constraints = NA,
                                  ...){

  df_og <- data.table::fread(file = file, nrows = 10)
  og_names <- adtabler::rename_adintel(x = names(df_og)) |> unname()

  q_fread <- quietly(data.table::fread)

  x <- q_fread(file = file, colClasses = unname(r_type), ...)

   if ( x$output == "" && is_empty(x$warnings) && is_empty(x$messages) ) {
    x <- x$result |> unclass() |> dplyr::as_tibble()
  } else {
    return(x[-1])
  }

  attr(x, "file_type_std") <-  unname(file_type_std)
  attr(x, "file_name_std") <-  unname(file_name_std)
  attr(x, "key") <-  unname(key)
  attr(x, "r_type") <-  unname(r_type)
  attr(x, "sql_type") <-  unname(sql_type)
  attr(x, "sql_length") <-  unname(sql_length) |> as.integer()
  attr(x, "sql_scale") <-  unname(sql_scale) |> as.integer()
  attr(x, "sql_constraints") <-  unname(sql_constraints)
  attr(x, "og_names") <-  og_names

  out <- new_adintel_tbl(x,
                  file_type_std,
                  file_name_std,
                  key,
                  r_type,
                  sql_type,
                  sql_length,
                  sql_scale,
                  sql_constraints )
  return(out)
}

is_adintel_tbl <- function(x){
  stopifnot(is_adintel_tbl(x))
  inherits(x, "file_type_std")
}


file_type_std <- function(x){
  stopifnot(is_adintel_tbl(x))
  attr(x, "file_type_std")
}

file_name_std <- function(x){
  stopifnot(is_adintel_tbl(x))
  attr(x, "file_name_std")
}

adintel_key <- function(x){
  stopifnot(is_adintel_tbl(x))
  attr(x, "key")
}

r_type <- function(x){
  stopifnot(is_adintel_tbl(x))
  attr(x, "r_type")
}

sql_type <- function(x){
  stopifnot(is_adintel_tbl(x))
  attr(x, "sql_type")
}

sql_length <- function(x){
  stopifnot(is_adintel_tbl(x))
  attr(x, "sql_length")
}

sql_scale <- function(x){
  stopifnot(is_adintel_tbl(x))
  attr(x, "sql_scale")
}

sql_constraints <- function(x){
  stopifnot(is_adintel_tbl(x))
  attr(x, "sql_constraints")
}

og_names <- function(x){
  stopifnot(is_adintel_tbl(x))
  attr(x, "og_names")
}



is_adintel_ref <- function(x){
  stopifnot(is_adintel_tbl(x))
  file_type_std(x) == "references"
}

is_adintel_occ <- function(x){
  stopifnot(is_adintel_tbl(x))
  file_type_std(x) == "occurences"
}

is_adintel_imp <- function(x){
  stopifnot(is_adintel_tbl(x))
  file_type_std(x) == "impressions"
}

is_adintel_ue <- function(x){
  stopifnot(is_adintel_tbl(x))
  file_type_std(x) == "universe_estimates"
}

is_adintel_mb <- function(x){
  stopifnot(is_adintel_tbl(x))
  file_type_std(x) == "market_breaks"
}


df0 <- data_info_list$col_info |>
  filter(file_type_std == "references") |>
  select(file_type_std,file_name_std,
         datatype_r : sql_scale) |>
  summarise(across(everything(), list), .by = c(file_type_std, file_name_std)) |>
  left_join(
    data_info_list$file_info |>
      filter(year %in% c(2010, NA)) |>
      select(file_name_std, full_file_name)
  ) |>
  filter(!is.na(full_file_name)) |>
  inner_join(data_info_list$unique_key |>
               select(file_name_std, col_unique_key)) |>
  mutate(col_unique_key = col_unique_key |> map(str_split_comma)) |>
  relocate(col_unique_key, .before = datatype_r) |>
  relocate(full_file_name, .before = 1)

