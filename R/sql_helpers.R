sql_coldef <- function(.name, .type, .d1, .d2, .con){
  n = length(.name)
  comma <- c( rep(",", n-1), "" )
  out <- dplyr::case_when(
    !is.na(.d1) & !is.na(.d2) ~ glue::glue("{.name} {.type}({ .d1 }, { .d2 }) {.con}",.na = ""),
    !is.na(.d1) & is.na(.d2) ~ glue::glue("{.name} {.type}({ .d1 }) {.con}",.na = ""),
    .default = glue::glue("{.name} {.type} {.con}",.na = "")
  )
  stringr::str_squish(out) |> str_flatten_comma()
  # glue::glue("\t{out}{comma}")
}

sql_coldef_from_tbl <- function(x){
  x <-  attributes(x)
  .n <- x$names
  .t <- x$sql_type
  .l <- x$sql_length
  .s <- x$sql_scale
  .c <- x$sql_constraints
  sql_coldef(.n, .t, .l, .s, .c)
}

