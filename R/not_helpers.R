not_in <- function(x, table) !x %in% table

`%notin%` <- function(x, table) not_in(x, table)

not_na <- function(x) !is_na(x)

not_null <- function(x) !is_null(x)

