#' Class adintel_tbl
#'
#' @name indintel_tbl_class
NULL

#' @rdname indintel_tbl_class
#' @export
new_adintel_tbl_cols <- function(x = list(),
                                       col_names_std = character(),
                                       col_classes = character(),
                                       sql_datatypes = character()) {
  structure(x,
            class = c('adintel_tbl_cols', class(x)),
            col_names_std = col_names_std,
            col_classes = col_classes,
            sql_datatypes = sql_datatypes)
}
is_adintel_tbl_cols <- function(x){
  inherits(x = x, what = 'adintel_tbl_cols')
}

#' @rdname indintel_tbl_class
#' @export
new_adintel_range_partition <- function(x = list(),
                                      partition_by = character(),
                                      partition_name = character(),
                                      range = list(from = numeric(), to = numeric())) {
  structure(x,
            class = c('adintel_range_partition', class(x)),
            partition_by = partition_by,
            partition_name = partition_name,
            range = range
            )
}
is_adintel_range_partition <- function(x){
  inherits(x = x, what = 'adintel_range_partition')
}

#' @rdname indintel_tbl_class
#' @export
new_adintel_list_partition <- function(x = list(),
                                        partition_by = character(),
                                        partition_name = character(),
                                        values) {
  structure(x,
            class = c('adintel_list_partition', class(x)),
            partition_by = partition_by,
            partition_name = partition_name,
            values = values
  )
}
is_adintel_range_partition <- function(x){
  inherits(x = x, what = 'adintel_range_partition')
}

#' @rdname indintel_tbl_class
#' @export
new_adintel_tbl_indices <- function(x = list(),
                                    index_cols = character(),
                                    pk_cols = character(),
                                    fk_tbl = data.frame()) {
  structure(
    x,
    class = c('adintel_tbl_indices', class(x)),
    index_cols = index_cols,
    pk_cols = pk_cols,
    fk_tbl = fk_tbl
  )
}
is_adintel_tbl_indices <- function(x){
  inherits(x = x, what = 'adintel_tbl_indices')
}
