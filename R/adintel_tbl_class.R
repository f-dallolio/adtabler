#" Class adintel_tbl
#"
#" @name adintel_tbl_class
#"
NULL


#" @rdname adintel_tbl_class
#" @export
new_tbl_cols <- function(x = list()) {

  stopifnot(all(names(x) %in% c("col_names", "col_classes", "sql_datatypes")))
  col_names <- x$col_names
  col_classes <- x$col_classes
  sql_datatypes <- x$sql_datatypes

  structure(x,
            class = c("tbl_cols", "adintel_tbl", class(x)),
            col_names = col_names,
            col_classes = col_classes,
            sql_datatypes = sql_datatypes)
}
is_tbl_cols <- function(x){
  inherits(x = x, what = c("tbl_cols", "adintel_tbl"))
}

#" @rdname adintel_tbl_class
#" @export
new_range_partition <- function(x = list()) {

  stopifnot(all(names(x) %in% c("partition_by", "partition_name", "range")))
  partition_by <- x$partition_by
  partition_name <- x$partition_name
  range <- x$range
  stopifnot(length(range) == 2 & (is.numeric(range) | is.Date(range)))

  structure(x,
            class = c("range_partition", "adintel_tbl", class(x)),
            partition_by = partition_by,
            partition_name = partition_name,
            range = range
            )
}
is_range_partition <- function(x){
  inherits(x = x, what = c("range_partition", "adintel_tbl"))
}

#" @rdname adintel_tbl_class
#" @export
new_list_partition <- function(x = list()) {
  stopifnot(all(names(x) %in% c("partition_by", "partition_name", "values")))
  partition_by <- x$partition_by
  partition_name <- x$partition_name
  values <- x$values

  structure(x,
            class = c("list_partition", "adintel_tbl", class(x)),
            partition_by = partition_by,
            partition_name = partition_name,
            values = values
  )
}
is_list_partition <- function(x){
  inherits(x = x, what = c("list_partition", "adintel_tbl"))
}

#" @rdname adintel_tbl_class
#" @export
new_tbl_indices <- function(x = list()) {

  stopifnot(all(names(x) %in% c("index_cols", "pk_cols", "fk_tbl")))
  index_cols <- x$index_cols
  pk_cols <- x$pk_cols
  fk_tbl <- x$fk_tbl

  structure(
    x,
    class = c("tbl_indices", "adintel_tbl", class(x)),
    index_cols = index_cols,
    pk_cols = pk_cols,
    fk_tbl = fk_tbl
  )
}
is_tbl_indices <- function(x){
  inherits(x = x, what = c("tbl_indices", "adintel_tbl"))
}

