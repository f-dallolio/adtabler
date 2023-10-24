#' Nielsen Adintel Dataset - Useful 'Occurrences' Lookup Tables
#'
#' These tables provide information and lookpup keys for AdIntel 'occurrences' data.
#'
#' @format ## `occurrences_columns`
#' A data frame (tibble) with 460 rows and 8 columns.
#' \describe{
#'   \item{file_name}{Type: character.The file name for the data on a specific media. Format looks like 'XXX.tsv' where XXX stands for 'NetworkTV', 'SpotTV', etc.}
#'   \item{media_type_id}{Type: integer. The id for each specific media.}
#'   \item{col_pos}{Type: integer. The position of the column name in 'col_name_man'. }
#'   \item{col_name_man}{Type: character. The name used in the AdIntel manual (and the original data).}
#'   \item{col_class_man}{Type: character. The description of the column SQL data type provided by Nielsen.}
#'   \item{col_p, col_n}{Type: integer. The expected length of the data in the column. Usually, 'p' (col_p) is referred to as 'precision' and 'n' (col_n) as scale. For the SQL type NUMERIC,
#'   col_p is the total number of digits while col_n is the number of decimals.}
#'   \item{description}{Type: The column description provided by Nielsen}
#' }

"occurrences_columns"
