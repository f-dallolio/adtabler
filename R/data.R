#' Nielsen Adintel Dataset - Description of Variables in 'Occurrences' Tables
#'
#' This table describes AdIntel 'occurrences' data.
#'
#' @format ## `occurrences_columns`
#' A data frame (tibble) with 460 rows and 8 columns. If a column name ends in "_man" it indicates that the value is as reported in the Adintel manual.
#' \describe{
#'   \item{file_name}{Type: character.The file name for the data on a specific media. Format looks like 'XXX.tsv' where XXX stands for 'NetworkTV', 'SpotTV', etc.}
#'   \item{media_type_id}{Type: integer/numeric. The id for each specific media.}
#'   \item{col_pos}{Type: integer/numeric. The position of the column name in 'col_name_man'. }
#'   \item{col_name_man}{Type: character. The name used in the AdIntel manual (and the original data).}
#'   \item{col_class_man}{Type: character. The description of the column SQL data type provided by Nielsen.}
#'   \item{col_p, col_n}{Type: integer/numeric. The expected length of the data in the column. Usually, 'p' (col_p) is referred to as 'precision' and 'n' (col_n) as scale. For the SQL type NUMERIC,
#'   col_p is the total number of digits while col_n is the number of decimals.}
#'   \item{description}{Type: The column description provided by Nielsen.}
#' }
"occurrences_columns"

#' Nielsen Adintel Dataset - Media Categories
#'
#' This table provides a lookup table for each of the media in the AdIntel dataset.
#'
#' @format ## `occurrences_categories`
#' A data frame (tibble) with 24 rows and 5 columns.
#' \describe{
#'   \item{media_type_id}{integer/numeric. The id for each specific media.}
#'   \item{media_type}{Type: character. An informative name for each 'media_type_id'. It is divided in two parts by the separator "__" (two underscores). Left of the separator is the name of the media (always one string), Right of the separator are abbreviation for the characteristics of the media separated by one underscore. E.g. 'network_tv__nat_spa' indicates that the 'media_type_id' refers to data for Network TV at the local level ("loc") and Spanish language ("spa").}
#'   \item{media_category}{Type: character. c("tv", "print", "radio", "outdoor", "online", "cinema"). }
#'   \item{media_geo}{Type: character. It indicates wheter a certain media is 'national' or 'local'.}
#'   \item{is_spanish}{Type: logical. TRUE if the 'media_type_id' is in Spanish language.}
#' }
"occurrences_categories"

#' Nielsen Adintel Dataset - Unique Keys of 'Occurrences' Tables
#'
#' Unique keys. If a column name ends in "_man" it indicates that the value is as reported in the Adintel manual.
#'
#' @format ## `occurrences_ukey`
#' A data frame (tibble) with 152 rows and 3 columns.
#' \describe{
#'   \item{media_type_id}{integer/numeric. The id for each specific media.}
#'   \item{ukey_pos}{Type: integer/numeric. Positions of the columns forming the unique key.}
#'   \item{ukey_man}{Type: character. Names (as per the AdIntel manual) of the columns forming the unique key for a specific 'media_type_id'.}
#' }
"occurrences_ukey"

#' Lookup Table for Dates (2009 - 2030)
#'
#' @format ## `date_reference`
#' A data frame (tibble) with 152 rows and 3 columns.
#' \describe{
#'   \item{date}{Date.}
#'   \item{year}{integer. Year.}
#'   \item{month}{integer. Month.}
#'   \item{mday}{integer. Day of the month.}
#'   \item{yquarter}{character. Ready for tsibble::yearquarter.}
#'   \item{ymonth}{character. Ready for tsibble::yearmonth.}
#'   \item{yweek}{character. Ready for tsibble::yearweek.}
#'   \item{is_53weeks}{logical. True if yweek has 53 weeks.}
#'   \item{wday}{integer/numeric. Day of the week. Monday = 1.}
#'   \item{wday_lbl}{integer/numeric. Day of the week as character.}
#'   \item{is_leap}{logical. True if leap year.}
#' }
"date_reference"
