sqlr <- read_csv("~/Documents/sqlr.csv") |>
  inner_join(lookup_sql |>
               nest(.by = Description)) |>
  unnest(everything())

# sqlr |>
#   mutate(
  x1 <-   sqlr$Name |>  str_replace_all(" \\[", ' / \\[') |>
      str_replace_all(" \\] ", " \\] / ") |>
      str_replace_all("/ /", "/") |>
      str_split(" / ")
  id <- x1 |> map( ~ length(.x)) |> simplify() > 1

  x1[id] |> imap(~tibble(id = .y,
                         type = .x[1],
                        specs0 = .x[-1]) |>
                  mutate(is_optional = str_detect(specs0,"\\["))) |>
    list_rbind() |>
    mutate(specs_opt_int = str_detect(specs0, "\\[") & str_detect(specs0, "\\("),
           specs_opt_chr = str_detect(specs0, "\\[") & !str_detect(specs0, "\\("),
           specs = str_remove_all(specs0, "\\(") |> str_remove_all("\\)") |>
             str_remove_all('\\[') |>  str_remove_all('\\]') |> str_squish() |> map(str_split_comma)) |>
    unnest(specs) |>
    mutate( class = if_else(specs %in% c('s', 'p', 'n'), "integer", "character")) |>
    nest(specs0 = (specs0),
         data = is_optional : class, .by = c(id, type))  |>
    mutate(specs0 = specs0 |> map(~ .x |> pull(specs0) |> unique() |> str_flatten(collapse = " "))) |>
    unnest(everything()) |>
    mutate(specs1 = specs0 |>
             str_replace_all( "\\] \\[", " x/x ") |>
             str_replace_all( "\\]", " x/ ") |>
             str_remove_all("\\]") |> str_remove_all("\\[")) |> nest(.by = c(id, type, specs0)) |>
    mutate(data = data |> set_names(type),
           specs1 = specs0 |>
             str_replace_all("\\[", "\\{ \\.") |>
             str_replace_all("\\]", "\\ }") |>
             str_remove_all(" \\(") |>
             str_remove_all("\\) ") |>
             str_replace_all('\\. ', '\\.') |>
             str_remove_all('\\, ') |>
             str_replace_all('.without time zone', 'wo_tz') |>
             str_squish()) |> view()
  |>
    pull(data)




  x2 <-   sqlr$Aliases |>  str_replace_all(" \\[", ' / \\[') |>
    str_replace_all(" \\] ", " \\] / ") |>
    str_replace_all("/ /", "/") |>
    str_split(" / ")
  id2 <- x2 |> map( ~ length(.x)) |> simplify() > 1

  x2[id2] |> imap(~tibble(id = .y,
                         type = .x[1],
                         specs0 = .x[-1]) |>
                   mutate(is_optional = str_detect(specs0,"\\["))) |>
    list_rbind() |>
    mutate(specs_opt_int = str_detect(specs0, "\\[") & str_detect(specs0, "\\("),
           specs_opt_chr = str_detect(specs0, "\\[") & !str_detect(specs0, "\\("),
           specs = str_remove_all(specs0, "\\(") |> str_remove_all("\\)") |>
             str_remove_all('\\[') |>  str_remove_all('\\]') |> str_squish() |> map(str_split_comma)) |>
    unnest(specs) |>
    mutate( class = if_else(specs %in% c('s', 'p', 'n'), "integer", "character"))
  setwd()
  save.image()
