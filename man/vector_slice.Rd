% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/vector_slice.R
\name{vector_slice}
\alias{vector_slice}
\alias{slice_vec}
\alias{slice_first}
\alias{slice_last}
\title{Vector Slicing}
\usage{
slice_vec(x, i, sort = TRUE, negate = FALSE, simplify = FALSE)

slice_first(x, n = 1, negate = FALSE, simplify = FALSE)

slice_last(x, n, negate = FALSE, simplify = FALSE)
}
\arguments{
\item{x}{vector.}

\item{i}{integer or integer vector. Negative integers indicate positions from the end.}
}
\value{
a vector of the same kind of x.
}
\description{
Vector Slicing
}
