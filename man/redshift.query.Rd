\name{redshift.query}
\alias{redshift.query}
\title{Executes a query}
\description{
Executes a query and returns the result set as a dataframe.
}
\usage{
redshift.query(conn, ...)
}
\arguments{
  \item{conn}{The database connection}
  \item{\dots}{Strings representing the query. These will be concatenated together with \code{\link{paste}} to make it easier to write multi-line queries}
}
\value{
Data frame containing the results.
}
\author{
Paul Ingles
}
\examples{
require(redshift)
# conn <- redshift.connect("jdbc:postgresql://blah.blah.eu-west-1.redshift.amazonaws.com:5439/data", "username", "password")
results <- redshift.query(conn, "SELECT MAX(timestamp)",
                                "FROM weblog")
}
\keyword{ database }