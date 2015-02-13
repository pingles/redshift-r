\name{redshift.columns}
\alias{redshift.columns}
\title{
Columns in Redshift table
}
\description{
Provides metadata about the columns available within a Redshift table
}
\usage{
redshift.columns(conn, schema='public', tableName)
}
\arguments{
  \item{conn}{The RJDBC connection object created with \link{redshift.connect}}
  \item{schema}{The schema name}
  \item{tableName}{Table name as a string}
}
\value{
Returns a dataframe listing: column name, data type, nullable.
}
\author{
Paul Ingles
}
\examples{
require(redshift)
# conn <- redshift.connect("jdbc:postgresql://blah.blah.eu-west-1.redshift.amazonaws.com:5439/data", "username", "password")
# cols <- redshift.columns(conn, "weblog")
# cols2 <- redshift.columns(conn, "dev", weblog")
}
\keyword{ database }
