\name{redshift.submitquery}
\alias{redshift.submitquery}
\title{Executes a DBML query}
\description{
Submits a DBML query to the database.
}
\usage{
redshift.submitquery(conn, ...)
}
\arguments{
  \item{conn}{The database connection}
  \item{\dots}{Strings representing the query. These will be concatenated together with \code{\link{paste}} to make it easier to write multi-line queries. Allows sending DBML like queries- requiring no response from the DB.}
}
\value{
NA
}
\author{
Eeshan Chatterjee
}
\examples{
require(redshift)
# conn <- redshift.connect("jdbc:postgresql://blah.blah.eu-west-1.redshift.amazonaws.com:5439/data", "username", "password")
#results <- redshift.query(conn, "INSERT INTO mytable",
#                                "VALUES (2,652,'Eeshan','Chatterjee')")
}
\keyword{ database }