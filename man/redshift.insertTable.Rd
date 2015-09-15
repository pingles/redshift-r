\name{redshift.insertTable}
\alias{redshift.insertTable}
\title{Insert a data.frame into a redshift table}
\description{
Insert a data.frame into a redshift table. Encoding must be utf-8. Use iconv() to enforce so.
}
\usage{
redshift.insertTable(conn, dataframe, rs.tablename)
}
\arguments{
  \item{conn}{The database connection}
  \item{dataframe}{The R dataframe to be inserted}
  \item{rs.tablename}{The destination Redshift table}
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
#redshift.insertTable(conn,dataframe = myDF, rs.tablename = "myRSTable")
}
\keyword{ insert table }

