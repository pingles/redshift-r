\name{redshift-package}
\alias{redshift-package}
\alias{redshift}
\docType{package}
\title{Little library to connect to Amazon's Redshift Service}
\description{
Provides a few functions (and the required JDBC driver) to connect to Amazon's Redshift service.
}
\details{
\tabular{ll}{
Package: \tab redshift\cr
Type: \tab Package\cr
Version: \tab 0.2\cr
Date: \tab 2013-04-29\cr
License: \tab Eclipse Public License\cr
}
}
\author{
Paul Ingles
Maintainer: Paul Ingles <paul@oobaloo.co.uk>
}
\keyword{ package }
\keyword{ database }
\examples{
## the JDBC URL can be found in Amazon's AWS Control Panel, just copy and paste it into here
#conn <- redshift.connect("jdbc:postgresql://blah.blah.eu-west-1.redshift.amazonaws.com:5439/data", "username", "password")

## list the names of the tables we can query
#tables <- redshift.tables(conn)
}
