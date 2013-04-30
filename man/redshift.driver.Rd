\name{redshift.driver}
\alias{redshift.driver}
\title{Create JDBC driver}
\description{
Creates the JDBC driver used by RJDBC to connect to Redshift. Doesn't need to be used directly by the user
\link{redshift.connect} calls this automatically.
}
\usage{
redshift.driver()
}
\value{
JDBC Driver
}
\author{Paul Ingles}
\examples{
require(redshift)
drvr <- redshift.driver()
}
\keyword{database}
