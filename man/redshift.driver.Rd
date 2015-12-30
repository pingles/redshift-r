\name{redshift.driver}
\alias{redshift.driver}
\title{Create JDBC driver}
\description{
Creates the JDBC driver used by RJDBC to connect to Redshift. Doesn't need to be used directly by the user
\link{redshift.connect} calls this automatically.
}
\usage{
redshift.driver(custom_driver = "default")
}
\arguments{
  \item{custom_driver}{Option to point to a custom PostgreSQL driver JAR. Class name must be "com.amazon.redshift.jdbc41.Driver"}
}
\value{
JDBC Driver
}
\author{Paul Ingles}
\examples{
require(redshift)
drvr <- redshift.driver()

# drvr <- redshift.driver("/home/myuser/psql/postgresql-9.2-1003.jdbc3.jar")
}
\keyword{database}
