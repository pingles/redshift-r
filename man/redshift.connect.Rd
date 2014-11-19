\name{redshift.connect}
\alias{redshift.connect}
\title{Connect to Redshift Cluster}
\description{
Creates an RJDBC connection to the Redshift cluster.
}
\usage{
redshift.connect(jdbcUrl, username, password, custom_driver = "default")
}
\arguments{
  \item{jdbcUrl}{The JDBC URL as specified in the Amazon AWS Management Console}
  \item{username}{The username to authenticate with}
  \item{password}{The password to authenticate with}
  \item{custom_driver}{Option to point to a custom PostgreSQL driver JAR. Class name must be "org.postgresql.Driver"}
}
\value{
RJDBC connection object
}
\author{Paul Ingles}
\examples{
require(redshift)

## connect to our cluster
#conn <- redshift.connect("jdbc:postgresql://blah.blah.eu-west-1.redshift.amazonaws.com:5439/data", "username", "password")

#conn <- redshift.connect("jdbc:postgresql://blah.blah.eu-west-1.redshift.amazonaws.com:5439/data", "username", "password","/home/myuser/psql/postgresql-9.2-1003.jdbc3.jar")
}
\keyword{database}
\keyword{connection}
