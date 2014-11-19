\name{redshift.disconnect}
\alias{redshift.disconnect}
\title{Disconnect from the Redshift Cluster}
\description{
Destroys the RJDBC connection to the Redshift cluster.
}
\usage{
redshift.disconnect(conn)
}
\arguments{
  \item{conn}{JDBC Connection returned by redshift.connect}
}
\value{
TRUE/Failure Notification
}
\author{Eeshan Chatterjee}
\examples{
require(redshift)

#redshift.disconnect(connObj)

}
\keyword{database}
\keyword{connection}
