\name{redshift.tables}
\alias{redshift.tables}
\title{List available Tables}
\description{
Lists the names of the tables available in the Redshift database
}
\usage{
redshift.tables(conn)
}
\arguments{
  \item{conn}{RJDBC connection created with \link{redshift.connect}}
}
\value{
Data frame containing the names of the tables.
}
\author{
Paul Ingles
}
\seealso{
\code{\link{redshift.connect}}
}
\examples{
require(redshift)
# conn <- redshift.connect("jdbc:postgresql://blah.blah.eu-west-1.redshift.amazonaws.com:5439/data", "username", "password")
# tables <- redshift.tables(conn)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
