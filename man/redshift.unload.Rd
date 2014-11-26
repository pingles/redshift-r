\name{redshift.unload}
\alias{redshift.unload}
\title{Unloads aquery to AWS S3 cloud}
\description{
Unloads a redshift query to the AWS S3 cloud
}
\usage{
redshift.unload(conn, query, filename, aws.accesskey, aws.secretkey, delim = ',', allowOverwrite = TRUE, parallel = TRUE, zip = TRUE)
}
\arguments{
  \item{conn}{The database connection}
  \item{query}{The Query to be unloaded}
  \item{filename}{The destination file path and name}
  \item{aws.accesskey}{The AWS Access Key}
  \item{aws.secretkey}{The AWS Secret Key}
  \item{delim}{The Delimiter to be used. Defaults to ','}
  \item{allowOverwrite}{Logical. Allow Redshift to overwrite existing files with same name? Defaults to TRUE.}
  \item{parallel}{Logical. Allow Redshift to run parallelqueries and create multiple files? Defaults to TRUE.}
  \item{zip}{Logical. Allow Redshift to dump a zipped file? Defaults to TRUE.}
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
#redshift.unload(conn,query = "SELECT * from mytable where name like 'Eeshan'",filename = "s3://Eeshan/files/test.txt",aws.accesskey = <AWS #Access Key>,aws.secretkey = <AWS Secret Key>,delim = ",",parallel = F)
}
\keyword{ unload }

