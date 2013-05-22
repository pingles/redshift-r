# redshift package code

library(RJDBC)

redshift.driver <- function() {
  driver.class.path <- system.file("java", "postgresql-8.4-703.jdbc4.jar", package = "redshift")
  return(JDBC("org.postgresql.Driver", driver.class.path, identifier.quote="`"))
}

redshift.connect <- function(jdbcUrl, username, password) {
  driver <- redshift.driver()
  lead <- ifelse(grepl("\\?", jdbcUrl), "&", "?")
  url <- paste(jdbcUrl, lead, "user=", username, "&password=", password, sep="")
  conn <- dbConnect(driver, url)
}

redshift.tables <- function(conn) {
  dbGetQuery(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
}

redshift.columns <- function(conn, tableName) {
  sql <- paste("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name ='", tableName, "';", sep="")
  dbGetQuery(conn, sql)
}

redshift.query <- function(conn, ...) {
  dbGetQuery(conn, paste(...))
}