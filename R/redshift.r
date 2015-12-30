# redshift package code

library(RJDBC)

redshift.driver <- function(postgresql_driver = "default") {
  if(postgresql_driver != "default"){
    driver.class.path <- postgresql_driver
  } else  driver.class.path <- system.file("java", "RedshiftJDBC41-1.1.7.1007.jar", package = "redshift")
  
  return(JDBC("com.amazon.redshift.jdbc41.Driver", driver.class.path, identifier.quote="`"))
}

redshift.connect <- function(jdbcUrl, username, password, custom_driver = "default") {
  driver <- redshift.driver(custom_driver)
  lead <- ifelse(grepl("\\?", jdbcUrl), "&", "?")
  url <- paste(jdbcUrl, lead, "user=", username, "&password=", password, sep="")
  conn <- dbConnect(driver, url)
}

redshift.disconnect <- function(conn){
  if(!dbDisconnect(conn)) print("Unable to Close Connection") 
}

redshift.tables <- function(conn, schema='public') {
  sql <- paste0("SELECT table_name FROM information_schema.tables WHERE table_schema = '", schema, "';")
  dbGetQuery(conn, sql)
}

redshift.columns <- function(conn, schema='public', tableName) {
  sql <- paste0("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '", schema, "' AND table_name ='", tableName, "';")
  dbGetQuery(conn, sql)
}

redshift.query <- function(conn, ...) {
  dbGetQuery(conn, paste(..., collapse=' ', sep=' '))
}

redshift.submitquery <- function(conn, ...) {
  dbSendUpdate(conn, paste(..., collapse=' ', sep=' '))
}

redshift.unload <- function(conn, query, filename, aws.accesskey, aws.secretkey, delim = ',', 
                            allowOverwrite = TRUE, parallel = TRUE, zip = TRUE, addquotes = FALSE){
  query = gsub("'","''",query)
  sql = paste0("('",gsub(";","",query),"')")
  loc = paste0("'",filename,"'")
  cred = paste0("'aws_access_key_id=",aws.accesskey,";aws_secret_access_key=",aws.secretkey,"'")
  delimiter = paste0("'",delim,"'")
  if(!parallel) par = "PARALLEL OFF" else par = ""
  if(!zip) gzip = "" else gzip = "GZIP"
  if(!addquotes) addquotes = "" else addquotes = "ADDQUOTES"
  if(!allowOverwrite) aow = "" else aow = "ALLOWOVERWRITE"
  
  unload.query = paste(c("UNLOAD",sql, "TO", loc, "CREDENTIALS", cred, "DELIMITER", delimiter, gzip, par, addquotes, aow,";"),collapse = " ")
  unload.query = gsub("\\s+"," ",unload.query)
  print(unload.query)
  dbSendUpdate(conn,unload.query)
}
