# redshift package code

library(RJDBC)

redshift.driver <- function(postgresql_driver = "default") {
  if(postgresql_driver != "default"){
    driver.class.path <- postgresql_driver
  } else  driver.class.path <- system.file("java", "postgresql-8.4-703.jdbc4.jar", package = "redshift")
  
  return(JDBC("org.postgresql.Driver", driver.class.path, identifier.quote="`"))
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

redshift.columns <- function(conn, tableName, schema='public') {
  sql <- paste0("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '", schema, "' AND table_name ='", tableName, "';")
  dbGetQuery(conn, sql)
}

redshift.query <- function(conn, ...) {
  dbGetQuery(conn, paste(..., collapse=' ', sep=' '))
}

redshift.submitquery <- function(conn, ...) {
  dbSendUpdate(conn, paste(..., collapse=' ', sep=' '))
}

redshift.unload <- function(conn, query, filename, aws.accesskey = '', aws.secretkey = '', delim = ',',
                            allowOverwrite = TRUE, parallel = TRUE, zip = TRUE, addquotes = FALSE, aws.role = ''){
  query = gsub("'","''",query)
  sql = paste0("('",gsub(";","",query),"')")
  loc = paste0("'",filename,"'")
  if (aws.role != '') {
      cred = paste0("'aws_iam_role=",aws.role,"'")
  } else if (aws.accesskey != '' && aws.secretkey != '') {
      cred = paste0("'aws_access_key_id=",aws.accesskey,";aws_secret_access_key=",aws.secretkey,"'")
      warning("AWS recommend role-based access control: http://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-access-permissions.html#copy-usage_notes-access-role-based")
  } else {
      stop("aws.accesskey and aws.secretkey, or aws.role must be set")
  }
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
