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

redshift.tables <- function(conn) {
  dbGetQuery(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
}

redshift.columns <- function(conn, tableName) {
  sql <- paste("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name ='", tableName, "';", sep="")
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

redshift.insertTable = function(conn, dataframe, rs.tablename){
  insertValuesString = paste(apply(dataframe,1,function(x){
    paste0("(",paste(sapply(x,function(val){
      paste0("'",gsub("'","''",val),"'")
    }),collapse = ","),")")
  }),collapse = ',')
  
  insertQuery = paste0("Insert into ",rs.tablename," values ",insertValuesString)
  dbSendUpdate(conn,insertQuery)
}