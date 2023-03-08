library(DBI)
library(tidyverse)
library(dbplyr)
library(stringr)
library(lubridate)
library(openair)

round2 <- function(x, n) {
  posneg = sign(x)
  z = abs(x)*10^n
  z = z + 0.5
  z = trunc(z)
  z = z/10^n
  z*posneg
}

MV_data_call_pollutant <- function(pollutant, start_time, end_time, timebase) {
  
  #uncomment below for testing the function
  #station_code <- c("006","018")
  #start_time <- '01/01/2021 00:00:00.0'
  #end_time   <- '12/31/2021 23:00:00.0'
  #timebase <- "60"
  #pollutant <- "PM2.5"
  
  if (pollutant == "PM2.5"){
    pollutant <- c("PM2.5_SHARP", "PM2.5_iSHARP")
  }
  
  station_list <- read.csv("C:/Users/khowe/Documents/Ambient_Data_R/helper_functions/stations.csv")
  
  pollutant_counter = 1
  station_code <- c("001","002","004","006","009","012","013","014","015","017","018","020","022","023","024","026","027","029","030","031","032","033","035","041","055","057","058","080","174","175","176","177","178","179","180","274","085","286","138","129","065","148","164","158", "287", "206","204")
  
  
  for (k in 1:length(station_code)){  
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    #get the data from the DB
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    
    #Start connection to database
    con <- dbConnect(odbc::odbc(), 
                     driver = "SQL Server",
                     server = "PRDAQMS02", 
                     database = "EnvAdmin",
                     uid = "MATLABReader",
                     pwd = "matlabrEADER2018")
    #start by getting the most up to date list of monitors
    query <- 'SELECT MOT_MonitorTypeCode, MOT_MonitorName FROM dbo.TB_MONITOR_TYPE'
    db_sta_monType <- dbGetQuery(con, query)
    
    
    #build the query to the station an monitor lists from the database
    query <- paste('SELECT MON_MonitorType, MON_MonitorChanel FROM dbo.TB_MONITOR WHERE MON_StationSerialCode=',station_code[k], sep = " ")
    query <- paste(query, 'ORDER BY MON_MonitorType ASC', sep = " ")
    
    #create a dataframe showing the channel number for the monitor
    db_sta_mon <- dbGetQuery(con, query)
    db_sta_mon <- merge(db_sta_monType, db_sta_mon, by.x = "MOT_MonitorTypeCode", by.y = "MON_MonitorType")
    #build a new query to get data from the database
    table_name <- paste("S", station_code[k], sep = "")
    table_name <- paste(table_name,"T",sep="")
    table_name <- paste(table_name,timebase,sep="")
    
    query1 <- paste('SELECT * FROM', table_name, sep = " ")
    query2 <- paste('WHERE Date_Time BETWEEN \'', start_time, '\' AND \'', end_time,"\'", sep = "")
    query <- paste(query1, query2, sep = " ")
    
    
    #print(paste("Table exists: ",query,sep = ""))
    res <- dbGetQuery(con,query)
    #some dataframes will be empty. Test to see if that is true and if it is, skip
    dim_res <- dim(res)
    if(dim_res[1] == 0) next
    #print(paste("There is data in the table: ",station_code[k],sep=""))
    
    
    #rename the columns based on the monitor list
    res_colnames <- colnames(res)
    #need to look at the number at the end of the string for the columns names
    res_moncode <- as.numeric(str_extract(res_colnames, "[0-9]+"))
    
    #skip the first one since it is the date column.
    #This is to skip any errors where there is no monitor specified
    for (j in 2:length(res_moncode)){  
      if(identical(db_sta_mon$MOT_MonitorName[db_sta_mon$MON_MonitorChanel == res_moncode[j]],character(0))){
        res_colnames[j] <- res_moncode[j]
      }else{  
        res_colnames[j] <- db_sta_mon$MOT_MonitorName[db_sta_mon$MON_MonitorChanel == res_moncode[j]]
      }
    }
    
    
    #This will append "status" to the status column names. Then, invalidate anything that is not Status = 1
    for (j in seq(2, length(res_colnames), 2)){
      res_colnames[j+1] <- paste(res_colnames[j+1],"_status", sep = "")
      res[which(!res[,j+1] %in% c(1,14,77,78,79,80,81,82,83,85,86,89,93,94,95)),j] <- NA
    }
    
    #rename the columns of the dataframe
    names(res) <- res_colnames
    colnames(res)[colnames(res)=="Date_Time"] <- "date"
    
    #This grabs only the data, without any status information
    res_ONLY <- res[,seq(2, ncol(res), 2)]
    res_ONLY$date <- res$date
    
    #The database contains hour ending data, which means the 1:00 average is for data recorded between
    #0:00 and 1:00. This needs to be converted to allow for the 0:00 average to be on the proper day. 
    
    #Subtract an hour off the dataset
    res_ONLY$date_time_ending <- res_ONLY$date
    res_ONLY$date <- res_ONLY$date - 3600
    
    
    #Check if pollutant exists in data
    #pollutant
    
    #-------------------------------------------------------------------------
    # Prep pollutant data
    #-------------------------------------------------------------------------
    if(pollutant_counter == 1){
      
      pollutant_master <- res_ONLY %>%
                            select_if(function(x) !(all(is.na(x)))) %>%
                            select(date,any_of(pollutant)) %>%
                            rename_with(~paste(.x,station_list[which(as.numeric(station_code[k]) == station_list$STA_SerialCode),c("STA_ShortName")],sep="_"),any_of(pollutant))
        
      pollutant_counter <- pollutant_counter + 1
      
    }else{
      
      pollutant_master <- res_ONLY %>%
                            select_if(function(x) !(all(is.na(x)))) %>%
                            select(date,any_of(pollutant)) %>%
                            rename_with(~paste(.x,station_list[which(as.numeric(station_code[k]) == station_list$STA_SerialCode),c("STA_ShortName")],sep="_"),any_of(pollutant)) %>%
                            full_join(pollutant_master, by = "date")
      

      pollutant_counter <- pollutant_counter + 1
      
    }
    
    
  }
  
  #clean things up by removing empty columns
  pollutant_master <- pollutant_master[colSums(is.na(pollutant_master)) != nrow(pollutant_master)]
  
  
  return(pollutant_master)
  
}


#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#This is a modification of the station call function. The original function concatenates
# the pollutant queried with the station ID. This makes it challenging to work with in Tableau
# because the labels are too clunky.

MV_data_call_pollutant_Tableau <- function(pollutant, start_date, end_date, timebase, statusFlags) {
  
  #uncomment below for testing the function
  #station_code <- c("018")
  #start_time <- '01/01/2022 00:00:00.0'
  #end_time   <- '01/31/2022 23:00:00.0'
  #timebase <- "01"
  #pollutant <- "Precip"
  #statusFlags <- "F"
  
  if (pollutant == "PM2.5"){
    pollutant <- c("PM2.5_SHARP", "PM2.5_iSHARP")
  }
  
  station_list <- read.csv("C:/Users/khowe/Documents/Ambient_Data_R/helper_functions/stations.csv", stringsAsFactors = FALSE)
  
  pollutant_counter = 1
  station_code <- c("001","002","004","006","009","012","013","014","015","017","018","020","022","023","024","026","027","029","030","031","032","033","035","038","041","055","057","058","080","174","175","176","177","178","179","180","274","085","286","138","129","065","148","164","158", "287", "278","279", "206","204","161","154")
  
  
  for (k in 1:length(station_code)){  
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    #get the data from the DB
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    
    #Start connection to database
    con <- dbConnect(odbc::odbc(), 
                     driver = "SQL Server",
                     server = "PRDAQMS02", 
                     database = "EnvAdmin",
                     uid = "MATLABReader",
                     pwd = "matlabrEADER2018")
    #start by getting the most up to date list of monitors
    query <- 'SELECT MOT_MonitorTypeCode, MOT_MonitorName FROM dbo.TB_MONITOR_TYPE'
    db_sta_monType <- dbGetQuery(con, query)
    
    
    #build the query to the station an monitor lists from the database
    query <- paste('SELECT MON_MonitorType, MON_MonitorChanel FROM dbo.TB_MONITOR WHERE MON_StationSerialCode=',station_code[k], sep = " ")
    query <- paste(query, 'ORDER BY MON_MonitorType ASC', sep = " ")
    
    #create a dataframe showing the channel number for the monitor
    db_sta_mon <- dbGetQuery(con, query)
    db_sta_mon <- merge(db_sta_monType, db_sta_mon, by.x = "MOT_MonitorTypeCode", by.y = "MON_MonitorType")
    #build a new query to get data from the database
    table_name <- paste("S", station_code[k], sep = "")
    table_name <- paste(table_name,"T",sep="")
    table_name <- paste(table_name,timebase,sep="")
    
    query1 <- paste('SELECT * FROM', table_name, sep = " ")
    query2 <- paste('WHERE Date_Time BETWEEN \'', start_time, '\' AND \'', end_time,"\'", sep = "")
    query <- paste(query1, query2, sep = " ")
    
    
    #print(paste("Table exists: ",query,sep = ""))
    res <- dbGetQuery(con,query)
    #some dataframes will be empty. Test to see if that is true and if it is, skip
    dim_res <- dim(res)
    if(dim_res[1] == 0) next
    #print(paste("There is data in the table: ",station_code[k],sep=""))
    
    
    #rename the columns based on the monitor list
    res_colnames <- colnames(res)
    #need to look at the number at the end of the string for the columns names
    res_moncode <- as.numeric(str_extract(res_colnames, "[0-9]+"))
    
    #skip the first one since it is the date column.
    #This is to skip any errors where there is no monitor specified
    for (j in 2:length(res_moncode)){  
      if(identical(db_sta_mon$MOT_MonitorName[db_sta_mon$MON_MonitorChanel == res_moncode[j]],character(0))){
        res_colnames[j] <- res_moncode[j]
      }else{  
        res_colnames[j] <- db_sta_mon$MOT_MonitorName[db_sta_mon$MON_MonitorChanel == res_moncode[j]]
      }
    }
    
    
    #This will append "status" to the status column names. Then, invalidate anything that is not Status = 1
    for (j in seq(2, length(res_colnames), 2)){
      res_colnames[j+1] <- paste(res_colnames[j+1],"_status", sep = "")
      res[which(!res[,j+1] %in% c(1,14,77,78,79,80,81,82,83,85,86,89,93,94,95)),j] <- NA
    }
    
    #rename the columns of the dataframe
    names(res) <- res_colnames
    colnames(res)[colnames(res)=="Date_Time"] <- "date"
    
    #This grabs only the data, without any status information
    if(statusFlags == "T"){
      res_ONLY <- res
    }else {
      res_ONLY <- res[,seq(2, ncol(res), 2)]
    }
    
    res_ONLY$date <- res$date
    
    #The database contains hour ending data, which means the 1:00 average is for data recorded between
    #0:00 and 1:00. This needs to be converted to allow for the 0:00 average to be on the proper day. 
    
    #Subtract an hour off the dataset
    res_ONLY$date_time_ending <- res_ONLY$date
    res_ONLY$date <- res_ONLY$date - 3600
    
    
    #Check if pollutant exists in data
    #pollutant
    
    #-------------------------------------------------------------------------
    # Prep pollutant data
    #-------------------------------------------------------------------------
    if(pollutant_counter == 1){
      
      pollutant_master <- res_ONLY %>%
        select_if(function(x) !(all(is.na(x)))) %>%
        select(date,any_of(pollutant)) %>%
        #rename_with(station_list[which(as.numeric(station_code[k]) == station_list$STA_SerialCode),c("STA_ShortName")])
        rename_with(~station_list[which(as.numeric(station_code[k]) == station_list$STA_SerialCode),c("STA_ShortName")],any_of(pollutant))
      
      pollutant_counter <- pollutant_counter + 1
      
    }else{
      
      pollutant_master <- res_ONLY %>%
        select_if(function(x) !(all(is.na(x)))) %>%
        select(date,any_of(pollutant)) %>%
        #rename_with(station_list[which(as.numeric(station_code[k]) == station_list$STA_SerialCode),c("STA_ShortName")]) %>%
        rename_with(~station_list[which(as.numeric(station_code[k]) == station_list$STA_SerialCode),c("STA_ShortName")],any_of(pollutant)) %>%
        full_join(pollutant_master, by = "date")
      
      
      pollutant_counter <- pollutant_counter + 1
      
    }
    
    
  }
  
  #clean things up by removing empty columns
  pollutant_master <- pollutant_master[colSums(is.na(pollutant_master)) != nrow(pollutant_master)]
  
  
  return(pollutant_master)
  
}




#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#Slight varition of the function above, returns ALL data and status columns
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
MV_data_call_pollutant_wStatus <- function(pollutant, start_date, end_date, timebase) {
  
  #uncomment below for testing the function
  #station_code <- "018"
  #start_time <- '01/01/2021 00:00:00.0'
  #end_time   <- '01/31/2021 23:00:00.0'
  #timebase <- "60"
  #pollutant <- "CO"
  
  pollutant_counter = 1
  station_code <- c("001","002","004","006","009","012","013","014","015","017","018","020","022","023","024","026","027","029","030","031","032","033","035","041","055","057","058","080","174","274","286","148","287", "206","204")
  station_list <- read.csv("C:/Users/khowe/Documents/Ambient_Data_R/helper_functions/stations.csv")
  
  for (k in 1:length(station_code)){  
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    #get the data from the DB
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    
    #Start connection to database
    con <- dbConnect(odbc::odbc(), 
                     driver = "SQL Server",
                     server = "PRDAQMS02", 
                     database = "EnvAdmin",
                     uid = "MATLABReader",
                     pwd = "matlabrEADER2018")
    #start by getting the most up to date list of monitors
    query <- 'SELECT MOT_MonitorTypeCode, MOT_MonitorName FROM dbo.TB_MONITOR_TYPE'
    db_sta_monType <- dbGetQuery(con, query)
    
    
    #build the query to the station an monitor lists from the database
    query <- paste('SELECT MON_MonitorType, MON_MonitorChanel FROM dbo.TB_MONITOR WHERE MON_StationSerialCode=',station_code[k], sep = " ")
    query <- paste(query, 'ORDER BY MON_MonitorType ASC', sep = " ")
    
    #create a dataframe showing the channel number for the monitor
    db_sta_mon <- dbGetQuery(con, query)
    db_sta_mon <- merge(db_sta_monType, db_sta_mon, by.x = "MOT_MonitorTypeCode", by.y = "MON_MonitorType")
    #build a new query to get data from the database
    table_name <- paste("S", station_code[k], sep = "")
    table_name <- paste(table_name,"T",sep="")
    table_name <- paste(table_name,timebase,sep="")
    
    query1 <- paste('SELECT * FROM', table_name, sep = " ")
    query2 <- paste('WHERE Date_Time BETWEEN \'', start_time, '\' AND \'', end_time,"\'", sep = "")
    query <- paste(query1, query2, sep = " ")
    
    
    #print(paste("Table exists: ",query,sep = ""))
    res <- dbGetQuery(con,query)
    #some dataframes will be empty. Test to see if that is true and if it is, skip
    dim_res <- dim(res)
    if(dim_res[1] == 0) next
    #print(paste("There is data in the table: ",station_code[k],sep=""))
    
    
    #rename the columns based on the monitor list
    res_colnames <- colnames(res)
    #need to look at the number at the end of the string for the columns names
    res_moncode <- as.numeric(str_extract(res_colnames, "[0-9]+"))
    
    #skip the first one since it is the date column.
    #This is to skip any errors where there is no monitor specified
    for (j in 2:length(res_moncode)){  
      if(identical(db_sta_mon$MOT_MonitorName[db_sta_mon$MON_MonitorChanel == res_moncode[j]],character(0))){
        res_colnames[j] <- res_moncode[j]
      }else{  
        res_colnames[j] <- db_sta_mon$MOT_MonitorName[db_sta_mon$MON_MonitorChanel == res_moncode[j]]
      }
    }
    
    
    #This will append "status" to the status column names. Then, invalidate anything that is not Status = 1
    for (j in seq(2, length(res_colnames), 2)){
      res_colnames[j+1] <- paste(res_colnames[j+1],"_status", sep = "")
      #res[which(!res[,j+1] %in% c(1,14,77,78,79,80,81,82,83,85,86,89,93,94,95)),j] <- NA
    }
    
    #rename the columns of the dataframe
    names(res) <- res_colnames
    colnames(res)[colnames(res)=="Date_Time"] <- "date"
    
    #This grabs only the data, without any status information
    #res_ONLY <- res[,seq(2, ncol(res), 2)]
    res_ONLY <- res
    res_ONLY$date <- res$date
    
    #The database contains hour ending data, which means the 1:00 average is for data recorded between
    #0:00 and 1:00. This needs to be converted to allow for the 0:00 average to be on the proper day. 
    
    #Subtract an hour off the dataset
    res_ONLY$date_time_ending <- res_ONLY$date
    res_ONLY$date <- res_ONLY$date - 3600
    
    poll_status <- paste(pollutant,"_status", sep = "")
    
    #-------------------------------------------------------------------------
    # Prep pollutant data
    #-------------------------------------------------------------------------
    if(pollutant_counter == 1){
      #Test to make sure the ozone column exists, if not, skip
      if(pollutant %in% names(res_ONLY)){
        pollutant_master <- res_ONLY %>%
                              select(date,pollutant,poll_status) %>%
                              mutate(stationID = as.character(station_list[which(as.numeric(station_code[k]) == station_list$STA_SerialCode),c("STA_ShortName")])) %>%
                              rename("status_flag"=poll_status )
          
        #res_ONLY[,c("date",pollutant,poll_status)] %>%
        pollutant_counter <- pollutant_counter + 2
      }
    }else{
      #Merge the ozone data
      if(pollutant %in% names(res_ONLY)){
        pollutant_holder <- res_ONLY %>%
                              select(date,pollutant,poll_status) %>%
                              mutate(stationID = as.character(station_list[which(as.numeric(station_code[k]) == station_list$STA_SerialCode),c("STA_ShortName")])) %>%
                              rename("status_flag"=poll_status )
        
        pollutant_master <- rbind(pollutant_master,pollutant_holder)
        pollutant_counter <- pollutant_counter + 2
      }
    }
    

    
  }
  

  return(pollutant_master)
  
}

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------


MV_station_call <- function(station_code, start_time, end_time, timebase, statusFlags) {

  #uncomment below for testing the function
  #station_code <- "002"
  #start_time <- '01/01/2015 00:00:00.0'
  #end_time   <- '12/31/2016 23:00:00.0'
  #timebase <- "60"
  #statusFlags <- "F"
  
  
  
  station_list <- read.csv("stations.csv")
  
  #if(station_code == "080"){
  #  station_code[2] = "065"
  #}
  
  for (k in 1:length(station_code)){  
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    #get the data from the DB
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    
    #Start connection to database
    con <- dbConnect(odbc::odbc(), 
                     driver = "SQL Server",
                     server = "PRDAQMS02", 
                     database = "EnvAdmin",
                     uid = "MATLABReader",
                     pwd = "matlabrEADER2018")
    #start by getting the most up to date list of monitors
    query <- 'SELECT MOT_MonitorTypeCode, MOT_MonitorName FROM dbo.TB_MONITOR_TYPE'
    db_sta_monType <- dbGetQuery(con, query)
    
    
    #build the query to the station an monitor lists from the database
    query <- paste('SELECT MON_MonitorType, MON_MonitorChanel FROM dbo.TB_MONITOR WHERE MON_StationSerialCode=',station_code[k], sep = " ")
    query <- paste(query, 'ORDER BY MON_MonitorType ASC', sep = " ")
    
    #create a dataframe showing the channel number for the monitor
    db_sta_mon <- dbGetQuery(con, query)
    db_sta_mon <- merge(db_sta_monType, db_sta_mon, by.x = "MOT_MonitorTypeCode", by.y = "MON_MonitorType")
    #build a new query to get data from the database
    table_name <- paste("S", station_code[k], sep = "")
    table_name <- paste(table_name,"T",sep="")
    table_name <- paste(table_name,timebase,sep="")
    
    query1 <- paste('SELECT * FROM', table_name, sep = " ")
    query2 <- paste('WHERE Date_Time BETWEEN \'', start_time, '\' AND \'', end_time,"\'", sep = "")
    query <- paste(query1, query2, sep = " ")
    
    
    #print(paste("Table exists: ",query,sep = ""))
    res <- dbGetQuery(con,query)
    #some dataframes will be empty. Test to see if that is true and if it is, skip
    dim_res <- dim(res)
    if(dim_res[1] == 0) {
       station_master <- NA
       next
    }
    #print(paste("There is data in the table: ",station_code[k],sep=""))
    
    
    #rename the columns based on the monitor list
    res_colnames <- colnames(res)
    #need to look at the number at the end of the string for the columns names
    res_moncode <- as.numeric(str_extract(res_colnames, "[0-9]+"))
    
    #skip the first one since it is the date column.
    #This is to skip any errors where there is no monitor specified
    for (j in 2:length(res_moncode)){  
      if(identical(db_sta_mon$MOT_MonitorName[db_sta_mon$MON_MonitorChanel == res_moncode[j]],character(0))){
        res_colnames[j] <- res_moncode[j]
      }else{  
        res_colnames[j] <- db_sta_mon$MOT_MonitorName[db_sta_mon$MON_MonitorChanel == res_moncode[j]]
      }
    }
    
    
    #This will append "status" to the status column names. Then, invalidate anything that is not Status = 1
    for (j in seq(2, length(res_colnames), 2)){
      res_colnames[j+1] <- paste(res_colnames[j+1],"_status", sep = "")
      res[which(!res[,j+1] %in% c(1,14,77,78,79,80,81,82,83,85,86,89,93,94,95)),j] <- NA
    }
    
    #rename the columns of the dataframe
    names(res) <- res_colnames
    colnames(res)[colnames(res)=="Date_Time"] <- "date"
    
    #This decides whether to return data flags
    #based on user input 
    if(statusFlags == "F"){
      res_ONLY <- res[,seq(2, ncol(res), 2)]
    }else{
      res_ONLY <- res
    }
    res_ONLY$date <- res$date
    
    #The database contains hour ending data, which means the 1:00 average is for data recorded between
    #0:00 and 1:00. This needs to be converted to allow for the 0:00 average to be on the proper day. 
    
    #Subtract an hour off the dataset if hourly. If it is minute, take a minute off
    if(timebase == "60"){
      res_ONLY$date_time_ending <- res_ONLY$date
      res_ONLY$date <- res_ONLY$date - 3600
    }else{
      res_ONLY$date_time_ending <- res_ONLY$date
      res_ONLY$date <- res_ONLY$date - 60
    }
    
    if(k == 1){
      station_master <- res_ONLY
    }else {
      station_master <- station_master %>%
        full_join(res_ONLY, by = 'date')
    }
    
  }
  
  return(station_master)
  
}



#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------


MV_logbooks <- function(start_time, end_time) {
  station_list <- read.csv("C:/Users/khowe/Documents/Ambient_Data_R/helper_functions/stations.csv", stringsAsFactors = FALSE)
  
  table_name <- "tb_logbook"
  query1 <- paste('SELECT StationID, Date_Time, Technician, Equipment, TendType, Description FROM', table_name, sep = " ")
  query2 <- paste('WHERE Date_Time BETWEEN \'', start_time, '\' AND \'', end_time,"\'", sep = "")
  query <- paste(query1, query2, sep = " ")
  
  #print(paste("LOGBOOK exists: ",query,sep = ""))
  tb_logbook <- dbGetQuery(con,query)
  
  tb_logbook <- tb_logbook %>% 
                  left_join(station_list, by = c('StationID' = 'STA_SerialCode')) %>%
                  select(Date_Time, STA_StationName,STA_Name, StationID, Technician, Equipment, TendType, Description)
                  
                  
  
  
}

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
# this pulls data from the RAW tables


MV_data_call_pollutant_RAW <- function(pollutant, start_date, end_date, timebase) {
  
  #uncomment below for testing the function
  #station_code <- c("006","018")
  #start_time <- '01/01/2021 00:00:00.0'
  #end_time   <- '12/31/2021 23:00:00.0'
  #timebase <- "60"
  #pollutant <- "PM2.5"
  
  if (pollutant == "PM2.5"){
    pollutant <- c("PM2.5_SHARP", "PM2.5_iSHARP")
  }
  
  station_list <- read.csv("C:/Users/khowe/Documents/Ambient_Data_R/helper_functions/stations.csv")
  
  pollutant_counter = 1
  station_code <- c("001","002","004","006","009","012","013","014","015","017","018","020","022","023","024","026","027","029","030","031","032","033","035","041","055","057","058","080","174","175","176","177","178","179","180","274","085","286","138","129","065","148","164","158", "287", "206","204")
  
  
  for (k in 1:length(station_code)){  
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    #get the data from the DB
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    
    #Start connection to database
    con <- dbConnect(odbc::odbc(), 
                     driver = "SQL Server",
                     server = "PRDAQMS02", 
                     database = "EnvAdmin",
                     uid = "MATLABReader",
                     pwd = "matlabrEADER2018")
    #start by getting the most up to date list of monitors
    query <- 'SELECT MOT_MonitorTypeCode, MOT_MonitorName FROM dbo.TB_MONITOR_TYPE'
    db_sta_monType <- dbGetQuery(con, query)
    
    
    #build the query to the station an monitor lists from the database
    query <- paste('SELECT MON_MonitorType, MON_MonitorChanel FROM dbo.TB_MONITOR WHERE MON_StationSerialCode=',station_code[k], sep = " ")
    query <- paste(query, 'ORDER BY MON_MonitorType ASC', sep = " ")
    
    #create a dataframe showing the channel number for the monitor
    db_sta_mon <- dbGetQuery(con, query)
    db_sta_mon <- merge(db_sta_monType, db_sta_mon, by.x = "MOT_MonitorTypeCode", by.y = "MON_MonitorType")
    #build a new query to get data from the database
    table_name <- paste("RAW_S", station_code[k], sep = "")
    table_name <- paste(table_name,"T",sep="")
    table_name <- paste(table_name,timebase,sep="")
    
    query1 <- paste('SELECT * FROM', table_name, sep = " ")
    query2 <- paste('WHERE Date_Time BETWEEN \'', start_time, '\' AND \'', end_time,"\'", sep = "")
    query <- paste(query1, query2, sep = " ")
    
    
    #print(paste("Table exists: ",query,sep = ""))
    res <- dbGetQuery(con,query)
    #some dataframes will be empty. Test to see if that is true and if it is, skip
    dim_res <- dim(res)
    if(dim_res[1] == 0) next
    #print(paste("There is data in the table: ",station_code[k],sep=""))
    
    
    #rename the columns based on the monitor list
    res_colnames <- colnames(res)
    #need to look at the number at the end of the string for the columns names
    res_moncode <- as.numeric(str_extract(res_colnames, "[0-9]+"))
    
    #skip the first one since it is the date column.
    #This is to skip any errors where there is no monitor specified
    for (j in 2:length(res_moncode)){  
      if(identical(db_sta_mon$MOT_MonitorName[db_sta_mon$MON_MonitorChanel == res_moncode[j]],character(0))){
        res_colnames[j] <- res_moncode[j]
      }else{  
        res_colnames[j] <- db_sta_mon$MOT_MonitorName[db_sta_mon$MON_MonitorChanel == res_moncode[j]]
      }
    }
    
    
    #This will append "status" to the status column names. Then, invalidate anything that is not Status = 1
    for (j in seq(2, length(res_colnames), 2)){
      res_colnames[j+1] <- paste(res_colnames[j+1],"_status", sep = "")
      res[which(!res[,j+1] %in% c(1,14,77,78,79,80,81,82,83,85,86,89,93,94,95)),j] <- NA
    }
    
    #rename the columns of the dataframe
    names(res) <- res_colnames
    colnames(res)[colnames(res)=="Date_Time"] <- "date"
    
    #This grabs only the data, without any status information
    res_ONLY <- res[,seq(2, ncol(res), 2)]
    res_ONLY$date <- res$date
    
    #The database contains hour ending data, which means the 1:00 average is for data recorded between
    #0:00 and 1:00. This needs to be converted to allow for the 0:00 average to be on the proper day. 
    
    #Subtract an hour off the dataset
    res_ONLY$date_time_ending <- res_ONLY$date
    res_ONLY$date <- res_ONLY$date - 3600
    
    
    #Check if pollutant exists in data
    #pollutant
    
    #-------------------------------------------------------------------------
    # Prep pollutant data
    #-------------------------------------------------------------------------
    if(pollutant_counter == 1){
      
      pollutant_master <- res_ONLY %>%
        select(date,any_of(pollutant)) %>%
        rename_with(~paste(.x,station_list[which(as.numeric(station_code[k]) == station_list$STA_SerialCode),c("STA_ShortName")],sep="_"),any_of(pollutant))
      
      pollutant_counter <- pollutant_counter + 1
      
    }else{
      
      pollutant_master <- res_ONLY %>%
        select(date,any_of(pollutant)) %>%
        rename_with(~paste(.x,station_list[which(as.numeric(station_code[k]) == station_list$STA_SerialCode),c("STA_ShortName")],sep="_"),any_of(pollutant)) %>%
        full_join(pollutant_master, by = "date")
      
      
      pollutant_counter <- pollutant_counter + 1
      
    }
    
    
  }
  
  #clean things up by removing empty columns
  pollutant_master <- pollutant_master[colSums(is.na(pollutant_master)) != nrow(pollutant_master)]
  
  
  return(pollutant_master)
  
}
