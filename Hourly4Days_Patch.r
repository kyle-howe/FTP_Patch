#This tool will grab the FTP profiles specified and provide a list 
#of stations and monitors in each of the profiles.
#From there, it will create an FTP file following the Envista 
#MetroVan profile type

library(DBI)
library(tidyverse)
library(stringr)
library(RCurl)
library(lubridate)

# Start the clock!
ptm <- proc.time()


#FYI, Profile 1 is Hourly4Days
#Profile 6 is 3 hours minute


#read in the basics about network/monitors
station_list <- read.csv("stations.csv", stringsAsFactors = FALSE)
monitor_list <- read.csv("monitors.csv", stringsAsFactors = FALSE)


#Start connection to database
con <- dbConnect(odbc::odbc(), 
                 driver = "SQL Server",
                 server = "PRDAQMS02", 
                 database = "EnvAdmin",
                 uid = "MATLABReader",
                 pwd = "matlabrEADER2018")

#Pull the FTP profiles
table_name <- "FtpExportMonitor"
query <- paste('SELECT * FROM dbo.',table_name, sep = "")
tb_FtpExportMonitor <- dbGetQuery(con,query)

#start by getting the most up to date list of monitors
query <- 'SELECT MOT_MonitorTypeCode, MOT_MonitorName FROM dbo.TB_MONITOR_TYPE'
db_sta_monType <- dbGetQuery(con, query)

#build the query to the station an monitor lists from the database
query <- 'SELECT MON_MonitorType, MON_MonitorChanel,MON_StationSerialCode  FROM dbo.TB_MONITOR ORDER BY MON_MonitorType ASC'

#create a dataframe showing the channel number for the monitor
db_sta_mon <- dbGetQuery(con, query)
db_sta_mon <- merge(db_sta_monType, db_sta_mon, by.x = "MOT_MonitorTypeCode", by.y = "MON_MonitorType")  


###################################################################
#Now merge together the station IDs with monitor names to get a position list for each station in the FTP file

FTP_export_profile <- tb_FtpExportMonitor %>%
  filter(ProfileId == 1) %>% #This is the profile for the MoE
  left_join(station_list, by = c("StationId" = "STA_SerialCode")) %>%
  left_join(db_sta_mon, by = c("MonitorId" = "MON_MonitorChanel", "StationId" = "MON_StationSerialCode")) %>%
  select(StationId,STA_StationName,STA_Name,MOT_MonitorName, Position)



#################

#---------------------------------------------------------------------
#
# Build the FTP file
#


source("Data_Query_fun.R")

#start_time <- '01/17/2022 09:00:00.0'
#end_time   <- '01/19/2022 09:00:00.0'

#Determine the time period to query
#modify query depending on if DST or not
if(dst(Sys.time())){
  end_time <- as.POSIXct(Sys.time()) - 1*3600
  start_time <- end_time - 4*24*3600
  start_time <- as.character(format(start_time,"%m/%d%/%Y %H:%M:%S"))
  end_time <- as.character(format(end_time,"%m/%d%/%Y %H:%M:%S"))
}else{
  end_time <- as.POSIXct(Sys.time())
  start_time <- end_time - 4*24*3600
  start_time <- as.character(format(start_time,"%m/%d%/%Y %H:%M:%S"))
  end_time <- as.character(format(end_time,"%m/%d%/%Y %H:%M:%S"))
}


FTP_station_list <- unique(as.character(FTP_export_profile$STA_StationName))
FTP_station_names <- unique(as.character(FTP_export_profile$STA_Name))
FTP_parameter_list <- unique(FTP_export_profile$MOT_MonitorName)


for (i in 1:length(FTP_station_list)){
  #lookup serial number of station
  station_serial <- station_list %>%
                      filter(STA_ShortName == FTP_station_list[i]) %>%
                      select(STA_SerialCode) %>%
                      mutate(STA_SerialCode = as.character(STA_SerialCode)) %>%
                      mutate(STA_SerialCode = case_when(nchar(STA_SerialCode) == 1 ~ paste("00",STA_SerialCode,sep=""),
                                                        nchar(STA_SerialCode) == 2 ~ paste("0", STA_SerialCode, sep = ""),
                                                        TRUE ~ STA_SerialCode))
  
  
  
  Raw_AQ_data <- MV_station_call(station_serial[[1]],start_time,end_time,"60","F")
  #This is a check from the function to see if data exists. If 1 is returned, station
  #doesn't have data, so skip it
  #if(is.na(Raw_AQ_data[1])) next
  
  if(!(exists("Raw_AQ_data")&&is.data.frame(get("Raw_AQ_data")))) next
  
  Raw_AQ_data <- Raw_AQ_data %>%
                  select(-date_time_ending) %>%
                  pivot_longer(cols = !date, names_to = "pollutant", values_to = "value") %>%
                  filter(pollutant %in% FTP_parameter_list) %>%
                  filter(!is.na(value)) %>%
                  mutate(value = round2(value,2)) %>%
                  mutate(stationID = FTP_station_list[i]) %>%
                  mutate(avg_period = "60") %>%
                  mutate(region = "GVRD") %>%
                  mutate(station_name = FTP_station_names[i]) %>%
                  mutate(units = "") %>%
                  select(region, stationID, station_name, pollutant, avg_period, date, value, units)
  
  if(i == 1){
    FINAL_ftp_AQ <- Raw_AQ_data
  }else{
    FINAL_ftp_AQ <- rbind(FINAL_ftp_AQ, Raw_AQ_data)
  }
  
  
}

write.table(FINAL_ftp_AQ, file = "Hourly4Days_patch.csv", sep = ",", col.names = FALSE, row.names = FALSE)


ftpUpload("Hourly4Days_patch.csv", "sftp://DMZ\\KHowe:greaSebegone@sftp.metrovancouver.org/MetroVancouver/AQMS/KJH_Test/Hourly4Days_DEV.csv")

# Stop the clock
proc.time() - ptm