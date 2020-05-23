#This script is to fetch data from cities' COVID-19 neighborhood level data feed
#and update it on a google sheet

#load libraries
library(dplyr)
library(data.table)
library(tidyr)
library(readr)
library(rvest)
library(stringr)
library(lubridate)
library(jsonlite)
library(tabulizer)
library(anytime)
library(googlesheets4)


options(scipen = 999)


#Download these pdfs into working diretory
#https://www.scdhec.gov/infectious-diseases/viruses/coronavirus-disease-2019-covid-19/sc-cases-county-zip-code-covid-19
#South Carolina
#South Carolina data
SC_time <- mdy(05212020) ##CHANGE DATE
SC_data <- extract_tables("~/Downloads/TableOption2 (2).pdf", output = "data.frame", method="stream")
SC_data <- bind_rows(lapply(SC_data, function(x){mutate_all(x, as.character)})) %>%
  filter(!is.na(Rep..Cases)) %>% rename(Positive=Rep..Cases, Geo_id=Zip) %>%
  mutate(Deaths=NA, Total=NA, Update_time=SC_time, Geo_id_type="ZCTA") %>% 
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="South Carolina") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)

#New York

NYC_time <- read_html("https://github.com/nychealth/coronavirus-data/commits/master/data-by-modzcta.csv") %>%
  html_nodes(xpath='//*[@id="js-repo-pjax-container"]/div[2]/div/div[2]/div[1]') %>%
  html_text() %>% { gsub("\n", "", .) } %>%
  { str_trim(.) } %>%
  { gsub("Commits on ", "", .) } %>% 
  { mdy(.) }

NYC_data <- fread("https://raw.githubusercontent.com/nychealth/coronavirus-data/master/data-by-modzcta.csv") %>%
  mutate(MODIFIED_ZCTA = ifelse(is.na(MODIFIED_ZCTA), "Unassigned", MODIFIED_ZCTA), 
         Update_time=NYC_time,
         Geo_id_type="Modified ZCTA", 
         Total=round(COVID_CASE_COUNT/(PERCENT_POSITIVE/100))) %>%
  rename(Geo_id=MODIFIED_ZCTA, 
         Positive=COVID_CASE_COUNT, Deaths=COVID_DEATH_COUNT) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="New York City") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)
# 
# NYC_data <- fread("https://raw.githubusercontent.com/nychealth/coronavirus-data/master/tests-by-zcta.csv") %>%
#   mutate(MODZCTA = ifelse(is.na(MODZCTA), "Unassigned", MODZCTA), Deaths=NA, Update_time=NYC_time,
#          Geo_id_type="ZCTA") %>%
#   select(-zcta_cum.perc_pos) %>%
#   rename(Geo_id = MODZCTA) %>% 
#   mutate(Geo_id=as.character(Geo_id),
#          Geo_id_type=as.character(Geo_id_type),
#          Positive=as.integer(Positive),
#          Total=as.integer(Total),
#          Deaths=as.integer(Deaths),
#          Source_geo="New York City") %>%
#   select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)


#Get data from Boston

Boston_time <- read_html("https://bphc.org/whatwedo/infectious-diseases/Infectious-Diseases-A-to-Z/covid-19/Pages/default.aspx") %>%
  html_nodes(xpath='//*[@id="WebPartWPQ3"]/div[1]/div[2]/p[3]/em') %>%
  html_text() %>%
  { gsub("WEEKLY NEIGHBORHOOD DATA (Updated ", "", ., fixed=TRUE) } %>%
  { gsub(")", "", ., fixed=TRUE) } %>%
  { mdy(.) }

Boston_data <- read_html("https://bphc.org/whatwedo/infectious-diseases/Infectious-Diseases-A-to-Z/covid-19/Pages/default.aspx") %>%
  html_nodes(xpath='//*[@id="WebPartWPQ3"]/div[1]/div[2]/table') %>%
  html_table() %>% `[[`(1) %>%
  rename(Geo_id = X1, Total = X2, Positivity = X3) %>%
  slice(-1) %>%
  mutate(Total = as.numeric(gsub(",", "", Total)),
         Positivity = as.numeric(gsub("%", "", Positivity))/100,
         Positive = round(Total * Positivity, digits = 0),
         Geo_id = gsub(".*-", "", Geo_id),
         Geo_id = gsub(" ", "", Geo_id),
         Geo_id = gsub(",", "_", Geo_id),
         Geo_id = ifelse(Geo_id == "Missing/Other", "Unassigned", Geo_id),
         Deaths=NA, Update_time=Boston_time, Geo_id_type="ZCTA aggregate") %>%
  filter(Geo_id != "Boston") %>% 
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Boston") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)



#Get data from Philadelphia

Philly_time <- fread("https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+covid_cases_by_zip&filename=covid_cases_by_zip&format=csv&skipfields=cartodb_id")
Philly_time <- Philly_time$etl_timestamp[[1]] %>%
{ ymd(gsub(" .*", "", .)) }


Philly_data_deaths <- fread("https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+covid_deaths_by_zip&filename=ccovid_deaths_by_zip&format=csv&skipfields=cartodb_id") %>%
  select(zip_code, count) %>% rename(Deaths=count) %>% 
  mutate(zip_code = as.character(zip_code), Deaths=ifelse(is.na(Deaths), 0, Deaths))

Philly_data <- fread("https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+covid_cases_by_zip&filename=covid_cases_by_zip&format=csv&skipfields=cartodb_id") %>%
  select(zip_code, covid_status, count) %>% 
  pivot_wider(names_from = covid_status, values_from = count) %>%
  mutate(Total = NEG + POS,
         POS = ifelse(is.na(POS), 0, POS),
         zip_code = as.character(zip_code), Geo_id_type = "ZCTA", Update_time = Philly_time) %>% 
  left_join(Philly_data_deaths, by="zip_code") %>%
  rename(Geo_id = zip_code, Positive=POS) %>%
  mutate(Total=ifelse(is.na(Total), 0, Total),
         Deaths=ifelse(is.na(Deaths), 0, Deaths)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Philadelphia") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)




#Get data from NOLA
NOLA_data <- fromJSON('https://gis.nola.gov/arcgis/rest/services/apps/LDH_Data/MapServer/2/query?where=0%3D0&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=false&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&f=pjson')$features$attributes
NOLA_time <- as.Date(NOLA_data$Date[1]/1000/60/60/24, "1970-01-01")
NOLA_data <- NOLA_data %>%
  select(TractID, CaseCount) %>% 
  rename(Geo_id = TractID, Positive=CaseCount) %>%
  mutate(Total=NA, Geo_id_type="CT", Update_time=NOLA_time, Deaths=NA,
         Positive=ifelse(is.na(Positive), 0, Positive)) %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths), 
         Source_geo="New Orleans") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)


#Get Chicago data
Chicago_data <- fread("https://data.cityofchicago.org/api/views/yhhz-zm2v/rows.csv") 
Chicago_time <- max(mdy(Chicago_data$`Week End`))

Chicago_data <- Chicago_data %>%
  filter(`Week Number` == max(`Week Number`)) %>% 
  rename(Total = `Tests - Cumulative`, Positive = `Cases - Cumulative`, 
         Deaths = `Death Rate - Cumulative`, Geo_id = `ZIP Code`) %>%
  mutate(Update_time=Chicago_time, Geo_id_type="ZCTA") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Chicago") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)



#Get Maryland data. # Less than 7 is protected, and these are converted to 0s
MD_time <- read_html("https://services.arcgis.com/njFNhDsUCentVYJW/ArcGIS/rest/services/MDH_COVID_19_Dashboard_Feature_Layer_ZIPCodes_MEMA/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

MD_data <- fromJSON('https://services.arcgis.com/njFNhDsUCentVYJW/ArcGIS/rest/services/MDH_COVID_19_Dashboard_Feature_Layer_ZIPCodes_MEMA/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes %>%
  rename(Geo_id = ZIPCODE1, Positive = ProtectedCount) %>%
  mutate(Positive = ifelse(is.na(Positive), 0, Positive), Total=NA) %>%
  mutate(Update_time=MD_time, Geo_id_type="ZCTA", Deaths=NA) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Maryland") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)
  

  

#Download SF data. Censored of less than 10 cases
SF_data <- fread("https://data.sfgov.org/api/views/favi-qct6/rows.csv")
SF_time <- ymd(max(SF_data$`Data as of`))

SF_data <- SF_data %>% 
  rename(Geo_id = `ZIP Code`, Positive = `Count of Confirmed Cases`) %>%
  mutate(Total=NA, Deaths=NA, Geo_id_type="ZCTA", Update_time=SF_time,
         Positive=ifelse(is.na(Positive),0, Positive)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="San Francisco") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)
  


#Download San Diego Data

SD_link <- read_html("https://www.sandiegocounty.gov/content/sdc/hhsa/programs/phs/community_epidemiology/dc/2019-nCoV/status.html") %>%
  html_nodes("a") %>% html_attr("href") %>% 
  { .[grepl("zip", ., ignore.case=TRUE)] } %>%
  { paste0("https://www.sandiegocounty.gov", .)}

SD_time <- anydate(extract_metadata(SD_link)$modified)

SD_data_1 <- extract_tables(SD_link, pages=1, output="data.frame", method="lattice",
                              area = list(c(121.7, 46.5, 662.9, 230.9))) %>% `[[`(1)

SD_data_2 <- extract_tables(SD_link, pages=1, output="data.frame", method="lattice",
                            area = list(c(121.7, 258.0, 662.9, 500))) %>% `[[`(1)

# SD_data1 <- SD_data_raw %>% select(X, Count) %>% rename(Geo_id=X, Positive = Count)
# SD_data2 <- SD_data_raw %>% select(Zip.Code.1, Count.1) %>% rename(Geo_id=Zip.Code.1, Positive=Count.1)

SD_data <- rbind(SD_data_1, SD_data_2) %>%
  rename(Geo_id = Zip.Code, Positive=Count) %>%
  mutate(Geo_id = ifelse(Geo_id=="Unknown***", "Unassigned", Geo_id),
         Total = NA, Positive= as.integer(gsub(",", "", Positive)), Geo_id_type="ZCTA", Deaths=NA) %>%
  filter(Geo_id != "San Diego County" & Geo_id != "") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Update_time=SD_time,
         Source_geo="San Diego County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)


#Get Florida data. It is suppressed if it is <5

FL_time <- read_html("https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Florida_Cases_Zips_COVID19/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


FL_data <- fromJSON('https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Florida_Cases_Zips_COVID19/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes %>%
  rename(Geo_id = ZIP, Positive = Cases_1) %>%
  mutate(Positive = ifelse(Positive == "<5" | Positive == "SUPPRESSED", 0, Positive), 
         Total=NA, Deaths=NA, Update_time=FL_time, Geo_id_type="ZCTA") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Florida") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)



#Get Wisconsin data. Data is suppressed if cases is 4 or less

WI_time <- read_html("https://services1.arcgis.com/ISZ89Z51ft1G16OK/ArcGIS/rest/services/COVID19_WI/FeatureServer/9") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

WI_data <- fromJSON('https://services1.arcgis.com/ISZ89Z51ft1G16OK/ArcGIS/rest/services/COVID19_WI/FeatureServer/9/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes %>%
  rename(Geo_id = GEOID, Positive = POSITIVE, Deaths=DEATHS) %>%
  mutate(Positive = ifelse(Positive == -999, 0, Positive), 
         NEGATIVE = ifelse(NEGATIVE == -999, 0, NEGATIVE),
         Deaths = ifelse(Deaths == -999, 0, Deaths),
         Geo_id_type="CT", Update_time = WI_time) %>%
  mutate(Total = Positive + NEGATIVE) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Wisconsin") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)



## Get Arizona Data. Data Suppressed for certain zipcodes for AN/AI majority, and 
#1-5 and #6-10 range.

AZ_time <- read_html("https://services1.arcgis.com/mpVYz37anSdrK4d8/ArcGIS/rest/services/CVD_ZIPS_FORWEBMAP/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


AZ_data <- fromJSON("https://services1.arcgis.com/mpVYz37anSdrK4d8/ArcGIS/rest/services/CVD_ZIPS_FORWEBMAP/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = POSTCODE, Positive = ConfirmedCaseCount) %>%
  mutate(Positive = ifelse(Positive ==  "Data Suppressed", NA, 
                           ifelse(Positive == "6-10", "6", 
                                  ifelse(Positive == "1-5", "1",
                                         Positive))), Total = NA, Deaths=NA,
         Geo_id_type="ZCTA", Update_time=AZ_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Arizona") %>%
  filter(!is.na(Positive)) %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)


##Get Alameda County data. Less than 10 is censored

Alameda_time <- read_html("https://services3.arcgis.com/1iDJcsklY3l3KIjE/arcgis/rest/services/Alameda_County_COVID-19_Zip_Code_Data/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Alameda_data <- fromJSON("https://services3.arcgis.com/1iDJcsklY3l3KIjE/arcgis/rest/services/Alameda_County_COVID-19_Zip_Code_Data/FeatureServer/0/query?where=0%3D0&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = Zip, Positive = Count) %>%
  mutate(Positive = ifelse(Positive ==  "<10", 0, Positive), Total = NA, Deaths=NA,
         Geo_id_type="ZCTA", Update_time=Alameda_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Alameda County") %>%
  filter(!is.na(Positive)) %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)



#Sacramento County

Sacramento_links <- fromJSON("https://services6.arcgis.com/yeTSZ1znt7H7iDG7/ArcGIS/rest/services?f=pjson")$services %>%
  filter(grepl("Updated", name, ignore.case = TRUE)) %>%
  slice(nrow(.))

Sacramento_time <- read_html(paste0(Sacramento_links$url, "/0")) %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Sacramento_data <- fromJSON(paste0(Sacramento_links$url, "/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson"))$features$attributes %>%
  rename(Geo_id = ZIP5, Positive = Cases) %>%
  mutate(Total = NA, Deaths=NA, Geo_id_type="ZCTA", Update_time=Sacramento_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Sacramento County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)



#Get St Louis, MO data

StLouis_date <- read_html("https://www.stlouis-mo.gov/covid-19/data/index.cfm") %>%
  html_nodes(xpath='//*[@id="CS_CCF_812675_812687"]/div/div[4]/div[1]/div[1]/span[1]') %>% 
  html_text() %>%
  { gsub(".*of ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


StLouis_data <- read_html("https://www.stlouis-mo.gov/covid-19/data/zip.cfm") %>%
  html_nodes("table") %>% html_table() %>% `[[`(1) %>%
  rename(Geo_id = `Zip Code`, Positive = `Case Count`) %>%
  mutate(Total = NA, Deaths=NA, Geo_id_type="ZCTA", Update_time=StLouis_date) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="St Louis") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)


#Las Vegas data. Counts under 5 suppressed.

LasVegas_link <- read_html("https://www.southernnevadahealthdistrict.org/covid-19-case-count-archive/") %>%
  html_nodes("a") %>% html_attr("href") %>% 
  { .[grepl("zip", ., ignore.case=TRUE)] } %>%
  { .[grepl("csv", ., ignore.case=TRUE)] } %>% { .[1] }

LasVegas_date <- LasVegas_link %>% 
{ gsub(".*updates/", "", .) } %>%
{ ymd(gsub("-zip.*", "", .)) }

LasVegas_data <- fread(LasVegas_link) %>%
  rename(Geo_id = zip_code, Positive = count) %>%
  mutate(Positive = ifelse(Positive == "<5 suppressed", 0, Positive), Total=NA, 
         Deaths=NA, Geo_id_type="ZCTA", Update_time=LasVegas_date) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Clark County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)


  
#Houston / harris county data

Harris_time <- read_html("https://services.arcgis.com/su8ic9KbA7PYVxPS/arcgis/rest/services/CITY_LIMITS_COVID/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

Harris_data <- fromJSON("https://services.arcgis.com/su8ic9KbA7PYVxPS/arcgis/rest/services/CITY_LIMITS_COVID/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZIP, Positive = TotalConfirmedCases, Deaths=Death) %>%
  mutate(Total = NA, Update_time=Harris_time, Geo_id_type="ZCTA") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Harris County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)



#North Carolina by ZIP. Suppressed when population < 500 and cases < 500

NC_time <- read_html("https://services.arcgis.com/iFBq2AW9XO0jYYF7/arcgis/rest/services/Covid19byZIPnew/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

NC_data <- fromJSON("https://services.arcgis.com/iFBq2AW9XO0jYYF7/arcgis/rest/services/Covid19byZIPnew/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZIPCode, Positive = Cases, Deaths=Deaths) %>%
  mutate(Positive = ifelse(is.na(Positive), 0, Positive)) %>%
  mutate(Total = NA, Geo_id_type="ZCTA", Update_time=NC_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="North Carolina") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo) 



#DeKalb County data

DeKalb_time <- read_html("https://www.dekalbhealth.net/covid-19dekalb/") %>%
  html_node(xpath = '//*[@id="cs-content"]/div[2]/div/div/div[1]/div/p[1]') %>% html_text() %>%
  { gsub(".*Updated: ", "", .) } %>%
  { mdy(.) }


DeKalb_data <- read_html("https://www.dekalbhealth.net/covid-19dekalb/") %>%
  html_nodes("table") %>% html_table() %>% `[[`(1) %>% slice(-1) %>%
  rename(Geo_id=X1, Positive = X3) %>%
  filter(Geo_id != "Grand Total") %>%
  mutate(Positive=ifelse(Positive=="<5", 0, Positive)) %>%
  mutate(Positive = as.integer(Positive), Total=NA, 
         Geo_id = ifelse(Geo_id=="Unknown", "Unassigned", Geo_id),
         Deaths=NA, Geo_id_type="ZCTA", Update_time=DeKalb_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="DeKalb County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)
  

#Fulton County. <10 is suppressed

Fulton_link <- read_html("https://www.fultoncountyga.gov/covid-19/epidemiology-reports") %>%
  html_nodes("a") %>% html_attr("href") %>% 
  { .[grepl("zip", ., ignore.case=TRUE)] } %>% `[[`(1) %>%
  { paste0("https://www.fultoncountyga.gov", .)}

Fulton_data1 <- extract_tables(Fulton_link, pages=5, output="data.frame", method="stream",
                               guess=FALSE, area = list(c(74.5, 33.4, 745, 571.2)), 
                                 columns = list(c(89.4,160,250,460))) %>% `[[`(1) 

Fulton_time <- mdy(Fulton_data1[2,3])

Fulton_data1 <- Fulton_data1 %>% slice(-1:-2) %>% 
  rename(Geo_id=X, Positive=Current.Count) %>% 
  mutate(Total=NA, Deaths=NA,
         Geo_id_type="ZCTA", Update_time=Fulton_time,
         Positive = as.integer(ifelse(Positive=="<10", 0, Positive))) %>% 
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Fulton County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)
  
Fulton_data2 <- extract_tables(Fulton_link, pages=6, method="stream",
                               guess=FALSE, area = list(c(47.6, 31.0, 150, 576.1)), 
                               columns = list(c(89.4,160,250,460))) %>% `[[`(1)  %>% 
  { as.data.frame(.) } %>% 
  rename(Geo_id = V1, Positive = V3) %>% 
  mutate(Positive=as.integer(as.character(Positive)),
         Geo_id = as.character(Geo_id)) %>%
  mutate(Total=NA, Deaths=NA,
         Geo_id = ifelse(Geo_id == "Unknown", "Unassigned", Geo_id),
         Geo_id_type="ZCTA", Update_time=Fulton_time,
         Positive = as.integer(ifelse(is.na(Positive), 0, Positive))) %>% 
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Fulton County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)


Fulton_data <- rbind(Fulton_data1, Fulton_data2)


#Get Illinois data 
#Illinois provides demographics by zip code as well
Illinois_time <- ymd(paste0(unlist(fromJSON("https://www.dph.illinois.gov/sitefiles/COVIDZip.json")$LastUpdateDate), 
                            collapse = "-"))


Illinois_data <- fromJSON("https://www.dph.illinois.gov/sitefiles/COVIDZip.json")$zip_values %>%
  select(1:3) %>% rename(Geo_id=zip, Positive=confirmed_cases, Total=total_tested) %>% 
  mutate(Deaths=NA,
         Geo_id_type="ZCTA", Update_time=Illinois_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Illinois") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)




## Get Penn data
#the -1 in the dataset indicates a Redacted value of 1-4 cases

Penn_time <- read_html("https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/Zip_Code_COVID19_Case_Data/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Penn_data <- fromJSON("https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/Zip_Code_COVID19_Case_Data/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZIP_CODE, Positive = Positive) %>%
  mutate(Positive = ifelse(Positive == -1, 0, Positive),
         Geo_id_type="ZCTA", Deaths=NA, Update_time=Penn_time) %>%
  mutate(Total = Positive + Negative) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Pennsylvania") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)




##Get Oregon data. Suppressed of 1-9

Oregon_link <- read_html("https://govstatus.egov.com/OR-OHA-COVID-19") %>%
  html_nodes("a") %>% html_attr("href") %>% 
  { .[grepl("weekly", ., ignore.case=TRUE)] } %>% `[[`(1) 

Oregon_time <- ymd(gsub("-FINAL.*", "", gsub(".*Report-", "", Oregon_link)))

Oregon_data <- extract_tables(Oregon_link, pages=c(8,9,10,11,12,13,14,15)) %>%
  { do.call(rbind, .[1:8])} %>% { as.data.frame(.)} %>%
  slice(-1) %>%
  rename(Geo_id=V1, Positive = V2) %>%
  mutate(Total=NA, Positive = ifelse(Positive=="1-9", 0, Positive),
         Deaths=NA, Geo_id_type="ZCTA", Update_time=Oregon_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Oregon") %>%
  filter(Geo_id != "Cases with unknown ZIP codes" &
           Geo_id != "ZIP codes with <1,000 population") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)
  


#Get Erie county data

Erie_time <- read_html("https://services1.arcgis.com/CgOSc11uky3egK6O/ArcGIS/rest/services/erie_zip_codes_confirmed_counts/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Erie_data <- fromJSON("https://services1.arcgis.com/CgOSc11uky3egK6O/ArcGIS/rest/services/erie_zip_codes_confirmed_counts/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZIP_CODE, Positive = CONFIRMED) %>%
  mutate(Total = NA, Deaths=NA, Geo_id_type="ZCTA", Update_time=Erie_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Erie County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)



#Omaha Douglas County 


Omaha_time <- read_html("https://services.arcgis.com/pDAi2YK0L0QxVJHj/arcgis/rest/services/COVID19_Cases_by_ZIP_(View)/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

Omaha_tests <- fromJSON("https://services.arcgis.com/pDAi2YK0L0QxVJHj/ArcGIS/rest/services/Tests_by_Zip/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZipCode, Total = TotalTests) %>%
  select(Geo_id, Total)
  
Omaha_data <- fromJSON("https://services.arcgis.com/pDAi2YK0L0QxVJHj/arcgis/rest/services/COVID19_Cases_by_ZIP_(View)/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZipCode, Positive = Cases) %>%
  left_join(Omaha_tests, by="Geo_id") %>%
  mutate(Deaths=NA, Update_time=Omaha_time, Geo_id_type="ZCTA") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Douglas County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)




#Get Austin data

Austin_links <- fromJSON("https://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services?f=pjson")$services %>%
  filter(grepl("web0522", name, ignore.case = TRUE) |
           grepl(paste0("web", str_pad(as.character(month(Sys.Date())), 2, pad="0"), as.character(day(Sys.Date()))), name, ignore.case = TRUE)) %>%
  slice(nrow(.))

Austin_time <- read_html(paste0(Austin_links$url, "/2")) %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

Austin_data <- fromJSON(paste0(Austin_links$url, "/2/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson"))$features$attributes %>%
  rename(Geo_id = ZIPCODE, Positive = Count_) %>%
  mutate(Total = NA, Geo_id_type="ZCTA", Update_time=Austin_time, Deaths=NA,
         Positive=ifelse(is.na(Positive), 0, Positive)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Travis County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)



#ElPaso data

ElPaso_time <- read_html("https://elpasocovid19tracker.com/") %>%
  html_nodes(xpath="/html/body/div[1]/div/div[1]/p") %>% html_text() %>% 
  { gsub(".*: ", "", .) } %>%
  { mdy(paste(gsub(" at.*", "", .), "2020")) }


ElPaso_data <- read_html("https://elpasocovid19tracker.com/") %>%
  html_nodes("table") %>% html_table() %>% `[[`(1) %>%
  rename(Geo_id = `Zip Code`, Positive = `Total Cases`) %>%
  mutate(Total = NA, Deaths=NA, Geo_id_type="ZCTA", Update_time=ElPaso_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="El Paso") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)




#Get Oklahoma data
OK_time <- ymd(fread("https://storage.googleapis.com/ok-covid-gcs-public-download/oklahoma_cases_zip.csv")$ReportDate[1])


OK_data <- fread("https://storage.googleapis.com/ok-covid-gcs-public-download/oklahoma_cases_zip.csv") %>%
  rename(Geo_id=Zip, Positive=Cases) %>% 
  mutate(Total=NA, Geo_id_type="ZCTA", Update_time=OK_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Oklahoma") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)



##Louisiana data

Louisiana_links <- fromJSON("https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services?f=pjson")$services %>%
  filter(grepl("Tracts_05172020", name, ignore.case = TRUE) |
           grepl(paste0("Tracts_", str_pad(as.character(month(Sys.Date())), 2, pad="0"), as.character(day(Sys.Date())), "2020"), name, ignore.case = TRUE)) %>%
  slice(nrow(.))

Louisiana_time <- read_html(paste0(Louisiana_links$url, "/0")) %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Louisiana_data <- fromJSON(paste0(Louisiana_links$url, "/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson"))$features$attributes %>%
  rename(Geo_id = TractID, Positive = CaseCount) %>%
  mutate(Positive = ifelse(Positive==" ", 0, 
                           ifelse(Positive=="1 to 5", 1, Positive))) %>%
  mutate(Total=NA, Deaths=NA, Geo_id_type="CT", Update_time=Louisiana_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Louisiana") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)


##Dallas

Dallas_links <- fromJSON("https://services3.arcgis.com/kJv6AkjtZisjy0Ed/ArcGIS/rest/services?f=pjson")$services %>%
  filter(grepl("ZipMap_Prd_522", name, ignore.case = TRUE) |
           grepl(paste0("ZipMap_Prd_", as.character(month(Sys.Date())), as.character(day(Sys.Date()))), name, ignore.case = TRUE)) %>%
  slice(nrow(.))

Dallas_data <- fromJSON(paste0(Dallas_links$url, "/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson"))$features$attributes

Dallas_time <- ymd(unique(Dallas_data$load_date))

Dallas_data <- Dallas_data %>%
  rename(Geo_id = GEOID10, Positive = Present) %>%
  mutate(Total=NA, Deaths=NA, Geo_id_type="ZCTA", Update_time=Dallas_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Dallas County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)



##Tarrant county data

Tarrant_time <- read_html("https://services8.arcgis.com/0emesQkjyT7tJv3q/ArcGIS/rest/services/ZIP_code_Tarrant_cases/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

Tarrant_data <- fromJSON("https://services8.arcgis.com/0emesQkjyT7tJv3q/ArcGIS/rest/services/ZIP_code_Tarrant_cases/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZIP, Positive = Cases) %>%
  mutate(Total=NA, Deaths=NA, Geo_id_type="ZCTA", Update_time=Tarrant_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Source_geo="Tarrant County") %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)


Manual_data <- read_sheet("https://docs.google.com/spreadsheets/d/1bk33v5C09NSfOisxFIa5mjB9tyocAbpJybztc5tSLtU/edit?usp=sharing")

Manual_data <- Manual_data %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positive=as.integer(Positive),
         Total=as.integer(Total),
         Deaths=as.integer(Deaths),
         Update_time=ymd(Update_time)) %>%
  select(Geo_id, Geo_id_type, Positive, Total, Deaths, Update_time, Source_geo)

range_write("https://docs.google.com/spreadsheets/d/1bk33v5C09NSfOisxFIa5mjB9tyocAbpJybztc5tSLtU/edit?usp=sharing",
            data.frame(Read_time=rep(Sys.time(), nrow(Manual_data))), "Sheet1",
            "H1")
  
all_data <- rbind(Alameda_data, Austin_data, AZ_data, Boston_data, Chicago_data, Dallas_data, DeKalb_data, ElPaso_data,
                  Erie_data, FL_data, Fulton_data, Harris_data, Illinois_data, LasVegas_data, Louisiana_data, MD_data, 
                  NC_data, NOLA_data, NYC_data, OK_data, Omaha_data, Oregon_data, Penn_data, Philly_data, Sacramento_data, 
                  SC_data, SD_data, SF_data, StLouis_data, Tarrant_data, WI_data, Manual_data)

write_csv(all_data, "Data_log/all_data_May23_2020.csv")
write_csv(all_data, "all_data_latest.csv")
