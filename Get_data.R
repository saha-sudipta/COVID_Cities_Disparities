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
options(scipen = 999)

#Get data from NYC
NYC_data <- fread("https://raw.githubusercontent.com/nychealth/coronavirus-data/master/tests-by-zcta.csv") %>%
  mutate(MODZCTA = ifelse(is.na(MODZCTA), "Unassigned", MODZCTA)) %>%
  select(-zcta_cum.perc_pos) %>%
  rename(Geo_id = MODZCTA)


NYC_time <- read_html("https://github.com/nychealth/coronavirus-data/commits/257399da1e7e974e9366bf4c5d1d3bfbc6dc9093/tests-by-zcta.csv") %>%
  html_nodes(xpath='//*[@id="js-repo-pjax-container"]/div[2]/div/div[2]/div[1]') %>%
  html_text() %>% { gsub("\n", "", .) } %>%
  { str_trim(.) } %>%
  { gsub("Commits on ", "", .) } %>% 
  { mdy(.) }

NYC_time <- gsub("\n", "", NYC_time)

NYC_time <- (str_extract_all(NYC_time, "[[:alnum:]]+[ /]*\\d{2}[ /]*\\d{4}")[[1]])

#Get data from Boston
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
         Geo_id = ifelse(Geo_id == "Missing/Other", "Unassigned", Geo_id)) %>%
  filter(Geo_id != "Boston") %>% select(Geo_id, Positive, Total)

Boston_time <- read_html("https://bphc.org/whatwedo/infectious-diseases/Infectious-Diseases-A-to-Z/covid-19/Pages/default.aspx") %>%
  html_nodes(xpath='//*[@id="WebPartWPQ3"]/div[1]/div[2]/p[2]/em') %>%
  html_text() %>%
  { gsub("WEEKLY NEIGHBORHOOD DATA (Updated ", "", ., fixed=TRUE) } %>%
  { gsub(")", "", ., fixed=TRUE) } %>%
  { mdy(.) }



#Get data from Philadelphia
Philly_data <- fread("https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+covid_cases_by_zip&filename=covid_cases_by_zip&format=csv&skipfields=cartodb_id") %>%
  select(zip_code, covid_status, count) %>% 
  pivot_wider(names_from = covid_status, values_from = count) %>%
  mutate(Total = NEG + POS,
         POS = ifelse(is.na(POS), 0, POS),
         zip_code = as.character(zip_code)) %>% 
  rename(Geo_id = zip_code, Positive=POS) %>%
  select(Geo_id, Positive, Total)

Philly_time <- fread("https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+covid_cases_by_zip&filename=covid_cases_by_zip&format=csv&skipfields=cartodb_id")
Philly_time <- Philly_time$etl_timestamp[[1]] %>%
{ ymd(gsub(" .*", "", .)) }


#Get data from NOLA
NOLA_data <- fromJSON('https://gis.nola.gov/arcgis/rest/services/apps/LDH_Data/MapServer/2/query?where=0%3D0&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=false&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&f=pjson')$features$attributes
NOLA_time <- as.Date(NOLA_data$Date[1]/1000/60/60/24, "1970-01-01")
NOLA_data <- NOLA_data %>%
  select(TractID, CaseCount) %>% 
  mutate(Total = NA) %>%
  rename(Geo_id = TractID, Positive=CaseCount)


#Get Chicago data
Chicago_data <- fread("https://data.cityofchicago.org/api/views/yhhz-zm2v/rows.csv") 
Chicago_time <- max(mdy(Chicago_data$`Week End`))

Chicago_data <- Chicago_data %>%
  filter(`Week Number` == max(`Week Number`)) %>% 
  rename(Total = `Tests - Cumulative`, Positive = `Cases - Cumulative`, Geo_id = `ZIP Code`) %>%
  select(Geo_id, Positive, Total)



#Get Maryland data. # Less than 7 is protected, and these are converted to 0s

MD_data <- fromJSON('https://services.arcgis.com/njFNhDsUCentVYJW/arcgis/rest/services/ZIPCodes_MD_1/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes %>%
  rename(Geo_id = ZIPCODE1, Positive = ProtectedCount) %>%
  mutate(Positive = ifelse(is.na(Positive), 0, Positive), Total=NA) %>%
  select(Geo_id, Positive, Total)
  
MD_time <- read_html("https://services.arcgis.com/njFNhDsUCentVYJW/arcgis/rest/services/ZIPCodes_MD_1/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }
  

#Download SF data
SF_data <- fread("https://data.sfgov.org/api/views/favi-qct6/rows.csv")
SF_time <- ymd(max(SF_data$`Data as of`))

SF_data <- SF_data %>% 
  rename(Geo_id = `ZIP Code`, Positive = `Count of Confirmed Cases`) %>%
  mutate(Total=NA) %>%
  select(Geo_id, Positive, Total)
  


#Download San Diego Data

SD_link <- read_html("https://www.sandiegocounty.gov/content/sdc/hhsa/programs/phs/community_epidemiology/dc/2019-nCoV/status.html") %>%
  html_nodes("a") %>% html_attr("href") %>% 
  { .[grepl("zip", ., ignore.case=TRUE)] } %>%
  { paste0("https://www.sandiegocounty.gov", .)}

SD_time <- anydate(extract_metadata(SD_link)$modified)

SD_data_raw <- extract_tables(SD_link, pages=1, output="data.frame") %>% `[[`(1)

SD_data1 <- SD_data_raw %>% select(X, Count) %>% rename(Geo_id=X, Positive = Count)
SD_data2 <- SD_data_raw %>% select(Zip.Code.1, Count.1) %>% rename(Geo_id=Zip.Code.1, Positive=Count.1)

SD_data <- rbind(SD_data1, SD_data2) %>%
  mutate(Geo_id = ifelse(Geo_id=="Unknown***", "Unassigned", Geo_id),
         Total = NA, Positive= as.integer(gsub(",", "", Positive))) %>%
  filter(Geo_id != "San Diego County Total")


#Get Florida data

FL_data <- fromJSON('https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Florida_Cases_Zips_COVID19/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes %>%
  rename(Geo_id = ZIP, Positive = Cases_1) %>%
  mutate(Positive = ifelse(Positive == "<5" | Positive == "SUPPRESSED", 0, Positive), Total=NA) %>%
  select(Geo_id, Positive, Total)

FL_time <- read_html("https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Florida_Cases_Zips_COVID19/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


#Get Wisconsin data



WI_data <- fromJSON('https://services1.arcgis.com/ISZ89Z51ft1G16OK/ArcGIS/rest/services/COVID19_WI/FeatureServer/9/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes %>%
  rename(Geo_id = GEOID, Positive = POSITIVE) %>%
  mutate(Positive = ifelse(Positive == -999, 0, Positive), ifelse(NEGATIVE == -999, 0, NEGATIVE)) %>%
  mutate(Total = Positive + NEGATIVE) %>%
  select(Geo_id, Positive, Total)

WI_time <- read_html("https://services1.arcgis.com/ISZ89Z51ft1G16OK/ArcGIS/rest/services/COVID19_WI/FeatureServer/9") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

## Get Arizona Data

AZ_data <- fromJSON("https://services1.arcgis.com/mpVYz37anSdrK4d8/ArcGIS/rest/services/CVD_ZIPS_FORWEBMAP/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = POSTCODE, Positive = ConfirmedCaseCount) %>%
  mutate(Positive = ifelse(Positive ==  "Data Suppressed", NA, 
                           ifelse(Positive == "6-10", "6", 
                                  ifelse(Positive == "1-5", "1",
                                         Positive))), Total = NA) %>%
  mutate(Positive = as.numeric(Positive)) %>%
  filter(!is.na(Positive)) %>%
  select(Geo_id, Positive, Total)

AZ_time <- read_html("https://services1.arcgis.com/mpVYz37anSdrK4d8/ArcGIS/rest/services/CVD_ZIPS_FORWEBMAP/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

##Get Alameda County data

Alameda_data <- fromJSON("https://services3.arcgis.com/1iDJcsklY3l3KIjE/arcgis/rest/services/Alameda_County_COVID-19_Zip_Code_Data/FeatureServer/0/query?where=0%3D0&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = Zip, Positive = Count) %>%
  mutate(Positive = ifelse(Positive ==  "<10", 5, Positive), Total = NA) %>%
  mutate(Positive = as.numeric(Positive)) %>%
  filter(!is.na(Positive)) %>%
  select(Geo_id, Positive, Total)

Alameda_time <- read_html("https://services3.arcgis.com/1iDJcsklY3l3KIjE/arcgis/rest/services/Alameda_County_COVID-19_Zip_Code_Data/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


#Sacramento County
Sacramento_data <- fromJSON("https://services6.arcgis.com/yeTSZ1znt7H7iDG7/arcgis/rest/services/Updated_05_11_20/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZIP5, Positive = Cases) %>%
  mutate(Total = NA) %>%
  select(Geo_id, Positive, Total)

Sacramento_time <- read_html("https://services6.arcgis.com/yeTSZ1znt7H7iDG7/arcgis/rest/services/Updated_05_11_20/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


#Get St Louis, MO data

StLouis_data <- read_html("https://www.stlouis-mo.gov/covid-19/data/zip.cfm") %>%
  html_nodes("table") %>% html_table() %>% `[[`(1) %>%
  rename(Geo_id = `Zip Code`, Positive = `Case Count`) %>%
  mutate(Total = NA) %>%
  select(Geo_id, Positive, Total)

StLouis_date <- read_html("https://www.stlouis-mo.gov/covid-19/data/index.cfm") %>%
  html_nodes(xpath='//*[@id="CS_CCF_812675_812687"]/div/div[4]/div[1]/div[1]/span[1]') %>% 
  html_text() %>%
  { gsub(".*of ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

#Las Vegas data

LasVegas_link <- read_html("http://www.southernnevadahealthdistrict.org/coronavirus") %>%
  html_nodes("a") %>% html_attr("href") %>% 
  { .[grepl("zip", ., ignore.case=TRUE)] } %>%
  { .[grepl("csv", ., ignore.case=TRUE)] }


LasVegas_data <- fread(LasVegas_link) %>%
  rename(Geo_id = zip_code, Positive = count) %>%
  mutate(Positive = ifelse(Positive == "<5 suppressed", 0, Positive), Total=NA) 

LasVegas_date <- LasVegas_link %>% 
{ gsub(".*updates/", "", .) } %>%
{ ymd(gsub("-zip.*", "", .)) }
  

#Houston / harris county data
Harris_data <- fromJSON("https://services.arcgis.com/su8ic9KbA7PYVxPS/arcgis/rest/services/CITY_LIMITS_COVID/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZIP, Positive = TotalConfirmedCases) %>%
  mutate(Total = NA) %>%
  select(Geo_id, Positive, Total)

Harris_time <- read_html("https://services.arcgis.com/su8ic9KbA7PYVxPS/arcgis/rest/services/CITY_LIMITS_COVID/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

#North Carolina by ZIP
NC_data <- fromJSON("https://services.arcgis.com/iFBq2AW9XO0jYYF7/arcgis/rest/services/Covid19byZIPnew/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZIPCode, Positive = Cases) %>%
  mutate(Total = NA) %>%
  select(Geo_id, Positive, Total)

NC_time <- read_html("https://services.arcgis.com/iFBq2AW9XO0jYYF7/arcgis/rest/services/Covid19byZIPnew/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

#DeKalb County data

DeKalb_data <- read_html("https://www.dekalbhealth.net/covid-19dekalb/") %>%
  html_nodes("table") %>% html_table() %>% `[[`(1) %>% slice(-1) %>%
  rename(Geo_id=X1, Positive = X2) %>%
  filter(Geo_id != "Grand Total") %>%
  mutate(Positive = as.integer(Positive), Total=NA)
  
DeKalb_time <- read_html("https://www.dekalbhealth.net/covid-19dekalb/") %>%
  html_nodes("strong") %>% html_text() %>%
  { .[grepl("Data Pulled", .)]} %>%
  { gsub(".*Pulled: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

#Fulton County

Fulton_link <- read_html("https://www.fultoncountyga.gov/covid-19/epidemiology-reports") %>%
  html_nodes("a") %>% html_attr("href") %>% 
  { .[grepl("zip", ., ignore.case=TRUE)] } %>% `[[`(1) %>%
  { paste0("https://www.fultoncountyga.gov", .)}

Fulton_data1 <- extract_tables(Fulton_link, pages=5, output="data.frame") %>% `[[`(1) 

Fulton_time <- Fulton_data1[2,2]

Fulton_data1 <- Fulton_data1 %>% slice(-1:-2) %>% 
  rename(Geo_id=X, Positive=Current.Count) %>% 
  mutate(Total=NA, Positive = as.integer(ifelse(Positive=="<10", 0, Positive))) %>% 
  select(Geo_id, Positive, Total)
  
Fulton_data2 <- extract_tables(Fulton_link, pages=6, method="stream") %>% 
  { as.data.frame(.) } %>% 
  rename(Geo_id = X1, Positive = X3) %>% 
  mutate(Total=NA, Positive = as.integer(ifelse(Positive=="<10", 0, Positive)), 
         Geo_id = ifelse(Geo_id == "Unknown", "Unassigned", Geo_id)) %>% 
  select(Geo_id, Positive, Total)


Fulton_data <- rbind(Fulton_data1, Fulton_data2)


#Get Illinois data 
#Illinois provides demographics by zip code as well

Illinois_data <- fromJSON("https://www.dph.illinois.gov/sitefiles/COVIDZip.json")$zip_values %>%
  select(1:3) %>% rename(Geo_id=zip, Positive=confirmed_cases, Total=total_tested) %>% 
  select(Geo_id, Positive, Total)

Illinois_time <- ymd(paste0(unlist(fromJSON("https://www.dph.illinois.gov/sitefiles/COVIDZip.json")$LastUpdateDate), 
                        collapse = "-"))


## Get Penn data
#the -1 in the dataset indicates a Redacted value of 1-4 cases
Penn_data <- fromJSON("https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/Zip_Code_COVID19_Case_Data/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZIP_CODE, Positive = Positive) %>%
  mutate(Positive = ifelse(Positive == -1, 0, Positive)) %>%
  mutate(Total = Positive + Negative) %>%
  select(Geo_id, Positive, Total)

Penn_time <- read_html("https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/Zip_Code_COVID19_Case_Data/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


##Get Oregon data

Oregon_link <- read_html("https://govstatus.egov.com/OR-OHA-COVID-19") %>%
  html_nodes("a") %>% html_attr("href") %>% 
  { .[grepl("weekly", ., ignore.case=TRUE)] } %>% `[[`(1) 

Oregon_data <- extract_tables(Oregon_link, pages=c(9,10,11,12,13,14,15,16)) %>%
  { do.call(rbind, .[2:8])} %>% { as.data.frame(.)} %>%
  slice(-1) %>%
  rename(Geo_id=V1, Positive = V2) %>%
  mutate(Total=NA, Positive = ifelse(Positive=="1-9", 0, Positive)) %>%
  select(Geo_id, Positive, Total)
   


#Get Erie county data

Erie_data <- fromJSON("https://services1.arcgis.com/CgOSc11uky3egK6O/ArcGIS/rest/services/erie_zip_codes_confirmed_counts/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZIP_CODE, Positive = CONFIRMED) %>%
  mutate(Total = NA) %>%
  select(Geo_id, Positive, Total)

Erie_time <- read_html("https://services1.arcgis.com/CgOSc11uky3egK6O/ArcGIS/rest/services/erie_zip_codes_confirmed_counts/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


#Omaha Douglas County

Omaha_data <- fromJSON("https://services.arcgis.com/pDAi2YK0L0QxVJHj/arcgis/rest/services/COVID19_Cases_by_ZIP_(View)/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes %>%
  rename(Geo_id = ZipCode, Positive = Cases) %>%
  mutate(Total = NA) %>%
  select(Geo_id, Positive, Total)

Omaha_time <- read_html("https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/Zip_Code_COVID19_Case_Data/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


#Get Austin data

Austin_links <- fromJSON("https://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services?f=pjson")$services %>%
  filter(grepl("web0511", name, ignore.case = TRUE) |
           grepl(paste0("web", str_pad(as.character(month(Sys.Date())), 2, pad="0"), as.character(day(Sys.Date()))), name, ignore.case = TRUE)) %>%
  slice(nrow(.))

Austin_data <- fromJSON(paste0(Austin_links$url, "/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson"))$features$attributes %>%
  rename(Geo_id = ZIPCODE, Positive = Count_) %>%
  mutate(Total = NA) %>%
  select(Geo_id, Positive, Total)

Austin_time <- read_html(paste0(Austin_links$url, "/0")) %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

#ElPaso data

ElPaso_data <- read_html("https://elpasocovid19tracker.com/") %>%
  html_nodes("table") %>% html_table() %>% `[[`(1) %>%
  rename(Geo_id = `Zip Code`, Positive = `Total Cases`) %>%
  mutate(Total = NA) %>%
  select(Geo_id, Positive, Total)

ElPaso_time <- read_html("https://elpasocovid19tracker.com/") %>%
  html_nodes(xpath="/html/body/div[1]/div/div[1]/p") %>% html_text() %>% 
  { gsub(".*: ", "", .) } %>%
  { mdy(paste(gsub(" at.*", "", .), "2020")) }


#Get Oklahoma data

OK_data <- fread("https://storage.googleapis.com/ok-covid-gcs-public-download/oklahoma_cases_zip.csv") %>%
  rename(Geo_id=Zip, Positive=Cases) %>% mutate(Total=NA) %>%
  select(Geo_id, Positive, Total)

OK_time <- ymd(fread("htt90ops://storage.googleapis.com/ok-covid-gcs-public-download/oklahoma_cases_zip.csv")$ReportDate[1])


##COMBINE DATA

all_data <- rbind(Alameda_data, Austin_data, AZ_data, Boston_data, Chicago_data, DeKalb_data, ElPaso_data,
                  Erie_data, FL_data, Fulton_data, Harris_data, Illinois_data, LasVegas_data, MD_data, NC_data, 
                  NOLA_data, NYC_data, OK_data, Omaha_data, Oregon_data, Penn_data, Philly_data, Sacramento_data, SD_data,
                  SF_data, StLouis_data, WI_data)

write_csv(all_data, "Data_log/all_data_May13_2020.csv")
write_csv(all_data, "all_data_latest.csv")
