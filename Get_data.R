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
library(readxl)
library(httr)
library(XML)


options(scipen = 999)

#See list of urban counties here:
#https://docs.google.com/spreadsheets/d/13IMThPywqCRIL9i_TzKpkmv6ASPsdcBXXskM4TEX5LU/edit#gid=336444733

# Data Definitions
# Cases are Persons positive
# Positives are number of positive tests
# Tests are total number of tests, or total number of people tested, whichever is provided 
# Deaths are people dead
# Source_geo is what level of geography the data was taken from. If from state level, no need to adjust pop
# for county overlap. If county, need to adjst


############################ Jefferson County, Alabama ############################

Jefferson_AL_time <- read_html("https://services3.arcgis.com/gVaFnXMJPMQCiYGT/ArcGIS/rest/services/Covid19_Cases__Regions_Rate/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

Jefferson_AL_data_raw <- fromJSON('https://services3.arcgis.com/gVaFnXMJPMQCiYGT/ArcGIS/rest/services/Covid19_Cases__Regions_Rate/FeatureServer/0/query?where=0%3D0&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=false&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&f=pjson')$features$attributes

Jefferson_AL_data <- Jefferson_AL_data_raw %>%
  mutate(ZipCodesInc = gsub(" ", "_", ZipCodesInc)) %>%
  select(ZipCodesInc, TotalCases, TotalPopulation) %>% 
  rename(Geo_id = ZipCodesInc, Cases = TotalCases) %>%
  mutate(Tests = NA, Positives = NA, Geo_id_type = "Merged ZCTA", Update_time = Jefferson_AL_time, Deaths = NA,
         Cases = ifelse(is.na(Cases), 0, Cases)) %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time) %>%
  mutate(Geo_id = as.character(Geo_id),
         Geo_id_type = as.character(Geo_id_type),
         Cases = as.integer(Cases),
         Tests = as.integer(Tests),
         Deaths = as.integer(Deaths), 
         Positives = as.integer(Positives),
         Source_geo = "Jefferson County, AL") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Jefferson_AL_data_raw, "raw_data_21_02_2021/Jefferson_AL_06_03_2021_raw.rds")

############################ Maricopa County, AZ ############################

## Get Arizona Data. Data Suppressed for certain zipcodes for AN/AI majority, and 
#1-10 range. Assign 5.

AZ_time <- read_html("https://services1.arcgis.com/mpVYz37anSdrK4d8/ArcGIS/rest/services/CVD_ZIPS_FORWEBMAP/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


AZ_data_raw <- fromJSON("https://services1.arcgis.com/mpVYz37anSdrK4d8/ArcGIS/rest/services/CVD_ZIPS_FORWEBMAP/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes

AZ_data <- AZ_data_raw %>%
  rename(Geo_id = POSTCODE, Cases = ConfirmedCaseCount) %>%
  mutate(Cases = ifelse(Cases ==  "Data Suppressed", NA, 
                           ifelse(Cases == "1-10", "5", Cases)), 
         Tests = NA, Deaths=NA, Positives = NA,
         Geo_id_type="ZCTA", Update_time = AZ_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Positives=as.integer(Positives),
         Source_geo="AZ") %>%
  filter(!is.na(Cases)) %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(AZ_data_raw, "raw_data_21_02_2021/AZ_06_03_2021_raw.rds")

############################ Alameda County, CA ############################


##Get Alameda County data. Less than 10 is censored. Assign 5

#Tests seem to be people tested.

Alameda_CA_time <- read_html("https://services5.arcgis.com/ROBnTHSNjoZ2Wm1P/arcgis/rest/services/COVID_19_Statistics/FeatureServer/1/") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

Alameda_CA_data_raw1 <- fromJSON("https://services5.arcgis.com/ROBnTHSNjoZ2Wm1P/arcgis/rest/services/COVID_19_Statistics/FeatureServer/1/query?where=0%3D0&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson")$features$attributes

Alameda_CA_data_raw2 <- fromJSON("https://services5.arcgis.com/ROBnTHSNjoZ2Wm1P/arcgis/rest/services/COVID_19_Statistics/FeatureServer/0/query?where=0%3D0&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson")$features$attributes

Alameda_CA_data_raw <- Alameda_CA_data_raw1 %>% left_join(select(Alameda_CA_data_raw2, Zip_Number, Cases),
                                                          by = "Zip_Number")

Alameda_CA_data <- Alameda_CA_data_raw %>%
  rename(Geo_id = Zip_Alpha, Tests = NumberOfTests) %>%
  mutate(Deaths=NA,
         Geo_id_type="ZCTA", Update_time = Alameda_CA_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Alameda County, CA") %>%
  filter(!is.na(Cases)) %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Alameda_CA_data_raw, "raw_data_21_02_2021/Alameda_CA_06_03_2021_raw.rds")

############################ Los Angeles County, CA ############################

#Los Angeles County only provides at city/community level, which are very large aggregations. May not make sense to use.

############################ Orange County, CA ############################


Orange_CA_time <- read_html("https://services2.arcgis.com/LORzk2hk9xzHouw9/arcgis/rest/services/C19ZIP_TPP1dayRate_VIEWLAYER/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

Orange_CA_data_raw <- fromJSON("https://services2.arcgis.com/LORzk2hk9xzHouw9/arcgis/rest/services/C19ZIP_TPP1dayRate_VIEWLAYER/FeatureServer/0/query?where=0%3D0&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson")$features$attributes

Orange_CA_data <- Orange_CA_data_raw %>%
  rename(Geo_id = ZIP, Cases = tot_cas, Deaths = tot_dth) %>%
  mutate(Tests = NA, Positives = NA,
         Geo_id_type="ZCTA", Deaths = ifelse(is.na(Deaths), 0, Deaths), Update_time = Orange_CA_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Orange County, CA") %>%
  filter(!is.na(Cases)) %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Orange_CA_data_raw, "raw_data_21_02_2021/Orange_CA_06_03_2021_raw.rds")


############################ Sacramento County, CA ############################

date <- ymd(Sys.Date())
layer_text <- paste0("Updated_0", month(date), "_0", day(date), "_", substr(year(date), 1, 2))
url <- paste0("https://services6.arcgis.com/yeTSZ1znt7H7iDG7/ArcGIS/rest/services/", layer_text, "/FeatureServer/0")
alt_url <- "https://services6.arcgis.com/yeTSZ1znt7H7iDG7/ArcGIS/rest/services/All_Cases_03_01_21/FeatureServer/0"

Sacramento_CA_time <- read_html(url) %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Sacramento_CA_data_raw <- fromJSON(paste0(url, "/query?where=0%3D0&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson"))$features$attributes

if (is.na(Sacramento_CA_time)){
  Sacramento_CA_time <- read_html(alt_url) %>%
    html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
    { gsub(".*Date: ", "", .) } %>%
    { mdy(gsub(" .*", "", .)) }
  
  Sacramento_CA_data_raw <- fromJSON(paste0(alt_url, "/query?where=0%3D0&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson"))$features$attributes
  
}

Sacramento_CA_data <- Sacramento_CA_data_raw %>%
  rename(Geo_id = ZIP5) %>%
  mutate(Tests = NA, Deaths = NA, Positives = NA,
         Geo_id_type="ZCTA", Update_time = Sacramento_CA_time) %>%
  group_by(Geo_id, Geo_id_type, Update_time) %>%
  summarize(Cases = first(Cases), Tests = first(Tests), Deaths = first(Deaths), Positives = first(Positives)) %>%
  ungroup() %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Sacramento County, CA") %>%
  filter(!is.na(Cases)) %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)


saveRDS(Sacramento_CA_data_raw, "raw_data_21_02_2021/Sacramento_CA_06_03_2021_raw.rds")

############################ San Diego County, CA ############################

#San Diego has over time, but is not extracted here.

SanDiego_CA_time <- max(fromJSON("https://services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services/CovidDashUpdate/FeatureServer/0/query?where=ZipText+%3D+91901&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes$UpdateDate)
SanDiego_CA_time <- as.Date(SanDiego_CA_time/1000/60/60/24, "1970-01-01")

url <- paste0("https://services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services/CovidDashUpdate/FeatureServer/0/query?where=", 
              "UpdateDate+>%3D+DATE+%27", SanDiego_CA_time,
              "%27&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")

SanDiego_CA_data_raw <- fromJSON(url)$features$attributes

SanDiego_CA_data <- SanDiego_CA_data_raw %>%
  rename(Geo_id = ZipText, Cases = Case_Count) %>%
  mutate(Tests = NA, Deaths = NA, Positives = NA,
         Geo_id_type="ZCTA", Update_time = SanDiego_CA_time,
         Cases = ifelse(is.na(Cases), 0, Cases)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="San Diego County, CA") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(SanDiego_CA_data_raw, "raw_data_21_02_2021/SanDiego_CA_06_03_2021_raw.rds")


############################ San Francisco County, CA ############################

#Censored when pop < 1000. 
#Will filter out one ZCTA with pop < 1000 (94104)
#SF has by ZCTA and Census tract

SanFrancisco_CA_data_raw <- fromJSON("https://data.sfgov.org/resource/tef6-3vsw.json")


SanFrancisco_CA_data <- SanFrancisco_CA_data_raw %>% 
  filter(id != "94104") %>%
  rename(Geo_id = id, Cases = count, Deaths = deaths, Update_time = last_updated_at) %>%
  mutate(Tests = NA, Positives = NA, Geo_id_type = "ZCTA", Update_time = ymd(substr(Update_time, 1, 10)),
         Cases = ifelse(is.na(Cases),0, Cases),
         Cases = ifelse(is.na(Cases),0, Cases)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="San Francisco, CA") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)


saveRDS(SanFrancisco_CA_data_raw, "raw_data_21_02_2021/SanFrancisco_CA_06_03_2021_raw.rds")


############################ Santa Clara County, CA ############################

SantaClara_CA_time <- read_html("https://services.arcgis.com/NkcnS0qk4w2wasOJ/arcgis/rest/services/COVIDCasesByZipCode/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

SantaClara_CA_data_raw <- fromJSON("https://services.arcgis.com/NkcnS0qk4w2wasOJ/arcgis/rest/services/COVIDCasesByZipCode/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes

SantaClara_CA_data <- SantaClara_CA_data_raw %>%
  rename(Geo_id = Zipcode) %>%
  mutate(Tests=NA, Deaths=NA, Positives=NA, Geo_id_type="ZCTA", Update_time = SantaClara_CA_time,
         Cases = ifelse(is.na(Cases), 0, Cases)) %>%
  filter(!is.na(Geo_id)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Santa Clara County, CA") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(SantaClara_CA_data_raw, "raw_data_21_02_2021/SantaClara_CA_06_03_2021_raw.rds")


############################ Denver County, CO ############################

#Denver reports at "neighborhood" level, defined by the county/city
#Denver has not been reporting cumulative cases, so is being left out.


############################ Washington, DC ############################

#DC reports at "neighborhood" level, defined by DC
#Has to be read in manually after download

#DC has data over time, but not being used here

#In DC Positives is number of cases, but Tests is number of tests (same person tested multiple times would count as multiple tests)
#Need to be careful when/if comparing positivity rates.


Washington_DC_data_raw <- read_xlsx("~/Downloads/COVID19_DCHealthStatisticsDataV3 (NewFileStructure) (4).xlsx",
                               sheet = "Total Positives by Neighborhood")

Washington_DC_data <- Washington_DC_data_raw %>%
  mutate(Date = ymd(Date)) %>%
  filter(Date == max(Date)) %>%
  rename(Geo_id = Neighborhood, Update_time = Date, Cases = `Total Positives`)

Washington_DC_data_raw2 <- read_xlsx("~/Downloads/COVID19_DCHealthStatisticsDataV3 (NewFileStructure) (4).xlsx",
                                    sheet = "Total Tests by Neighborhood")


Washington_DC_data <- Washington_DC_data_raw2 %>%
  mutate(Date = ymd(Date)) %>%
  filter(Date == max(Date)) %>%
  rename(Geo_id = Neighborhood, Update_time = Date, Tests = `Total Tests`) %>%
  left_join(Washington_DC_data, by = c("Geo_id", "Update_time")) %>%
  mutate(Deaths=NA, Geo_id_type="Neighbourhood", Positives = NA,
         Cases = ifelse(is.na(Cases), 0, Cases)) %>%
  filter(!is.na(Geo_id)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Positives=as.integer(Positives),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Washington, DC") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Washington_DC_data_raw, "raw_data_21_02_2021/Washington_DC_06_03_2021_raw_cases.rds")
saveRDS(Washington_DC_data_raw2, "raw_data_21_02_2021/Washington_DC_06_03_2021_raw_tests.rds")

############################ Florida State ############################
##### Duval, Hillsborough, Miami-Dade, Orange and Pinellas County 

#Suppressed when cases <5. These are assigned 0.

FL_time <- read_html("https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/ArcGIS/rest/services/Florida_Cases_Zips_COVID19/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


FL_data_raw <- fromJSON('https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Florida_Cases_Zips_COVID19/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes

FL_data <- FL_data_raw %>%
  rename(Geo_id = ZIP, Cases = Cases_1) %>%
  mutate(Cases = ifelse(Cases == "<5" | Cases == "SUPPRESSED", 0, Cases), 
         Tests=NA, Deaths=NA, Positives = NA, Update_time=FL_time, Geo_id_type="ZCTA") %>%
  group_by(Geo_id, Update_time, Geo_id_type) %>% mutate(Cases = as.integer(Cases)) %>%
  summarize(Cases = sum(Cases), Tests = sum(Tests), Deaths = sum(Deaths), Positives = sum(Positives)) %>%
  ungroup() %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="FL") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(FL_data_raw, "raw_data_21_02_2021/FL_06_03_2021_raw.rds")

############################ Fulton County, GA ############################

##NEED TO CHECK MANUALLY

#Suppressed if <10. Assigned 5.
# 
# Fulton_link <- read_html("https://www.fultoncountyga.gov/covid-19/epidemiology-reports") %>%
#   html_nodes("a") %>% html_attr("href") %>% 
#   { .[grepl("zip", ., ignore.case=TRUE)] } %>% `[[`(1) %>%
#   { paste0("https://www.fultoncountyga.gov", .)}
# 
# Fulton_GA_data_raw1 <- extract_tables(Fulton_link, pages=5, output="data.frame", method="stream",
#                                guess=FALSE, area = list(c(74.5, 33.4, 745, 571.2)), 
#                                columns = list(c(89.4,160,250,460))) %>% `[[`(1) 
# 
# Fulton_GA_time <- mdy(substr(Fulton_link, str_length(Fulton_link)-12, str_length(Fulton_link)-5))
# 
# Fulton_GA_data1 <- Fulton_GA_data_raw1 %>% slice(-1:-4) %>% 
#   rename(Geo_id=X, Cases=X.20..Curren) %>% 
#   mutate(Tests=NA, Deaths=NA, Positives = NA,
#          Geo_id_type="ZCTA", Update_time=Fulton_GA_time,
#          Cases = as.integer(ifelse(Cases=="<10", "5", Cases))) %>% 
#   filter(Cases != "-") %>%
#   mutate(Geo_id=as.character(Geo_id),
#          Geo_id_type=as.character(Geo_id_type),
#          Positives=as.integer(Positives),
#          Cases=as.integer(Cases),
#          Tests=as.integer(Tests),
#          Deaths=as.integer(Deaths),
#          Source_geo="Fulton County, GA") %>%
#   select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)
# 
# Fulton_GA_raw_data2 <- extract_tables(Fulton_link, pages=6, method="stream",
#                                guess=FALSE, area = list(c(49.17219, 37.19205, 175.05298, 573.49669))) %>% `[[`(1)  %>% 
#                                { as.data.frame(.) }
# 
# Fulton_GA_raw_data2$V2 <- as.character(Fulton_GA_raw_data2$V2)
# Fulton_GA_raw_data2[1,2] <- gsub("[^[:digit:].]", "", gsub("COVID19 ", "", Fulton_GA_raw_data2[1,2]))
# 
# 
# Fulton_GA_data2 <- Fulton_GA_raw_data2 %>%
#   rename(Geo_id = V1, Cases = V2) %>% 
#   mutate(Cases = as.character(Cases), Geo_id = as.character(Geo_id)) %>% 
#   filter(Cases != "-") %>%
#   mutate(Tests=NA, Deaths=NA, Positives = NA,
#          Geo_id = ifelse(Geo_id == "Unknown", "Unassigned", Geo_id),
#          Geo_id_type="ZCTA", Update_time=Fulton_GA_time,
#          Cases = as.integer(ifelse(Cases=="<10", "5", Cases))) %>%
#   mutate(Geo_id=as.character(Geo_id),
#          Geo_id_type=as.character(Geo_id_type),
#          Positives=as.integer(Positives),
#          Cases=as.integer(Cases),
#          Tests=as.integer(Tests),
#          Deaths=as.integer(Deaths),
#          Source_geo="Fulton County, GA") %>%
#   select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)
# 
# 
# Fulton_GA_data <- rbind(Fulton_GA_data1, Fulton_GA_data2)
# 
# saveRDS(Fulton_GA_data_raw1, "raw_data_13_09_2020/Fulton_GA_13_09_2020_raw1.rds")
# saveRDS(Fulton_GA_raw_data2, "raw_data_13_09_2020/Fulton_GA_13_09_2020_raw2.rds")

############################ Cook County, IL ############################

#Illinois provides by age by zip as well

#Historical data may also be possible to obtain

#Zip codes with less than 5 cases are not included

IL_time <- ymd(paste0(unlist(fromJSON("https://idph.illinois.gov/DPHPublicInformation/api/COVID/GetZip")$lastUpdatedDate), 
                            collapse = "-"))


IL_data_raw <- fromJSON("https://idph.illinois.gov/DPHPublicInformation/api/COVID/GetZip")$zip_values 

IL_data <- IL_data_raw %>%
  select(1:3) %>% rename(Geo_id=zip, Cases=confirmed_cases, Tests=total_tested) %>% 
  mutate(Deaths=NA, Positives = NA,
         Geo_id_type="ZCTA", Update_time=IL_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="IL") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(IL_data_raw, "raw_data_21_02_2021/IL_06_03_2021_raw.rds")


############################ Marion County, IN ############################

#A Record may be suppressed if the population of a zip code is less than or equal to 1500 or the number of cases is less than 5.
#No suppressed ZCTAs within Marion county, so filtering out

#Suppressed if <5 cases or pop <= 1500. Assigned 0

IN_time <- read_html("https://hub.mph.in.gov/dataset/covid-19-cases-by-zip/resource/3ea01356-42e4-42aa-8935-493709313ca3") %>%
  html_node(xpath='//*[@id="content"]/div[3]/div/section[2]/div[2]/table') %>%
  html_table() %>%
  .[1,2] %>% mdy(.)
  
IN_data_raw <- read_csv("https://hub.mph.in.gov/dataset/14a59397-9ebc-4902-a7c7-fd7ca3c08101/resource/3ea01356-42e4-42aa-8935-493709313ca3/download/covid_count_per_zip_all.csv")

IN_data <- IN_data_raw %>%
  select(1:3) %>% rename(Geo_id=ZIP_CD, Cases=PATIENT_COUNT) %>% 
  mutate(Deaths=NA, Tests = NA, Positives = NA, 
         Cases = ifelse(Cases == "Suppressed", "0", Cases),
         Geo_id_type="ZCTA", Update_time=IN_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="IN") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)
  
saveRDS(IN_data_raw, "raw_data_21_02_2021/IN_06_03_2021_raw.rds")


############################ Jefferson County, KY ############################

Jefferson_KY_time <- read_html("https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/Cases_Zip_Pop_woLTC_Public/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Jefferson_KY_data_raw <- fromJSON('https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/Cases_Zip_Pop_woLTC_Public/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes

Jefferson_KY_data <- Jefferson_KY_data_raw %>%
  rename(Geo_id = ZIPCODE, Cases = Conf_count_1) %>%
  group_by(Geo_id) %>%
  summarize(Cases = first(Cases)) %>% ungroup() %>%
  mutate(Tests=NA, Deaths=NA, Positives=NA, Update_time=Jefferson_KY_time, Geo_id_type="ZCTA") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Jefferson County, KY") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Jefferson_KY_data_raw, "raw_data_21_02_2021/Jefferson_KY_06_03_2021_raw.rds")


############################ LA ############################
#####for Orleans Parish 

GET("https://ldh.la.gov/assets/oph/Coronavirus/data/LA_COVID_TESTBYWEEK_TRACT_PUBLICUSE.xlsx", write_disk(tf <- tempfile(fileext = ".xlsx")))
LA_data_raw <- read_xlsx(tf, 1)
unlink(tf)

LA_data <- LA_data_raw %>%
  rename(Geo_id = Tract, Tests = `Weekly Test Count`, Positives = `Weekly Positive Test Count`,
         Cases = `Weekly Case Count`) %>%
  group_by(Geo_id) %>% summarize(Tests = sum(Tests), Positives = sum(Positives), Cases = sum(Cases)) %>%
  mutate(Deaths=NA, Update_time = max(LA_data_raw$`Date for end of week`), Geo_id_type="CT") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Positives = as.integer(Positives),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="LA") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(LA_data_raw, "raw_data_21_02_2021/LA_06_03_2021_raw.rds")

############################ Maryland ############################
############## for Baltimore City 

#seven or fewer cases suppressed. #Assigned 0

MD_time <- read_html("https://services.arcgis.com/njFNhDsUCentVYJW/ArcGIS/rest/services/MDH_COVID_19_Dashboard_Feature_Layer_ZIPCodes_MEMA/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


MD_data_raw <- fromJSON('https://services.arcgis.com/njFNhDsUCentVYJW/ArcGIS/rest/services/MDH_COVID_19_Dashboard_Feature_Layer_ZIPCodes_MEMA/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes

MD_data <- MD_data_raw %>%
  rename(Geo_id = ZIPCODE1, Cases = ProtectedCount) %>%
  mutate(Deaths=NA, Positives = NA, Tests = NA, Cases = ifelse(is.na(Cases), 0, Cases),
         Update_time = MD_time, Geo_id_type="ZCTA") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Positives = as.integer(Positives),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="MD") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(MD_data_raw, "raw_data_21_02_2021/MD_06_03_2021_raw.rds")


############################ Boston City, Suffolk County, MA ############################

#https://www.bphc.org/whatwedo/infectious-diseases/Infectious-Diseases-A-to-Z/covid-19/Pages/default.aspx

#Boston will be read in manually because of unclear definitions on web data
# 
# ########Old webscraping code
# 
# Boston_MA_time <- read_html("https://bphc.org/whatwedo/infectious-diseases/Infectious-Diseases-A-to-Z/covid-19/Pages/default.aspx") %>%
#   html_nodes(xpath='//*[@id="WebPartWPQ3"]/div[1]/div[2]/p[4]/em') %>%
#   html_text() %>%
#   { gsub(".*As of ", "", .) } %>%
#   { gsub(", there .*", "", .) } %>%
#   { strsplit(., " ") } %>%
#   { .[[1]] } %>%
#   { mdy(paste(.[2], .[3], .[4], sep=" ")) }
# 
# Boston_MA_data_raw <- read_html("https://bphc.org/whatwedo/infectious-diseases/Infectious-Diseases-A-to-Z/covid-19/Pages/default.aspx") %>%
#   html_nodes(xpath='//*[@id="WebPartWPQ3"]/div[1]/div[2]/table') %>%
#   html_table() %>% `[[`(1)
# 
# Boston_MA_data <- Boston_MA_data_raw %>%
#   rename(Geo_id = X1, Tests = X2, Positivity = X3) %>%
#   slice(-1) %>%
#   mutate(Tests = as.numeric(gsub(",", "", Tests)),
#          Positivity = as.numeric(gsub("%", "", Positivity))/100,
#          Positives = round(Tests * Positivity, digits = 0),
#          Geo_id = gsub(".*-", "", Geo_id),
#          Geo_id = gsub(" ", "", Geo_id),
#          Geo_id = gsub(",", "_", Geo_id),
#          Geo_id = ifelse(Geo_id == "Other", "Unassigned", Geo_id),
#          Cases = NA,
#          Deaths = NA, Update_time = Boston_MA_time, Geo_id_type="ZCTA aggregate") %>%
#   mutate(Tests = ifelse(is.na(Tests), 0, Tests),
#          Positives = ifelse(is.na(Positives), 0, Positives)) %>%
#   filter(Geo_id != "Boston") %>% 
#   mutate(Geo_id=as.character(Geo_id),
#          Geo_id_type=as.character(Geo_id_type),
#          Positives =as.integer(Positives),
#          Tests = as.integer(Tests),
#          Deaths=as.integer(Deaths),
#          Cases =as.integer(Cases),
#          Source_geo="Boston, MA") %>%
#   select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)
# 
# saveRDS(Boston_MA_data_raw, "raw_data_13_09_2020/Boston_MA_13_09_2020_raw.rds")


############################ Kent County, MI ############################

# https://www.accesskent.com/Health/covid-19-data.htm
#Needs to be read in from Google Sheet

#Under 20 suppressed


############################ Wayne County, MI ############################

#Needs to be read in from Google Sheet

#https://codtableau.detroitmi.gov/t/DHD/views/CityofDetroit-PublicCOVIDDashboard/ZIPCodeDeathDashboard?%3AisGuestRedirectFromVizportal=y&%3Aembed=y

############################ Minneapolis ############################

#Hennepin and Ramsey County
#Suppresed under 5. Assign 1, because true zeroes appear as zeroes.

MN_time <- read_html("https://www.health.state.mn.us/diseases/coronavirus/stats/index.html") %>%
  html_nodes(xpath='//*[@id="body"]/div/ul/li/strong/a') %>%
  html_text() %>%
  { gsub(".*Report: ", "", .) } %>%
  { gsub(" .*", "", .) } %>%
  { mdy(.) }
  
MN_data_raw <- read_csv("https://www.health.state.mn.us/diseases/coronavirus/stats/wmapcz07.csv")

MN_data <- MN_data_raw %>%
  rename(Geo_id = ZIP) %>%
  mutate(Geo_id = ifelse(is.na(Geo_id), "Unassigned", Geo_id),
         Cases = ifelse(Cases == "<=5", 1, Cases),
         Tests = NA, Positives = NA,
         Deaths = NA, Update_time = MN_time, Geo_id_type="ZCTA") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives =as.integer(Positives),
         Cases =as.integer(Cases),
         Tests = as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="MN") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)
  
saveRDS(MN_data_raw, "raw_data_21_02_2021/MN_06_03_2021_raw.rds")

######################### Kansas_City, MO ############################

#Not sure what suppresed is, but lowest case number unsupressed is 8.
#So likely <7. Assign 0.

#Tested is residents tested

KansasCity_MO_time <- fromJSON("http://api.us.socrata.com/api/catalog/v1?ids=374j-h7xt") %>%
  { .$results$resource$updatedAt } %>%
  { gsub("T.*", "", .) } %>%
  { ymd(.) }

KansasCity_MO_data_raw <- read_csv("https://data.kcmo.org/resource/374j-h7xt.csv")

KansasCity_MO_data <- KansasCity_MO_data_raw %>%
  rename(Geo_id = zipcode, Cases = cases, Tests=total_residents_tested) %>%
  mutate(Cases = ifelse(Cases == "SUPP*", 0, Cases),
         Positives = NA,
         Deaths = NA, Update_time = KansasCity_MO_time, Geo_id_type="ZCTA") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives =as.integer(Positives),
         Cases =as.integer(Cases),
         Tests = as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Kansas City, MO") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(KansasCity_MO_data_raw, "raw_data_21_02_2021/KansasCity_MO_06_03_2021_raw.rds")

######################### St Louis City, MO ############################

#Not sure what suppresed is, but lowest case number unsupressed is 8
#CHeck date manually

StLouis_MO_time <- Sys.Date()


StLouis_MO_data_raw <- fromJSON('https://maps6.stlouis-mo.gov/arcgis/rest/services/HEALTH/Case_Rate_per_100k_Residents/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes

StLouis_MO_data <- StLouis_MO_data_raw  %>%
  rename(Geo_id = ZCTA5CE10) %>%
  mutate(Deaths=NA, Positives = NA, Tests = NA, Update_time = StLouis_MO_time, Geo_id_type="ZCTA") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Positives = as.integer(Positives),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="St Louis, MO") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(StLouis_MO_data_raw, "raw_data_21_02_2021/StLouis_MO_06_03_2021_raw.rds")

######################### Clark County, NV ############################

Clark_NV_time <- read_html("https://www.southernnevadahealthdistrict.org/covid-19-case-count-archive/") %>%
  html_nodes(xpath = '/html/body/div[1]/div[2]/main/div/section/div/div/div[1]/div/div/div/div[2]/table') %>%
  html_table() %>% `[[`(1) %>%
  filter(., grepl("Download", `ZIP Code Count`, ignore.case = T)) %>%
  .[1,1] %>% { mdy(.) }
  

Clark_NV_link <- read_html("https://www.southernnevadahealthdistrict.org/covid-19-case-count-archive/") %>%
  html_nodes(xpath = '//td/a') %>%
  html_attr("href") %>%
  { .[grepl(".csv", .)][1]}


Clark_NV_data_raw <- fread(Clark_NV_link) 

Clark_NV_data <- Clark_NV_data_raw %>%
  rename(Geo_id = zip_code, Cases = count) %>%
  mutate(Cases = ifelse(Cases == "<5 suppressed", 0, Cases), Positives=NA, Tests=NA, 
         Deaths=NA, Geo_id_type="ZCTA", Update_time=Clark_NV_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Clark County, NV") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Clark_NV_data_raw, "raw_data_21_02_2021/Clark_NV_06_03_2021_raw.rds")

######################### Newark, Essex County, NJ ############################
# 
# Newark_NJ_data_raw <- fread("https://newark-dashboardcovid.trial.opendatasoft.com/explore/dataset/aggregates-test/download/?format=csv&timezone=America/New_York&lang=en&use_labels_for_header=true&csv_separator=%2C")
# 
# Newark_NJ_time <- max(ymd(Newark_NJ_data_raw$date))
# 
# #Converting unknown death status to no death status
# 
# Newark_NJ_data_tests <- Newark_NJ_data_raw %>% 
#   mutate(patient_died=ifelse(patient_died=="U", "N", patient_died)) %>%
#   mutate(zip_code = str_pad(as.character(zip_code), 5, "left", "0")) %>%
#   filter(status != "Pending" & status != "Inconclusive") %>%
#   group_by(zip_code, status) %>% 
#   summarize(total=sum(number_of_cases)) %>%
#   filter(zip_code %in% c("07101","07106","07107","07104","07029","07105","07114","07112","07108","07103","07102")) %>%
#   pivot_wider(names_from = status, values_from = total) %>%
#   mutate(Negative=ifelse(is.na(Negative),0,Negative)) %>%
#   mutate(Tests=Positive+Negative) %>%
#   rename(Geo_id=zip_code) %>%
#   select(Geo_id, Positive, Tests)
# 
# Newark_NJ_data_deaths <- Newark_NJ_data_raw %>% 
#   mutate(patient_died=ifelse(patient_died=="U", "N", patient_died)) %>%
#   mutate(zip_code = str_pad(as.character(zip_code), 5, "left", "0")) %>%
#   filter(status=="Positive") %>%
#   group_by(zip_code, patient_died) %>% 
#   summarize(total=sum(number_of_cases)) %>%
#   filter(zip_code %in% c("07101","07106","07107","07104","07029","07105","07114","07112","07108","07103","07102")) %>%
#   pivot_wider(names_from = patient_died, values_from = total) %>%
#   mutate(Y=ifelse(is.na(Y),0,Y)) %>%
#   rename(Deaths=Y, Geo_id=zip_code) %>%
#   select(Geo_id, Deaths)
# 
# Newark_NJ_data <- left_join(Newark_NJ_data_tests, Newark_NJ_data_deaths, by="Geo_id") %>%
#   mutate(Positives = NA,
#          Geo_id_type="ZCTA", 
#          Update_time = Newark_NJ_time, 
#          Source_geo="Newark, NJ") %>% ungroup() %>%
#   rename(Cases = Positive) %>%
#   mutate(Geo_id=as.character(Geo_id),
#          Geo_id_type=as.character(Geo_id_type),
#          Positives=as.integer(Positives),
#          Cases = as.integer(Cases),
#          Tests=as.integer(Tests),
#          Deaths=as.integer(Deaths),
#          Source_geo="Newark, NJ") %>%
#   select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)
# 
# saveRDS(Newark_NJ_data_raw, "raw_data_21_02_2021/Newark_NJ_26_02_2021_raw.rds")

######################### NJ ############################

NJ_time <- read_html("https://services7.arcgis.com/Z0rixLlManVefxqY/arcgis/rest/services/ZipCodeCases/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


NJ_data_raw <- fromJSON("https://services7.arcgis.com/Z0rixLlManVefxqY/arcgis/rest/services/ZipCodeCases/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes

saveRDS(NJ_data_raw, "raw_data_21_02_2021/NJ_06_03_2021_raw.rds")


######################### Erie County, NY ############################


Erie_NY_time <- read_html("https://services1.arcgis.com/CgOSc11uky3egK6O/ArcGIS/rest/services/erie_zip_codes_confirmed_counts/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Erie_NY_data_raw <- fromJSON("https://services1.arcgis.com/CgOSc11uky3egK6O/ArcGIS/rest/services/erie_zip_codes_confirmed_counts/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes

Erie_NY_data <- Erie_NY_data_raw %>%
  rename(Geo_id = ZIP_CODE, Cases = CONFIRMED) %>%
  mutate(Tests = NA, Deaths=NA, Positives = NA, Geo_id_type="ZCTA", Update_time=Erie_NY_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Erie County, NY") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Erie_NY_data_raw, "raw_data_21_02_2021/Erie_NY_06_03_2021_raw.rds")


######################### New York City, NY ############################

NYC_NY_time <- read_html("https://github.com/nychealth/coronavirus-data/commits/master/totals/data-by-modzcta.csv") %>%
  html_nodes(xpath='//*[@id="js-repo-pjax-container"]/div[2]/div/div[2]/div[1]') %>%
  html_text() %>% { gsub("\n", "", .) } %>%
  { str_trim(.) } %>%
  { gsub("Commits on ", "", .) } %>% 
  { substr(., 1, 12) } %>%
  { mdy(.) }

NYC_NY_data_raw <- fread("https://raw.githubusercontent.com/nychealth/coronavirus-data/master/totals/data-by-modzcta.csv")

#For new york positives and cases seem the same

NYC_NY_data <- NYC_NY_data_raw %>%
  mutate(MODIFIED_ZCTA = ifelse(is.na(MODIFIED_ZCTA), "Unassigned", MODIFIED_ZCTA), 
         Update_time=NYC_NY_time,
         Geo_id_type="Modified ZCTA",
         Positives = round(TOTAL_COVID_TESTS*(PERCENT_POSITIVE/100))) %>%
  rename(Geo_id=MODIFIED_ZCTA, 
         Cases=COVID_CASE_COUNT, 
         Tests = TOTAL_COVID_TESTS,
         Deaths=COVID_DEATH_COUNT) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases = as.integer(Cases),
         Positives=as.integer(Positives),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="New York City, NY") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(NYC_NY_data_raw, "raw_data_21_02_2021/NYC_NY_06_03_2021_raw.rds")


######################### Monroe County, NY ############################

Monroe_NY_time <- read_html("https://services2.arcgis.com/I9nxaBNNOyopX343/ArcGIS/rest/services/Cases_current/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Monroe_NY_data_raw <- fromJSON('https://services2.arcgis.com/I9nxaBNNOyopX343/ArcGIS/rest/services/Cases_current/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes

Monroe_NY_data <- Monroe_NY_data_raw %>%
  rename(Geo_id = name, Cases = confirmed) %>%
  mutate(Tests=NA, Deaths=NA, Positives = NA, Update_time=Monroe_NY_time, Geo_id_type="ZCTA") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Positives=as.integer(Positives),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Monroe County, NY") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Monroe_NY_data_raw, "raw_data_21_02_2021/Monroe_NY_06_03_2021_raw.rds")

######################### North Carolina ############################
######################### Mecklenburg and Wake County 
#Suppressed when pop < 500 and cases < 5. Assign 0

NC_time <- read_html("https://services.arcgis.com/iFBq2AW9XO0jYYF7/arcgis/rest/services/Covid19byZIPnew/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

NC_data_raw <- fromJSON("https://services.arcgis.com/iFBq2AW9XO0jYYF7/arcgis/rest/services/Covid19byZIPnew/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes

NC_data <- NC_data_raw %>%
  rename(Geo_id = ZIPCode, Deaths=Deaths) %>%
  mutate(Cases = ifelse(is.na(Cases), 0, Cases),
         Deaths = ifelse(is.na(Deaths), 0, Deaths)) %>%
  mutate(Tests = NA, Positives = NA, Geo_id_type="ZCTA", Update_time=NC_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases = as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="NC") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo) 

saveRDS(NC_data_raw, "raw_data_21_02_2021/NC_06_03_2021_raw.rds")


######################### Cincinnati, Hamilton County, OH ############################

#needs to be done manually


######################### OK ############################
################ Oklahoma County 

OK_time <- ymd(fread("https://storage.googleapis.com/ok-covid-gcs-public-download/oklahoma_cases_zip.csv")$ReportDate[1])

OK_data_raw <- fread("https://storage.googleapis.com/ok-covid-gcs-public-download/oklahoma_cases_zip.csv")

OK_data <- OK_data_raw %>%
  rename(Geo_id=Zip) %>% 
  mutate(Geo_id = ifelse(Geo_id == "Other***", "Unassigned", Geo_id),
         Tests=NA, Positives = NA, Geo_id_type="ZCTA", Update_time=OK_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases = as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Oklahoma") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(OK_data_raw, "raw_data_21_02_2021/OK_06_03_2021_raw.rds")

######################### OR ############################
######################### Multnomah County 

##Get Oregon data. Suppressed of 1-9. Assigned 1


OR_time <- ymd("2021-03-03")

OR_data_raw <- fread("~/Downloads/Cases by ZIP Code (2).csv")


OR_data <- OR_data_raw %>%
  rename(Geo_id=`ZIP Code`, Cases = `Case Count`) %>%
  mutate(Tests=NA, Positives = NA,
         Cases = ifelse(Cases=="*N/A", 1, Cases),
         Deaths=NA, Geo_id_type="ZCTA", Update_time=OR_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Positives=as.integer(Positives),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="OR") %>%
  mutate(Geo_id = ifelse(Geo_id=="Missing", "Unassigned", Geo_id)) %>%
  filter(Geo_id != "Suppressed") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)


saveRDS(OR_data_raw, "raw_data_21_02_2021/OR_06_03_2021_raw.rds")

######################### Philadelphia ############################

## Get Phillly data

Philadelphia_PA_raw_data1 <- fread("https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+covid_cases_by_zip&filename=covid_cases_by_zip&format=csv&skipfields=cartodb_id")

Philadelphia_PA_raw_data2 <- fread("https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+covid_deaths_by_zip&filename=covid_deaths_by_zip&format=csv&skipfields=cartodb_id") 

Philadelphia_PA_time <- ymd(str_sub(Philadelphia_PA_raw_data1$etl_timestamp[1], 1, 10))

Philadelphia_PA_data <- Philadelphia_PA_raw_data1 %>%
  group_by(zip_code) %>%
  summarize(Tests = sum(count)) %>%
  left_join(select(filter(Philadelphia_PA_raw_data1, covid_status == "POS"), zip_code, count), by = "zip_code") %>%
  rename(Cases = count, Geo_id = zip_code) %>%
  mutate(Cases = ifelse(Cases == -1, 0, Cases),
         Geo_id_type="ZCTA", Update_time=Philadelphia_PA_time, Positives = NA) %>%
  mutate(Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Source_geo="Philadelphia, PA") %>%
  left_join(select(Philadelphia_PA_raw_data2, zip_code, count), by = c("Geo_id" = "zip_code")) %>%
  mutate(Geo_id=as.character(Geo_id), Deaths = as.integer(count)) %>%
  mutate(Cases = ifelse(is.na(Cases), 0, Cases),
         Deaths = ifelse(is.na(Deaths), 0, Deaths)) %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)
  

saveRDS(Philadelphia_PA_raw_data1, "raw_data_21_02_2021/Philadelphia_PA_06_03_2021_raw1.rds")
saveRDS(Philadelphia_PA_raw_data2, "raw_data_21_02_2021/Philadelphia_PA_06_03_2021_raw2.rds")


######################### Alleghany County (PA data) ############################

## Get Pittsburgh data

#Suppressed 1-4. Assigned 1

PA_time <- read_html("https://services1.arcgis.com/Nifc7wlHaBPig3Q3/arcgis/rest/services/ZIP_Code_PA_COVID/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


PA_data_raw <- fromJSON("https://services1.arcgis.com/Nifc7wlHaBPig3Q3/arcgis/rest/services/ZIP_Code_PA_COVID/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes

PA_data <- PA_data_raw %>%
  rename(Geo_id = ZIP_CODE, Cases = Positive) %>%
  mutate(Cases = ifelse(Cases == -1, 1, Cases),
         Geo_id_type="ZCTA", Deaths=NA, Update_time=PA_time, Positives = NA) %>%
  mutate(Tests = Cases + Negative) %>%
  filter(!(Geo_id %in% Philadelphia_PA_data$Geo_id)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="PA") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)


saveRDS(PA_data_raw, "raw_data_21_02_2021/PA_06_03_2021_raw.rds")

######################### Rhode Island ############################

RI_time <- mdy(colnames(read_sheet("https://docs.google.com/spreadsheets/d/1c2QrNMz8pIbYEKzMJL7Uh2dtThOJa2j1sSMwiDo5Gz4/edit#gid=365656702", 
                                   "Cases by ZCTA", range = "B86")))
RI_data_raw <- read_sheet("https://docs.google.com/spreadsheets/d/1c2QrNMz8pIbYEKzMJL7Uh2dtThOJa2j1sSMwiDo5Gz4/edit#gid=365656702", 
                      "Cases by ZCTA", range = "A1:B79")

RI_data <- RI_data_raw %>%
  rename(Geo_id=`Zip code tabluation area`, Cases=`Rhode Island COVID-19 cases`) %>%
  mutate(Geo_id_type="ZCTA", Update_time=RI_time, Source_geo="RR",
         Total=NA, Deaths=NA, Positives=NA, Tests = NA,
         Geo_id=ifelse(Geo_id=="Pending further info", "Unassigned", Geo_id)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Positives=as.integer(Positives),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths)) %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)


saveRDS(RI_data_raw, "raw_data_21_02_2021/RI_06_03_2021_raw.rds")

######################### Bexar County, TX ############################

Bexar_TX_time <- read_html("https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/vBexarCountyZipCodes_EnrichClip/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Bexar_TX_data_raw <- fromJSON("https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/vBexarCountyZipCodes_EnrichClip/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes

Bexar_TX_data <- Bexar_TX_data_raw %>%
  rename(Geo_id = ZIP_CODE, Cases = Positive) %>%
  mutate(Tests = NA,
         Geo_id_type="ZCTA", Deaths=NA, Update_time=PA_time, Positives = NA) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Bexar County, TX") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Bexar_TX_data_raw, "raw_data_21_02_2021/Bexar_TX_06_03_2021_raw.rds")


######################### Collin County, TX ############################

# 
# Collin_TX_time <- read_html("https://services1.arcgis.com/fdWXd5OobWR1E3er/ArcGIS/rest/services/DSHS_Live/FeatureServer/2") %>%
#   html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
#   { gsub(".*Date: ", "", .) } %>%
#   { mdy(gsub(" .*", "", .)) }
# 
# 
# Collin_TX_data_raw <- fromJSON("https://services1.arcgis.com/fdWXd5OobWR1E3er/ArcGIS/rest/services/DSHS_Live/FeatureServer/2/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes 
#   
# Collin_TX_data <- Collin_TX_data_raw %>%
#   rename(Geo_id = ZIP_NO, Cases = CollinInternalCaseNum) %>%
#   mutate(Total=NA, Deaths=NA, Positives = NA, Tests = NA, Geo_id_type="ZCTA", Update_time=Collin_TX_time,
#          Cases=ifelse(is.na(Cases), 0, Cases)) %>%
#   mutate(Geo_id=as.character(Geo_id),
#          Geo_id_type=as.character(Geo_id_type),
#          Cases = as.integer(Cases),
#          Positives=as.integer(Positives),
#          Total=as.integer(Total),
#          Deaths=as.integer(Deaths),
#          Source_geo="Collin County, TX") %>%
#   select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)
# 
# 
# saveRDS(Collin_TX_data_raw, "raw_data_21_02_2021/Collin_TX_26_02_2021_raw.rds")

######################### Dallas County, TX ############################

Dallas_TX_links <- fromJSON("https://services3.arcgis.com/kJv6AkjtZisjy0Ed/ArcGIS/rest/services?f=pjson")$services %>%
  filter(grepl("ZipPrd_FeatureAccess_20210224", name, ignore.case = TRUE) |
           grepl(paste0("ZipPrd_FeatureAccess_", as.character(year(Sys.Date())), as.character(month(Sys.Date())), as.character(day(Sys.Date()))), name, ignore.case = TRUE)) %>%
  slice(nrow(.))

Dallas_TX_data_raw <- fromJSON(paste0(Dallas_TX_links$url, "/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson"))$features$attributes

Dallas_TX_time <- ymd(unique(Dallas_TX_data_raw$load_date))

Dallas_TX_data <- Dallas_TX_data_raw %>%
  rename(Geo_id = GEOID10, Cases = Present) %>%
  mutate(Tests=NA, Deaths=NA, Positives=NA, Geo_id_type="ZCTA", Update_time=Dallas_TX_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Tests = as.integer(Tests),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Deaths=as.integer(Deaths),
         Source_geo="Dallas County, TX") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Dallas_TX_data_raw, "raw_data_21_02_2021/Dallas_TX_06_03_2021_raw.rds")

######################### Harris County, TX ############################


Harris_TX_time <- read_html("https://services.arcgis.com/su8ic9KbA7PYVxPS/ArcGIS/rest/services/HCPH_COVID19_Zip_Codes_download/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

Harris_TX_data_raw <- fromJSON("https://services.arcgis.com/su8ic9KbA7PYVxPS/ArcGIS/rest/services/HCPH_COVID19_Zip_Codes_download/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes

Harris_TX_data <- Harris_TX_data_raw %>%
  rename(Geo_id = ZIP, Cases = TotalConfirmedCases, Deaths=Death_str) %>%
  mutate(Tests = NA, Positives = NA, Update_time=Harris_TX_time, Geo_id_type="ZCTA") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Tests=as.integer(Tests),
         Cases=as.integer(Cases),
         Deaths=as.integer(Deaths),
         Source_geo="Harris County, TX") %>%
  mutate(Deaths = ifelse(is.na(Deaths), 0, Deaths)) %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Harris_TX_data_raw, "raw_data_21_02_2021/Harris_TX_06_03_2021_raw.rds")

######################### Tarrant County, TX ############################


Tarrant_TX_time <- read_html("https://services8.arcgis.com/0emesQkjyT7tJv3q/arcgis/rest/services/Tarrant_30_day/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }

Tarrant_TX_data_raw <- fromJSON("https://services8.arcgis.com/0emesQkjyT7tJv3q/arcgis/rest/services/Tarrant_30_day/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes

Tarrant_TX_data <- Tarrant_TX_data_raw %>%
  rename(Geo_id = ZIP_CODE) %>%
  mutate(Tests=NA, Deaths=Deaths, Positives=NA, Geo_id_type="ZCTA", Update_time=Tarrant_TX_time) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Tarrant County, TX") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)


saveRDS(Tarrant_TX_data_raw, "raw_data_21_02_2021/Tarrant_TX_06_03_2021_raw.rds")

######################### Travis County, TX ############################


Travis_TX_time<- read_html("https://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/Austin_Travis_Zip_code_Counts_(Public_View)/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Travis_TX_data_raw <- fromJSON("https://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/Austin_Travis_Zip_code_Counts_(Public_View)/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson")$features$attributes

Travis_TX_data <- Travis_TX_data_raw %>%
  filter(Date == max(Date)) %>%
  pivot_longer(cols = Zip_76574:Zip_78759, names_to = "ZIPCODE", values_to = "Cases") %>%
  mutate(ZIPCODE = gsub("Zip_", "", ZIPCODE)) %>%
  rename(Geo_id = ZIPCODE) %>%
  mutate(Tests = NA, Geo_id_type="ZCTA", Update_time=Travis_TX_time, Deaths=NA, Positives = NA,
         Cases=ifelse(is.na(Cases), 0, Cases)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Travis County, TX") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Travis_TX_data_raw, "raw_data_21_02_2021/Travis_TX_06_03_2021_raw.rds")

######################### Salt Lake County, UT ############################

#Needs to be manual
# https://slco.org/health/COVID-19/data/

######################### VA ############################

#Case counts between 1 and 4 are suppresed. But each date has a separate observation so suppression can build up. 
#Going to randomly assign cases between 1 and 4 for suppressed ones

set.seed(1993)
VA_data_raw <- fread("https://data.virginia.gov/api/views/8bkr-zfqv/rows.csv?accessType=DOWNLOAD")

VA_time <- fromJSON("https://data.virginia.gov/api/views/8bkr-zfqv") %>%
{ .$columns$cachedContents$largest[1] } %>%
{ gsub("T.*", "", .) } %>%
{ ymd(.) }


VA_data <- VA_data_raw %>%
  mutate(`Number of Cases` = ifelse(`Number of Cases` == "Suppressed*", as.character(sample.int(4, 1)), `Number of Cases`),
         `Number of PCR Testing Encounters` = ifelse(`Number of PCR Testing Encounters` == "Suppressed*", as.character(sample.int(4, 1)), `Number of PCR Testing Encounters`)) %>%
  mutate(`Number of Cases` = as.integer(`Number of Cases`), `Number of PCR Testing Encounters` = as.integer(`Number of PCR Testing Encounters`)) %>%
  filter(`Report Date` == "02/24/2021") %>%
  rename(Geo_id = `ZIP Code`, Cases = `Number of Cases`, Tests = `Number of PCR Testing Encounters`) %>%
  mutate(Geo_id_type="ZCTA", Update_time=VA_time, Deaths=NA, Positives = NA,
         Cases=ifelse(is.na(Cases), 0, Cases)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="VA") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)
  
saveRDS(VA_data_raw, "raw_data_21_02_2021/VA_06_03_2021_raw.rds")


######################### WI ############################

#WI also has over time

#Suppressed if 0-4
# 
# WI_time <- read_html("https://dhsgis.wi.gov/server/rest/services/DHS_COVID19/COVID19_WI/MapServer/13") %>%
#   html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
#   { gsub(".*Date: ", "", .) } %>%
#   { mdy(gsub(" .*", "", .)) }

WI_data_raw <- fromJSON('https://dhsgis.wi.gov/server/rest/services/DHS_COVID19/COVID19_WI/MapServer/13/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson')$features$attributes


WI_time <- as.Date(max(WI_data_raw$DATE)/1000/60/60/24, "1970-01-01")

WI_data <- WI_data_raw %>%
  rename(Geo_id = GEOID, Cases = POSITIVE) %>%
  mutate(Cases = ifelse(Cases == -999, 0, Cases), 
         NEGATIVE = ifelse(NEGATIVE == -999, 0, NEGATIVE),
         Geo_id_type="CT", Update_time = WI_time, Deaths = NA, Positives = NA) %>%
  mutate(Tests = Cases + NEGATIVE) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="WI") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(WI_data_raw, "raw_data_21_02_2021/WI_06_03_2021_raw.rds")

######################### King County, WA ############################

GET("https://www.kingcounty.gov/depts/health/covid-19/data/~/media/depts/health/communicable-diseases/documents/C19/data/overall-counts-rates-geography-feb-24.ashx", 
    config = httr::config(ssl_verifypeer = F),
    write_disk(tf <- tempfile(fileext = ".xlsx")))
King_WA_data_raw <- read_xlsx(tf, "ZIP")
unlink(tf)


King_WA_time <- read_html(httr::GET("https://www.kingcounty.gov/depts/health/covid-19/data/daily-summary.aspx", config = httr::config(ssl_verifypeer = F))) %>%
  html_nodes(xpath='//*[@id="heading4A25CFF959E04B63BEAFD5045EDCBD05"]/h4/a') %>%
  html_text() %>%
  { gsub(".*updated ", "", .) } %>%
  { gsub(").*", "", .) } %>%
  { mdy(gsub("t.", "", .)) }



King_WA_data <- King_WA_data_raw %>%
  filter(Location_Name != "All King County") %>%
  rename(Geo_id = Location_Name, Cases = Positives) %>%
  mutate(Geo_id_type="ZCTA", Update_time = King_WA_time, Deaths = Deaths, 
         Tests = People_Tested, Positives = NA) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="King County, WA") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(King_WA_data_raw, "raw_data_21_02_2021/King_WA_06_03_2021_raw.rds")


######################### Toronto City, ON ############################

#Set time manually

Toronto_ON_data_raw <- fread("https://ckan0.cf.opendata.inter.prod-toronto.ca/download_resource/e5bf35bc-e681-43da-b2ce-0242d00922ad?format=csv")

Toronto_ON_data <- Toronto_ON_data_raw %>%
  group_by(`Neighbourhood Name`) %>%
  summarize(Cases = n(), Deaths = sum(Outcome == "FATAL")) %>%
  rename(Geo_id = `Neighbourhood Name`) %>%
  mutate(Geo_id_type="Neighbourhood", Update_time = mdy("March 01, 2021"), Tests = NA, Positives = NA,
         Geo_id = ifelse(Geo_id == "", "Unassigned", Geo_id)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Toronto, ON") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Toronto_ON_data_raw, "raw_data_21_02_2021/Toronto_ON_06_03_2021_raw.rds")


######################### Montreal, QC ############################


#Under 5 suppressed

Montreal_QC_data_raw <- fread("https://santemontreal.qc.ca/fileadmin/fichiers/Campagnes/coronavirus/situation-montreal/municipal.csv",
                              encoding = 'Latin-1')


Montreal_QC_time <- read_html("https://santemontreal.qc.ca/en/public/coronavirus-covid-19/situation-of-the-coronavirus-covid-19-in-montreal/#c43674") %>%
  html_nodes(xpath='//*[@id="c46934"]/div[2]/p[4]/text()[1]') %>%
  html_text() %>%
  { gsub(".*on ", "", .)} %>%
  { gsub(",.*", "", .) } %>%
  { dmy(paste(., "2021")) }



Montreal_QC_data <- Montreal_QC_data_raw %>%
  rename(Geo_id = `Arrondissement ou ville liée`,
         Cases = `Nombre de cas cumulatif, depuis le début de la pandémie`,
         Deaths = `Nombre de décès cumulatif, depuis le début de la pandémie`) %>%
  mutate(Geo_id_type="Neighbourhood", Update_time = Montreal_QC_time, Tests = NA, Positives = NA,
         Geo_id = ifelse(Geo_id == "Territoire à confirmer", "Unassigned", Geo_id),
         Cases = ifelse(Cases == "< 5", "0", gsub(",", "", Cases)),
         Deaths = ifelse(Deaths == "< 5", "0", gsub(",", "", Deaths))) %>%
  filter(Geo_id != "Total à Montréal") %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Montreal, QC") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)


saveRDS(Montreal_QC_data_raw, "raw_data_21_02_2021/Montreal_QC_06_03_2021_raw.rds")


######################### Peel, ON ############################


Peel_ON_time<- read_html("https://services6.arcgis.com/ONZht79c8QWuX759/arcgis/rest/services/COVIDCaseData_PeelCensusTract/FeatureServer/0") %>%
  html_node(xpath = "/html/body/div[2]") %>% html_text() %>%
  { gsub(".*Date: ", "", .) } %>%
  { mdy(gsub(" .*", "", .)) }


Peel_ON_data_raw <- fromJSON("https://services6.arcgis.com/ONZht79c8QWuX759/arcgis/rest/services/COVIDCaseData_PeelCensusTract/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json")$features$attributes

Peel_ON_data <- Peel_ON_data_raw %>%
  rename(Geo_id = CTUID, Cases = Total) %>%
  mutate(Tests = NA, Geo_id_type="CT", Update_time=Peel_ON_time, Deaths=NA, Positives = NA,
         Cases=ifelse(is.na(Cases), 0, Cases)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Peel, ON") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Peel_ON_data_raw, "raw_data_21_02_2021/Peel_ON_06_03_2021_raw.rds")


######################### Hamilton, ON ############################

Hamilton_ON_time<- mdy("Feb 28, 2021")

Hamilton_ON_data_raw <- fromJSON("https://spatialsolutions.hamilton.ca/webgis/rest/services/OpenData/Spatial_Collection_5/MapServer/10/query?where=1%3D1&outFields=*&outSR=4326&f=json")$features$attributes

Hamilton_ON_data <- Hamilton_ON_data_raw %>%
  rename(Geo_id = CTNAME, Cases = NUMBER_OF_CASES) %>%
  mutate(Tests = NA, Geo_id_type="CT", Update_time=Hamilton_ON_time, Deaths=NA, Positives = NA,
         Cases=ifelse(is.na(Cases), 0, Cases)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Hamilton, ON") %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Hamilton_ON_data_raw, "raw_data_21_02_2021/Hamilton_ON_06_03_2021_raw.rds")


######################### Durham, ON ############################

##  Manual

# https://maps.durham.ca/arcgis/rest/services/Health_Neighbourhoods/HN_EDI_Indicators/MapServer

######################### Ottawa, ON ############################

Ottawa_ON_time <- mdy("Jan 31, 2021")

Ottawa_ON_data_raw <- fread("https://www.arcgis.com/sharing/rest/content/items/7cf545f26fb14b3f972116241e073ada/data")

Ottawa_ON_data <- Ottawa_ON_data_raw %>%
  rename(Geo_id = `ONS ID`, Cases = `Cumulative Number of Cases Excluding Those Linked to Outbreaks in LTCH & RH`) %>%
  mutate(Tests = NA, Geo_id_type="Neighbourhood", Update_time=Ottawa_ON_time, Deaths=NA, Positives = NA) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Ottawa, ON") %>%
  mutate(Cases = ifelse(is.na(Cases), 0, Cases)) %>% 
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Ottawa_ON_data_raw, "raw_data_21_02_2021/Ottawa_ON_06_03_2021_raw.rds")


######################### Ohio ############################

Ohio_time <- mdy("March 06, 2021")

Ohio_data_raw <- fread("https://coronavirus.ohio.gov/static/dashboards/COVIDSummaryDataZIP.csv")

Ohio_data <- Ohio_data_raw %>%
  rename(Geo_id = `Zip Code`, Cases = `Case Count - Cumulative`) %>%
  mutate(Cases = gsub(",", "", Cases)) %>%
  mutate(Tests = NA, Geo_id_type="ZCTA", Update_time=Ohio_time, Deaths=NA, Positives = NA) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Source_geo="Ohio") %>%
  mutate(Cases = ifelse(is.na(Cases), 0, Cases)) %>% 
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)

saveRDS(Ohio_data_raw, "raw_data_21_02_2021/Ohio_06_03_2021_raw.rds")


######################### Manual Data ############################
# Kent County, MI - <20 suppressed - assigned 10
# Wayne County, MI - <16 suppressed
# Salt Lake County, UT
# Boston, MA
# Durham, ON
# Fulton, GA

Manual_data_raw <- read_sheet("https://docs.google.com/spreadsheets/d/1bk33v5C09NSfOisxFIa5mjB9tyocAbpJybztc5tSLtU/edit?usp=sharing", 3, col_types = "c")

Manual_data <- as.data.frame(Manual_data_raw) %>%
  mutate(Cases = ifelse(Cases == "Less than 20", 10,
                        ifelse(Cases == "<5", 0, Cases)),
         Deaths = ifelse(Deaths == "<16", 8, Deaths)) %>%
  mutate(Geo_id=as.character(Geo_id),
         Geo_id_type=as.character(Geo_id_type),
         Positives=as.integer(Positives),
         Cases=as.integer(Cases),
         Tests=as.integer(Tests),
         Deaths=as.integer(Deaths),
         Update_time=mdy(Update_time)) %>%
  select(Geo_id, Geo_id_type, Cases, Tests, Positives, Deaths, Update_time, Source_geo)
  

range_write("https://docs.google.com/spreadsheets/d/1bk33v5C09NSfOisxFIa5mjB9tyocAbpJybztc5tSLtU/edit?usp=sharing",
            data.frame(Read_time=rep(Sys.time(), nrow(Manual_data))), "Sheet5",
            "I1")
  
##### Merge All ####

all_data <- rbind(Jefferson_AL_data, AZ_data, 
                  Alameda_CA_data, Orange_CA_data, Sacramento_CA_data, SanDiego_CA_data, SanFrancisco_CA_data, SantaClara_CA_data,
                  Washington_DC_data, FL_data, IL_data, IN_data, 
                  Jefferson_KY_data, LA_data, MD_data, MN_data, KansasCity_MO_data,
                  StLouis_MO_data, Clark_NV_data, 
                  Erie_NY_data, NYC_NY_data, Monroe_NY_data, 
                  NC_data, OK_data, OR_data, Philadelphia_PA_data, PA_data, RI_data, Ohio_data,
                  Bexar_TX_data, Dallas_TX_data, Harris_TX_data, Tarrant_TX_data, Travis_TX_data,
                  VA_data, WI_data, King_WA_data, 
                  Toronto_ON_data, Montreal_QC_data, Ottawa_ON_data, Peel_ON_data, Hamilton_ON_data,
                  Manual_data)

write_csv(all_data, "data/merged_case_data_March_06_2021.csv")
