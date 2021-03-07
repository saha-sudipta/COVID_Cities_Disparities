#### Create CT-NB map for Toronto and Montreal

library(rgdal)
library(sf)


TO_nb <- st_read("data/Toronto Shapefile/Neighbourhoods/Neighbourhoods.shp")

Mont_nb <- st_read("data/Montreal Shapefile/limadmin-shp/LIMADMIN.shp")

CTs <- st_read("data/CDN Census Tracts/lct_000b16a_e/lct_000b16a_e.shp")
CDAs <- st_read("data/CDN Census Tracts/lda_000b16a_e/lda_000b16a_e.shp")

CTs <- CTs %>% filter(CMANAME == "Toronto") 
CDAs <- CDAs %>% filter(CMANAME == "Montréal")

CTs <- st_transform(CTs, st_crs(TO_nb))
CDAs <- st_transform(CDAs, st_crs(Mont_nb))

CTs <- mutate(CTs, CT_area = st_area(CTs))
CDAs <- mutate(CDAs, CDA_area = st_area(CDAs))


intersect_TO_pct <- st_intersection(CTs %>% filter(CMANAME == "Toronto"), TO_nb) %>% 
  mutate(intersect_area = st_area(.)) %>%   # create new column with shape area
  dplyr::select(CTUID, FIELD_7, intersect_area) %>%   # only select columns needed to merge
  st_drop_geometry() 


CTs_TO_nb <- merge(CTs, intersect_TO_pct, by = "CTUID")

CTs_TO_nb <- CTs_TO_nb %>% 
  mutate(coverage = as.numeric(intersect_area/CT_area)*100) %>%
  filter(coverage > 80) %>%
  select(CTUID, FIELD_7, coverage) %>%
  mutate(coverage=100, CTUID = as.character(CTUID)) %>%
  rename(Geo_id = FIELD_7, CensusID=CTUID)



intersect_Mont_pct <- st_intersection(CDAs %>% filter(CMANAME == "Montréal"), Mont_nb) %>% 
  mutate(intersect_area = st_area(.)) %>%   # create new column with shape area
  dplyr::select(DAUID, NOM, intersect_area) %>%   # only select columns needed to merge
  st_drop_geometry() 

CDAs_Mont_nb <- merge(CDAs, intersect_Mont_pct, by = "DAUID")

CDAs_Mont_nb <- CDAs_Mont_nb %>% 
  mutate(coverage = round(as.numeric(intersect_area/CDA_area)*100, digits=0), DAUID = as.character(DAUID)) %>%
  select(DAUID, NOM, coverage) %>%
  rename(Geo_id = NOM, CensusID=DAUID) 

write_csv(st_drop_geometry(CTs_TO_nb), "Toronto_neighborhoods_to_CT_map.csv")
write_csv(st_drop_geometry(CDAs_Mont_nb), "Montreal_arrondissement_to_CDA_map.csv")

######

OT_nb <- st_read("data/Ottawa Shapefile/Ottawa_nb.shp")
CDAs <- st_read("data/CDN Census Tracts/lda_000b16a_e/lda_000b16a_e.shp")
CDAs <- CDAs %>% filter(CMANAME == "Ottawa - Gatineau (Ontario part / partie de l'Ontario)") 
CDAs <- mutate(CDAs, CDA_area = st_area(CDAs))

OT_nb <- st_transform(OT_nb, st_crs(CDAs))

OT_nb <- st_buffer(OT_nb, 0)

intersect_OT_pct <- st_intersection(CDAs, OT_nb) %>% 
  mutate(intersect_area = st_area(.)) %>%   # create new column with shape area
  dplyr::select(DAUID, ONS_ID, intersect_area) %>%   # only select columns needed to merge
  st_drop_geometry() 

CDs_OT_nb <- merge(CDAs %>% st_drop_geometry(), intersect_OT_pct, by = "DAUID") 

CDs_OT_nb <- CDs_OT_nb %>% 
  mutate(coverage = as.numeric(intersect_area/CDA_area)*100) 


CDs_OT_nb2 <- CDs_OT_nb %>% group_by(DAUID) %>%
  arrange(DAUID, -intersect_area) %>%
  filter(row_number()==1) %>%
  filter(coverage > 50) %>%
  select(DAUID, ONS_ID, coverage) %>%
  ungroup() %>%
  mutate(DAUID = as.character(DAUID)) %>%
  rename(Geo_id = ONS_ID, CensusID=DAUID)

write_csv(CDs_OT_nb2, "Ottawa_NBs_to_CDA_map.csv")

####

Dur_nb <- st_read("data/Durham Shapefile/Health_Neighbourhoods.shp")
CDAs <- st_read("data/CDN Census Tracts/lda_000b16a_e/lda_000b16a_e.shp")
CDAs <- CDAs %>% filter(CMANAME == "Oshawa") 
CDAs <- mutate(CDAs, CDA_area = st_area(CDAs))


Dur_nb <- st_transform(Dur_nb, st_crs(CDAs))


intersect_Dur_pct <- st_intersection(CDAs, Dur_nb) %>% 
  mutate(intersect_area = st_area(.)) %>%   # create new column with shape area
  dplyr::select(DAUID, NEIGH_ID, intersect_area) %>%   # only select columns needed to merge
  st_drop_geometry() 

CDs_Dur_nb <- merge(CDAs %>% st_drop_geometry(), intersect_Dur_pct, by = "DAUID") 

CDs_Dur_nb <- CDs_Dur_nb %>% 
  mutate(coverage = as.numeric(intersect_area/CDA_area)*100) %>% 
  group_by(DAUID) %>%
  arrange(DAUID, -intersect_area) %>%
  filter(row_number()==1) %>%
  select(DAUID, NEIGH_ID) %>%
  ungroup() %>%
  mutate(DAUID = as.character(DAUID)) %>%
  rename(Geo_id = NEIGH_ID, CensusID=DAUID)


write_csv(CDs_Dur_nb, "Durham_NBs_to_CDA_map.csv")


