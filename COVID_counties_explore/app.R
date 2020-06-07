library(shiny)
library(dplyr)
library(readr)
library(ggplot2)
library(tigris)
library(sp)
library(leaflet)
library(spdep)

#Read in data
# counties_covid <- read_csv("data/covid_confirmed_usafacts.csv") %>% 
#   filter(countyFIPS > 1)
# 
# counties_pop <- read_csv("data/covid_county_population_usafacts.csv") %>% 
#   select(countyFIPS, State, population) 
# 
# counties_urbanrural <- read_csv("data/NCHSURCodes2013.csv") %>% 
#   select(`FIPS code`, `CBSA title`, `2013 code`) %>%
#   rename(countyFIPS = `FIPS code`, CBSA_title=`CBSA title`, urban_rural_code=`2013 code`)
# 
# counties_msa <- read_csv("data/County_MSA_CBSA.csv") %>% 
#   mutate(countyFIPS=as.numeric(paste0(`FIPS State Code`, `FIPS County Code`))) %>%
#   rename(MMSA=`Metropolitan/Micropolitan Statistical Area`,
#          central=`Central/Outlying County`) %>%
#   select(countyFIPS, MMSA, central)
# 
# 
# counties_shp <- counties(cb=TRUE, resolution="20m")
# 
# 
# counties_shp@data <- counties_shp@data %>%
#   mutate(GEOID=as.integer(GEOID)) %>%
#   left_join(counties_covid, by=c("GEOID"="countyFIPS")) %>%
#   left_join(select(counties_pop, countyFIPS, population), by=c("GEOID"="countyFIPS")) %>%
#   left_join(select(counties_msa, countyFIPS, MMSA, central), by=c("GEOID"="countyFIPS")) %>%
#   left_join(select(counties_urbanrural, countyFIPS, urban_rural_code), by=c("GEOID"="countyFIPS")) %>%
#   mutate_at(.funs = list(rate = ~(./population)*100000), .vars = vars(`1/22/20`:`5/22/20`))

#saveRDS(counties_shp, "./COVID_counties_explore/app_data/counties_shp.rds")

counties_shp <- readRDS("app_data/counties_shp.rds")
states_shp <- readRDS("app_data/states.rds")
counties_adj <- poly2nb(counties_shp, queen=TRUE, row.names = as.character(counties_shp$GEOID))

dates <- colnames(counties_shp@data)[13:134]
  
# Define UI for application that draws a histogram
ui <- fluidPage(
   
   # Application title
   titlePanel("Explore COVID-19 at County Level"),
   
   # Sidebar with a slider input for number of bins 
   sidebarLayout(
      sidebarPanel(width = 4,
        checkboxInput("states", "Show State boundaries"),
        radioButtons("value", "Choropleth value:", 
                     choices = c("No. of Cases" = "cases", "Cases / 100,000" = "rate")),
        p("Select date for data:"),
        selectInput("date",
                    "Date:",
                    dates, 
                    selected = "5/1/20", multiple = FALSE),
                    # timeFormat="%m/%d/%y"),
        p("Select which counties to highlight:"),
        selectInput("urban", "Urban-Rural Classification", c("1","2","3","4","5","6"), 
                    selected = "1", multiple = TRUE),
        sliderInput("cases",
                     "Number of Cases:",
                     min = 0,
                     max = 70000,
                     value = c(0,70000), step = 50),
        sliderInput("case_rate",
                     "Cases per 100,000:",
                     min = 0,
                     max = 15000,
                     value = c(0,15000), step = 50),
        checkboxInput("nbs", "Show neighbours of counties filtered above")
      
      ),
      
      # Show a plot of the generated distribution
      mainPanel(width=8,
        leafletOutput("map"),
        textOutput("text")
      )
   )
)

# Define server logic required to draw a histogram
server <- function(input, output) {
  
  # Filter shapefile
  counties_shp_date <- reactive({
    counties_shp_date1 <- counties_shp
    counties_shp_date1@data <- counties_shp_date1@data[,c("GEOID", "County Name", "State", "population", 
                                                        "MMSA", "urban_rural_code", "central",
                                                        as.character(input$date),
                                                        paste0(as.character(input$date), "_rate"))]
    colnames(counties_shp_date1@data)[8] <- "cases"
    colnames(counties_shp_date1@data)[9] <- "rate"
    counties_shp_date1
  })
  
  filt_counties_shp <- reactive({
    counties_shp_date2 <- counties_shp_date()
    counties_shp_date2@data$selected <-
      ifelse(counties_shp_date2@data$urban_rural_code %in% input$urban & 
                        counties_shp_date2@data[,8] >= input$cases[1] &
                        counties_shp_date2@data[,8] <= input$cases[2] &
                        counties_shp_date2@data[,9] >= input$case_rate[1] &
                        counties_shp_date2@data[,9] <= input$case_rate[2],
             1, 0)
    counties_shp_date2
    
  })
  
  counties_shp_nbs <- reactive({
    nb_county_geoids <- vector()
    for (i in 1:nrow(filt_counties_shp()@data)){
      if (filt_counties_shp()@data$selected[i]==1){
        nb_county_geoids <- c(nb_county_geoids, filt_counties_shp()@data$GEOID[counties_adj[[i]]])
      }
    }
    nb_county_geoids <- unique(nb_county_geoids)
    filt_counties_shp()[filt_counties_shp()@data$GEOID %in% nb_county_geoids,] 
  })
  
  counties_shp_selected <- reactive({
    filt_counties_shp()[filt_counties_shp()@data$selected==1,] 
  })
  
  labels <- reactive({
    sprintf("<strong>%s , %s</strong> <br/> NCHSUR Code: %g <br/> %g Cases<br/> %g per 100k",
            counties_shp_date()$`County Name`, counties_shp_date()$State, counties_shp_date()$urban_rural_code,
            counties_shp_date()$cases, counties_shp_date()$rate
  ) %>% lapply(htmltools::HTML)
  })
  
  labels_selected <- reactive({
    sprintf("<strong>%s , %s</strong> <br/> NCHSUR Code: %g <br/> %g Cases<br/> %g per 100k",
            counties_shp_selected()$`County Name`, counties_shp_selected()$State, counties_shp_selected()$urban_rural_code,
            counties_shp_selected()$cases, counties_shp_selected()$rate
    ) %>% lapply(htmltools::HTML)
  })
  
  labels_nb <- reactive({
    sprintf("<strong>%s , %s</strong> <br/> NCHSUR Code: %g <br/> %g Cases<br/> %g per 100k",
            counties_shp_nbs()$`County Name`, counties_shp_nbs()$State, counties_shp_nbs()$urban_rural_code,
            counties_shp_nbs()$cases, counties_shp_nbs()$rate
    ) %>% lapply(htmltools::HTML)
  })
  
  
  # This reactive expression represents the palette function,
  # which changes as the user makes selections in UI.
  # colorpal <- reactive({
  #   if (input$value == "cases"){
  #     colorNumeric("inferno",  filt_counties_shp()@data$cases)
  #   }
  #   if (input$value == "rate"){
  #     colorNumeric("inferno",  filt_counties_shp()@data$rate)
  #   }
  # })
  
  # col_index <- reactive({
  #   if (input$value == "cases"){
  #     8
  #   }
  #   if (input$value == "rate"){
  #     9
  #   }
  # })
  # 
  
  output$map <- renderLeaflet({
    # pal <- colorNumeric("inferno",  log(filt_counties_shp()@data$cases+1))
    leaflet(counties_shp) %>% setView(lng = -98.55, lat = 39.81, zoom = 4) %>% 
      addTiles() 
      # addPolygons(color = "#8D8D8D", weight = 0.5, 
      #               fillColor = ~pal(log(cases+1)),
      #               fillOpacity = 0.8,
      #               label = labels(),
      #               labelOptions = labelOptions(
      #                 style = list("font-weight" = "normal", padding = "3px 8px"),
      #                 textsize = "15px",
      #                 direction = "auto"), group = "choropleth") 
  })
  
  observe({
    proxy1 <- leafletProxy("map", data=states_shp) %>% clearGroup("states")
    if (input$states){
      proxy1 %>% addPolylines(color = "black", weight = 1, group = "states", smoothFactor = 0.5)
    }

  observe({
    proxy2 <- leafletProxy("map", data=counties_shp_date()) %>% clearGroup("choropleth")
    if (input$value == "rate"){
      pal <- colorNumeric("inferno",  log(counties_shp_date()@data$rate+1))
      proxy2 %>% 
        addPolygons(color = "#8D8D8D", weight = 0.5,
                  fillColor = ~pal(log(rate+1)),
                  fillOpacity = 0.7,
                  label = labels(),
                  labelOptions = labelOptions(
                    style = list("font-weight" = "normal", padding = "3px 8px"),
                    textsize = "15px",
                    direction = "auto"), group = "choropleth", smoothFactor = 0.5)
    }
    if (input$value == "cases"){
      pal <- colorNumeric("inferno",  log(counties_shp_date()@data$cases+1))
      proxy2 %>% 
        addPolygons(color = "#8D8D8D", weight = 0.5,
                  fillColor = ~pal(log(cases+1)),
                  fillOpacity = 0.7,
                  label = labels(),
                  labelOptions = labelOptions(
                    style = list("font-weight" = "normal", padding = "3px 8px"),
                    textsize = "15px",
                    direction = "auto"),  group = "choropleth", smoothFactor = 0.5)
    }
  })

# 
# 
#   # Use a separate observer to recreate the legend as needed.
#   observe({
#     proxy <- leafletProxy("map", data=filt_counties_shp())
#     
#     # Remove any existing legend, and only if the legend is
#     # enabled, create a new one.
#     proxy %>% clearControls()
#     if (input$value == "rate"){
#       pal <- colorNumeric("inferno",  log(filt_counties_shp()@data$cases+1))
#       proxy %>% addLegend(position = "bottomright",
#                           pal = pal, values = ~log(rate+1)
#       )
#     }
#     if (input$value == "cases"){
#       pal <- colorNumeric("inferno",  log(filt_counties_shp()@data$cases+1))
#       proxy %>% addLegend(position = "bottomright",
#                           pal = pal, values = ~log(cases+1)
#       )
# 
#     }
#   })

  observe({
    proxy <- leafletProxy("map", data=counties_shp_selected()) %>% clearGroup("selected")
    proxy %>%
      addPolygons(color = "white", weight = 2, opacity=1, fillOpacity = 0,
                  label = labels_selected(),
                  labelOptions = labelOptions(
                    style = list("font-weight" = "normal", padding = "3px 8px"),
                    textsize = "15px",
                    direction = "auto"), smoothFactor = 0.5, group="selected")
  })
  observe({
    proxy <- leafletProxy("map", data=counties_shp_nbs()) %>% clearGroup("neighbours")
    if (input$nbs){
      proxy %>% addPolygons(color = "white", weight = 2, group = "neighbours", 
                            dashArray = "3", opacity=1, fillOpacity = 0,
                            label = labels_nb(),
                            labelOptions = labelOptions(
                              style = list("font-weight" = "normal", padding = "3px 8px"),
                              textsize = "15px",
                              direction = "auto"), smoothFactor = 0.5)
    }
  })


  })
  text <- reactive({
    counties_select_nb <- counties_shp_date()[counties_shp_date()$GEOID %in% counties_shp_selected()$GEOID |
                                                counties_shp_date()$GEOID %in% counties_shp_nbs()$GEOID,]
    
    paste0("The ", nrow(counties_shp_selected()@data), " counties had ", 
           sum(counties_shp_selected()@data$cases, na.rm=TRUE), "(", 
           round(sum(counties_shp_selected()@data$cases, na.rm = TRUE)/sum(filt_counties_shp()@data$cases, na.rm=TRUE),2)*100, "%)", " cases on ",
           input$date, " and consist of ",
           round(sum(counties_shp_selected()@data$population, na.rm=TRUE)/sum(filt_counties_shp()@data$population, na.rm=TRUE),2)*100, 
           "% of the national population. If neighbours are included, there are ",
           nrow(counties_select_nb@data), " counties, with ",
           sum(counties_select_nb@data$cases, na.rm=TRUE), "(", 
           round((sum(counties_select_nb@data$cases, na.rm = TRUE))/
                   sum(filt_counties_shp()@data$cases, na.rm=TRUE),1)*100, "%)", " cases on ",
           input$date, " cases and consist of ",
           round((sum(counties_select_nb@data$population, na.rm = TRUE))
                 /sum(filt_counties_shp()@data$population, na.rm=TRUE),1)*100, "% of the national population.")
  })
    
  output$text <- renderText({
    text()
  })
 }


# Run the application 
shinyApp(ui = ui, server = server)

