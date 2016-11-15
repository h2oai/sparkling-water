#RSparkling Demo 
#This demo demonstrates the following:
  #1.How to set up your R environment for RSparkling
  #2.Sparklyr/Spark for data munging
  #3.H2O for model building
  #4.Bunding the previous two together in a Shiny Application

#Problem Statement:
#Given that your flight was delayed by 15 minutes or more, what is the likelihood your airline carrier will make up time in route? 
#Some of the most signficant factors for making up time are flight distance and airline carrier.

#Dataset:
#Provided by the nycflights13 package.
#This package contains information about all flights that departed from NYC (e.g. EWR, JFK and LGA) in 2013: 336,776 flights in total. 
#To help understand what causes delays, it also includes a number of other useful datasets:
#weather: hourly meterological data for each airport
#planes: construction information about each plane
#airports: airport names and locations
#airlines: translation between two letter carrier codes and names
##############################################################################################################
#Set up environment for RSparkling

#Install H2O if not already installed
#The following two commands remove any previously installed H2O packages for R.
#if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
#if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

# Next, we download packages that H2O depends on.
#pkgs <- c("methods","statmod","stats","graphics","RCurl","jsonlite","tools","utils")
#for (pkg in pkgs) {
#  if (! (pkg %in% rownames(installed.packages()))) { install.packages(pkg) }
#}

# Now we download, install and initialize the H2O package for R.
#install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-turing/10/R")

#Install sparklyr if not already installed
#install.packages("sparklyr")

#The latest stable version of rsparkling can be installed as follows:
#library(devtools)
#devtools::install_github("h2oai/sparkling-water", ref = "master", subdir = "/r/rsparkling")

##############################################################################################################
#If Spark needs to be installed, that can be done using the following sparklyr command:
library(sparklyr)
spark_install(version = "2.0.0")

#The call to library(rsparkling) will make the H2O functions available on the R search path and will also ensure that the dependencies 
#required by the Sparkling Water package are included when we connect to Spark. Also, we need to set up a valid Sparkling Water version as a global variable. 
#This will be sent to sparklyr to set up a connection to Sparkling Water.
options(rsparkling.sparklingwater.version = "2.0.0")
library(rsparkling)
sc <- spark_connect(master = "local")

#Let's inspect the H2OContext for our Spark connection:
h2o_context(sc,FALSE)

#We can also view the H2O Flow web UI:
h2o_flow(sc,FALSE)
##############################################################################################################
#Start model building process

# Attach packages
library(dplyr)
library(ggplot2)
library(DT)
library(leaflet)
library(geosphere)
library(readr)
library(sparklyr)
library(h2o)
library(shinythemes)

#Upload data
flights_tbl <- copy_to(sc, nycflights13::flights, "flights")
airports_tbl <- copy_to(sc, nycflights13::airports, "airports")
airlines_tbl <- copy_to(sc, nycflights13::airlines, "airlines")

#Prepare data for modelling
model_tbl <- flights_tbl %>%
  filter(!is.na(arr_delay) & !is.na(dep_delay) & !is.na(distance)) %>%
  filter(dep_delay > 15 & dep_delay < 240) %>%
  filter(arr_delay > -60 & arr_delay < 360) %>%
  left_join(airlines_tbl, by = c("carrier" = "carrier")) %>%
  mutate(gain = dep_delay - arr_delay) %>%
  select(origin, dest, carrier, airline = name, distance, dep_delay, arr_delay, gain)

#Data has been processed using Sparklyr. Now we go into our H2O session
#Convert munged spark data frame to an h2o frame
df_hex <- as_h2o_frame(sc,model_tbl,name="model_hex",FALSE)

#Take a look at dimensions, head, tail, and summary of h2o frame
dim(df_hex)
head(df_hex)
tail(df_hex)
summary(df_hex,exact_quantiles=TRUE)

#Pick a response for the supervised problem
response <- "gain"

#Use all other columns (except for response) as predictors
predictors <- setdiff(names(df_hex), c(response)) 
print(predictors)

#Set up train, validation, and test set
splits <- h2o.splitFrame(
  data = df_hex, 
  ratios = c(0.7,0.2),   ## only need to specify 2 fractions, the 3rd is implied
  destination_frames = c("train.hex","valid.hex", "test.hex"), seed = 1234
)
train <- splits[[1]]
valid <- splits[[2]]
test <- splits[[3]]

#Run H2O's GLM 
#We only provide the required parameters, everything else is default
glm <- h2o.glm(x = predictors, y = response, training_frame = train)

#Show a detailed model summary
glm

#Get the rmse on the validation set
h2o.rmse(h2o.performance(glm, newdata = valid)) 

#The second model is another default glm, but trained on 80% of the data (here, we combine the training and validation splits to get more training data),
#and cross-validated using 4 folds. Note that cross-validation takes longer and is not usually done for really large datasets.
#Note:h2o.rbind makes a copy here, so it's better to use splitFrame with `ratios = c(0.8)` instead
glm <- h2o.glm(x = predictors, y = response, training_frame = h2o.rbind(train, valid), nfolds = 4, seed = 0xDECAF)

#Show a detailed summary of the cross validation metrics
#This gives you an idea of the variance between the folds
glm@model$cross_validation_metrics_summary

#Get the cross-validated RMSE by scoring the combined holdout predictions.
#(Instead of taking the average of the metrics across the folds
h2o.rmse(h2o.performance(glm, xval = TRUE))

#Make prediction on test set
preds <- h2o.predict(glm, test)
preds <- h2o.cbind(test,preds)

#Convert back to Spark to utilize dplyr backend for aggregations that will later be used for visuals (Shiny app)
pred_tbl <- as_spark_dataframe(sc,preds,strict_version_check=FALSE)

#Create scored look up data for Shiny app 
lookup_tbl <- pred_tbl %>%
  group_by(origin, dest, carrier, airline) %>%
  summarize(
    flights = n(),
    distance = mean(distance),
    avg_dep_delay = mean(dep_delay),
    avg_arr_delay = mean(arr_delay),
    avg_gain = mean(gain),
    pred_gain = mean(predict)
  )

#Cache the look up table
sdf_register(lookup_tbl, "lookup")
tbl_cache(sc, "lookup")

#Find distinct airport codes
carrier_origin <- c("JFK", "LGA", "EWR")
carrier_dest <- c("BOS", "DCA", "DEN", "HNL", "LAX", "SEA", "SFO", "STL")

#Shiny UI
ui <- fluidPage(theme = shinytheme("yeti"),
  
  # Set display mode to bottom
  tags$script(' var setInitialCodePosition = function() 
              { setCodePosition(false, false); }; '),
  
  # Title
  titlePanel("NYCFlights13: Time Gained in Flight"),
  
  # Create sidebar 
  sidebarLayout(
    sidebarPanel(
      radioButtons("origin", "Flight origin:",
                   carrier_origin, selected = "JFK"),
      br(),
      
      radioButtons("dest", "Flight destination:",
                   carrier_dest, selected = "SFO")
      
    ),
    
    # Show a tabset that includes a plot, model, and table view
    mainPanel(
      tabsetPanel(type = "tabs", 
                  tabPanel("Plot", plotOutput("plot")),
                  tabPanel("Variable Importance", plotOutput("plotvarimp")),
                  tabPanel("Data", dataTableOutput("datatable")),
                  tabPanel("Map", leafletOutput("map"))
      )
    )
  )
  )

#Shiny server function
server <- function(input, output) {
  
  #Identify origin lat and log
  origin <- reactive({
    req(input$origin)
    filter(nycflights13::airports, faa == input$origin)
  })
  
  #Identify destination lat and log
  dest <- reactive({
    req(input$dest)
    filter(nycflights13::airports, faa == input$dest)
  })
  
  #Create plot data
  plot_data <- reactive({
    req(input$origin, input$dest)
    lookup_tbl %>%
      filter(origin==input$origin & dest==input$dest) %>%
      ungroup() %>%
      select(airline, flights, distance, avg_gain, pred_gain) %>%
      collect
  })
  
  #Plot observed versus predicted time gain for carriers and route
  output$plot <- renderPlot({
    ggplot(plot_data(), aes(factor(airline), pred_gain)) + 
      geom_bar(stat = "identity", fill = '#2780E3') +
      geom_point(aes(factor(airline), avg_gain)) +
      coord_flip() +
      labs(x = "", y = "Time gained in flight (minutes)") +
      labs(title = "Observed gain (point) vs Predicted gain (bar)")
  })
  
  #Output the route map  
  output$map <- renderLeaflet({
    gcIntermediate(
      select(origin(), lon, lat),
      select(dest(), lon, lat),
      n=100, addStartEnd=TRUE, sp=TRUE
    ) %>%
      leaflet() %>%
      addProviderTiles("CartoDB.Positron") %>%
      addPolylines()
  })  
  
  #Print table of observed and predicted gains by airline
  output$datatable <- renderDataTable(
    datatable(plot_data()) %>%
      formatRound(c("flights", "distance"), 0) %>%
      formatRound(c("avg_gain", "pred_gain"), 1)
  )
  
  output$plotvarimp <- renderPlot({
    h2o.varimp_plot(glm)
  })
  
}

#Run Shiny Application
shinyApp(ui = ui, server = server)

#Disconnect from Spark and end session
#spark_disconnect(sc)
