import h2o
from datetime import datetime
from pytz import timezone
from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql import SQLContext, Row
import os
from pysparkling import *

# Refine date column
def refine_date_col(data, col, pattern):
    data[col] = data[col].as_date(pattern)
    data["Day"] = data[col].day()
    data["Month"] = data[col].month()
    data["Year"] = data[col].year()
    data["WeekNum"] = data[col].week()
    data["WeekDay"] = data[col].dayOfWeek()
    data["HourOfDay"] = data[col].hour()

    data.describe()  # HACK: Force evaluation before ifelse and cut. See PUBDEV-1425.

    # Create weekend and season cols
    # Spring = Mar, Apr, May. Summer = Jun, Jul, Aug. Autumn = Sep, Oct. Winter = Nov, Dec, Jan, Feb.
    # data["Weekend"]   = [1 if x in ("Sun", "Sat") else 0 for x in data["WeekDay"]]
    data["Weekend"] = ((data["WeekDay"] == "Sun") | (data["WeekDay"] == "Sat"))
    data["Season"] = data["Month"].cut([0, 2, 5, 7, 10, 12], ["Winter", "Spring", "Summer", "Autumn", "Winter"])


def get_season(dt):
    if (dt >= 3 and dt <= 5):
        return "Spring"
    elif (dt >= 6 and dt <= 8):
        return "Summer"
    elif (dt >= 9 and dt <= 10):
        return "Autumn"
    else:
        return "Winter"


# Create crime class which is used as a data holder on which prediction is done
def crime(date,
          iucr,
          primaryType,
          locationDescr,
          domestic,
          beat,
          district,
          ward,
          communityArea,
          fbiCode,
          minTemp = 77777,
          maxTemp = 77777,
          meanTemp = 77777,
          datePattern = "%d/%m/%Y %I:%M:%S %p",
          dateTimeZone = "Etc/UTC"):

    dt = datetime.strptime("02/08/2015 11:43:58 PM",'%d/%m/%Y %I:%M:%S %p')
    dt.replace(tzinfo=timezone("Etc/UTC"))

    crime = Row(
            Year = dt.year,
            Month = dt.month,
            Day = dt.day,
            WeekNum = dt.isocalendar()[1],
            HourOfDay = dt.hour,
            Weekend = 1 if dt.weekday() == 5 or dt.weekday() == 6 else 0,
            Season = get_season(dt.month),
            WeekDay = dt.strftime('%a'),  #gets the day of week in short format - Mon, Tue ...
            IUCR = iucr,
            Primary_Type = primaryType,
            Location_Description = locationDescr,
            Domestic = True if domestic else False,
            Beat = beat,
            District = district,
            Ward = ward,
            Community_Area = communityArea,
            FBI_Code = fbiCode,
            minTemp = minTemp,
            maxTemp = maxTemp,
            meanTemp = meanTemp
    )
    return crime

# This is just helper function returning path to data-files
def _locate(example_name):
    return "../examples/smalldata/" + example_name

conf = SparkConf().setAppName("ChicagoCrimeTest").setIfMissing("spark.master", os.getenv("spark.master", "local[*]"))
sc = SparkContext(conf=conf)
# SQL support
sqlContext = SQLContext(sc)
# Start H2O services
h2oContext = H2OContext(sc).start()
# Define file names
chicagoAllWeather = "chicagoAllWeather.csv"
chicagoCensus = "chicagoCensus.csv"
chicagoCrimes10k = "chicagoCrimes10k.csv"

# Add files to Spark Cluster
sc.addFile(_locate(chicagoAllWeather))
sc.addFile(_locate(chicagoCensus))
sc.addFile(_locate(chicagoCrimes10k))

# Since we have already loaded files into spark, we have to use h2o.upload_file instead of h2o.import_file since
# h2o.import_file expects cluster-relative path (ie. the file on this path can be accessed from all the machines on the cluster)
# but SparkFiles.get(..) already give us relative path to the file on a current node which h2o.upload_file can handle ( it uploads file
# located on current node and distributes it to the H2O cluster)
f_weather = h2o.upload_file(SparkFiles.get(chicagoAllWeather))
f_census = h2o.upload_file(SparkFiles.get(chicagoCensus))
f_crimes = h2o.upload_file(SparkFiles.get(chicagoCrimes10k))


# Transform weather table
# Remove 1st column (date)
f_weather = f_weather[1:]

# Transform census table
# Remove all spaces from column names (causing problems in Spark SQL)
col_names = map(lambda s: s.strip().replace(' ', '_').replace('+', '_'), f_census.col_names)

# Update column names in the table
# f_weather.names = col_names
f_census.names = col_names


# Transform crimes table
# Drop useless columns
f_crimes = f_crimes[2:]

# Set time zone to UTC for date manipulation
h2o.set_timezone("Etc/UTC")

# Replace ' ' by '_' in column names
col_names = map(lambda s: s.replace(' ', '_'), f_crimes.col_names)
f_crimes.names = col_names
refine_date_col(f_crimes, "Date", "%m/%d/%Y %I:%M:%S %p")
f_crimes = f_crimes.drop("Date")

# Expose H2O frames as Spark DataFrame

df_weather = h2oContext.as_spark_frame(f_weather)
df_census = h2oContext.as_spark_frame(f_census)
df_crimes = h2oContext.as_spark_frame(f_crimes)

# Use Spark SQL to join datasets

# Register DataFrames as tables in SQL context
sqlContext.registerDataFrameAsTable(df_weather, "chicagoWeather")
sqlContext.registerDataFrameAsTable(df_census, "chicagoCensus")
sqlContext.registerDataFrameAsTable(df_crimes, "chicagoCrime")


crimeWithWeather = sqlContext.sql("""SELECT
a.Year, a.Month, a.Day, a.WeekNum, a.HourOfDay, a.Weekend, a.Season, a.WeekDay,
a.IUCR, a.Primary_Type, a.Location_Description, a.Community_Area, a.District,
a.Arrest, a.Domestic, a.Beat, a.Ward, a.FBI_Code,
b.minTemp, b.maxTemp, b.meanTemp,
c.PERCENT_AGED_UNDER_18_OR_OVER_64, c.PER_CAPITA_INCOME, c.HARDSHIP_INDEX,
c.PERCENT_OF_HOUSING_CROWDED, c.PERCENT_HOUSEHOLDS_BELOW_POVERTY,
c.PERCENT_AGED_16__UNEMPLOYED, c.PERCENT_AGED_25__WITHOUT_HIGH_SCHOOL_DIPLOMA
FROM chicagoCrime a
JOIN chicagoWeather b
ON a.Year = b.year AND a.Month = b.month AND a.Day = b.day
JOIN chicagoCensus c
ON a.Community_Area = c.Community_Area_Number""")

# Publish Spark DataFrame as H2OFrame with given name
crimeWithWeatherHF = h2oContext.as_h2o_frame(crimeWithWeather, "crimeWithWeatherTable")

# Transform selected String columns to categoricals
crimeWithWeatherHF["Arrest"] = crimeWithWeatherHF["Arrest"].asfactor()
crimeWithWeatherHF["Season"] = crimeWithWeatherHF["Season"].asfactor()
crimeWithWeatherHF["WeekDay"] = crimeWithWeatherHF["WeekDay"].asfactor()
crimeWithWeatherHF["Primary_Type"] = crimeWithWeatherHF["Primary_Type"].asfactor()
crimeWithWeatherHF["Location_Description"] = crimeWithWeatherHF["Location_Description"].asfactor()
crimeWithWeatherHF["Domestic"] = crimeWithWeatherHF["Domestic"].asfactor()

# Split frame into two - we use one as the training frame and the second one as the validation frame
splits = crimeWithWeatherHF.split_frame(ratios=[0.8])
train = splits[0]
test = splits[1]

# Prepare column names
predictor_columns = train.drop("Arrest").col_names
response_column = "Arrest"

# Create and train GBM model
from h2o.estimators.gbm import H2OGradientBoostingEstimator

# Prepare model based on the given set of parameters
gbm_model = H2OGradientBoostingEstimator(  ntrees       = 50,
                                           max_depth    = 3,
                                           learn_rate   = 0.1,
                                           distribution = "bernoulli"
                                           )

# Train the model
gbm_model.train(x            = predictor_columns,
                y                = response_column,
                training_frame   = train,
                validation_frame = test
                )

# Create and train deeplearning model
from h2o.estimators.deeplearning import H2ODeepLearningEstimator

# Prepare model based on the given set of parameters
dl_model = H2ODeepLearningEstimator()

# Train the model
dl_model.train(x            = predictor_columns,
               y                = response_column,
               training_frame   = train,
               validation_frame = test
               )

# Create crimes examples
crime_examples = [
    crime("02/08/2015 11:43:58 PM", 1811, "NARCOTICS", "STREET",False, 422, 4, 7, 46, 18),
    crime("02/08/2015 11:00:39 PM", 1150, "DECEPTIVE PRACTICE", "RESIDENCE",False, 923, 9, 14, 63, 11)]

# For given crime and model returns probability of crime.
def score_event(crime, model, censusTable):
    srdd = sqlContext.createDataFrame([crime])
    # Join table with census data
    df_row = censusTable.join(srdd).where("Community_Area = Community_Area_Number")
    row = h2oContext.as_h2o_frame(df_row)
    row["Season"] = row["Season"].asfactor()
    row["WeekDay"] = row["WeekDay"].asfactor()
    row["Primary_Type"] = row["Primary_Type"].asfactor()
    row["Location_Description"] = row["Location_Description"].asfactor()
    row["Domestic"] = row["Domestic"].asfactor()

    predictTable = model.predict(row)
    probOfArrest = predictTable["true"][0,0]
    return probOfArrest

for crime in crime_examples:
    arrestProbGBM = 100*score_event(crime, gbm_model, df_census)
    arrestProbDLM = 100*score_event(crime, dl_model, df_census)

    print("""
       |Crime: """+str(crime)+"""
       |  Probability of arrest best on DeepLearning: """+str(arrestProbDLM)+"""
       |  Probability of arrest best on GBM: """+str(arrestProbGBM)+"""
        """)

# stop H2O and Spark services
h2o.shutdown(prompt=False)
sc.stop()

