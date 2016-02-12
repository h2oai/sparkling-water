import h2o
from datetime import datetime
from pytz import timezone
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import os
from pysparkling.context import H2OContext

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


# create Crime representation
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

    crime = {}
    crime["Year"] = dt.year
    crime["Month"] = dt.month
    crime["Day"] = dt.day
    crime["WeekNum"] = dt.isocalendar()[1]
    crime["HourOfDay"] = dt.hour
    crime["Weekend"] = 1 if dt.weekday() == 5 or dt.weekday() == 6 else 0
    crime["Season"] = get_season(dt.month)
    crime["WeekDay"] = dt.strftime('%a')  #gets the day of week in short format - Mon, Tue ...
    crime["IUCR"] = iucr
    crime["Primary_Type"] = primaryType
    crime["Location_Description"] = locationDescr
    crime["Domestic"] = True if domestic else False
    crime["Beat"] = beat
    crime["District"] = district
    crime["Ward"] = ward
    crime["Community_Area"] = communityArea
    crime["FBI_Code"] = fbiCode
    crime["minTemp"] = minTemp
    crime["maxTemp"] = maxTemp
    crime["meanTemp"] = meanTemp
    return crime

conf = SparkConf().setAppName("ChicagoCrimeTest").setIfMissing("spark.master", os.getenv("spark.master", "local[*]"))
sc = SparkContext(conf=conf)
# SQL support
sqlContext = SQLContext(sc)
# Start H2O services
h2oContext = H2OContext(sc).start()

f_weather = h2o.import_file("hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoAllWeather.csv")
f_census = h2o.import_file("hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoCensus.csv")
f_crimes = h2o.import_file("hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoCrimes.csv")

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

# Split final data table
ratios = [0.8]
frs = crimeWithWeatherHF.split_frame(ratios)
train = frs[0]
test = frs[1]

gbm_model = h2o.gbm(x=train.drop("Arrest"),
                    y=train["Arrest"],
                    validation_x=test.drop("Arrest"),
                    validation_y=test["Arrest"],
                    ntrees=50,
                    max_depth=3,
                    learn_rate=0.1,
                    distribution="bernoulli")

dl_model = h2o.deeplearning(x=train.drop("Arrest"),
                            y=train["Arrest"],
                            validation_x=test.drop("Arrest"),
                            validation_y=test["Arrest"])

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
    #FIXME: for glmModel table predictTable does not contain column "true"
    probOfArrest = predictTable["true"][0,0]
    return probOfArrest

for crime in crime_examples:
    arrestProbGLM = 100*score_event(crime, gbm_model, df_census)
    arrestProbGBM = 100*score_event(crime, dl_model, df_census)

    print("""
       |Crime: """+str(crime)+"""
       |  Probability of arrest best on DeepLearning: """+str(arrestProbGLM)+"""
       |  Probability of arrest best on GBM: """+str(arrestProbGBM)+"""
        """)

# Need to shutdown Spark at the end
#sc.stop()
#h2o.shutdown()

# Kill process directly
import os
os._exit(0)

