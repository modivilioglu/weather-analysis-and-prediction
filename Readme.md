## Synopsis

This is a Scala Project, based on Big Data Analysis using Apache Spark APIs as well as Spark MLLib basic APIs for prediction. The project aims 
- to retrieve information from historical weather data
- make some predictions based on the future data

## Motivation:

The information retrieval aims to find the 
- historical monthly temperature avarages
- year with lowest historical avarage temperatures in January as the coldest year
- year with highest historical avarage temperatures in July as the hottest year
- the day that hit the highest historical temperature
- the data that hit the lowest historical temperatue

The prediction part aims to
- predict the avarage temperature of the day, given the DAY_OF_YEAR feature
- test the prediction accuracy

The prediction logic is based on a very simple Linear Regression Model. 
The model is formed from the feature Vector with the variable representing the DAY_OF_YEAR and formed on the following basic logic:

The closer the days are to the first days of July, the higher the temperatures are.
The far the days go from the mid year (taken as the 183th DAY_OF_YEAR), the lower the temperatures go.
So the variable is the distance from the midyear in day-basis. 1st of January is 182 days away from the mid-year which is the farest day.
## Data:

Data has been fetched from 
ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/
The historical data for Brooklyn, NY has been selected and the related filename is 
USC00305796.dly

## Code Example: 

From the client's perspective the commands can be called as below:
```
	val analysis = new WeatherAnalysis
    val maxTemperatureEverDayData = analysis.getMaximumTemperature(validData)
    maxTemperatureEverDayData.getMaximumInDegreesCelcius should be >= 35

    val monthlyAvarages = analysis.getMonthAvarages(validData)
    val (_, januaryAvarageTemp) = monthlyAvarages(MONTH_JANUARY)
    val (_, julyAvarageTemp) = monthlyAvarages(MONTH_JULY)
    januaryAvarageTemp should be < julyAvarageTemp 
```

```
	val prediction = new WeatherPrediction
    ...
    model = prediction.getModel(trainData)
    val dayOfYear = 195
    val predictedValue = prediction.predictAvarageTemperature(model, dayOfYear)
    predictedValue should be < 30.0
    predictedValue should be > 20.0
```
## Installation:
```
sh> git clone https://github.com/modivilioglu/weather-analysis-and-prediction.git
sh> cd weather-analysis-and-prediction
sh> sbt test

## Tests

There are test cases both for analysis and prediction. They have been put to the same file for Spark Context initialization reasons, as so to keep things simple. You can run them using the following command
```sh
sh> sbt test
```

## Technical Notes
The package object serves as a utility package object, that encapsulates the implicits as well as utility functions and constants. Normally these could be
separated into different objects, however, the tradeoff would be the simplicity of the main workflow of the problem.

Cats library and monoids' combine operation has been applied on DailyWeather data.
|+| operation serves as a merge function between 2 DailyWeather objects
that have the same key.(reduceByKey). Different values for the same day
are found in seperate lines in the input file. So what is done is
create a few objects for the same day, and them merge them into one.
So simply:
```
val dailyWeather0 = DailyWeather(keys, ..., TMinValue, 0, ...)
val dailyWeather1 = DailyWeather(keys, ..., 0, TMaxValue, ...)
```
dailyWeather0 |+| dailyWeather1 operation merges the 2 and forms
DailyWeather(keys, ..., TminValue, TMaxValue, 0, ...)

## Contributors

Mehmet Oguz Divilioglu, Email: mo.divilioglu@gmail.com

