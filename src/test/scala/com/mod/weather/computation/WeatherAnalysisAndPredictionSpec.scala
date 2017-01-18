package com.mod.weather.computation

/**
  * Created by mehmetoguzdivilioglu on 18/01/2017.
  */

import com.mod.weather.model.DailyWeather
import com.mod.weather.weather._
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class WeatherAnalysisAndPredictionSpec extends FlatSpec with BeforeAndAfter with Matchers {
  private val master = "local[2]"
  private val appName = "Weather"

  private val filePath = "src/main/resources/USC00305796.dly"

  private var sc: SparkContext = _
  private var validData: RDD[((Int, Int, Int), DailyWeather)] = _
  private var analysis: WeatherAnalysis = _
  private var prediction: WeatherPrediction = _
  private var model: LinearRegressionModel = _
  private var test: RDD[LabeledPoint] = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
    analysis = new WeatherAnalysis
    validData = analysis.getValidData(sc, filePath)
    prediction = new WeatherPrediction
    val preparedVectorData = prediction.getPreparedData(validData)
    val (trainData, testData) = prediction.getTrainingAndTestData(preparedVectorData)
    test = testData
    model = prediction.getModel(trainData)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
  "The coldest year result " should " give reasonable temperature for January avarage " in {

    val (yearCold, temperatureCold) = analysis.getColdestYear(validData)
    temperatureCold should be <= 5
  }
  "The coldest year result " should " give reasonable temperature for July avarage " in {

    val (yearCold, temperatureHot) = analysis.getHottestYear(validData)
    temperatureHot should be >= 20
  }
  "The minimum historical temperature result " should "be reasonable " in {

    val minTemperatureEverDayData = analysis.getMinimumTemperature(validData)
    minTemperatureEverDayData.getMinimumInDegreesCelcius should be <= -10
  }
  "The maximum historical temperature result " should "be reasonable " in {

    val maxTemperatureEverDayData = analysis.getMaximumTemperature(validData)
    maxTemperatureEverDayData.getMaximumInDegreesCelcius should be >= 35
  }
  "The historical monthly avarages " should "be logically ordered" in {
    val monthlyAvarages = analysis.getMonthAvarages(validData)
    val (_, januaryAvarageTemp) = monthlyAvarages(MONTH_JANUARY)
    val (_, julyAvarageTemp) = monthlyAvarages(MONTH_JULY)
    januaryAvarageTemp should be < julyAvarageTemp
  }

  "The accuracy of the model " should " be less than 0.05 " in {
    prediction.getModelAccuracy(model, test) should be < 0.05
  }

  "The prediction for avarage temperature for a day in July " should " be between 20 and 30 degrees" in {
    val predictedValue = prediction.predictAvarageTemperature(model, 195)
    predictedValue should be < 30.0
    predictedValue should be > 20.0
  }

  "The prediction for avarage temperature for a day in January " should " be between -10 and 10 degrees" in {
    val predictedValue = prediction.predictAvarageTemperature(model, 15)
    predictedValue should be < 10.0
    predictedValue should be > -10.0
  }

}
