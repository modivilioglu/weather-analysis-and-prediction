package com.mod.weather.computation

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd._
import com.mod.weather.model.DailyWeather
import com.mod.weather.weather._
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import java.text._
import java.util.Calendar

/**
  * Created by mehmetoguzdivilioglu on 17/01/2017.
  */
class WeatherPrediction {

  def getPreparedData(validData: RDD[((Int, Int, Int), DailyWeather)]) = {
    val formatter: DateFormat = new SimpleDateFormat("MM/dd/yy");
    validData.map(x => {
      val calendar = Calendar.getInstance()
      val day = "%02d".format(x._1._2)
      calendar.setTime(formatter.parse(s"${x._1._2}/$day/${x._1._3}"))

      LabeledPoint(x._2.getAverage.toDouble.fromTemperatureToWeight, Vectors.dense(calendar.get(Calendar.DAY_OF_YEAR).fromDayToWeight))
    }).cache()
  }
  def getTrainingAndTestData(preparedData: RDD[LabeledPoint]) = {
    val splits = preparedData.randomSplit (Array (0.9, 0.1), seed = 11L)
    val training = splits (0).cache ()
    val test = splits (1)
    (training, test)
  }
  def getModel(trainingData: RDD[LabeledPoint]) = {
    val model = LinearRegressionWithSGD.train(trainingData, 100)
    model
  }

  def getModelAccuracy(model: LinearRegressionModel, testData: RDD[LabeledPoint]) = {
    val valuesAndPredictions = testData.map { x =>
      val prediction = model.predict(x.features)
      (x.label, prediction)
    }
    val meanSquaredError = valuesAndPredictions.map{ case(v, p) => math.pow((v - p), 2) }.mean()

    meanSquaredError
  }

  def predictAvarageTemperature(model: LinearRegressionModel, day: Day) = {
    val vectorWeightForDay = day.fromDayToWeight
    val featuresVector = Vectors.dense(vectorWeightForDay)
    val result = model.predict(featuresVector)
    result.fromVectorWeightToTemperature / TEMPERATURE_MULTIPLICATOR
  }
}
