package com.mod.weather.computation
import com.mod.weather.model.DailyWeather
import org.apache.spark._
import org.apache.spark.rdd._
import com.mod.weather.weather._
import cats.implicits._
/**
  * Created by mehmetoguzdivilioglu on 17/01/2017.
  */
class WeatherAnalysis {
  def getValidData(sc: SparkContext, filePath: String) = {
    val textRDD = sc.textFile(filePath)
    val daysRDD = textRDD.map(parseDay).flatMap(y => y)
    val daysRDDWithKey = daysRDD.map(x => ((x.day, x.month, x.year), x))

    //Then we merge by date to get all findings in one value!!!
    val aggregated = daysRDDWithKey.reduceByKey((a, b) => a |+| b)

    aggregated.filter(x => (x._2.TMIN != 0) && (x._2.TMIN != -9999) && (x._2.TMAX != 0) && (x._2.TMAX != -9999)).cache()
  }

  //Calculate historical avarage per month
  def getMonthAvarages(aggregatedFiltered: RDD[((Int, Int, Int), DailyWeather)]) = {
    val monthAvarages = aggregatedFiltered.map(x => (x._1._2, (1, x._2.getAverage))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    monthAvarages.map(x => ((x._1, x._2._2 / (x._2._1 * TEMPERATURE_MULTIPLICATOR)))).sortBy(x => x._1).collect() // Sort by month asc
  }

  // Calculate the coldest year(By avarage temperature in January)
  def getColdestYear(aggregatedFiltered: RDD[((Int, Int, Int), DailyWeather)]) = {
    val yearAvarages = aggregatedFiltered.filter(x => x._1._2 == MONTH_JANUARY).map(x => (x._1._3, (1, x._2.TMIN))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    val (year, (numberOfValidDays, totalTemp )) = yearAvarages.sortBy(x => x._2._2 / x._2._1).take(1)(0)
    (year, totalTemp / (numberOfValidDays * TEMPERATURE_MULTIPLICATOR) )
  }
  // Calculate the hottest year(By avarage temperature in July)
  def getHottestYear(aggregatedFiltered: RDD[((Int, Int, Int), DailyWeather)]) = {
    val yearAvaragesMax = aggregatedFiltered.filter(x => x._1._2 == MONTH_JULY).map(x => (x._1._3, (1, x._2.TMAX))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    val (year, (numberOfValidDays, totalTemp )) = yearAvaragesMax.sortBy(x => x._2._2 / x._2._1, false).take(1)(0)
    (year, totalTemp / (numberOfValidDays * TEMPERATURE_MULTIPLICATOR) )
  }

  // Get Lowest Temperature in history
  def getMinimumTemperature(aggregatedFiltered: RDD[((Int, Int, Int), DailyWeather)]) = {
    val (key, dailyWeather) = aggregatedFiltered.sortBy(x => x._2.TMIN).take(1)(0)
    dailyWeather
  }

  // Get Highest Temperature in history
  def getMaximumTemperature(aggregatedFiltered: RDD[((Int, Int, Int), DailyWeather)]) = {
    val (key, dailyWeather) = aggregatedFiltered.sortBy(x => x._2.TMAX, false).take(1)(0)
    dailyWeather
  }
}
