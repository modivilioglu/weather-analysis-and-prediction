package com.mod.weather
import java.util.Calendar

import com.mod.weather.model._
/**
  * Created by mehmetoguzdivilioglu on 17/01/2017.
  */
// Covers implicits and utility functions
package object weather {
  import cats.Monoid

  implicit val monoid: Monoid[DailyWeather] = new Monoid[DailyWeather] {
    override def combine (d1: DailyWeather, d2: DailyWeather) =
      DailyWeather(d1.day, d1.month, d1.year, d1.PRCP + d2.PRCP, d1.SNOW + d2.SNOW, d1.SNWD + d2.SNWD, d1.TMAX + d2.TMAX, d1.TMIN + d2.TMIN, 0)

    override def empty = DailyWeather(0, 0, 0, 0, 0, 0, 0, 0, 0)
  }
  val MONTH_JANUARY = 1
  val MONTH_JULY = 7
  val TEMPERATURE_MULTIPLICATOR = 10

  private val MIN_APPROXIMATE_AVARAGE = -20
  private val MAX_APPROXIMATE_AVARAGE = 250

  private val TOTAL_DAYS_IN_YEAR = 365
  private val DAY_IN_YEAR_HALF = 183

  implicit class Convertor(value: Double) {
    def fromTemperatureToWeight: Double = {
      (value - (MIN_APPROXIMATE_AVARAGE)) / MAX_APPROXIMATE_AVARAGE
    }
    def fromVectorWeightToTemperature = {
      value * MAX_APPROXIMATE_AVARAGE + MIN_APPROXIMATE_AVARAGE
    }
  }

  type Day = Int
  implicit class DayConvertor(value: Day) {
    def fromDayToWeight: Double = (DAY_IN_YEAR_HALF.toDouble - Math.abs(value - DAY_IN_YEAR_HALF)) / DAY_IN_YEAR_HALF.toDouble
  }

  val ID_OFFSET_RANGE = (0, 11)
  val YEAR_OFFSET_RANGE = (11, 15)
  val MONTH_OFFSET_RANGE = (15, 17)
  val VALUE_TYPE_OFFSET_RANGE = (17, 21)
  val FIELDS_START_OFFSET = 21
  val FIELD_LENGTH = 8
  val FIELD_VALUE_LENGTH = 5
  val PRCP = "PRCP"
  val SNOW = "SNOW"
  val SNWD = "SNWD"
  val TMIN = "TMIN"
  val TMAX = "TMAX"

  def parseDay(str: String): List[DailyWeather] = {
    val id = str.substring(ID_OFFSET_RANGE._1, ID_OFFSET_RANGE._2)
    val year = str.substring(YEAR_OFFSET_RANGE._1, YEAR_OFFSET_RANGE._2).trim.toInt
    val month = str.substring(MONTH_OFFSET_RANGE._1, MONTH_OFFSET_RANGE._2).trim.toInt
    val element = str.substring(VALUE_TYPE_OFFSET_RANGE._1, VALUE_TYPE_OFFSET_RANGE._2).trim

    // Beginning from 0, 31 days
    val values = (0 to 30).map(x => {
      val valueString = str.substring(FIELDS_START_OFFSET + x * FIELD_LENGTH, FIELDS_START_OFFSET + x * FIELD_LENGTH + FIELD_VALUE_LENGTH)
      val value = valueString.trim.toInt
      val key = (x, month, year)
      element match {
        case PRCP => DailyWeather(x + 1, month, year, value, 0, 0, 0, 0, 0)
        case SNOW => DailyWeather(x + 1, month, year, 0, value, 0, 0, 0, 0)
        case SNWD => DailyWeather(x + 1, month, year, 0, 0, value, 0, 0, 0)
        case TMAX => DailyWeather(x + 1, month, year, 0, 0, 0, value, 0, 0)
        case TMIN => DailyWeather(x + 1, month, year, 0, 0, 0, 0, value, 0)
        case _ => DailyWeather(x + 1, month, year, 0, 0, 0, 0, 0, value)
      }
    })
    values.toList
  }
}
