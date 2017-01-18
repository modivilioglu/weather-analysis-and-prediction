package com.mod.weather.model
import com.mod.weather.weather._
/**
  * Created by mehmetoguzdivilioglu on 17/01/2017.
  */
case class DailyWeather(day: Int, month: Int, year: Int, PRCP: Int, SNOW: Int,
                        SNWD: Int, TMAX: Int, TMIN: Int, other: Int) {
  def getAverage = (TMAX + TMIN) / 2
  def getMinimumInDegreesCelcius = TMIN / TEMPERATURE_MULTIPLICATOR
  def getMaximumInDegreesCelcius = TMAX / TEMPERATURE_MULTIPLICATOR
  override def toString: String = s"$day/$month/$year: maxTemp: ${TMAX / TEMPERATURE_MULTIPLICATOR} minTemp: ${TMIN / TEMPERATURE_MULTIPLICATOR} avgTemp: ${getAverage / TEMPERATURE_MULTIPLICATOR} "
}
