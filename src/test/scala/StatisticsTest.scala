import java.sql.DriverManager

import org.apache.spark.sql.SparkSession

import org.scalatest.matchers.should.Matchers._

class StatisticsTest extends org.scalatest.FunSuite {
  test("") {
    val sparkSession = SparkSession.builder.master("local").getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")

    val statistics = Statistics.calculate(
      sparkSession,
      Seq.range(0, 12).map(i => getClass.getResource(f"GlobalLandTemperaturesByCity-$i%02d.csv").toURI.toString): _*
    )
    val connection = DriverManager.getConnection("jdbc:sqlite::memory:")

    statistics.insertCityYearTemperature(connection)
    connection.prepareStatement("SELECT COUNT(*) FROM city_year_temperature")
      .executeQuery().getInt(1) should be(681569)

    statistics.insertCityCenturyTemperature(connection)
    connection.prepareStatement("SELECT COUNT(*) FROM city_century_temperature")
      .executeQuery().getInt(1) should be(11632)

    statistics.insertCityOverallTemperature(connection)
    connection.prepareStatement("SELECT COUNT(*) FROM city_overall_temperature")
      .executeQuery().getInt(1) should be(3448)

    statistics.insertCountryYearTemperature(connection)
    connection.prepareStatement("SELECT COUNT(*) FROM country_year_temperature")
      .executeQuery().getInt(1) should be(31556)

    statistics.insertCountryCenturyTemperature(connection)
    connection.prepareStatement("SELECT COUNT(*) FROM country_century_temperature")
      .executeQuery().getInt(1) should be(540)

    statistics.insertCountryOverallTemperature(connection)
    connection.prepareStatement("SELECT COUNT(*) FROM country_overall_temperature")
      .executeQuery().getInt(1) should be(159)
  }
}
