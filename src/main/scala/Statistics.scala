import java.sql.Connection
import java.util.function.Consumer

import org.apache.spark.sql._

final case class Statistics
(
  cityYearTemperatures: DataFrame,
  cityCenturyTemperatures: DataFrame,
  cityOverallTemperatures: DataFrame,
  countryYearTemperatures: DataFrame,
  countryCenturyTemperatures: DataFrame,
  countryOverallTemperatures: DataFrame
) {
  def insertCityYearTemperature(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE city_year_temperature (city TEXT, year INTEGER, min_temperature DOUBLE, avg_temperature DOUBLE, max_temperature DOUBLE)"
    ).execute()
    val statement = connection.prepareStatement("INSERT INTO city_year_temperature VALUES (?, ?, ?, ?, ?)")
    cityYearTemperatures.toLocalIterator().forEachRemaining(new Consumer[Row] {
      override def accept(row: Row): Unit = {
        statement.setString(1, row.getString(0))
        statement.setInt(2, row.getInt(1))
        statement.setDouble(3, row.getDouble(2))
        statement.setDouble(4, row.getDouble(3))
        statement.setDouble(5, row.getDouble(4))
        statement.addBatch()
      }
    })
    statement.executeBatch()
  }

  def insertCityCenturyTemperature(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE city_century_temperature (city TEXT, century INTEGER, min_temperature DOUBLE, max_temperature DOUBLE)"
    ).execute()
    val statement = connection.prepareStatement("INSERT INTO city_century_temperature VALUES (?, ?, ?, ?)")
    cityCenturyTemperatures.toLocalIterator().forEachRemaining(new Consumer[Row] {
      override def accept(row: Row): Unit = {
        statement.setString(1, row.getString(0))
        statement.setInt(2, row.getInt(1))
        statement.setDouble(3, row.getDouble(2))
        statement.setDouble(4, row.getDouble(3))
        statement.addBatch()
      }
    })
    statement.executeBatch()
  }

  def insertCityOverallTemperature(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE city_overall_temperature (city TEXT, min_temperature DOUBLE, max_temperature DOUBLE)"
    ).execute()
    val statement = connection.prepareStatement("INSERT INTO city_overall_temperature VALUES (?, ?, ?)")
    cityOverallTemperatures.toLocalIterator().forEachRemaining(new Consumer[Row] {
      override def accept(row: Row): Unit = {
        statement.setString(1, row.getString(0))
        statement.setDouble(2, row.getDouble(1))
        statement.setDouble(3, row.getDouble(2))
        statement.addBatch()
      }
    })
    statement.executeBatch()
  }

  def insertCountryYearTemperature(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE country_year_temperature (country TEXT, year INTEGER, min_temperature DOUBLE, max_temperature DOUBLE)"
    ).execute()
    val statement = connection.prepareStatement("INSERT INTO country_year_temperature VALUES (?, ?, ?, ?)")
    countryYearTemperatures.toLocalIterator().forEachRemaining(new Consumer[Row] {
      override def accept(row: Row): Unit = {
        statement.setString(1, row.getString(0))
        statement.setInt(2, row.getInt(1))
        statement.setDouble(3, row.getDouble(2))
        statement.setDouble(4, row.getDouble(3))
        statement.addBatch()
      }
    })
    statement.executeBatch()
  }

  def insertCountryCenturyTemperature(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE country_century_temperature (country TEXT, century INTEGER, min_temperature DOUBLE, max_temperature DOUBLE)"
    ).execute()
    val statement = connection.prepareStatement("INSERT INTO country_century_temperature VALUES (?, ?, ?, ?)")
    countryCenturyTemperatures.toLocalIterator().forEachRemaining(new Consumer[Row] {
      override def accept(row: Row): Unit = {
        statement.setString(1, row.getString(0))
        statement.setInt(2, row.getInt(1))
        statement.setDouble(3, row.getDouble(2))
        statement.setDouble(4, row.getDouble(3))
        statement.addBatch()
      }
    })
    statement.executeBatch()
  }

  def insertCountryOverallTemperature(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE country_overall_temperature (country TEXT, min_temperature DOUBLE, max_temperature DOUBLE)"
    ).execute()
    val statement = connection.prepareStatement("INSERT INTO country_overall_temperature VALUES (?, ?, ?)")
    countryOverallTemperatures.toLocalIterator().forEachRemaining(new Consumer[Row] {
      override def accept(row: Row): Unit = {
        statement.setString(1, row.getString(0))
        statement.setDouble(2, row.getDouble(1))
        statement.setDouble(3, row.getDouble(2))
        statement.addBatch()
      }
    })
    statement.executeBatch()
  }
}

object Statistics {
  def calculate(sparkSession: SparkSession, paths: String*): Statistics = {
    val cityYearTemperatures = sparkSession.read.format("csv").option("header", "true").load(paths: _*)
      .select(
        functions.col("city"),
        functions.col("country"),
        functions.expr("year(to_date(dt)) as year"),
        functions.col("AverageTemperature").cast("double").as("temperature")
      )
      .filter("temperature IS NOT NULL")
      .groupBy("city", "year").agg(
      functions.expr("min(temperature)").as("min_temperature"),
      functions.expr("avg(temperature)").as("avg_temperature"),
      functions.expr("max(temperature)").as("max_temperature"),
      functions.expr("first(country)").as("country")
    ).cache

    val cityCenturyTemperatures = withCentury(cityYearTemperatures).groupBy("city", "century").agg(
      functions.expr("min(min_temperature)").as("min_temperature"),
      functions.expr("max(max_temperature)").as("max_temperature"),
      functions.expr("first(country)").as("country")
    ).cache

    val cityOverallTemperatures = cityCenturyTemperatures.groupBy("city").agg(
      functions.expr("min(min_temperature)").as("min_temperature"),
      functions.expr("max(max_temperature)").as("max_temperature"),
      functions.expr("first(country)").as("country")
    ).cache

    val countryYearTemperatures = cityYearTemperatures.groupBy("country", "year").agg(
      functions.expr("min(min_temperature)").as("min_temperature"),
      functions.expr("max(max_temperature)").as("max_temperature")
    ).cache

    val countryCenturyTemperatures = withCentury(countryYearTemperatures).groupBy("country", "century").agg(
      functions.expr("min(min_temperature)").as("min_temperature"),
      functions.expr("max(max_temperature)").as("max_temperature")
    ).cache

    val countryOverallTemperatures = countryCenturyTemperatures.groupBy("country").agg(
      functions.expr("min(min_temperature)").as("min_temperature"),
      functions.expr("max(max_temperature)").as("max_temperature")
    ).cache

    Statistics(
      cityYearTemperatures,
      cityCenturyTemperatures,
      cityOverallTemperatures,
      countryYearTemperatures,
      countryCenturyTemperatures,
      countryOverallTemperatures
    )
  }

  private def withCentury(dataset: Dataset[Row]) =
    dataset.withColumn("century", functions.expr("(year - 1) / 100").cast("integer"))
}
