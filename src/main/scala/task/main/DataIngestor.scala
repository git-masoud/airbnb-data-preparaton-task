package task.main

import task.common.{Constants, DataQuality, SparkUtils}
import org.apache.spark.sql.functions.{col, concat, dayofmonth, month, year}

import scala.sys.process.Process

object DataIngestor {
  val usage =
    """
    Usage: DataIngestor source_CSVs_path target_table_path table_name(listings) [number of rows]
  """

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println(usage)
      System.exit(1)
    }
    if (args.apply(2) != "listings") {
      println(usage)
      System.exit(1)
    }

    val (sourcePath, targetPath, tableName) = (args.apply(0), args.apply(1), args.apply(2))
    //These options are useful to set in order to help spark parse csv files better
    val options = Map(
      "treatEmptyValuesAsNulls" -> "true",
      "wholeFile" -> "true",
      "header" -> "true",
      "parserLib" -> "univocity",
      "escape" -> "\"",
      "multiLine" -> "true",
      "ignoreLeadingWhiteSpace" -> "true",
      "ignoreTrailingWhiteSpace" -> "true",
      "inferSchema" -> "true"
    )

    val rawDataFrame = SparkUtils.readCsv(sourcePath + s"/$tableName*.csv", options)

    //if the number of rows was in the arguments, this will evaluate it with the number of rows from spark
    if (args.length == 4) {
      val numberOfRows = args.apply(3).toInt
      val rowCount = rawDataFrame.count()
      if (rowCount != args.apply(3).toInt)
        throw new Exception(s"dataframe rows number is not equal to the number of rows of the source file! $rowCount!=$numberOfRows")
    }

    DataQuality.validateDataFrameAndSchema(rawDataFrame, "listings")

    SparkUtils.writeParquet(rawDataFrame.withColumn("year", year(col(Constants.dateColumnName)))
      .withColumn("month", concat(col("year"), month(col(Constants.dateColumnName))))
      .withColumn("day", concat(col("year"), month(col(Constants.dateColumnName)), dayofmonth(col(Constants.dateColumnName))))
      , targetPath, Seq("year", "month", "day"))

  }


}
