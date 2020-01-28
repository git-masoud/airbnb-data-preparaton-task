package task.common

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lower, regexp_replace, trim, when}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object SparkUtils {

  val spark = SparkSession.builder().master("local")
    .getOrCreate()

  // Specifics partitions will be over written by this option not the whole table
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  val log: Logger = Logger.getLogger(DataQuality.getClass)

  /**
   * split string date two three different integers
   * @param date format should be yyyy-mm-DD
   * @return (Int, Int, Int)
   */
  //TODO:add test cases
  def parseArgDate(date: String) = {
    val dates = date.split("-")
    if (dates.length != 3)
      throw new Exception("date format is incorrect. yyyy-mm-DD")
    (dates.apply(0).toInt, dates.apply(1).toInt, dates.apply(2).toInt)
  }

  /**
   * Read parquets files from the input path as a data frame
   *
   * @param sourcePath
   * @return
   */
  //TODO:add test cases
  def readParquet(sourcePath: String) = {
    Try(spark.read
      .parquet(sourcePath))
    match {
      case Success(df) =>
        log.info(s"Read parquet table at $sourcePath")
        df
      case Failure(exception) =>
        log.debug(s"Error reading parquet table at $sourcePath", exception)
        throw exception
    }
  }

  /**
   * Read csv files from the input path as a data frame
   *
   * @param sourcePath inputs path
   * @param options    spark options
   * @return DataFrame
   */
  //TODO:add test cases
  def readCsv(sourcePath: String, options: Map[String, String]): DataFrame = {
    Try {
      spark.read
        .options(options)
        .csv(sourcePath)
    } match {
      case Success(df) =>
        log.info(s"Read csv file at $sourcePath")
        df
      case Failure(exception) =>
        log.debug(s"Error reading csv file at $sourcePath", exception)
        throw exception
    }
  }

  /**
   * Save data frame as parquet into the targetPath
   *
   * @param dataFrame        source data frame
   * @param targetPath       target path
   * @param partitionColumns columns shich spark should use them for partitioning
   * @param saveMode
   */
  //TODO:add test cases
  def writeParquet(dataFrame: DataFrame, targetPath: String, partitionColumns: Seq[String], saveMode: SaveMode = SaveMode.Overwrite) = {
    Try {
      dataFrame
        //TODO: should change to an optimize partitioner method
        .coalesce(5)
        .write.partitionBy(partitionColumns: _*).mode(saveMode).parquet(targetPath)
    }
    match {
      case Success(_) =>
        log.info(s"Saved table at path $targetPath partitioned by $partitionColumns")
      case Failure(exception) =>
        log.error(s"Unable to save table (partitioned by $partitionColumns) at path $targetPath", exception)
        throw exception
    }
  }

  /**
   * cast a group of columns of a data frame to boolean
   *
   * @param sourceDf
   * @param columns
   * @param trueValue
   * @param falseValue
   * @param caseSensitive
   * @return
   */
  def castToBoolean(sourceDf: DataFrame, columns: Array[String], trueValue: String, falseValue: String, caseSensitive: Boolean = false): DataFrame = {
    columns.foldLeft(sourceDf) { (tempDf, column) =>
      if (caseSensitive)
        tempDf.withColumn(column, when(col(column) === trueValue, true).when(col(column) === falseValue, false).otherwise(null).cast(BooleanType))
      else
        tempDf.withColumn(column, when(lower(col(column)) === trueValue.toLowerCase(), true).when(lower(col(column)) === falseValue.toLowerCase(), false).otherwise(null).cast(BooleanType))
    }
  }

  /**
   * removing multiple spaces and converting the line breakers and tab seperator to the text format
   *
   * @param sourceDf
   * @param columns
   * @return
   */
  def trimDataframeColumns(sourceDf: DataFrame, columns: Array[String]) = {
    columns.foldLeft(sourceDf) { (tempDf, column) =>
      sourceDf.withColumn(column, trim(regexp_replace(col(column), "\r", "[NewLine]")))
        .withColumn(column, regexp_replace(col(column), "\n", "[NewLine]"))
        .withColumn(column, regexp_replace(col(column), "\t", "[Tab]"))
        .withColumn(column, regexp_replace(col(column), " +", " "))
    }
  }

  /**
   * filter datafram with multiple columns conditions
   *
   * @param sourceDf
   * @param columns
   * @return
   */
  //TODO:add test cases
  def filterRows(sourceDf: DataFrame, columns: Array[String]) = {
    val conditions = columns.map(c => s"($c)").mkString(" and ")
    sourceDf.where(conditions)
  }

}
