package task.common

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lower, regexp_replace, trim, when}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object SparkUtils {
  def parseArgDate(date: String) = {
    val dates=date.split("-")
    if(dates.length!=3)
      throw new Exception("date format is incorrect. yyyy-mm-DD")
    (dates.apply(0).toInt,dates.apply(1).toInt,dates.apply(2).toInt)
  }


  val spark = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()
  val log: Logger = Logger.getLogger(DataQuality.getClass)

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

  def readCsv(sourcePath: String, options: Map[String, String]): DataFrame = {
    //TODO: reader should be able to read from different sources
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

  def writeParquet(dataFrame: DataFrame, targetPath: String,partitionColumns: Seq[String],saveMode:SaveMode=SaveMode.Overwrite) = {
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

  def castToBoolean(sourceDf: DataFrame, columns: Array[String], trueValue: String, falseValue: String, caseSensetive: Boolean = false): DataFrame = {
    columns.foldLeft(sourceDf) { (tempDf, column) =>
      if (caseSensetive)
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

  def dropColumns(sourceDf: DataFrame, columns: Array[String]) = {
    val conditions = columns.map(c => s"($c)").mkString(" and ")
    sourceDf.where(conditions)
  }

}
