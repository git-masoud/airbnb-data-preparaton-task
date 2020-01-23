package task.common

import org.apache.spark.sql.functions.{col, lower, when,regexp_replace,trim}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils {

  val spark = SparkSession.builder().master("local").getOrCreate()

  def readParquet(sourcePath: String) = {
    try {
      Some(spark.read.parquet(sourcePath))
    }
    catch {
      case exception: Exception => None
    }
  }

  def readCsv(sourcePath: String, header: Boolean, multiLine: Boolean, escapeCharachters: String, inferSchema: Boolean): Option[DataFrame] = {
    //TODO: reader should be able to read from different sources
    try {

      Some(spark.read
        .option("wholeFile", true)
        .option("header",true)
        .option("sep",",")
        .option("quote","\"")
        .option("escape","\"")
        .option("multiLine",true)
        .option("ignoreLeadingWhiteSpace",true)
        .option("ignoreTrailingWhiteSpace",true)
        //.option("mode","FAILFAST")
        .option("inferSchema",true)
        .csv(sourcePath))
    }
    catch {
      case exception: Exception => None
    }
  }

  def castToBoolean(sourceDf:DataFrame, columns:Array[String], trueValue:String, falseValue:String,caseSensetive:Boolean=false):DataFrame={
    columns.foldLeft(sourceDf) {(tempDf,column)=>
      if(caseSensetive)
        tempDf.withColumn(column,when(col(column)===trueValue,true).when(col(column)===falseValue,false).otherwise(null).cast(BooleanType))
      else
        tempDf.withColumn(column,when(lower(col(column))===trueValue.toLowerCase(),true).when(lower(col(column))===falseValue.toLowerCase(),false).otherwise(null).cast(BooleanType))
    }
  }

  /**
   * removing multiple spaces and converting the line breakers and tab seperator to the text format
   * @param sourceDf
   * @param columns
   * @return
   */
  def trimDataframeColumns(sourceDf: DataFrame,columns:Array[String])= {
    columns.foldLeft(sourceDf) { (tempDf, column) =>
      sourceDf.withColumn(column, trim(regexp_replace(col(column), "\r", "[NewLine]")))
        .withColumn(column, regexp_replace(col(column), "\n", "[NewLine]"))
        .withColumn(column, regexp_replace(col(column), "\t", "[Tab]"))
        .withColumn(column, regexp_replace(col(column), " +", " "))
    }
  }


}
