package task.common

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}

object ETLUtils {

  def IngestData(sourcePath: String, targetPath: String) = {
    val rawDataframe = SparkUtils.readCsv(sourcePath + "/listings*.csv", true, true, "\n", true).getOrElse(null)
    DataQuality.validateDataframeAndSchema(rawDataframe, "listings")
    rawDataframe.withColumn("year", year(col(Constants.dateColumnName)))
      .withColumn("month", concat(col("year"), month(col(Constants.dateColumnName))))
      .withColumn("day", concat(col("year"), col("month"), dayofmonth(col(Constants.dateColumnName))))
      .write.partitionBy("year", "month", "day").mode(SaveMode.Append)
      .parquet(targetPath)
  }

  def cleanListsData(sourcePath: String, targetPath: String) = {
    val sourceDf = SparkUtils.readParquet(sourcePath).get
    var cleanedDf = SparkUtils.castToBoolean(sourceDf, Array("host_is_superhost",
      "host_has_profile_pic",
      "host_identity_verified",
      "is_location_exact",
      "has_availability",
      "requires_license",
      "instant_bookable",
      "is_business_travel_ready",
      "require_guest_profile_picture",
      "require_guest_phone_verification"), "t", "f")
    cleanedDf = cleanedDf.withColumn("experiences_offered", when(col("experiences_offered") === "none", null).otherwise(col("experiences_offered")))
    //convert string which are in shape of list to real list
    cleanedDf = cleanedDf.withColumn("host_verifications", split(regexp_replace(col("host_verifications"), "(\\[)|(')|(\\])", ""), ","))
    cleanedDf = cleanedDf.withColumn("amenities", split(regexp_replace(col("amenities"), "(\\{)|(\")|(\\})", ""), ","))
    cleanedDf = cleanedDf.withColumn("jurisdiction_names", split(regexp_replace(col("jurisdiction_names"), "(\\{)|(\")|(\\})", ""), ","))
    //remove whitespaces and line breakers
    cleanedDf= SparkUtils.trimDataframeColumns(cleanedDf,
      Array("name","summary","space","description","neighborhood_overview","notes","transit","access","interaction","house_rules","host_about"))
    // Remove $ from currency fields
    val currencyColumns= Array("price","weekly_price","monthly_price","security_deposit","cleaning_fee","extra_people")
    cleanedDf = currencyColumns.foldLeft(cleanedDf){(tempDf,column)=> tempDf.withColumn(column,regexp_replace(col(column),"\\$","").cast(LongType))}
    // Remove % from percentage fields
    cleanedDf = cleanedDf.withColumn("host_response_rate",regexp_replace(col("host_response_rate"),"\\%","").cast(LongType))

    cleanedDf.printSchema()//write.parquet(targetPath)
  }


}
