package task.main

import org.apache.spark.sql.functions.{col, regexp_replace, split, when}
import org.apache.spark.sql.types.LongType
import task.common.SparkUtils

/**
 * This class pre-process data without dropping any columns or values
 */
object ListingsTablePreprocessor {
  val usage =
    """
    Usage: ListingsTablePreprocessor source_table_path target_table_path [yyyy:mm:DD or all]
  """

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println(usage)
      System.exit(1)
    }

    val (sourcePath, targetPath, date) = (args.apply(0), args.apply(1), args.apply(2))

    var sourceDf = SparkUtils.readParquet(sourcePath)

    if (date.toLowerCase() != "all") {
      val (year, month, day) = SparkUtils.parseArgDate(date)
      sourceDf = sourceDf.filter(s"year=$year and month=$month and day=$day")
    }

    //TODO: This part shoudld be configurable in future
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

    //convert non to null
    cleanedDf = cleanedDf.withColumn("experiences_offered", when(col("experiences_offered") === "none", null).otherwise(col("experiences_offered")))

    //convert string which are in shape of list to real list
    cleanedDf = cleanedDf.withColumn("host_verifications", split(regexp_replace(col("host_verifications"), "(\\[)|(')|(\\])", ""), ","))
    cleanedDf = cleanedDf.withColumn("amenities", split(regexp_replace(col("amenities"), "(\\{)|(\")|(\\})", ""), ","))
    cleanedDf = cleanedDf.withColumn("jurisdiction_names", split(regexp_replace(col("jurisdiction_names"), "(\\{)|(\")|(\\})", ""), ","))

    //remove whitespaces and line breakers
    cleanedDf = SparkUtils.trimDataframeColumns(cleanedDf,
      Array("name", "summary", "space", "description", "neighborhood_overview", "notes", "transit", "access", "interaction", "house_rules", "host_about"))

    // Remove $ from currency fields
    val currencyColumns = Array("price", "weekly_price", "monthly_price", "security_deposit", "cleaning_fee", "extra_people")
    cleanedDf = currencyColumns.foldLeft(cleanedDf) { (tempDf, column) => tempDf.withColumn(column, regexp_replace(col(column), "\\$", "").cast(LongType)) }

    // Remove % from percentage fields
    cleanedDf = cleanedDf.withColumn("host_response_rate", regexp_replace(col("host_response_rate"), "\\%", "").cast(LongType))
    //Dropping duplicates column and save
    SparkUtils.writeParquet(cleanedDf.dropDuplicates(), targetPath, Seq("year", "month", "day"))
  }
}
