package task.main

import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace, when}
import org.apache.spark.sql.types.IntegerType
import task.common.SparkUtils

object ListingsModelDataPreparation {
    val usage = """
    Usage: ListingsModelDataPreparation source_table_path target_table_path [yyyy:mm:DD or all]
  """
    def main(args: Array[String]): Unit = {
      if (args.length != 3) {
        println(usage)
        System.exit(1)
      }
      val (sourcePath, targetPath,date) = (args.apply(0), args.apply(1), args.apply(2))
      var sourceDf = SparkUtils.readParquet(sourcePath)
      if(date.toLowerCase()!="all") {
         val (year,month,day)=SparkUtils.parseArgDate(date)
        sourceDf=sourceDf.filter(s"year=$year and month=$month and day=$day")
      }
      sourceDf = sourceDf
        .withColumn("has_picture",when(col("picture_url").isNull,false).otherwise(true))
        .withColumn("amenities",concat_ws(",",col("amenities")))

      val requiredColumns=Array("id","has_picture","host_is_superhost","host_listings_count","host_has_profile_pic","zipcode","cancellation_policy"
        ,"property_type","room_type","accommodates","bathrooms","bedrooms","beds","bed_type","price","cleaning_fee","minimum_nights","maximum_nights","availability_30"
        ,"availability_60","availability_90","availability_365","review_scores_rating","amenities","year","month","day")

      var featuresDf= sourceDf.select(requiredColumns.head,requiredColumns.tail:_*).withColumn("zipcode",regexp_replace(col("zipcode"),"[\\D]","").cast(IntegerType))
      featuresDf= SparkUtils.dropColumns(featuresDf
        ,Array("price is not null and price > 0","zipcode is not null and zipcode > 0", "property_type is not null", "bedrooms is not null and bedrooms>0", "beds is not null and beds >0", "bathrooms is not null and bathrooms>0","accommodates is not null and accommodates>0"))
      SparkUtils.writeParquet(featuresDf, targetPath,Seq("year","month","day"))
    }
}
