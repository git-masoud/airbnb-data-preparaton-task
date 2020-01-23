import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SaveMode
import task.common.{Constants, DataQuality, ETLUtils, SparkUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
object main {

  def main2(args: Array[String]): Unit = {
    import SparkUtils.spark.implicits._
    val sourcePath=args.apply(0)
    val targetPath=args.apply(1)
    val df = SparkUtils.readParquet("/home/masoud/data/airbnb/lists.parquet").getOrElse(null)
//df.withColumn("dayOfWeek",)
  }

  def main(args: Array[String]): Unit = {
    //TODO: using a proper arguments parser
    val sourcePath=args.apply(0)
    val targetPath=args.apply(1)
    ETLUtils.cleanListsData("/home/masoud/data/airbnb/lists.parquet","")
  }
}
