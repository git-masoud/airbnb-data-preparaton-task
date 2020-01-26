package task.main

;

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{FeatureHasher, StringIndexer, VectorIndexer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.DataFrame
import task.common.SparkUtils;

object MLListingsModel {

  import SparkUtils.spark.implicits._

  val usage = """
    Usage: MLListingsModel cleansed_data_path target_model_path from_date to_date
  """

  def main(args: Array[String]): Unit = {
    val currentDateTime = java.time.LocalDate.now
    args.foreach(println(_))
    if (args.length != 4) {
      println(usage)
      System.exit(1)
    }
    val (sourcePath, targetPath, fromDate, toDate) = (args.apply(0), args.apply(1), SparkUtils.parseArgDate(args.apply(2)), SparkUtils.parseArgDate(args.apply(3)))
    val sourceDf = SparkUtils.readParquet(sourcePath)
      .filter(s"day >= ${(fromDate._1.toString + fromDate._2.toString +fromDate._3.toString)} and day <= ${toDate._1.toString + toDate._2.toString + toDate._3.toString}")
    val model = train(sourceDf)
    model.write.save(targetPath + "/" + currentDateTime.getYear + "/" + currentDateTime.getMonthValue + "/" + currentDateTime.getDayOfMonth)
  }

  def train(sourceDf: DataFrame): PipelineModel = {
    val mlColumns = sourceDf.columns.filter(c => c != "id") //&& c!= "cancellation_policy" && c!="jurisdiction_names")
    val df = sourceDf.select(mlColumns.head, mlColumns.tail: _*)
    //One-Hot encoding
    val hasher = new FeatureHasher()
      .setInputCols(mlColumns.filter(c => c != "price"))
      .setOutputCol("features")

    val featurizedDf = hasher.transform(df)

    val Array(train, test) = featurizedDf.randomSplit(Array(.8, .2), 11L)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(featurizedDf)
    val labelIndexer = new StringIndexer().setInputCol("price").setOutputCol("label").fit(featurizedDf)
    val lr = new LinearRegression().setLabelCol("label").setFeaturesCol("features").setMaxIter(2)
    //    val labelConverter = new IndexToString()
    //      .setInputCol("prediction")
    //      .setOutputCol("predictedLabel")
    //      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, lr))
    // Train model. This also runs the indexers.
    print("create model")
    val model = pipeline.fit(train)

    // Make predictions.
    print("predictions")
    val predictions = model.transform(test)
    predictions.show()
    val metrics = new RegressionMetrics(predictions.select("label", "prediction").map(c => (c.getDouble(0), c.getDouble(1))).toJavaRDD)
    println(s"R-squared = ${metrics.r2}")
    model
  }
}
