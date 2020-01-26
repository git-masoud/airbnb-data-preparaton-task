package task.common

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{FeatureHasher, IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

object ETLUtils {



}
