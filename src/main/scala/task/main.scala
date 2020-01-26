package task

import task.common.SparkUtils

object mains {
  def main(args: Array[String]): Unit = {
    println(SparkUtils.readParquet("/home/masoud/data/airbnb/test").count())
  }
}
