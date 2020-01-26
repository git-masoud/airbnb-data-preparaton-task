package task

import task.common.SparkUtils
import task.main.MLListingsModel

object mains {
  def main(args: Array[String]): Unit = {
    val df=SparkUtils.readParquet("/home/masoud/projects/testProject/data/airbnb/model_data/year=2018/month=20182/day=201821")
    df.show()

  }
}
