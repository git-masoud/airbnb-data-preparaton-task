package task.common

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object DataQuality {
  val log: Logger = Logger.getLogger(DataQuality.getClass)
  import SparkUtils.spark.implicits._
  case class Schema(field:String,fieldType:String)
  case class DataRule(field: String, rule: String, extra: String)

  def validateSchema(dataFrameSchema:Seq[Schema], tableFileds: Seq[Schema]) = {
    if(!dataFrameSchema.length.equals(tableFileds.length))
    {
      val msg=s"Number of fields has been changed! ${dataFrameSchema.length} != ${tableFileds.length}"
      log.error(msg)
      throw new Exception(msg)
    }
    val dfSchemaMap=dataFrameSchema.map(c=> c.field-> c.fieldType).toMap

    tableFileds.foreach(c=>{
      if(c.fieldType!=dfSchemaMap.get(c.field).get)
        {
          val msg=s"Field ${c.field} has different type than it should have. ${c.fieldType} != ${dfSchemaMap.get(c.field).get}"
          log.error(msg)
          throw new Exception(msg)
        }
    })
  }

  def validateDataframeAndSchema(sourceDf: DataFrame, tableName: String) = {
    val rulesPath = getClass.getClassLoader.getResource(s"${tableName}_rules.json").getPath
    val schemaPath = getClass.getClassLoader.getResource(s"${tableName}_schema.json").getPath
    val tableFileds = SparkUtils.spark.read
      .option("multiline", "true")
      .json(schemaPath).as[Schema].collect()
    validateSchema(sourceDf.schema.map(c=>Schema(c.name,c.dataType.toString)),tableFileds)

    val tableRules = SparkUtils.spark.read
      .option("multiline", "true")
      .json(rulesPath).as[DataRule].collect()
    val verificationSuite = VerificationSuite.apply().onData(sourceDf)
    tableRules.foreach(dataRule => {
      dataRule.rule match {
        case "Complete" => verificationSuite.addCheck(Check(CheckLevel.Warning, dataRule.field).isComplete(dataRule.field))
        case "NonNegative" => verificationSuite.addCheck(Check(CheckLevel.Error, dataRule.field).isNonNegative(dataRule.field))
        case "ContainedIn" => verificationSuite.addCheck(Check(CheckLevel.Warning, dataRule.field).isContainedIn(dataRule.field, dataRule.extra.split(",")))
        //case default => println(dataRule.field)
      }
    })
    verificationSuite.run().checkResults.foreach(c=>
      {
        if(!c._2.status.equals(CheckStatus.Success)){
          if (c._1.level == CheckLevel.Error) {
            log.error("There are some issues in the input data!")
            throw new Exception(s"${c._2.constraintResults.map(c=>c.constraint+"\t"+c.message).mkString("\n")}")
          } else
            log.warn(s"Warning: ${c._2.constraintResults.map(_.message).mkString("\n")}")
        }
      })
//    val resultDataFrame = checkResultsAsDataFrame(SparkUtils.spark, )
//    resultDataFrame.show(500,false)
    ()
  }
}