package task.common

import java.io.File
import java.nio.file.{Files, Path}

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.io.Source

object DataQuality {
  val log: Logger = Logger.getLogger(DataQuality.getClass)

  import SparkUtils.spark.implicits._

  case class Schema(field: String, fieldType: String)

  case class DataRule(field: String, rule: String, extra: String)

  /**
   * validates schema and evaluate values and throw exception or warning logs in case of issues
   *
   * @param sourceDf
   * @param tableName
   */
  def validateDataFrameAndSchema(sourceDf: DataFrame, tableName: String) = {
    //in order to read the content from a file in the fat jar in runtime
    val schemaFileContent = SparkUtils.spark.createDataset[String](Source.fromInputStream(
      getClass.getClassLoader.getResourceAsStream(s"${tableName}_schema.json")).getLines().toSeq)
    val rulesFileContent = SparkUtils.spark.createDataset[String](Source.fromInputStream(
      getClass.getClassLoader.getResourceAsStream(s"${tableName}_rules.json")).getLines().toSeq)

    val tableFileds = SparkUtils.spark.read
      .option("multiline", "true")
      .json(schemaFileContent).as[Schema].collect()

    validateSchema(sourceDf.schema.map(c => Schema(c.name, c.dataType.toString)), tableFileds)

    val tableRules = SparkUtils.spark.read
      .option("multiline", "true")
      .json(rulesFileContent).as[DataRule].collect()
    evaluateDataQuality(tableRules, sourceDf)
  }

  /**
   *
   * @param dataFrameSchema
   * @param tableFields
   */
  def validateSchema(dataFrameSchema: Seq[Schema], tableFields: Seq[Schema]) = {

    if (!dataFrameSchema.length.equals(tableFields.length)) {
      val msg = s"Number of fields is not matched with the schema definition! ${dataFrameSchema.length} != ${tableFields.length}"
      log.error(msg)
      throw new Exception(msg + " : " + tableFields.diff(dataFrameSchema).mkString(","))
    }

    val dfSchemaMap = dataFrameSchema.map(c => c.field -> c.fieldType).toMap
    tableFields.foreach(c => {
      if (c.fieldType != dfSchemaMap.get(c.field).get) {
        val msg = s"Field ${c.field} has different type than it should have. ${c.fieldType} != ${dfSchemaMap.get(c.field).get}"
        log.error(msg)
        throw new Exception(msg)
      }
    })
  }

  /**
   *
   * @param rules
   * @param sourceDf
   */
  def evaluateDataQuality(rules: Array[DataRule], sourceDf: DataFrame) = {
    val verificationSuite = VerificationSuite.apply().onData(sourceDf)
    rules.foreach(dataRule => {
      dataRule.rule match {
        case "Complete" => verificationSuite.addCheck(Check(CheckLevel.Warning, dataRule.field).isComplete(dataRule.field))
        case "NonNegative" => verificationSuite.addCheck(Check(CheckLevel.Error, dataRule.field).isNonNegative(dataRule.field))
        case "ContainedIn" => verificationSuite.addCheck(Check(CheckLevel.Warning, dataRule.field).isContainedIn(dataRule.field, dataRule.extra.split(",")))
        //TODO: add other use cases
      }
    })

    verificationSuite.run().checkResults.foreach(c => {
      if (!c._2.status.equals(CheckStatus.Success)) {
        if (c._1.level == CheckLevel.Error) {
          log.error("There are some issues in the input data!")
          throw new Exception(s"${c._2.constraintResults.map(c => c.constraint + "\t" + c.message).mkString("\n")}")
        } else
          log.warn(s"Warning: ${c._2.constraintResults.map(_.message).mkString("\n")}")
      }
    })
  }


}
