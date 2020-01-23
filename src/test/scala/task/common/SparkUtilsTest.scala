package task.common

import org.apache.spark.sql.types.BooleanType
import org.scalatest.FunSuite


class SparkUtilsTest extends FunSuite{
  import SparkUtils.spark.implicits._

  val sampleDf= List((22,"t","f"),(23,"T","F"),(24,"T","t"),(25,"t","F"),(26,null,"t")
  ,(27,"true","f"),(28,"f","fff")).toDF("id","is_success","has_money")

  test("castToBoolean - columns types") {
    val castedDf=SparkUtils.castToBoolean(sampleDf,Array("is_success","has_money"),"t","f")
    castedDf.show()
    assert(castedDf.schema.apply("is_success").dataType===BooleanType,"Field is_success type didn't chang to boolean!")
    assert(castedDf.schema.apply("has_money").dataType===BooleanType,"Field is_success type didn't chang to boolean!")
  }

  test("castToBoolean - columns value") {
    val castedDf=SparkUtils.castToBoolean(sampleDf,Array("is_success","has_money"),"t","f")
    assert(castedDf.filter("id=22").first().getBoolean(1),"is_success for row with id 22 should be true!")
    assert(castedDf.filter("id=23").first().getBoolean(1),"is_success for row with id 23 should be true!")
    assert(castedDf.filter("id=26").first().get(1)===null,"is_success for row with id 26 should be null!")
    assert(!castedDf.filter("id=28").first().getBoolean(1),"is_success for row with id 28 should be true!")
  }

  test("trim text")
  {
    val badTextDf=List("my      name  is \n\t masoud: \r I offer  my place for\n\n  free!","hello there\n!",
    "Upon arriving in Amsterdam, one can imagine asking oneself: Where is the fun nightlife? What are the local hot spots? \n\nHow can I experience the real life in this city? I  offer you the opportunity to act, eat and sleep like-a-local!  \n\nI provide the traveler with the opportunity to connect with the local life in Amsterdam.                                                                                                                                                                                                                                                                                                                                                                                                                                                            ").toDF()
    badTextDf.show(false)
    val trimmedDf=SparkUtils.trimDataframeColumns(badTextDf,badTextDf.columns).collect()
    SparkUtils.trimDataframeColumns(badTextDf,Array("value")).show(false)
    assert(!trimmedDf.apply(0).getString(0).contains("\n"),"This column should not have any line breakers!")
    assert(!trimmedDf.apply(0).getString(0).contains("\r"),"This column should not have any line breakers!")
    assert(!trimmedDf.apply(0).getString(0).contains("\t"),"This column should not have any tab spliter!")
    assert(!trimmedDf.apply(0).getString(0).contains("  "),"This column should not have more than one space!")
    assert(!trimmedDf.apply(1).getString(0).contains("\n"),"This column should not have any line breakers!")
    assert(!trimmedDf.apply(1).getString(0).contains("\r"),"This column should not have any line breakers!")
    assert(!trimmedDf.apply(1).getString(0).contains("\t"),"This column should not have any tab spliter!")
    assert(!trimmedDf.apply(1).getString(0).contains("  "),"This column should not have more than one space!")

    assert(!trimmedDf.apply(2).getString(0).contains("\n"),"This column should not have any line breakers!")
    assert(!trimmedDf.apply(2).getString(0).contains("\r"),"This column should not have any line breakers!")
    assert(!trimmedDf.apply(2).getString(0).contains("\t"),"This column should not have any tab spliter!")
    assert(!trimmedDf.apply(2).getString(0).contains("  "),"This column should not have more than one space!")
  }

}
