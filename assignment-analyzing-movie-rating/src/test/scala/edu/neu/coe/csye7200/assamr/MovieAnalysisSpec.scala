package edu.neu.coe.csye7200.assamr

import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.tagobjects.Slow
import org.scalatest.{BeforeAndAfter, flatspec}

import scala.util.{Success, Try}

class MovieAnalysisSpec extends flatspec.AnyFlatSpec with Matchers with BeforeAndAfter {
  implicit var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .appName("MovieAnalysis")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  after {
    if (spark != null) spark.stop()
  }

  behavior of "Spark"

  it should "test mean" taggedAs Slow in {
    val triedPath = Try(getClass.getResource("movie_metadata.csv").getPath.replaceAll("%20", " "))
    triedPath.isSuccess shouldBe true
    for (path <- triedPath)
      (for (x <- MovieAnalysis.mean(spark.read.option("header", true).csv(path))) yield x) shouldBe Success(6.453200745804848)
  }

  it should "test std_dev" taggedAs Slow in {
    val triedPath = Try(getClass.getResource("movie_metadata.csv").getPath.replaceAll("%20", " "))
    triedPath.isSuccess shouldBe true
    for (path <- triedPath)
      (for (x <- MovieAnalysis.std_dev(spark.read.option("header", true).csv(path))) yield x) shouldBe Success(0.9988071293753289)
  }
}
