package edu.neu.coe.csye7200.assamr

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Success, Try}
import org.apache.spark.sql.functions.{avg, stddev}

object MovieAnalysis extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("MovieAnalysis")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val rAbsolute = """(/.*)""".r
  val filename = args.headOption.getOrElse("movie_metadata.csv")
  val path: String = filename match {
    case rAbsolute(fullPath) => fullPath
    case _ =>
      (for (uo <- Try(Option(getClass.getResource(filename)))) yield for (u <- uo) yield u) match {
        case Success(Some(p)) => p.getPath.replaceAll("%20", " ")
        case _ => throw new Exception(s"cannot get resource: $filename")
      }
  }

  val df = spark.read.option("header", true).csv(path)
  def mean(df: DataFrame): Try[Double] = Try(df.select(avg("imdb_score")).first().getDouble(0))
  def std_dev(df: DataFrame): Try[Double] = Try(df.select(stddev("imdb_score")).first().getDouble(0))
  for (x <- mean(df)) println(s"mean: $x")
  for (x <- std_dev(df)) println(s"std_dev: $x")
}
