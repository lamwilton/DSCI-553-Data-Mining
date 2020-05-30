import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import java.io.{File, FileWriter}
import scala.io.Source

object task2 {
  def no_spark(review_file: String, business_file: String, n:Int): JArray = {

    val review_lines = Source.fromFile(review_file, enc="utf-8").getLines.toList
    val reviews = review_lines.map((x: String) => (convertstr(parse(x) \ "business_id"), compact(parse(x) \ "stars").toDouble))
    val reviews2 = reviews.groupBy(_._1).mapValues(_.map { case (_, b) => b })  // Group by key equiv
    val review_avg = reviews2.map(x => average_helper(x))

    val business_lines = Source.fromFile(business_file, enc="utf-8").getLines.toList
    val business_cat = (business_lines.filter(s => convertstr(parse(s) \ "categories") != "null")
      .map(s => (convertstr(parse(s) \ "business_id"), convertstr(parse(s) \ "categories").split(", "))))
    val business_cat_map = business_cat.toMap

    // Inner Joining both maps, output as Array[Array[String], Float]
    val cat_scores = review_avg.collect { case (k, v) if business_cat_map.contains(k) => (business_cat_map(k), v) }
    // Flatten the String Array of categories, output Array[(String, Float)]
    var cat_scores1: Array[(String, Float)] = Array()
    for (i<- cat_scores) {
      for (j <- i._1) {
        cat_scores1 :+= (j, i._2.toFloat)
      }
    }
    val cat_scores2 = cat_scores1.groupBy(_._1).mapValues(_.map { case (_, b) => b })  // Group by key
    // Average and sort
    var cat_scores3 = cat_scores2.map(x => (x._1, (x._2.sum / x._2.length))).toList.sortBy((x: (String, Float)) => (-x._2, x._1))
    cat_scores3 = cat_scores3.slice(0, n)

    // Format results to Json4s format
    var result: List[JArray] = List()
    for (i <- cat_scores3) {
      result :+= JArray(List(JString(i._1.replace("\"", "")), i._2))
    }
    val result1 = JArray(result)
    return result1
  }

  def flatmapping2(in: (Array[String], Double)): Array[(String, Double)] = {
    var out: Array[(String, Double)] = Array()
    for (i <- in._1) {
      out :+= (i, in._2)
    }
    return out
  }

  def average_helper(in: (String, List[Double])): (String, Double) = {
    return (in._1, (in._2.sum / in._2.length).toDouble)
  }

  // Helper method to compute average by keys
  def spark_average(rdd: RDD[(String, Double)]): RDD[(String, Double)] = {
    return (rdd.map((x: (String, Double)) => (x._1, (x._2, 1)))
    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    .mapValues(x => x._1 / x._2))
  }

  def use_spark(review_file: String, business_file: String, n:Int): JArray = {
    val ss = SparkSession.builder().appName("task2").config("spark.master", "local[*]").getOrCreate()
    val sc = ss.sparkContext
    sc.setLogLevel("ERROR")

    val review_lines = sc.textFile(review_file)
    val reviews = review_lines.map((x: String) => (convertstr(parse(x) \ "business_id"), compact(parse(x) \ "stars").toDouble))
    val review_avg = spark_average(reviews)

    val business_lines = sc.textFile(business_file)
    val business_cat = (business_lines.filter(s => convertstr(parse(s) \ "categories") != "null")
      .map(s => (convertstr(parse(s) \ "business_id"), convertstr(parse(s) \ "categories").split(", "))))
    val cat_scores = review_avg.join(business_cat).map(x => (x._2._2, x._2._1))
    var cat_scores2 = cat_scores.flatMap(x => flatmapping(x))
    cat_scores2 = spark_average(cat_scores2)
    val cat_scores3 = cat_scores2.sortBy((x: (String, Double)) => (-x._2, x._1)).take(n)

    // Format results to Json4s format
    var result: List[JArray] = List()
    for (i <- cat_scores3) {
      result :+= JArray(List(JString(i._1.replace("\"", "")), i._2))
    }
    val result1 = JArray(result)
    return result1
  }

  // Helper method of converting JString to String to remove extra quotes
  def convertstr(in: JValue): String = {
    return compact(in).replace("\"", "")
  }

  // Helper method for flatmapping to Ungrouping the keys convert (["Restaurant", "Shop"], 5.0) to (["Restaurant"], 5.0), (["Shop"], 5.0)
  def flatmapping(in: (Array[String], Double)): Array[(String, Double)] = {
    var out: Array[(String, Double)] = Array()
    for (i <- in._1) {
      out :+= (i, in._2)
    }
    return out
  }

  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime

    val review_file = args(0)
    val business_file = args(1)
    val output = args(2)
    val if_spark = args(3)
    val n = args(4).toInt

    var result = JArray(List())
    if (if_spark == "no_spark") {
      result = no_spark(review_file, business_file, n)
    }
    else {
      result = use_spark(review_file, business_file, n)
    }
    val answer: JObject = ("result", result)

    val file = new File(output)
    val fw = new FileWriter(file)
    fw.write(compact(answer))
    fw.close()

    println("Answer written to output")
    val duration = (System.nanoTime - t1) / 1e9d
    println("Time taken in s: " + duration)
  }
}
