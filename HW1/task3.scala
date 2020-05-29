import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import java.io.{File, FileWriter}
import scala.io.Source


object task3 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("task3").config("spark.master", "local[*]").getOrCreate()
    val sc = ss.sparkContext
    sc.setLogLevel("ERROR")
    val input_file = args(0)
    val output = args(1)
    val part_type = args(2)
    val part = args(3).toInt
    val n = args(4).toInt

    val review_lines = sc.textFile(input_file)
    var review_lines2 = review_lines.map((x: String) => (compact(parse(x) \ "business_id"), 1))

    if (part_type == "customized") {
      review_lines2 = review_lines2.repartition(part)
    }
    // Count and filter for more than n reviews
    val review_lines4 = (review_lines2.reduceByKey((x, y) => x + y)
      .filter(x => x._2 >= n))

    val n_items: Array[Int] = review_lines2.mapPartitions(it => Array(it.size).iterator, true).collect()
    val n_partitions = review_lines2.partitions.length
    val num_reviews = review_lines4.collect()

    // Format results to Json4s format
    var result: List[JArray] = List()
    for (i <- num_reviews) {
      result :+= JArray(List(JString(i._1.replace("\"", "")), i._2))
    }
    val result1 = JArray(result)

    var n_items1: List[JInt] = List()
    for (i <- n_items) {
      n_items1 :+= JInt(i)
    }
    val n_items2 = JArray(n_items1)

    val answer: JObject = (("n_partitions", n_partitions) ~ ("n_items", n_items2) ~ ("result", result1))

    val file = new File(output)
    val fw = new FileWriter(file)
    fw.write(compact(answer))
    fw.close()
  }
}
