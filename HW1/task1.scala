import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import java.io.{File, FileWriter}
import scala.io.Source


object task1 {
  def part_a(lines: RDD[String]): Long = {
    val total_reviews = lines.count()
    println("The total number of reviews is " + total_reviews.toString())
    return total_reviews
  }

  def part_b(lines: RDD[String], y: String): Long = {
    val year_filter = lines.filter((s: String) => compact(parse(s) \ "date").contains(y))
    val year_count = year_filter.count()
    println("The number of reviews in " + y + " is " + year_count.toString())
    return year_count
  }

  def part_c(lines: RDD[String]): Long = {
    val users = lines.map((s: String) => (compact(parse(s) \ "user_id"), 1))
    val users_group = users.groupByKey()
    val users_count: Long = users_group.count()
    return users_count
  }

  def part_d(lines: RDD[String], m: Int): JArray = {
    val users = lines.map((s: String) => (compact(parse(s)\"user_id"), 1))
    val top_users = users.reduceByKey((a: Int, b: Int) => a + b)
    var top_users_col = top_users.collect()
    top_users_col = top_users_col.sortWith(_._2 > _._2)

    val top_m = top_users_col.slice(0, m)

    // Convert output to Json4s JArray format
    var top_m_list: List[JArray] = List()
    for (i <- top_m) {
      top_m_list :+= JArray(List(JString(i._1.replace("\"", "")), i._2))
    }
    val top_m_list1 = JArray(top_m_list)
    return top_m_list1
  }

  def part_e(lines:RDD[String], n: Int, stopwords_file: String): JArray = {
    val stopwords = Source.fromFile(stopwords_file).getLines.toList

    val reviews = (lines.map((s: String) => compact(parse(s) \ "text"))
      .map((line: String) => line.filter(!"([,.!?:;])".contains(_)).toLowerCase()))
    // Remove stopwords by filter as opposed to python

    val counts = (reviews.flatMap((line: String) => line.split(" "))
      .map((word: String) => (word, 1))
      .filter((x: (String, Int)) => !stopwords.contains(x._1) && x._1 != "")
      .reduceByKey((a: Int, b: Int) => a + b)
        .sortBy((x: (String, Int)) => -x._2))
    val count_result = counts.take(n)

    // Convert output to Json4s JArray format
    var top_n_list: List[JString] = List()
    for (i <- count_result) {
      top_n_list :+= JString(i._1.replace("\"", ""))
    }
    val top_n_list1 = JArray(top_n_list)
    return top_n_list1
  }

  def main(args: Array[String]) {
    val ss = SparkSession.builder().appName("task1").config("spark.master", "local[*]").getOrCreate()
    val sc = ss.sparkContext
    sc.setLogLevel("ERROR")
    val m = args(4).toInt
    val output = args(1)
    val y = args(3).toString
    val stopwords_file = args(2)
    val n = args(5).toInt

    val lines = sc.textFile(args(0))
    var answer: JObject = (("A", part_a(lines)) ~ ("B", part_b(lines, y))
      ~ ("C", part_c(lines)) ~ ("D", part_d(lines, m))) ~ ("E", part_e(lines, n, stopwords_file))
    println("Final answer:" + compact(answer))

    val file = new File(output)
    val bw = new FileWriter(file)
    bw.write(compact(answer))
    bw.close()
  }
}
