import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(WordCount.getClass.getName)
    val sc = new SparkContext(conf)
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val textFile = sc.textFile("hdfs://127.0.0.1:9000/input")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://127.0.0.1:9000/output")

    sc.stop
  }
}
