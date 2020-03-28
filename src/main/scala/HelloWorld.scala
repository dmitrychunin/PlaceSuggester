import java.sql.{Date, Timestamp}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(HelloWorld.getClass.getName)
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

//    todo time is not in appropriate format
//    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HH")
    val persons = Seq(
        Person(1, Timestamp.valueOf("2019-02-02 01:00:00.0"), 55.752161, 37.590964, 1, Date.valueOf("2019-02-01")),
        Person(1, Timestamp.valueOf("2019-02-02 05:00:00.0"), 55.769786, 37.601826, 1, Date.valueOf("2019-02-01")),
        Person(1, Timestamp.valueOf("2019-02-02 10:00:00.0"), 55.747496, 37.601826, 2, Date.valueOf("2019-02-01")),
        Person(2, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.757904, 37.597563, 1, Date.valueOf("2019-02-01"))
    ).toDS
    persons.show

    val places = Seq(
            Place(1, "Государственный академический  малый театр",  "Театр", "Государственный академический малый театр...", 55.760176, 37.619699, 1, Date.valueOf("2019-02-01")),
            Place(2, "Музей Ю. В. Никулина",  "Музей", "Музей Ю. В. Никулина...", 55.757666, 37.634706, 1, Date.valueOf("2019-02-01")),
            Place(3, "Losted house",  "House", "Just losted house...", 55.757666, 37.634706, 1, Date.valueOf("2017-01-01")),
            Place(4, "Bolshoi театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01"))
    ).toDS
    places.show

    val joined = persons
      .joinWith(places,
        persons("regionId") === places("regionId") &&
        places("firstDayDate") >= persons("firstDayDate")
      )
      .withColumn("distance", distanceBetweenTwoPoint("_1.latitude", "_2.latitude", "_1.longtitude", "_2.longtitude"))
    joined.orderBy("_1").show

    val showTopNearestPlaces = 1
    val window = Window.partitionBy("_1").orderBy("distance")
    joined.select('*, rank().over(window) as 'rank)
      .where(col("rank") <= showTopNearestPlaces)
      .orderBy("_1")
      .select(
        col("_1.id").alias("person id"),
        col("_2.id").alias("place id"),
        col("_2.name").alias("name"),
        col("_2.description").alias("description"),
        col("_2.latitude").alias("place latitude"),
        col("_2.longtitude").alias("place longtitude"),
        col("_2.regionId").alias("place region id"),
        col("_2.firstDayDate").alias("place date")
      )
      .show

    sc.stop
  }

  def distanceBetweenTwoPoint(x1: String, x2: String, y1: String, y2: String): Column = sqrt(pow(col(x1) - col(x2),2) + pow(col(y1) - col(y2), 2))
}
