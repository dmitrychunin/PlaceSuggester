import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(HelloWorld.getClass.getName)
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

//    todo time is not in appropriate format
//    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HH")
    val personDS = Seq(
        Person(1, Timestamp.valueOf("2019-02-02 01:00:00.0"), 55.752161, 37.590964, 1, Date.valueOf("2019-02-02")),
        Person(1, Timestamp.valueOf("2019-02-02 10:00:00.0"), 55.747496, 37.601826, 2, Date.valueOf("2019-02-02")),
        Person(2, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.757904, 37.597563, 1, Date.valueOf("2019-02-02"))
    ).toDS()

    personDS.show()
    val placeDS = Seq(
            Place(1, "Государственный академический  малый театр",  "Театр", "Государственный академический малый театр...", 55.760176, 37.619699, 1, Date.valueOf("2019-02-01")),
            Place(2, "Музей Ю. В. Никулина",  "Музей", "Музей Ю. В. Никулина...", 55.757666, 37.634706, 1, Date.valueOf("2019-02-01"))
    ).toDS()
    placeDS.show()

    // terminate spark context
    sc.stop()
  }
}
