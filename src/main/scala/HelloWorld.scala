import java.sql.{Date, Timestamp}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{window, _}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//todo rename and move to package
object HelloWorld {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(HelloWorld.getClass.getName)
    val sc = new SparkContext(conf)
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val persons = generatePersonsDataSet
    persons.show

    val places = generatePlacesDataSet
    places.show

//  save data as bucket tables, partitioned by regionId to avoid cross-partition's interaction
    persons.write.bucketBy(50, "regionId").saveAsTable("persons")
    places.write.bucketBy(50, "regionId").saveAsTable("places")

    //фильтруем опечатки по косвенным признакам (широте и долготе)
    val filteredPlaces = filterDuplicatedPlaces(places)
    filteredPlaces.show

    val personsToPlacesWithTheSameRegionIdAndYetActive = joinPersonsAndPlacesWithDistanceColumn(persons, filteredPlaces)

    val topNearestPlaces = 1
    val suggestions = selectTopNNearestPlacesForEachPerson(personsToPlacesWithTheSameRegionIdAndYetActive, topNearestPlaces)

    defineOutputColumns(suggestions).show

//    пауза до ввода ENTER, для просмотра результатов в web ui
//    System.in.read()
    sc.stop
  }

  // есть второй возможный вариант решения: self join таблицы places и использование алгоритма Левенштейна,
  // но он не доработан:
  // 1. как определить оригинал или наиболее близкий к оригиналу вариант?
  // 2. если строк с опечатками несколько (1,2,3,4) как точно определить что 1 и 3 из одного множества? -> 1 == 3' (3 - опечатка 1го названия),
  // a 2 и 4 из другого -> 2 == 4' и вывести, например только 1 и 2, а не 1 и 3
  //    val placesNameEquality = 3
  //      places.as("place_1").joinWith(places.as("place_2"),
  //        levenshtein(col("place_1.name"), col("place_2.name")) > placesNameEquality
  //      )
  //    .select("_1.*")

  // фильтруем список мест от дубликатов (опечатка в названии) по широте и долготе, предполагая что в них опечаток быть не может,
  // но тогда название будет первым попавшимся из множества с уникальной широтой и долготой
  def filterDuplicatedPlaces(ds: Dataset[Place]): Dataset[Place] = ds.dropDuplicates(Seq("latitude", "longitude"))

  // джойним персон с местами по идентификатору области, но при этом, чтобы дата места (последнее обновление по этому месту)
  // было активно на момент посещения персоной
  def joinPersonsAndPlacesWithDistanceColumn(persons: Dataset[Person], places: Dataset[Place]): DataFrame = persons
      .joinWith(places,
        persons("regionId") === places("regionId") &&
        places("firstDayDate") >= persons("firstDayDate")
      )
      .withColumn("distance", distanceBetweenTwoPoint("_1.latitude", "_2.latitude", "_1.longitude", "_2.longitude"))

  // оконной функцией рассчитываем рейтинг удаленности каждого места от персоны и оставляем топ N, где N - параметр функции
  def selectTopNNearestPlacesForEachPerson(df: DataFrame, topN: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val window = Window.partitionBy("_1").orderBy("distance")
    df
      .select('*, rank().over(window) as 'rank)
      .where(col("rank") <= topN)
      .orderBy("_1")
  }

  def defineOutputColumns(ds: DataFrame): DataFrame = {
    ds.select(
        col("_1.id").alias("Идентификатор персоны"),
        col("_2.id").alias("Идентификатор места"),
        col("_2.name").alias("Название места"),
        col("_2.description").alias("Описание места"),
        col("_2.latitude").alias("Широта"),
        col("_2.longitude").alias("Долгота"),
        col("_2.regionId").alias("Идентификатор области"),
        col("_2.firstDayDate").alias("Дата")
      )
  }

  def generatePersonsDataSet(implicit spark: SparkSession): Dataset[Person] = {
    import spark.implicits._
    Seq(
      Person(1, Timestamp.valueOf("2019-02-02 01:00:00.0"), 55.752161, 37.590964, 1, Date.valueOf("2019-02-01")),
      Person(1, Timestamp.valueOf("2019-02-02 05:00:00.0"), 55.769786, 37.601826, 1, Date.valueOf("2019-02-01")),
      Person(1, Timestamp.valueOf("2019-02-02 10:00:00.0"), 55.747496, 37.601826, 2, Date.valueOf("2019-02-01")),
      Person(2, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.757904, 37.597563, 1, Date.valueOf("2019-02-01"))
    ).toDS
  }

  def generatePlacesDataSet(implicit spark: SparkSession): Dataset[Place] = {
    import spark.implicits._
    Seq(
      Place(1, "Государственный академический  малый театр",  "Театр", "Государственный академический малый театр...", 55.760176, 37.619699, 1, Date.valueOf("2019-02-01")),
      Place(2, "Музей Ю. В. Никулина",  "Музей", "Музей Ю. В. Никулина...", 55.757666, 37.634706, 2, Date.valueOf("2019-02-01")),
      Place(3, "Losted house",  "House", "Just losted house...", 55.737666, 37.634706, 1, Date.valueOf("2017-01-01")),
      Place(4, "Bolshoi театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
      Place(5, "Bolshoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
      Place(6, "Bol'shoi театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
      Place(7, "Bol'shoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
      Place(8, "Bolsoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01"))
    ).toDS
  }

  def distanceBetweenTwoPoint(x1: String, x2: String, y1: String, y2: String): Column = sqrt(pow(col(x1) - col(x2),2) + pow(col(y1) - col(y2), 2))
}
