import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, pow, sqrt}

object Common {
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

  //задача: оценить расстояние между двумя точками, зная их широту и долготу, сравнивать расстояния будем в пределах одного города,
  //тогда карта города достаточно точно может быть представлена как двумерная x-y плоскость
  //расчитывать расстояние между двумя точками будем по теореме пифагора, где
  //катеты: |x2 - x1| = a и |y2 - y1| = b
  //искомая гипотенуза: sqrt(a^2 + b^2)
  def distanceBetweenTwoPoint(x1: String, x2: String, y1: String, y2: String): Column = sqrt(pow(col(x1) - col(x2),2) + pow(col(y1) - col(y2), 2))

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
}
