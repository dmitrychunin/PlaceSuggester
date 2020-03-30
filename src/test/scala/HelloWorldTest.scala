import java.sql.{Date, Timestamp}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

//todo rename and move to package
class HelloWorldTest extends FunSuite with SharedSparkContext with DatasetSuiteBase {
  override implicit def reuseContextIfPossible: Boolean = true
//  todo fix
  lazy override val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

//  todo use DatasetGenerator?
  val places: Dataset[Place] = Seq(
      Place(1, "Государственный академический  малый театр",  "Театр", "Государственный академический малый театр...", 55.760176, 37.619699, 1, Date.valueOf("2019-02-01")),
      Place(2, "Музей Ю. В. Никулина",  "Музей", "Музей Ю. В. Никулина...", 55.757666, 37.634706, 2, Date.valueOf("2019-02-01")),
      Place(3, "Losted house",  "House", "Just losted house...", 55.737666, 37.634706, 1, Date.valueOf("2017-01-01")),
      Place(4, "Bolshoi театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
      Place(5, "Bolshoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
      Place(6, "Bol'shoi театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
      Place(7, "Bol'shoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
      Place(8, "Bolsoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01"))
    ).toDS

  val distinctPlaces: Dataset[Place] = Seq(
    Place(1, "Государственный академический  малый театр",  "Театр", "Государственный академический малый театр...", 55.760176, 37.619699, 1, Date.valueOf("2019-02-01")),
    Place(2, "Музей Ю. В. Никулина",  "Музей", "Музей Ю. В. Никулина...", 55.757666, 37.634706, 2, Date.valueOf("2019-02-01")),
    Place(3, "Losted house",  "House", "Just losted house...", 55.737666, 37.634706, 1, Date.valueOf("2017-01-01")),
    Place(4, "Bolshoi театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01"))
  ).toDS

  val persons: Dataset[Person] = Seq(
      Person(1, Timestamp.valueOf("2019-02-02 01:00:00.0"), 55.752161, 37.590964, 1, Date.valueOf("2019-02-01")),
      Person(1, Timestamp.valueOf("2019-02-02 05:00:00.0"), 55.769786, 37.601826, 1, Date.valueOf("2019-02-01")),
      Person(1, Timestamp.valueOf("2019-02-02 10:00:00.0"), 55.747496, 37.601826, 2, Date.valueOf("2019-02-01")),
      Person(2, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.757904, 37.597563, 1, Date.valueOf("2019-02-01"))
    ).toDS

  test("filterDuplicatedPlaces should filter places with not distinct latitude and longitude combination") {
    val placesWithDistinctLatitudeAndLongitude = HelloWorld.filterDuplicatedPlaces(places)
    assert(places.count() == 8)
    assert(placesWithDistinctLatitudeAndLongitude.count() == 4)
//    assertDatasetEquals(distinctPlaces, placesWithDistinctLatitudeAndLongitude)
//    todo compare dataset with dataset suite base
  }

  test("joinPersonsAndPlacesWithDistanceColumn should join two dataset and add distance column") {
    val dataFrame = HelloWorld.joinPersonsAndPlacesWithDistanceColumn(persons, distinctPlaces)
//    val frame = spark.read.json("src/test/resources/exam.json")
    assert(dataFrame.columns.length == 3)
    assert(dataFrame.count() == 7)
//    assertDataFrameEquals(frame, dataFrame)
    //    todo compare dataset with dataset suite base
  }

  test("selectTopNNearestPlacesForEachPerson should keep only top N nearest to person places") {
    val dataFrame = HelloWorld.selectTopNNearestPlacesForEachPerson(HelloWorld.joinPersonsAndPlacesWithDistanceColumn(persons, distinctPlaces), 1)(spark)
    assert(dataFrame.count() == 4)
    //    todo compare dataset with dataset suite base
  }

}
