import com.holdenkarau.spark.testing.{DatasetSuiteBase, SharedSparkContext}
import data.{distinctPlacesSeq, manualJoinedPersonWithPlacesSchema, personsSeq, placesSeq, topNearestPlacesForEachPersonSchema, personsSeq2, placesSeq2, distinctPlacesSeq2, personRecordToPlaceSchema, personRecommendedPlaceSchema}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

//todo rename and move to package
class FindNearestPlaceTest extends FunSuite with SharedSparkContext with DatasetSuiteBase with BeforeAndAfter {
  override implicit def reuseContextIfPossible: Boolean = true
//  todo fix
  lazy override val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

  var places: Dataset[Place] = _
  var distinctPlaces: Dataset[Place] = _
  var persons: Dataset[Person] = _
  var joinedPersonWithPlaces: DataFrame = _
  var topNearestPlacesForEachPerson: DataFrame = _

  before {
    places = sc.parallelize(placesSeq).toDS
    distinctPlaces = sc.parallelize(distinctPlacesSeq).toDS
    persons = sc.parallelize(personsSeq).toDS
    joinedPersonWithPlaces = spark.read.schema(manualJoinedPersonWithPlacesSchema).option("multiline", "true").json("src/test/resources/joinedPersonWithPlaces.json")
    topNearestPlacesForEachPerson = spark.read.schema(topNearestPlacesForEachPersonSchema).option("multiline", "true").json("src/test/resources/topNearestPlacesForEachPerson.json")
  }

  test("filterDuplicatedPlaces should filter places with not distinct latitude and longitude combination") {
    val placesWithDistinctLatitudeAndLongitude: Dataset[Place] = Common.filterDuplicatedPlaces(places)
    assert(places.count() == 8)
    assert(placesWithDistinctLatitudeAndLongitude.count() == 4)
    assertDatasetApproximateEquals(distinctPlaces, placesWithDistinctLatitudeAndLongitude.orderBy("id"), 0.01)
  }

  test("joinPersonsAndPlacesWithDistanceColumn should join two dataset and add distance column") {
    val joinPersonsAndPlacesWithDistance = FindNearestPlace.joinPersonsAndPlacesWithDistanceColumn(persons, distinctPlaces)
    val actualJoinedDF = joinPersonsAndPlacesWithDistance.sqlContext.createDataFrame(joinPersonsAndPlacesWithDistance.rdd, manualJoinedPersonWithPlacesSchema)
    val expectedJoinedDF = joinedPersonWithPlaces
    assert(actualJoinedDF.columns.length == 3)
    assert(actualJoinedDF.count() == 7)
    assert(expectedJoinedDF.schema == actualJoinedDF.schema)
    assertDataFrameApproximateEquals(expectedJoinedDF.orderBy("distance"), actualJoinedDF.orderBy("distance"), 0.01)
  }

  test("selectTopNNearestPlacesForEachPerson should keep only top N nearest to person places") {
    val actualTopNearestDF = FindNearestPlace.selectTopNNearestPlacesForEachPerson(joinedPersonWithPlaces, 1)(spark)
    assert(actualTopNearestDF.count() == 4)
    val expectedTopNearestDF = topNearestPlacesForEachPerson
    assertDataFrameApproximateEquals(actualTopNearestDF.orderBy("distance"), expectedTopNearestDF.orderBy("distance"), 0.01)
  }
}
