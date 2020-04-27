import com.holdenkarau.spark.testing.{DatasetSuiteBase, SharedSparkContext}
import data.{distinctPlacesSeq2, personRecommendedPlaceSchema, personRecordToPlaceSchema, personsSeq2, placesSeq2}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SuggestUnvisitedPlacesFromSimilarPersonsTest extends FunSuite with SharedSparkContext with DatasetSuiteBase with BeforeAndAfter {
  //  todo fix
  lazy override val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

  var places2: Dataset[Place] = _
  var distinctPlaces2: Dataset[Place] = _
  var persons2: Dataset[Person] = _
  var personRecordToPlace: DataFrame = _
  var personRecommendedPlace: DataFrame = _

  before {
    places2 = sc.parallelize(placesSeq2).toDS
    distinctPlaces2 = sc.parallelize(distinctPlacesSeq2).toDS
    persons2 = sc.parallelize(personsSeq2).toDS
    personRecordToPlace = spark.read.schema(personRecordToPlaceSchema).option("multiline", "true").json("src/test/resources/personRecordToPlace.json")
    personRecommendedPlace = spark.read.schema(personRecommendedPlaceSchema).option("multiline", "true").json("src/test/resources/personRecommendedPlace.json")
  }

  test("should suggest for each person places which visited by other person with similar") {
    val actual = SuggestUnvisitedPlacesFromSimilarPersons.suggestForEachPersonPlacesWhichVisitedByOtherPersonWithSimilar(persons2, distinctPlaces2, 0.6, 2, 0.001)(spark)
    val actualWithExpectedSchema = spark.sqlContext.createDataFrame(actual.rdd, personRecommendedPlaceSchema)
    assert(actualWithExpectedSchema.schema == personRecommendedPlace.schema)
    assertDataFrameApproximateEquals(personRecommendedPlace.orderBy("personId1", "similarCount"), actualWithExpectedSchema,0.001)
  }
}
