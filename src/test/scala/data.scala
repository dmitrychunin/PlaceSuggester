import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types._


//todo use auto instead manual
//case class JoinedPersonWithPlaces(_1: Person, _2: Place, distance: Double)
//val autoJoinedPersonWithPlacesSchema = Encoders.product[JoinedPersonWithPlaces].schema
//all is nullable=true, because reading from json result schema is always nullable=true
object data {
  val manualJoinedPersonWithPlacesSchema: StructType = StructType(Array(
    StructField("_1", StructType(Array(
      StructField("id", LongType, nullable = true),
      StructField("time", TimestampType, nullable = true),
      StructField("latitude", DoubleType, nullable = true),
      StructField("longitude", DoubleType, nullable = true),
      StructField("regionId", LongType, nullable = true),
      StructField("firstDayDate", DateType, nullable = true)
    )), nullable=true),
    StructField("_2", StructType(Array(
      StructField("id", LongType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("latitude", DoubleType, nullable = true),
      StructField("longitude", DoubleType, nullable = true),
      StructField("regionId", LongType, nullable = true),
      StructField("firstDayDate", DateType, nullable = true)
    )), nullable=true),
    StructField("distance", DoubleType, nullable=true)
  ))

  val personRecordToPlaceSchema: StructType = StructType(Array(
    StructField("_1", StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("time", TimestampType, nullable = true),
      StructField("latitude", DoubleType, nullable = false),
      StructField("longitude", DoubleType, nullable = false),
      StructField("regionId", LongType, nullable = false),
      StructField("firstDayDate", DateType, nullable = true)
    )), nullable=false),
    StructField("_2", StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("latitude", DoubleType, nullable = false),
      StructField("longitude", DoubleType, nullable = false),
      StructField("regionId", LongType, nullable = false),
      StructField("firstDayDate", DateType, nullable = true)
    )), nullable=true)
  ))

  val topNearestPlacesForEachPersonSchema: StructType = StructType(Array(
    StructField("_1", StructType(Array(
      StructField("id", LongType, nullable = true),
      StructField("time", TimestampType, nullable = true),
      StructField("latitude", DoubleType, nullable = true),
      StructField("longitude", DoubleType, nullable = true),
      StructField("regionId", LongType, nullable = true),
      StructField("firstDayDate", DateType, nullable = true)
    )), nullable=true),
    StructField("_2", StructType(Array(
      StructField("id", LongType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("latitude", DoubleType, nullable = true),
      StructField("longitude", DoubleType, nullable = true),
      StructField("regionId", LongType, nullable = true),
      StructField("firstDayDate", DateType, nullable = true)
    )), nullable=true),
    StructField("distance", DoubleType, nullable=true),
    StructField("rank", IntegerType, nullable=true)
  ))

  val personRecommendedPlaceSchema: StructType = StructType(Array(
    StructField("personId1", LongType, nullable = true),
    StructField("placeId2", LongType, nullable = true),
    StructField("similarCount", DoubleType, nullable = true)
  ))

  val placesSeq = Seq(
    Place(1, "Государственный академический  малый театр",  "Театр", "Государственный академический малый театр...", 55.760176, 37.619699, 1, Date.valueOf("2019-02-01")),
    Place(2, "Музей Ю. В. Никулина",  "Музей", "Музей Ю. В. Никулина...", 55.757666, 37.634706, 2, Date.valueOf("2019-02-01")),
    Place(3, "Losted house",  "House", "Just losted house...", 55.737666, 37.634706, 1, Date.valueOf("2017-01-01")),
    Place(4, "Bolshoi театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
    Place(5, "Bolshoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
    Place(6, "Bol'shoi театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
    Place(7, "Bol'shoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
    Place(8, "Bolsoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01"))
  )

  val distinctPlacesSeq = Seq(
    Place(1, "Государственный академический  малый театр",  "Театр", "Государственный академический малый театр...", 55.760176, 37.619699, 1, Date.valueOf("2019-02-01")),
    Place(2, "Музей Ю. В. Никулина",  "Музей", "Музей Ю. В. Никулина...", 55.757666, 37.634706, 2, Date.valueOf("2019-02-01")),
    Place(3, "Losted house",  "House", "Just losted house...", 55.737666, 37.634706, 1, Date.valueOf("2017-01-01")),
    Place(4, "Bolshoi театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01"))
  )

  val personsSeq = Seq(
    Person(1, Timestamp.valueOf("2019-02-02 01:00:00.0"), 55.752161, 37.590964, 1, Date.valueOf("2019-02-01")),
    Person(1, Timestamp.valueOf("2019-02-02 05:00:00.0"), 55.769786, 37.601826, 1, Date.valueOf("2019-02-01")),
    Person(1, Timestamp.valueOf("2019-02-02 10:00:00.0"), 55.747496, 37.601826, 2, Date.valueOf("2019-02-01")),
    Person(2, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.757904, 37.597563, 1, Date.valueOf("2019-02-01"))
  )

  val placesSeq2 = Seq(
    Place(1, "Государственный академический  малый театр",  "Театр", "Государственный академический малый театр...", 55.760176, 37.619699, 1, Date.valueOf("2019-02-01")),
    Place(2, "Музей Ю. В. Никулина",  "Музей", "Музей Ю. В. Никулина...", 55.757666, 37.634706, 2, Date.valueOf("2019-02-01")),
    Place(3, "Дореволюционная усадьба",  "Дом", "Дореволюционная усадьба...", 55.737666, 37.634706, 1, Date.valueOf("2019-02-01")),
    Place(4, "Большой театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
    Place(5, "Bolshoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
    Place(6, "Bol'shoi театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
    Place(7, "Bol'shoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
    Place(8, "Bolsoy театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
    Place(8, "Консерватория",  "Театр", "Big театр...", 55.765297, 37.616578, 1, Date.valueOf("2019-02-01")),
    Place(9, "Кремль",  "Театр", "Big театр...", 55.775623, 37.612359, 1, Date.valueOf("2019-02-01")),
    Place(10, "Losted house",  "House", "Just losted house...", 55.458692, 37.967493, 1, Date.valueOf("2017-01-01"))
  )

  val distinctPlacesSeq2 = Seq(
    Place(1, "Государственный академический  малый театр",  "Театр", "Государственный академический малый театр...", 55.760176, 37.619699, 1, Date.valueOf("2019-02-01")),
    Place(2, "Музей Ю. В. Никулина",  "Музей", "Музей Ю. В. Никулина...", 55.757666, 37.634706, 2, Date.valueOf("2019-02-01")),
    Place(3, "Дореволюционная усадьба",  "Дом", "Дореволюционная усадьба...", 55.737666, 37.634706, 1, Date.valueOf("2019-02-01")),
    Place(4, "Большой театр",  "Театр", "Big театр...", 55.770176, 37.619123, 1, Date.valueOf("2019-02-01")),
    Place(5, "Консерватория",  "Театр", "Big театр...", 55.765297, 37.616578, 1, Date.valueOf("2019-02-01")),
    Place(6, "Кремль",  "Театр", "Big театр...", 55.775623, 37.612359, 1, Date.valueOf("2019-02-01")),
    Place(7, "Losted house",  "House", "Just losted house...", 55.458692, 37.967493, 1, Date.valueOf("2017-01-01"))
  )

  val personsSeq2 = Seq(
    Person(1, Timestamp.valueOf("2019-02-02 01:00:00.0"), 55.760186, 37.619789, 1, Date.valueOf("2019-02-01")), //1
    Person(1, Timestamp.valueOf("2019-02-02 05:00:00.0"), 55.757766, 37.634806, 2, Date.valueOf("2019-02-01")), //2
    Person(1, Timestamp.valueOf("2019-02-02 10:00:00.0"), 55.737766, 37.634806, 1, Date.valueOf("2019-02-01")), //3
    Person(2, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.757566, 37.634606, 2, Date.valueOf("2019-02-01")), //2
    Person(2, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.737766, 37.634806, 1, Date.valueOf("2019-02-01")), //3
    Person(2, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.770276, 37.619133, 1, Date.valueOf("2019-02-01")), //4
    Person(2, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.789234, 37.236724, 1, Date.valueOf("2019-02-01")), //неизвестное место
    Person(3, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.757698, 37.634567, 2, Date.valueOf("2019-02-01")), //2
    Person(3, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.737703, 37.634759, 1, Date.valueOf("2019-02-01")), //3
    Person(4, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.760154, 37.619736, 1, Date.valueOf("2019-02-01")), //1
    Person(4, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.765312, 37.616623, 1, Date.valueOf("2019-02-01")), //5
    Person(4, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.775573, 37.612323, 1, Date.valueOf("2019-02-01")), //6
    Person(5, Timestamp.valueOf("2019-02-01 07:00:00.0"), 55.760186, 37.619789, 1, Date.valueOf("2019-02-01"))  //2
  )
}



