import java.sql.{Date, Timestamp}

case class Person(id: Long, time: Timestamp, latitude: Double, longtitude: Double, placeId: Long, firstDayDate: Date)
case class Person2(id: Long,  latitude: Double, longtitude: Double, placeId: Long)
//todo use enum for category
case class Place(id: Long, name: String, category: String, description: String, latitude: Double, longtitude: Double, placeId: Long, firstDayDate: Date)

object PlaceCategory extends Enumeration {
  type PlaceCategory = Value
  val Музей, Театр = Value
}