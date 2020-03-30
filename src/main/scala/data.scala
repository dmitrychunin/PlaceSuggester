import java.sql.{Date, Timestamp}

case class Person(id: Long, time: Timestamp, latitude: Double, longitude: Double, regionId: Long, firstDayDate: Date)
//todo use enum for category
case class Place(id: Long, name: String, category: String, description: String, latitude: Double, longitude: Double, regionId: Long, firstDayDate: Date)

object PlaceCategory extends Enumeration {
  type PlaceCategory = Value
  val Музей, Театр = Value
}