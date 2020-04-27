import Common.{generatePlacesDataSet, generatePersonsDataSet, distanceBetweenTwoPoint, filterDuplicatedPlaces}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//todo rename and move to package
object FindNearestPlace {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(FindNearestPlace.getClass.getName)
    val sc = new SparkContext(conf)
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

//    todo прочитать книгу High performance spark чтобы решить проблему с big data

    //фильтруем опечатки по косвенным признакам (широте и долготе)
    val filteredPlaces = filterDuplicatedPlaces(generatePlacesDataSet)

    findNearestPlaces(generatePersonsDataSet, filteredPlaces).show(false)

//    для просмотра результатов выполнения джобы в web ui нужно выставить паузу, иначе SparkContext сразу же закроется вместе с web ui
//    пауза до ввода ENTER в консоли
//    System.in.read()
    sc.stop
  }

  def findNearestPlaces(persons: Dataset[Person], filteredPlaces: Dataset[Place])(implicit spark: SparkSession): DataFrame = {
    //джойним два датасета, добавляем колонку distance - расстояние между персоной и местом
    val personsToPlacesWithTheSameRegionIdAndYetActive = joinPersonsAndPlacesWithDistanceColumn(persons, filteredPlaces)
    //из всех сочетаний персона-место выбираем только самое ближайшее (topNearestPlaces = 1, можно выдавать
    //для каждой записи о персоне топ 5 ближайших мест, тогда topNearestPlaces = 5)
    val topNearestPlaces = 1
    val suggestions = selectTopNNearestPlacesForEachPerson(personsToPlacesWithTheSameRegionIdAndYetActive, topNearestPlaces)

    //для красоты вывода маппим поля в получившемся датафрейме на описанные в задании
    defineOutputColumns(suggestions)
  }

  // джойним персон с местами по идентификатору области (нет смысла предлагать персоне достопримечательности в другом городе),
  // дата места - последнее обновление по этому месту, если дата места давно не обновлялась предполагаем,
  // что место закрылось/на реконструкции -> нельзя его рекомендовать, нужно чтобы место было активно на момент посещения персоной
  def joinPersonsAndPlacesWithDistanceColumn(persons: Dataset[Person], places: Dataset[Place]): DataFrame = persons
      .joinWith(places,
        persons("regionId") === places("regionId") &&
        places("firstDayDate") >= persons("firstDayDate")
      )
      .withColumn("distance", distanceBetweenTwoPoint("_1.latitude", "_2.latitude", "_1.longitude", "_2.longitude"))

  // имея сочетания запись о персоне - место - расстояние между ними, оконной функцией рассчитываем рейтинг удаленности
  // для каждого сочетания персона-место, оставляем топ N самых близких мест для персоны, где N - параметр функции
  def selectTopNNearestPlacesForEachPerson(df: DataFrame, topN: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val window = Window.partitionBy("_1").orderBy("distance")
    df
      .select('*, rank().over(window) as 'rank)
      .where(col("rank") <= topN)
      .orderBy("_1")
  }

  // маппим "технические" имена колонок на "бизнесовые"
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
}
