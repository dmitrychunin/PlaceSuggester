import Common.{generatePlacesDataSet, generatePersonsDataSet, distanceBetweenTwoPoint, filterDuplicatedPlaces}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SuggestUnvisitedPlacesFromSimilarPersons {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(SuggestUnvisitedPlacesFromSimilarPersons.getClass.getName)
    val sc = new SparkContext(conf)
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //    todo прочитать книгу High performance spark чтобы решить проблему с big data

    //фильтруем опечатки по косвенным признакам (широте и долготе)
    val filteredPlaces = filterDuplicatedPlaces(generatePlacesDataSet)

    val similarityThreshold = 0.6
    val minimalCountOfPlacesToCalcSimilarityWithOtherPersons = 2
    val equalCoordinateThreshold = 0.001
    suggestForEachPersonPlacesWhichVisitedByOtherPersonWithSimilar(generatePersonsDataSet, filteredPlaces, similarityThreshold, minimalCountOfPlacesToCalcSimilarityWithOtherPersons, equalCoordinateThreshold)


    //    для просмотра результатов выполнения джобы в web ui нужно выставить паузу, иначе SparkContext сразу же закроется вместе с web ui
    //    пауза до ввода ENTER в консоли
    //    System.in.read()
    sc.stop
  }

  def suggestForEachPersonPlacesWhichVisitedByOtherPersonWithSimilar(persons: Dataset[Person], filteredPlaces: Dataset[Place], similarityThreshold: Double, minimalCountOfPlacesToCalcSimilarityWithOtherPersons: Int, equalCoordinateThreshold: Double)(implicit spark: SparkSession): DataFrame = {
    val personsToPlaces = findPersonPlaces(persons, filteredPlaces, equalCoordinateThreshold)
//      .orderBy("personId", "placeId")
//    println("personsToPlaces:")
//    personsToPlaces.show(false)
    val countOfPersonPlacesDf = countPersonPlacesAndFilterUnderThreshold(personsToPlaces, minimalCountOfPlacesToCalcSimilarityWithOtherPersons)
//  .orderBy("personId", "placeId")
//    println("countOfPersonPlacesDf:")
//    countOfPersonPlacesDf.show(false)
    val personsToPersonSimilarPlacesCountDf = countEachOtherPersonSimilarPlaces(personsToPlaces)
//      .orderBy("personId1", "personId2")
//    println("personsToPersonSimilarPlacesCountDf:")
//    personsToPersonSimilarPlacesCountDf.show(false)
    val filteredByThresholdSimilarityOfEachOtherPersonDf = findSimilarCountAndFilterUnderThreshold(countOfPersonPlacesDf, personsToPersonSimilarPlacesCountDf, similarityThreshold)
//    println("filteredByThresholdSimilarityOfEachOtherPersonDf:")
//    filteredByThresholdSimilarityOfEachOtherPersonDf.show(false)
    val personsToPersonWithUnfamiliarPlacesDf = countEachOtherPersonUnfamiliarPlaces(personsToPlaces, countOfPersonPlacesDf)
//      .orderBy("personId1", "personId2", "placeId2")
//    println("personsToPersonWithUnfamiliarPlacesDf:")
//    personsToPersonWithUnfamiliarPlacesDf.show(false)
    joinToEachPersonUnfamiliarPlacesOfDifferentPersonsWithHighSimilarCount(filteredByThresholdSimilarityOfEachOtherPersonDf, personsToPersonWithUnfamiliarPlacesDf)
      .orderBy("personId1", "similarCount")
//    println("resultDf:")
//    resultDf.show(false)
  }

//каждой персоне сопоставляем по координатам место, которое она посещала
  def findPersonPlaces(persons: Dataset[Person], places: Dataset[Place], equalCoordinateThreshold: Double): DataFrame = persons.as("pr")
    .joinWith(places.as("pl"),
      col("pr.regionId") === col("pl.regionId") &&
        col("pl.firstDayDate") >= col("pr.firstDayDate") &&
        distanceBetweenTwoPoint("pr.latitude", "pl.latitude", "pr.longitude", "pl.longitude") <= equalCoordinateThreshold
    ).select(
    col("_1.id").alias("personId"),
    col("_2.id").alias("placeId")
  )

//  для каждой персоны считаем сколько мест она посетила и фильтруем персоны, посетившие недостаточно мест, для участия в рекомендации
//  (подразумевается что чем больше мест посетила персона тем более полно этот набор описывает тип посетителя и тем вероятнее неявная взаимосвязь посещаемых мест)
  def countPersonPlacesAndFilterUnderThreshold(ds: DataFrame, minimalCountOfPlacesToCalcSimilarityWithOtherPersons: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val window = Window.partitionBy("personId")
    ds
      .select('*, count('*).over(window) as 'countOfPersonPlaces)
      .where(col("countOfPersonPlaces") >= minimalCountOfPlacesToCalcSimilarityWithOtherPersons)
      //      todo можно ли сгруппировать без дублей?
      .dropDuplicates("personId", "countOfPersonPlaces")
  }

  //    каждой записи о персоне1 сопоставляется запись о персоне2, посещавшей это же место
  def countEachOtherPersonSimilarPlaces(personsToPlaces: DataFrame): DataFrame = {
    //    self-join записей персона-место друг к другу, где персоны разные, а места одинаковые
    personsToPlaces.as('p1)
      .join(personsToPlaces.as('p2), (col("p1.placeId") === col("p2.placeId")) && (col("p1.personId") !== col("p2.personId")))
      .select(col("p1.personId").alias("personId1"), col("p2.personId").alias("personId2"), count("*").over(Window.partitionBy("p1.personId", "p2.personId")) as "similarPlacesWithOtherPersonCount")
      .dropDuplicates("personId1", "personId2")
  }

  //    подсчитываем для каждой пары персон "похожесть" их посещаемых мест
  def findSimilarCountAndFilterUnderThreshold(countOfPersonPlacesDf: DataFrame, personsToPersonSimilarPlacesCountDf: DataFrame, similarityThreshold: Double): DataFrame = {
    //    join суммарного числа посещенных мест каждой персоны к количеству схожих мест других персон
    countOfPersonPlacesDf
      .joinWith(personsToPersonSimilarPlacesCountDf, countOfPersonPlacesDf("personId") === personsToPersonSimilarPlacesCountDf("personId1"))
      .select(col("_1.personId").alias("personId1"), col("_2.personId2").alias("personId2"),  col("_2.similarPlacesWithOtherPersonCount").alias("similarPlacesWithOtherPersonCount"), col("_1.countOfPersonPlaces").alias("countOfPersonPlaces"))
      //    расчет "похожести" каждой пары персон
      .withColumn("similarCount", col("similarPlacesWithOtherPersonCount") / col("countOfPersonPlaces"))
      //    отсекаем пары персон, которые недостаточно "похожи" друг на друга по посещаемым местам
      .where(col("similarCount") >= similarityThreshold)
  }

  //    каждой записи о персоне1 сопоставляется запись о персоне2, которая посетило место, неизвестное персоне1
  def countEachOtherPersonUnfamiliarPlaces(personsToPlaces: DataFrame, countOfPersonPlacesDf: DataFrame): DataFrame = {
    //    self-join записей персона-место друг к другу, где персоны и места разные
    personsToPlaces.as('p1)
      .join(personsToPlaces.as('p2), (col("p1.placeId") !== col("p2.placeId")) && (col("p1.personId") !== col("p2.personId")))
      //    подсчитать сколько раз сджойнилось место персоны2 к местам персоны1 (неизвестные персоне1 места сджойнятся макисмальное количество раз)
      .select(col("p1.personId").alias("personId1"), col("p2.personId").alias("personId2"), col("p2.placeId").alias("placeId2"), count("*").over(Window.partitionBy("p1.personId", "p2.personId", "p2.placeId")) as "count")
      .dropDuplicates("personId1", "personId2", "placeId2")
      //    inner-join суммарного количества посещенных мест - отсекаем лишние джойны из предыдущего шага, оставляя только неизвестные персоне1 места
      .join(countOfPersonPlacesDf, col("personId") === col("personId1") && col("count") === col("countOfPersonPlaces"))
      .select("personId1", "personId2", "placeId2")
  }

  //  каждой персоне1 сопоставляем неизвестные ей места персоны2, которая имеет высокий процент "схожести"
  def joinToEachPersonUnfamiliarPlacesOfDifferentPersonsWithHighSimilarCount(filteredByThresholdSimilarityOfEachOtherPersonDf: DataFrame, personsToPersonWithUnfamiliarPlacesDf: DataFrame): DataFrame = {
    filteredByThresholdSimilarityOfEachOtherPersonDf.select(col("personId1").alias("similarPersonId1"), col("personId2").alias("similarPersonId2"), col("similarCount"))
      .join(personsToPersonWithUnfamiliarPlacesDf, col("personId1") === col("similarPersonId1") && col("personId2") === col("similarPersonId2"))
      .dropDuplicates("similarPersonId1", "similarPersonId2", "placeId2")
      .select("personId1", "placeId2", "similarCount")
  }
}
